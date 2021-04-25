use crate::{
    executor::{count_rows_slow_helper, read_super_row_header, SuperRow, SuperRows},
    memory_manager::MemoryManager,
    planner::{IndexType, IndexedJoinPlan, IntersectionPlan, JoinPlan},
    types::{VId, VIdPos},
};
use std::{collections::HashMap, mem::size_of};

pub struct Intersection<'a> {
    images: Vec<&'a [VId]>,
    offsets: Vec<usize>,
}

impl<'a> Intersection<'a> {
    pub fn new(images: Vec<&'a [VId]>) -> Self {
        Self {
            offsets: vec![0; images.len()],
            images,
        }
    }
}

impl<'a> Iterator for Intersection<'a> {
    type Item = VId;

    fn next(&mut self) -> Option<Self::Item> {
        while self.offsets[0] < self.images[0].len() {
            let x = self.images[0][self.offsets[0]];
            self.offsets[0] += 1;
            let mut emit = true;
            for depth in 1..self.images.len() {
                let image = self.images[depth];
                let offset = &mut self.offsets[depth];
                while *offset < image.len() && x > image[*offset] {
                    *offset += 1;
                }
                if *offset == image.len() {
                    return None;
                } else if x < image[*offset] {
                    emit = false;
                    break;
                } else {
                    *offset += 1;
                }
            }
            if emit {
                return Some(x);
            }
        }
        None
    }
}

enum IndexInner<'a> {
    Sorted(&'a [VIdPos]),
    Hash(HashMap<VId, usize>),
}

impl<'a> IndexInner<'a> {
    fn new(index_mm: &'a MemoryManager, index_type: &IndexType) -> Self {
        let vid_poses: &[VIdPos] = index_mm.read_slice(0, index_mm.len() / size_of::<VIdPos>());
        match index_type {
            IndexType::Sorted => IndexInner::Sorted(vid_poses),
            IndexType::Hash => IndexInner::Hash(
                vid_poses
                    .iter()
                    .map(|vid_pos| (vid_pos.vid, vid_pos.pos))
                    .collect(),
            ),
        }
    }

    fn get(&self, vid: VId) -> Option<usize> {
        match self {
            IndexInner::Sorted(vid_poses) => vid_poses
                .binary_search_by_key(&vid, |vid_pos| vid_pos.vid)
                .map_or(None, |offset| Some(vid_poses[offset].pos)),
            IndexInner::Hash(vid_poses) => vid_poses.get(&vid).map(|&pos| pos),
        }
    }
}

pub struct Index<'a> {
    super_row_mm: &'a MemoryManager,
    num_eqvs: usize,
    inner: IndexInner<'a>,
}

impl<'a> Index<'a> {
    pub fn new(
        super_row_mm: &'a MemoryManager,
        index_mm: &'a MemoryManager,
        index_type: &IndexType,
    ) -> Self {
        let (_, num_eqvs, _) = read_super_row_header(super_row_mm);
        Self {
            super_row_mm,
            num_eqvs,
            inner: IndexInner::new(index_mm, index_type),
        }
    }

    pub fn get(&self, vid: VId) -> Option<SuperRow<'a>> {
        self.inner
            .get(vid)
            .map(|sr_pos| SuperRow::new(self.super_row_mm, sr_pos, self.num_eqvs))
    }
}

pub fn create_indices<'a, 'b>(
    super_row_mms: &'a [MemoryManager],
    index_mms: &'a [MemoryManager],
    index_type: &'b IndexType,
) -> Vec<Index<'a>> {
    super_row_mms
        .iter()
        .zip(index_mms)
        .map(|(super_row_mm, index_mm)| Index::new(super_row_mm, index_mm, index_type))
        .collect()
}

pub struct OneJoin<'a, 'b> {
    num_cover: usize,
    vc: Vec<VId>,
    srs: Vec<SuperRow<'a>>,
    scans: Vec<Intersection<'a>>,
    indices: Vec<&'a Index<'a>>,
    indexed_joins: &'b [IndexedJoinPlan],
    intersections: &'b [IntersectionPlan],
}

impl<'a, 'b> OneJoin<'a, 'b> {
    pub fn new(sr0: SuperRow<'a>, indices: Vec<&'a Index<'a>>, plan: &'b JoinPlan) -> Self {
        let num_cover = plan.num_cover();
        let (mut vc, mut srs, mut scans) = (
            Vec::with_capacity(num_cover),
            Vec::with_capacity(num_cover),
            Vec::with_capacity(num_cover - 1),
        );
        vc.push(sr0.images()[0][0]);
        srs.push(sr0);
        scans.push(Intersection::new(
            plan.indexed_joins()[0]
                .scan()
                .iter()
                .map(|&(sr, eqv)| srs[sr].images()[eqv])
                .collect(),
        ));
        Self {
            num_cover,
            vc,
            srs,
            scans,
            indices,
            indexed_joins: plan.indexed_joins(),
            intersections: plan.intersections(),
        }
    }
}

impl<'a, 'b> Iterator for OneJoin<'a, 'b> {
    type Item = JoinedSuperRow<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(v1) = self.scans.last_mut().and_then(|x| x.next()) {
                if let Some(sr1) = self.indices[self.scans.len() - 1].get(v1) {
                    self.srs.push(sr1);
                    self.vc.push(v1);
                    if self.vc.len() == self.num_cover {
                        let emit = JoinedSuperRow::new(
                            self.vc.clone(),
                            self.intersections
                                .iter()
                                .map(|x| {
                                    Intersection::new(
                                        x.intersection()
                                            .iter()
                                            .map(|&(sr, eqv)| self.srs[sr].images()[eqv])
                                            .collect(),
                                    )
                                })
                                .collect(),
                        );
                        self.vc.pop();
                        self.srs.pop();
                        return Some(emit);
                    } else {
                        self.scans.push(Intersection::new(
                            self.indexed_joins[self.vc.len() - 1]
                                .scan()
                                .iter()
                                .map(|&(sr, eqv)| self.srs[sr].images()[eqv])
                                .collect(),
                        ));
                    }
                }
            } else {
                if self.vc.pop().is_none() {
                    return None;
                }
                self.srs.pop();
                self.scans.pop();
            }
        }
    }
}

pub struct JoinedSuperRow<'a> {
    vertex_cover: Vec<VId>,
    leaves: Vec<Intersection<'a>>,
}

impl<'a> JoinedSuperRow<'a> {
    pub fn new(vertex_cover: Vec<VId>, leaves: Vec<Intersection<'a>>) -> Self {
        Self {
            vertex_cover,
            leaves,
        }
    }

    pub fn count_rows(self, _vertex_eqv: &[(VId, usize)]) -> usize {
        self.leaves.into_iter().map(|x| x.count()).product()
    }

    pub fn count_rows_slow(self, __vertex_eqv: &[(VId, usize)]) -> usize {
        let mut intersection_vecs: Vec<Vec<VId>> = Vec::with_capacity(self.leaves.len());
        let mut mappings: Vec<&[VId]> = Vec::with_capacity(self.leaves.len());
        self.leaves.into_iter().for_each(|x| {
            if x.images.len() == 1 {
                mappings.push(x.images[0]);
            } else {
                intersection_vecs.push(x.collect::<Vec<_>>());
            }
        });
        intersection_vecs.iter().for_each(|x| {
            mappings.push(x.as_slice());
        });
        count_rows_slow_helper(&mappings)
    }

    pub fn decompress(mut self, vertex_eqv: &[(VId, usize)]) -> JoinedSuperRowIter {
        let leaf_mappings = vertex_eqv
            .iter()
            .filter_map(|&(_, eqv)| {
                if eqv >= self.vertex_cover.len() {
                    Some(eqv - self.vertex_cover.len())
                } else {
                    None
                }
            })
            .collect();
        JoinedSuperRowIter::new(
            self.vertex_cover,
            self.leaves.iter_mut().map(|x| x.collect()).collect(),
            leaf_mappings,
        )
    }
}

pub struct JoinedSuperRowIter {
    vertex_cover: Vec<VId>,
    leaves: Vec<Vec<VId>>,
    leaf_mappings: Vec<usize>,
    leaf_mappings_offsets: Vec<usize>,
    row: Vec<VId>,
}

impl JoinedSuperRowIter {
    fn new(vertex_cover: Vec<VId>, leaves: Vec<Vec<VId>>, leaf_mappings: Vec<usize>) -> Self {
        let leaf_mappings_offsets = vec![0; leaf_mappings.len()];
        let row = vertex_cover.clone();
        Self {
            vertex_cover,
            leaves,
            leaf_mappings,
            leaf_mappings_offsets,
            row,
        }
    }
}

impl Iterator for JoinedSuperRowIter {
    type Item = Vec<VId>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let col = self.row.len() - self.vertex_cover.len();
            if col == self.leaf_mappings.len() {
                let row = self.row.clone();
                self.row.pop();
                return Some(row);
            } else if self.leaf_mappings_offsets[col] < self.leaves[self.leaf_mappings[col]].len() {
                let v = self.leaves[self.leaf_mappings[col]][self.leaf_mappings_offsets[col]];
                self.row.push(v);
                self.leaf_mappings_offsets[col] += 1;
            } else if col == 0 {
                return None;
            } else {
                self.leaf_mappings_offsets[col] = 0;
                self.row.pop();
            }
        }
    }
}
pub struct JoinedSuperRows<'a, 'b> {
    indices: Vec<Index<'a>>,
    plan: &'b JoinPlan,
    top_scan: SuperRows<'a>,
    srs: Vec<(usize, SuperRow<'a>)>,// (star id, super row)
    vc: Vec<VId>,
    scans: Vec<Intersection<'a>>,
}

impl<'a, 'b> JoinedSuperRows<'a, 'b> {
    fn new(
        super_row_mms: &'a [MemoryManager],
        index_mms: &'a [MemoryManager],
        plan: &'b JoinPlan,
    ) -> Self {
        Self {
            indices: super_row_mms
                .iter()
                .zip(index_mms)
                .map(|(super_row_mm, index_mm)| {
                    Index::new(super_row_mm, index_mm, plan.index_type())
                })
                .collect(),
            plan,
            top_scan: SuperRows::new(&super_row_mms[plan.indexed_joins()[0].index_id()]),
            srs: Vec::with_capacity(plan.num_cover()),
            vc: Vec::with_capacity(plan.num_cover()),
            scans: Vec::with_capacity(plan.num_cover()),
        }
    }
}

impl<'a, 'b> Iterator for JoinedSuperRows<'a, 'b> {
    type Item = JoinedSuperRow<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.vc.is_empty() {
                if let Some(sr0) = self.top_scan.next() {
                    let v1 = sr0.images()[0][0];
                    self.srs.push((self.plan.indexed_joins()[0].index_id() ,sr0));
                    self.vc.push(v1);
                    self.scans.push(Intersection::new(
                        self.plan.indexed_joins()[1]
                            .scan()
                            .iter()
                            .map(|&(sr, eqv)| {
                                //find the star id in self.srs
                                if let Some(sc) = self.srs.iter().find(|&(star, _)| *star == sr)  
                                {
                                    sc.1.images()[eqv]
                                }
                                else{
                                    self.srs[0].1.images()[eqv]
                                }
                            })
                            .collect(),
                    ));
                } else {
                    return None;
                }
            } else if let Some(v1) = self.scans.last_mut().and_then(|x| x.next()) {
                let indexed_join = &self.plan.indexed_joins()[self.vc.len()];
                if let Some(sr1) = self.indices[indexed_join.index_id()].get(v1) {
                    self.srs.push((indexed_join.index_id() , sr1));
                    self.vc.push(v1);
                    if self.vc.len() == self.plan.num_cover() {
                        let emit = JoinedSuperRow::new(
                            self.vc.clone(),
                            self.plan
                                .intersections()
                                .iter()
                                .map(|x| {
                                    Intersection::new(
                                        x.intersection()
                                            .iter()
                                            .map(|&(sr, eqv)| {
                                                if let Some(sc) = self.srs.iter().find(|&(star, _)| *star == sr)  
                                                {
                                                    sc.1.images()[eqv]
                                                }
                                                else{
                                                    self.srs[0].1.images()[eqv]

                                                }
                                            })
                                            .collect(),
                                    )
                                })
                                .collect(),
                        );
                        self.vc.pop();
                        self.srs.pop();
                        return Some(emit);
                    } else {
                        self.scans.push(Intersection::new(
                            self.plan.indexed_joins()[self.vc.len()]
                                .scan()
                                .iter()
                                .map(|&(sr, eqv)| {
                                    if let Some(sc) = self.srs.iter().find(|&(star, _)| *star == sr)  
                                    {
                                        sc.1.images()[eqv]
                                    }
                                    else{
                                        self.srs[0].1.images()[eqv]
                                    }
                                })
                                .collect(),
                        ));
                    }
                }
            } else {
                self.vc.pop();
                self.srs.pop();
                self.scans.pop();
            }
        }
    }
}

pub fn join<'a, 'b>(
    super_row_mms: &'a [MemoryManager],
    index_mms: &'a [MemoryManager],
    plan: &'b mut JoinPlan,
    stars: &[VId],
) -> JoinedSuperRows<'a, 'b> {
    
    reorder_join_plan(super_row_mms, plan, stars);
    JoinedSuperRows::new(super_row_mms, index_mms, plan)
}

#[derive(Debug, PartialEq)]
pub struct OrderIndexedJoinPlan {
    index_id: usize,//star id
    value: usize,
    root: VId,
}

impl OrderIndexedJoinPlan {
    pub fn new(index_id: usize, value : usize, root:VId) -> Self {
        Self { index_id, value, root }
    }


    pub fn index_id(&self) -> usize {
        self.index_id
    }

    pub fn value(&self) -> usize {
        self.value
    }

    pub fn root(&self) -> VId {
        self.root
    }
}
//calculate reorder value by rows
fn reorder_value(srs: &mut SuperRows) -> usize{
    let rows = srs.num_rows();
    let mut total_image_size : usize = 0;
    //every super row
    srs.for_each(|sr| {
        sr.images().iter().for_each(|vids| total_image_size+=(**vids).len());
    });
    let avg_image_size = total_image_size/srs.num_eqvs();
    println!("value:{} ", &(rows + avg_image_size));

    rows + avg_image_size
}
//reorder join_plan's index
fn reorder_join_plan<'a, 'b> (
    super_row_mms: &'a [MemoryManager],
    
    plan: &'b mut   JoinPlan,
    stars: &[VId],
){
    let mut to_order_indexed: Vec<OrderIndexedJoinPlan> = Vec::with_capacity(plan.indexed_joins().len());
    let mut srs = SuperRows::new(&super_row_mms[0]);
    let value = reorder_value(&mut srs);

    to_order_indexed.insert(0, OrderIndexedJoinPlan::new(0, value, stars[0]));
    plan.indexed_joins().iter().for_each(|index_plan| {
        let mut srs = SuperRows::new(&super_row_mms[index_plan.index_id()]);
        let value = reorder_value(&mut srs);
        to_order_indexed.insert(0, OrderIndexedJoinPlan::new(index_plan.index_id(), value, stars[index_plan.index_id()]));
    });
    to_order_indexed.sort_by(|a, b| a.value().cmp(&b.value()));// Descending b cmp a, Ascending a cmp b
    //checked index_id of to_order_indexed 
    let mut prev_index : Vec<usize> = Vec::new();
    let ordered_indexed :Vec<IndexedJoinPlan> = to_order_indexed.iter().map(|index| {

        let indexed_join_plan = IndexedJoinPlan::new(
        plan.uid_sr_eqvs()
            .get(&index.root())
            .unwrap()
            .iter()
            .filter_map(|&(sr, eqv)| if prev_index.contains(&sr) { Some((sr, eqv)) } else { None })
            .collect(),
            index.index_id(),
            );
        prev_index.push(index.index_id());
        indexed_join_plan
    }
        // IndexedJoinPlan::new(index.scan().clone().to_vec(),  index.index_id().clone())
    ).collect();
    plan.set_indexed_joins(ordered_indexed);
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_intersection() {
        let images: Vec<&[VId]> = vec![&[1, 2, 3, 4, 5]];
        assert_eq!(
            Intersection::new(images).collect::<Vec<_>>(),
            vec![1, 2, 3, 4, 5]
        );
        let images: Vec<&[VId]> = vec![&[1, 2, 3, 4, 5, 6, 7, 8, 9], &[0, 2, 4, 6, 8]];
        assert_eq!(
            Intersection::new(images).collect::<Vec<_>>(),
            vec![2, 4, 6, 8]
        );
        let images: Vec<&[VId]> = vec![
            &[1, 2, 3, 4, 5, 6, 7, 8, 9],
            &[2, 4, 6, 8, 10],
            &[0, 2, 4, 8],
        ];
        assert_eq!(Intersection::new(images).collect::<Vec<_>>(), vec![2, 4, 8]);
    }

    #[test]
    fn test_joined_super_row_iter() {
        assert_eq!(
            JoinedSuperRowIter::new(vec![1, 2, 3], vec![vec![5, 6]], vec![0]).collect::<Vec<_>>(),
            vec![vec![1, 2, 3, 5], vec![1, 2, 3, 6]]
        );
    }
}
