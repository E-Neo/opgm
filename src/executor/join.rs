use crate::{
    executor::{
        read_super_row_header, write_num_bytes, write_pos_len, write_super_row_header, write_vid,
        SuperRow, SuperRows,
    },
    memory_manager::MemoryManager,
    planner::{IndexType, JoinPlan},
    types::{PosLen, SuperRowHeader, VId, VIdPos},
};
use std::{
    collections::{HashMap, HashSet},
    mem::size_of,
};

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

    fn bound(&self) -> usize {
        self.images.iter().map(|img| img.len()).min().unwrap()
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

pub struct JoinedSuperRow<'a> {
    vertex_cover: Vec<VId>,
    leaves: Vec<Intersection<'a>>,
}

impl<'a> JoinedSuperRow<'a> {
    fn new(vertex_cover: Vec<VId>, leaves: Vec<Intersection<'a>>) -> Self {
        Self {
            vertex_cover,
            leaves,
        }
    }

    fn num_eqv(&self) -> usize {
        self.vertex_cover.len() + self.leaves.len()
    }

    fn num_bytes_bound(&self) -> usize {
        size_of::<usize>()
            + size_of::<PosLen>() * self.num_eqv()
            + size_of::<VId>()
                * (self.vertex_cover.len() + self.leaves.iter().map(|x| x.bound()).sum::<usize>())
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
    row_set: HashSet<VId>,
}

impl JoinedSuperRowIter {
    fn new(vertex_cover: Vec<VId>, leaves: Vec<Vec<VId>>, leaf_mappings: Vec<usize>) -> Self {
        let leaf_mappings_offsets = vec![0; leaf_mappings.len()];
        let row = vertex_cover.clone();
        let row_set = vertex_cover.iter().map(|&vid| vid).collect();
        Self {
            vertex_cover,
            leaves,
            leaf_mappings,
            leaf_mappings_offsets,
            row,
            row_set,
        }
    }
}

impl Iterator for JoinedSuperRowIter {
    type Item = Vec<VId>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.row_set.len() < self.row.len() {
            return None;
        }
        loop {
            let col = self.row.len() - self.vertex_cover.len();
            if col == self.leaf_mappings.len() {
                let row = self.row.clone();
                if let Some(v) = self.row.pop() {
                    self.row_set.remove(&v);
                }
                return Some(row);
            } else if self.leaf_mappings_offsets[col] < self.leaves[self.leaf_mappings[col]].len() {
                let v = self.leaves[self.leaf_mappings[col]][self.leaf_mappings_offsets[col]];
                if self.row_set.insert(v) {
                    self.row.push(v);
                }
                self.leaf_mappings_offsets[col] += 1;
            } else if col == 0 {
                return None;
            } else {
                self.leaf_mappings_offsets[col] = 0;
                if let Some(v) = self.row.pop() {
                    self.row_set.remove(&v);
                }
            }
        }
    }
}

pub struct JoinedSuperRows<'a, 'b> {
    indices: Vec<Index<'a>>,
    plan: &'b JoinPlan,
    top_scan: SuperRows<'a>,
    srs: Vec<SuperRow<'a>>,
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
            top_scan: SuperRows::new(&super_row_mms[0]),
            srs: Vec::with_capacity(plan.num_cover()),
            vc: Vec::with_capacity(plan.num_cover()),
            scans: Vec::with_capacity(plan.num_cover()),
        }
    }

    fn num_eqvs(&self) -> usize {
        self.num_cover() + self.plan.intersections().len()
    }

    fn num_cover(&self) -> usize {
        self.plan.num_cover()
    }

    pub fn write(self, output: &mut MemoryManager) -> usize {
        let num_eqvs = self.num_eqvs();
        let num_cover = self.num_cover();
        output.resize(size_of::<SuperRowHeader>());
        let mut sr_pos = size_of::<SuperRowHeader>();
        let mut num_rows = 0;
        for sr in self {
            let vc_set: HashSet<VId> = sr.vertex_cover.iter().map(|&vid| vid).collect();
            if vc_set.len() == num_cover {
                output.resize(sr_pos + sr.num_bytes_bound());
                let mut pos = sr_pos + size_of::<usize>() + size_of::<PosLen>() * num_eqvs;
                output.write(pos, sr.vertex_cover.as_ptr(), num_cover);
                for eqv in 0..num_cover {
                    write_pos_len(output, sr_pos, eqv, pos, 1);
                    pos += size_of::<VId>();
                }
                let new_pos = write_leaves(output, num_cover, sr_pos, pos, sr.leaves);
                if new_pos != pos {
                    write_num_bytes(output, sr_pos, new_pos - sr_pos);
                    sr_pos = new_pos;
                    num_rows += 1;
                }
            }
        }
        write_super_row_header(output, num_rows, num_eqvs, num_cover);
        output.resize(sr_pos);
        num_rows
    }
}

fn write_leaves(
    output: &mut MemoryManager,
    num_cover: usize,
    sr_pos: usize,
    pos: usize,
    leaves: Vec<Intersection>,
) -> usize {
    let mut new_pos = pos;
    for (eqv, x) in (num_cover..).zip(leaves) {
        let pos_start = new_pos;
        for vid in x {
            write_vid(output, new_pos, vid);
            new_pos += size_of::<VId>();
        }
        if new_pos == pos_start {
            return pos;
        }
        write_pos_len(
            output,
            sr_pos,
            eqv,
            pos_start,
            (new_pos - pos_start) / size_of::<VId>(),
        );
    }
    new_pos
}

impl<'a, 'b> Iterator for JoinedSuperRows<'a, 'b> {
    type Item = JoinedSuperRow<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.vc.is_empty() {
                if let Some(sr0) = self.top_scan.next() {
                    let v1 = sr0.images()[0][0];
                    self.srs.push(sr0);
                    self.vc.push(v1);
                    self.scans.push(Intersection::new(
                        self.plan.indexed_joins()[0]
                            .scan()
                            .iter()
                            .map(|&(sr, eqv)| self.srs[sr].images()[eqv])
                            .collect(),
                    ));
                } else {
                    return None;
                }
            } else if let Some(v1) = self.scans.last_mut().and_then(|x| x.next()) {
                let indexed_join = &self.plan.indexed_joins()[self.vc.len() - 1];
                if let Some(sr1) = self.indices[indexed_join.index_id()].get(v1) {
                    self.srs.push(sr1);
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
                            self.plan.indexed_joins()[self.vc.len() - 1]
                                .scan()
                                .iter()
                                .map(|&(sr, eqv)| self.srs[sr].images()[eqv])
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
    plan: &'b JoinPlan,
) -> JoinedSuperRows<'a, 'b> {
    JoinedSuperRows::new(super_row_mms, index_mms, plan)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        executor::{add_super_row_and_index_compact, empty_super_row_mm},
        planner::{IndexedJoinPlan, IntersectionPlan},
    };

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
            JoinedSuperRowIter::new(vec![1, 2, 2], vec![vec![5, 6]], vec![0]).count(),
            0
        );
        assert_eq!(
            JoinedSuperRowIter::new(vec![1, 2, 3], vec![vec![5, 6]], vec![0]).collect::<Vec<_>>(),
            vec![vec![1, 2, 3, 5], vec![1, 2, 3, 6]]
        );
    }

    #[test]
    fn test_q02() {
        let mut super_row_mms = vec![MemoryManager::Mem(vec![]), MemoryManager::Mem(vec![])];
        let mut index_mms = vec![MemoryManager::Mem(vec![]), MemoryManager::Mem(vec![])];
        empty_super_row_mm(&mut super_row_mms[0], 2, 1);
        empty_super_row_mm(&mut super_row_mms[1], 3, 1);
        let srs: Vec<&[&[VId]]> = vec![
            &[&[1], &[2, 3, 4]],
            &[&[2], &[5, 6]],
            &[&[3], &[5, 6]],
            &[&[4], &[5, 6]],
        ];
        for sr in srs {
            add_super_row_and_index_compact(&mut super_row_mms[0], &mut index_mms[0], sr);
        }
        let srs: Vec<&[&[VId]]> = vec![
            &[&[2], &[5, 6], &[1]],
            &[&[3], &[5, 6], &[1]],
            &[&[4], &[5, 6], &[1]],
        ];
        for sr in srs {
            add_super_row_and_index_compact(&mut super_row_mms[1], &mut index_mms[1], sr);
        }
        let plan = JoinPlan::new(
            vec![(1, 0), (2, 1), (3, 2), (4, 3)].into_iter().collect(),
            3,
            IndexType::Hash,
            vec![
                IndexedJoinPlan::new(vec![(0, 1)], 1),
                IndexedJoinPlan::new(vec![(0, 1)], 1),
            ],
            vec![IntersectionPlan::new(vec![(1, 1), (2, 1)])],
        );
        assert_eq!(
            join(&super_row_mms, &index_mms, &plan)
                .flat_map(|sr| sr.decompress(plan.sorted_vertex_eqv()))
                .collect::<Vec<_>>(),
            vec![
                vec![1, 2, 3, 5],
                vec![1, 2, 3, 6],
                vec![1, 2, 4, 5],
                vec![1, 2, 4, 6],
                vec![1, 3, 2, 5],
                vec![1, 3, 2, 6],
                vec![1, 3, 4, 5],
                vec![1, 3, 4, 6],
                vec![1, 4, 2, 5],
                vec![1, 4, 2, 6],
                vec![1, 4, 3, 5],
                vec![1, 4, 3, 6]
            ]
        );
    }
}
