use crate::{
    executor::{read_super_row_header, SuperRow, SuperRows},
    memory_manager::MemoryManager,
    types::{VId, VIdPos},
};
use itertools::{EitherOrBoth, Itertools};
use std::{
    collections::{HashMap, HashSet},
    mem::size_of,
};

pub enum IndexType {
    Sorted,
    Hash,
}

pub struct IndexedJoinPlan {
    scan: Vec<(usize, usize)>, // (sr, eqv)
    index_id: usize,
    contains_checks: Vec<(usize, usize)>, // (eqv, vc_offset)
}

impl IndexedJoinPlan {
    pub fn new(
        scan: Vec<(usize, usize)>,
        index_id: usize,
        contains_checks: Vec<(usize, usize)>,
    ) -> Self {
        Self {
            scan,
            index_id,
            contains_checks,
        }
    }

    pub fn scan(&self) -> &[(usize, usize)] {
        &self.scan
    }

    pub fn index_id(&self) -> usize {
        self.index_id
    }

    pub fn contains_checks(&self) -> &[(usize, usize)] {
        &self.contains_checks
    }
}

pub struct IntersectionPlan {
    intersection: Vec<(usize, usize)>, // (sr, eqv)
}

impl IntersectionPlan {
    pub fn new(intersection: Vec<(usize, usize)>) -> Self {
        Self { intersection }
    }

    pub fn intersection(&self) -> &[(usize, usize)] {
        &self.intersection
    }
}

pub struct JoinPlan {
    num_cover: usize,
    index_type: IndexType,
    indexed_joins: Vec<IndexedJoinPlan>,
    intersections: Vec<IntersectionPlan>,
}

impl JoinPlan {
    pub fn new(
        num_cover: usize,
        index_type: IndexType,
        indexed_joins: Vec<IndexedJoinPlan>,
        intersections: Vec<IntersectionPlan>,
    ) -> Self {
        Self {
            num_cover,
            index_type,
            indexed_joins,
            intersections,
        }
    }

    pub fn num_cover(&self) -> usize {
        self.num_cover
    }

    pub fn index_type(&self) -> &IndexType {
        &self.index_type
    }

    pub fn indexed_joins(&self) -> &[IndexedJoinPlan] {
        &self.indexed_joins
    }

    pub fn intersections(&self) -> &[IntersectionPlan] {
        &self.intersections
    }
}

struct Intersection<'a> {
    inner: Box<dyn Iterator<Item = &'a VId> + 'a>,
}

impl<'a> Intersection<'a> {
    fn new<I: IntoIterator<Item = &'a [VId]>>(images: I) -> Self {
        let mut tail = images.into_iter();
        let mut inner: Box<dyn Iterator<Item = &'a VId> + 'a> =
            Box::new(tail.next().unwrap().into_iter());
        for other in tail {
            inner = Box::new(inner.merge_join_by(other, |x, y| x.cmp(y)).filter_map(|x| {
                if let EitherOrBoth::Both(x, _) = x {
                    Some(x)
                } else {
                    None
                }
            }));
        }
        Self { inner }
    }
}

impl<'a> Iterator for Intersection<'a> {
    type Item = &'a VId;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
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

struct Index<'a> {
    super_row_mm: &'a MemoryManager,
    num_eqvs: usize,
    inner: IndexInner<'a>,
}

impl<'a> Index<'a> {
    fn new(
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

    fn get(&self, vid: VId) -> Option<SuperRow<'a>> {
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

    pub fn decompress(mut self) -> JoinedSuperRowIter {
        JoinedSuperRowIter::new(
            self.vertex_cover,
            self.leaves
                .iter_mut()
                .map(|x| x.map(|&vid| vid).collect())
                .collect(),
        )
    }
}

pub struct JoinedSuperRowIter {
    vertex_cover: Vec<VId>,
    leaves: Vec<Vec<VId>>,
    leaves_offsets: Vec<usize>,
    row: Vec<VId>,
    row_set: HashSet<VId>,
}

impl JoinedSuperRowIter {
    fn new(vertex_cover: Vec<VId>, leaves: Vec<Vec<VId>>) -> Self {
        let leaves_offsets = vec![0; leaves.len()];
        let row = vertex_cover.clone();
        let row_set = vertex_cover.iter().map(|&vid| vid).collect();
        Self {
            vertex_cover,
            leaves,
            leaves_offsets,
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
            if col == self.leaves.len() {
                let row = self.row.clone();
                if let Some(v) = self.row.pop() {
                    self.row_set.remove(&v);
                }
                return Some(row);
            } else if self.leaves_offsets[col] < self.leaves[col].len() {
                let v = self.leaves[col][self.leaves_offsets[col]];
                if self.row_set.insert(v) {
                    self.row.push(v);
                }
                self.leaves_offsets[col] += 1;
            } else if col == 0 {
                return None;
            } else {
                self.leaves_offsets[col] = 0;
                if let Some(v) = self.row.pop() {
                    self.row_set.remove(&v);
                }
            }
        }
    }
}

pub struct JoinedSuperRows<'a> {
    indices: Vec<Index<'a>>,
    plan: &'a JoinPlan,
    top_scan: SuperRows<'a>,
    srs: Vec<SuperRow<'a>>,
    vc: Vec<VId>,
    scans: Vec<Intersection<'a>>,
}

impl<'a> JoinedSuperRows<'a> {
    fn new(
        super_row_mms: &'a [MemoryManager],
        index_mms: &'a [MemoryManager],
        plan: &'a JoinPlan,
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
}

impl<'a> Iterator for JoinedSuperRows<'a> {
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
                            .map(|&(sr, eqv)| self.srs[sr].images()[eqv]),
                    ));
                } else {
                    return None;
                }
            } else if self.vc.len() == self.plan.num_cover() {
                let emit = JoinedSuperRow::new(
                    self.vc.clone(),
                    self.plan
                        .intersections()
                        .iter()
                        .map(|x| {
                            Intersection::new(
                                x.intersection()
                                    .iter()
                                    .map(|&(sr, eqv)| self.srs[sr].images()[eqv]),
                            )
                        })
                        .collect(),
                );
                self.vc.pop();
                self.srs.pop();
                self.scans.pop();
                return Some(emit);
            } else {
                if let Some(&v1) = self.scans.last_mut().and_then(|x| x.next()) {
                    let indexed_join = &self.plan.indexed_joins()[self.vc.len() - 1];
                    if let Some(sr1) = self.indices[indexed_join.index_id()].get(v1) {
                        if indexed_join
                            .contains_checks()
                            .iter()
                            .all(|&(eqv, vc_offset)| {
                                sr1.images()[eqv].binary_search(&self.vc[vc_offset]).is_ok()
                            })
                        {
                            self.srs.push(sr1);
                            self.vc.push(v1);
                            self.scans.push(Intersection::new(
                                indexed_join
                                    .scan()
                                    .iter()
                                    .map(|&(sr, eqv)| self.srs[sr].images()[eqv]),
                            ));
                        }
                    }
                } else {
                    self.vc.pop();
                    self.scans.pop();
                }
            }
        }
    }
}

pub fn join<'a>(
    super_row_mms: &'a [MemoryManager],
    index_mms: &'a [MemoryManager],
    plan: &'a JoinPlan,
) -> JoinedSuperRows<'a> {
    JoinedSuperRows::new(super_row_mms, index_mms, plan)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::executor::{add_super_row_and_index_compact, empty_super_row_mm};

    #[test]
    fn test_joined_super_row_iter() {
        assert_eq!(
            JoinedSuperRowIter::new(vec![1, 2, 2], vec![vec![5, 6]]).count(),
            0
        );
        assert_eq!(
            JoinedSuperRowIter::new(vec![1, 2, 3], vec![vec![5, 6]]).collect::<Vec<_>>(),
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
            3,
            IndexType::Hash,
            vec![
                IndexedJoinPlan::new(vec![(0, 1)], 1, vec![(2, 0)]),
                IndexedJoinPlan::new(vec![(0, 1)], 1, vec![(2, 0)]),
            ],
            vec![IntersectionPlan::new(vec![(1, 1), (2, 1)])],
        );
        assert_eq!(
            join(&super_row_mms, &index_mms, &plan)
                .flat_map(|sr| sr.decompress())
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
