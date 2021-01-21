use crate::{
    executor::{Index, Intersection, JoinedSuperRow, SuperRow, SuperRows},
    memory_manager::MemoryManager,
    planner::{IndexType, IndexedJoinPlan, IntersectionPlan, JoinPlan, Plan},
    types::VId,
};
use rayon::prelude::*;

struct OneJoin<'a, 'b> {
    num_cover: usize,
    vc: Vec<VId>,
    srs: Vec<SuperRow<'a>>,
    scans: Vec<Intersection<'a>>,
    indices: Vec<&'a Index<'a>>,
    indexed_joins: &'b [IndexedJoinPlan],
    intersections: &'b [IntersectionPlan],
}

impl<'a, 'b> OneJoin<'a, 'b> {
    fn new(sr0: SuperRow<'a>, indices: Vec<&'a Index<'a>>, plan: &'b JoinPlan) -> Self {
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

fn create_indices<'a, 'b>(
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

fn count_rows_star(super_row_mm: &MemoryManager, vertex_eqv: &[(VId, usize)]) -> usize {
    SuperRows::new(super_row_mm)
        .par_bridge()
        .map(|sr| sr.decompress(vertex_eqv).count())
        .sum()
}

fn count_rows_join<'a, 'b>(
    super_row_mms: &'a [MemoryManager],
    index_mms: &'a [MemoryManager],
    plan: &'b JoinPlan,
) -> usize {
    let indices = create_indices(super_row_mms, index_mms, plan.index_type());
    SuperRows::new(&super_row_mms[0])
        .par_bridge()
        .map(|sr0| {
            OneJoin::new(
                sr0,
                plan.indexed_joins()
                    .iter()
                    .map(|p| &indices[p.index_id()])
                    .collect(),
                plan,
            )
            .map(|sr| sr.decompress(plan.sorted_vertex_eqv()).count())
            .sum::<usize>()
        })
        .sum()
}

pub fn count_rows<'a, 'b>(
    super_row_mms: &'a [MemoryManager],
    index_mms: &'a [MemoryManager],
    plan: &'b Plan,
) -> usize {
    match plan.stars().len() {
        0 => 0,
        1 => {
            let mut vertex_eqv: Vec<_> = plan.stars()[0]
                .vertex_eqv()
                .iter()
                .map(|(&vid, &eqv)| (vid, eqv))
                .collect();
            vertex_eqv.sort();
            count_rows_star(&super_row_mms[0], &vertex_eqv)
        }
        _ => count_rows_join(super_row_mms, index_mms, plan.join_plan().unwrap()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::executor::{add_super_row_and_index_compact, empty_super_row_mm};

    #[test]
    fn test_count_rows_join() {
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
        assert_eq!(count_rows_join(&super_row_mms, &index_mms, &plan), 12);
    }
}
