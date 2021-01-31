use crate::{
    executor::{create_indices, OneJoin, SuperRows},
    memory_manager::MemoryManager,
    planner::{JoinPlan, Plan},
    types::VId,
};
use rayon::prelude::*;

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
    use crate::{
        executor::{add_super_row_and_index_compact, empty_super_row_mm},
        planner::{IndexType, IndexedJoinPlan, IntersectionPlan},
    };

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
