use crate::{
    executor::{create_indices, OneJoin, SuperRows},
    memory_manager::MemoryManager,
    old_planner::{JoinPlan, Plan},
    types::VId,
};
use rayon::prelude::*;

fn count_rows_star(super_row_mm: &MemoryManager, vertex_eqv: &[(VId, usize)]) -> usize {
    SuperRows::new(super_row_mm)
        .par_bridge()
        .map(|sr| sr.count_rows(vertex_eqv))
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
            .map(|sr| sr.count_rows(plan.sorted_vertex_eqv()))
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

fn count_rows_slow_star(super_row_mm: &MemoryManager, vertex_eqv: &[(VId, usize)]) -> usize {
    SuperRows::new(super_row_mm)
        .par_bridge()
        .map(|sr| sr.count_rows_slow(vertex_eqv))
        .sum()
}

fn count_rows_slow_join<'a, 'b>(
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
            .map(|sr| sr.count_rows_slow(plan.sorted_vertex_eqv()))
            .sum::<usize>()
        })
        .sum()
}

pub fn count_rows_slow<'a, 'b>(
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
            count_rows_slow_star(&super_row_mms[0], &vertex_eqv)
        }
        _ => count_rows_slow_join(super_row_mms, index_mms, plan.join_plan().unwrap()),
    }
}
