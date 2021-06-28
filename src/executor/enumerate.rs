use crate::{
    executor::{create_indices, OneJoin, SuperRows},
    memory_manager::MemoryManager,
    old_planner::{JoinPlan, Plan},
    types::VId,
};
use std::io::Write;

fn write_row<W: Write>(buf: &mut W, row: &[VId]) -> std::io::Result<()> {
    write!(buf, "{}", row[0])?;
    for &x in &row[1..] {
        write!(buf, ",{}", x)?
    }
    writeln!(buf, "")
}

fn enumerate_star<W: Write>(
    buf: &mut W,
    super_row_mm: &MemoryManager,
    vertex_eqv: &[(VId, usize)],
) -> std::io::Result<usize> {
    let mut num_rows = 0;
    for sr in SuperRows::new(super_row_mm) {
        for row in sr.decompress(vertex_eqv) {
            write_row(buf, &row)?;
            num_rows += 1;
        }
    }
    Ok(num_rows)
}

fn enumerate_join<'a, 'b, W: Write>(
    buf: &mut W,
    super_row_mms: &'a [MemoryManager],
    index_mms: &'a [MemoryManager],
    plan: &'b JoinPlan,
) -> std::io::Result<usize> {
    let indices = create_indices(super_row_mms, index_mms, plan.index_type());
    let mut num_rows = 0;
    for sr0 in SuperRows::new(&super_row_mms[0]) {
        for sr in OneJoin::new(
            sr0,
            plan.indexed_joins()
                .iter()
                .map(|p| &indices[p.index_id()])
                .collect(),
            plan,
        ) {
            for row in sr.decompress(plan.sorted_vertex_eqv()) {
                write_row(buf, &row)?;
                num_rows += 1;
            }
        }
    }
    Ok(num_rows)
}

pub fn enumerate<'a, 'b, W: Write>(
    buf: &mut W,
    super_row_mms: &'a [MemoryManager],
    index_mms: &'a [MemoryManager],
    plan: &'b Plan,
) -> std::io::Result<usize> {
    match plan.stars().len() {
        0 => Ok(0),
        1 => {
            let mut vertex_eqv: Vec<_> = plan.stars()[0]
                .vertex_eqv()
                .iter()
                .map(|(&vid, &eqv)| (vid, eqv))
                .collect();
            vertex_eqv.sort();
            enumerate_star(buf, &super_row_mms[0], &vertex_eqv)
        }
        _ => enumerate_join(buf, super_row_mms, index_mms, plan.join_plan().unwrap()),
    }
}
