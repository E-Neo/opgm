use crate::{
    memory_manager::MemoryManager,
    old_executor::SuperRows,
    types::{GlobalConstraint, VId},
};
use std::io::Write;

pub fn decompress<'a>(
    super_row_mm: &'a MemoryManager,
    vertex_eqv: &'a [(VId, usize)],
    global_constraint: &'a Option<GlobalConstraint>,
) -> Box<dyn Iterator<Item = Vec<VId>> + 'a> {
    let iter = SuperRows::new(super_row_mm).flat_map(move |sr| sr.decompress(vertex_eqv));
    if let Some(gc) = global_constraint {
        Box::new(iter.filter(move |row| gc.f()(row)))
    } else {
        Box::new(iter)
    }
}

pub fn write_results<I>(
    writer: &mut dyn Write,
    rows: I,
    vertex_eqv: &[(VId, usize)],
) -> std::io::Result<usize>
where
    I: IntoIterator<Item = Vec<VId>>,
{
    let mut iter = vertex_eqv.iter().map(|&(vertex, _)| vertex);
    if let Some(v) = iter.next() {
        write!(writer, "u{}", v)?;
        while let Some(v) = iter.next() {
            write!(writer, ",u{}", v)?;
        }
    }
    writeln!(writer, "")?;
    let mut num_rows = 0;
    for row in rows.into_iter() {
        write_row(writer, &row)?;
        num_rows += 1;
    }
    Ok(num_rows)
}

fn write_row(writer: &mut dyn Write, row: &[VId]) -> std::io::Result<()> {
    let mut iter = row.iter();
    if let Some(v) = iter.next() {
        write!(writer, "{}", v)?;
        while let Some(v) = iter.next() {
            write!(writer, ",{}", v)?;
        }
    }
    writeln!(writer, "")?;
    Ok(())
}
