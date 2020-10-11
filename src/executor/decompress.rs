use crate::{
    executor::SuperRows,
    memory_manager::MemoryManager,
    types::{GlobalConstraint, VId},
};
use std::io::Write;

pub fn decompress<'a>(
    super_row_mm: &'a MemoryManager,
    vertex_eqv: &'a [(VId, usize)],
) -> impl Iterator<Item = Vec<VId>> + 'a {
    SuperRows::new(super_row_mm).flat_map(move |sr| sr.decompress(vertex_eqv))
}

pub fn write_results<I>(
    writer: &mut dyn Write,
    rows: I,
    vertex_eqv: &[(VId, usize)],
    global_constraint: &Option<GlobalConstraint>,
) -> std::io::Result<()>
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
    if let Some(gc) = global_constraint {
        for row in rows.into_iter().filter(|row| gc.f()(&row)) {
            write_row(writer, &row)?;
        }
    } else {
        for row in rows.into_iter() {
            write_row(writer, &row)?;
        }
    }
    Ok(())
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
