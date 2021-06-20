use crate::{
    memory_manager::MemoryManager,
    types::{ELabel, NeighborHeader, VId, VLabelPosLen, VertexHeader},
};
use std::mem::size_of;

fn header_size(mm: &MemoryManager) -> usize {
    let num_vlabels = unsafe { *mm.read::<usize>(0) };
    size_of::<usize>() + num_vlabels * size_of::<VLabelPosLen>()
}

fn neighbor_size(mm: &MemoryManager, n_pos: usize) -> usize {
    let neighbor_header = mm.read::<NeighborHeader>(n_pos);
    unsafe {
        std::mem::size_of::<VId>()
            + 2 * std::mem::size_of::<usize>()
            + ((*neighbor_header).num_n_to_v + (*neighbor_header).num_v_to_n)
                * std::mem::size_of::<ELabel>()
    }
}

fn num_vertices(mm: &MemoryManager) -> usize {
    let num_vlabels = unsafe { *mm.read::<usize>(0) };
    let vlabel_pos_lens = mm.read_slice::<VLabelPosLen>(size_of::<usize>(), num_vlabels);
    vlabel_pos_lens.iter().map(|x| x.len).sum()
}

fn num_neighbors(mm: &MemoryManager, pos: usize) -> usize {
    let num_vlabels = unsafe { (*mm.read::<VertexHeader>(pos)).num_vlabels };
    let vlabel_pos_lens =
        mm.read_slice::<VLabelPosLen>(pos + size_of::<VertexHeader>(), num_vlabels);
    vlabel_pos_lens.iter().map(|x| x.len).sum()
}

fn display_vlabel_pos_lens(
    vlabel_pos_lens: &[VLabelPosLen],
    f: &mut std::fmt::Formatter<'_>,
) -> std::fmt::Result {
    writeln!(f, "|      vlabel |                 pos |         len |")?;
    writeln!(f, "+-------------+---------------------+-------------+")?;
    for vlabel_pos_len in vlabel_pos_lens {
        writeln!(
            f,
            "|{:>12} |  {:#018x} |{:>12} |",
            vlabel_pos_len.vlabel, vlabel_pos_len.pos, vlabel_pos_len.len
        )?;
    }
    writeln!(f, "+-------------+---------------------+-------------+")
}

fn display_header(mm: &MemoryManager, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    let num_vlabels = unsafe { *mm.read::<usize>(0) };
    let vlabel_pos_lens = mm.read_slice::<VLabelPosLen>(size_of::<usize>(), num_vlabels);
    writeln!(f, "+-------------------------------------------------+")?;
    writeln!(f, "|{:^49}|", format!("num_vlabels: {}", num_vlabels))?;
    writeln!(f, "+-------------+---------------------+-------------+")?;
    display_vlabel_pos_lens(vlabel_pos_lens, f)
}

fn display_neighbor(
    mm: &MemoryManager,
    n_pos: usize,
    f: &mut std::fmt::Formatter<'_>,
) -> std::fmt::Result {
    let neighbor_header = mm.read::<NeighborHeader>(n_pos);
    let nid = unsafe { (*neighbor_header).nid };
    let num_n_to_v = unsafe { (*neighbor_header).num_n_to_v };
    let num_v_to_n = unsafe { (*neighbor_header).num_v_to_n };
    writeln!(f, "+-------------------------------------------------+")?;
    writeln!(f, "|{:^49}|", format!("nid: {}", nid))?;
    writeln!(f, "+------------------------+------------------------+")?;
    writeln!(
        f,
        "| num_n_to_v: {:<11}| num_v_to_n: {:<11}|",
        num_n_to_v, num_v_to_n
    )?;
    writeln!(f, "+------------------------+------------------------+")?;
    for elabel in
        mm.read_slice::<ELabel>(n_pos + size_of::<NeighborHeader>(), num_n_to_v + num_v_to_n)
    {
        writeln!(f, "|{:^49}|", elabel)?;
    }
    writeln!(f, "+-------------------------------------------------+")
}

fn display_vertex(
    mm: &MemoryManager,
    v_pos: usize,
    f: &mut std::fmt::Formatter<'_>,
) -> std::fmt::Result {
    let vertex_header = mm.read::<VertexHeader>(v_pos);
    writeln!(f, "{:#018x}", v_pos)?;
    writeln!(f, "+-------------------------------------------------+")?;
    writeln!(
        f,
        "|{:^49}|",
        format!("num_bytes: {}", unsafe { (*vertex_header).num_bytes })
    )?;
    writeln!(f, "+------------------------+------------------------+")?;
    unsafe {
        writeln!(
            f,
            "| vid: {:<18}| in_deg: {:<15}|",
            (*vertex_header).vid,
            (*vertex_header).in_deg
        )?;
        writeln!(
            f,
            "| out_deg: {:<14}| num_vlabels: {:<10}|",
            (*vertex_header).out_deg,
            (*vertex_header).num_vlabels
        )?;
    }
    let num_vlabels = unsafe { (*vertex_header).num_vlabels };
    let vlabel_pos_lens =
        mm.read_slice::<VLabelPosLen>(v_pos + size_of::<VertexHeader>(), num_vlabels);
    writeln!(f, "+-------------+----------+----------+-------------+")?;
    display_vlabel_pos_lens(vlabel_pos_lens, f)?;
    let num_neighbors = num_neighbors(mm, v_pos);
    let mut n_pos = v_pos + size_of::<VertexHeader>() + size_of::<VLabelPosLen>() * num_vlabels;
    for _ in 0..num_neighbors {
        display_neighbor(mm, n_pos, f)?;
        n_pos += neighbor_size(mm, n_pos);
    }
    Ok(())
}

pub fn display(mm: &MemoryManager, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    display_header(mm, f)?;
    let mut v_pos = header_size(mm);
    for _ in 0..num_vertices(mm) {
        display_vertex(mm, v_pos, f)?;
        v_pos += unsafe { (*mm.read::<VertexHeader>(v_pos)).num_bytes };
    }
    Ok(())
}
