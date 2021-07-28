use super::types::{IndexEntry, NeighborEntry, VertexHeader};
use crate::{
    constants::MAGIC_SINGLE,
    data::types::{InfoEdge, InfoEdgeHeader},
    memory_manager::MemoryManager,
    tools::GroupBy,
    types::{VId, VLabel},
};
use std::mem::size_of;

pub fn mm_from_info_edges(mm: &mut MemoryManager, info_edges_mm: &MemoryManager) {
    let &InfoEdgeHeader {
        num_vertices,
        num_edges,
        num_vlabels,
        ..
    } = unsafe { info_edges_mm.as_ref::<InfoEdgeHeader>(0) };
    let (num_vertices, num_edges, num_vlabels) = (
        num_vertices as usize,
        num_edges as usize,
        num_vlabels as usize,
    );
    mm.resize(estimate_datagraph_size(
        num_vlabels,
        num_vertices,
        num_edges,
    ));
    unsafe {
        mm.copy_from_slice::<u64>(0, &[MAGIC_SINGLE]);
        mm.copy_from_slice::<u64>(size_of::<u64>(), &[num_vlabels as u64]);
    }
    let mut pos = 2 * size_of::<u64>() + num_vlabels * size_of::<IndexEntry>();
    for (i, (vlabel, vlabel_group)) in GroupBy::new(
        unsafe { info_edges_mm.as_slice::<InfoEdge>(size_of::<InfoEdgeHeader>(), num_edges * 2) },
        |e| e.0,
    )
    .enumerate()
    {
        let (new_pos, len) = write_vertices(mm, pos, vlabel_group);
        write_index_entry(
            mm,
            2 * size_of::<u64>() + i * size_of::<IndexEntry>(),
            vlabel,
            pos as u64,
            len,
        );
        pos = new_pos;
    }
    mm.resize(pos);
}

fn estimate_datagraph_size(num_vlabels: usize, num_vertices: usize, num_edges: usize) -> usize {
    let vlabel_pos_len_size = size_of::<IndexEntry>() * num_vlabels;
    let header_size = 2 * size_of::<u64>() + vlabel_pos_len_size;
    let vertex_header_size = size_of::<VertexHeader>() * num_vertices;
    let vertex_vlabel_pos_len_size = vlabel_pos_len_size * num_vertices;
    let neighbor_header_size = size_of::<NeighborEntry>() * num_edges * 2;
    header_size + vertex_header_size + vertex_vlabel_pos_len_size + neighbor_header_size
}

fn write_vertices(mm: &mut MemoryManager, mut pos: usize, edges: &[InfoEdge]) -> (usize, u32) {
    let mut len = 0;
    for (vid, vid_group) in GroupBy::new(edges, |e| e.1) {
        pos = write_vertex(mm, pos, vid, vid_group);
        len += 1;
    }
    (pos, len)
}

fn write_vertex(mm: &mut MemoryManager, pos: usize, vid: VId, edges: &[InfoEdge]) -> usize {
    let (mut in_deg, mut out_deg) = (0, 0);
    let num_nlabels = GroupBy::new(edges, |e| e.2).count();
    let mut new_pos = pos + size_of::<VertexHeader>() + num_nlabels * size_of::<IndexEntry>();
    for (i, (nlabel, nlabel_group)) in GroupBy::new(edges, |e| e.2).enumerate() {
        let (new_neighbors_pos, neighbors_len, neighbors_in_deg, neighbors_out_deg) =
            write_neighbors(mm, new_pos, nlabel_group);
        write_index_entry(
            mm,
            pos + size_of::<VertexHeader>() + i * size_of::<IndexEntry>(),
            nlabel,
            new_pos as u64,
            neighbors_len,
        );
        new_pos = new_neighbors_pos;
        in_deg += neighbors_in_deg;
        out_deg += neighbors_out_deg;
    }
    write_vertex_header(
        mm,
        pos,
        (new_pos - pos) as u64,
        vid,
        in_deg,
        out_deg,
        num_nlabels as u16,
    );
    new_pos
}

fn write_neighbors(
    mm: &mut MemoryManager,
    mut pos: usize,
    edges: &[InfoEdge],
) -> (usize, u32, u32, u32) {
    let (mut in_deg, mut out_deg) = (0, 0);
    let mut neighbors_len = 0;
    for (nid, nid_group) in GroupBy::new(edges, |e| e.3) {
        let mut neighbor = NeighborEntry {
            nid,
            n_to_v_elabel: -1,
            v_to_n_elabel: -1,
        };
        for &(_, _, _, _, direction, elabel) in nid_group {
            if direction {
                neighbor.v_to_n_elabel = elabel;
            } else {
                neighbor.n_to_v_elabel = elabel;
            }
        }
        if neighbor.n_to_v_elabel >= 0 {
            in_deg += 1;
        }
        if neighbor.v_to_n_elabel >= 0 {
            out_deg += 1;
        }
        unsafe {
            mm.copy_from_slice(pos, &[neighbor]);
        }
        pos += size_of::<NeighborEntry>();
        neighbors_len += 1;
    }
    (pos, neighbors_len, in_deg, out_deg)
}

fn write_index_entry(mm: &mut MemoryManager, mm_pos: usize, vlabel: VLabel, pos: u64, len: u32) {
    unsafe {
        mm.copy_from_slice(mm_pos, &[IndexEntry { vlabel, pos, len }]);
    }
}

fn write_vertex_header(
    mm: &mut MemoryManager,
    pos: usize,
    num_bytes: u64,
    vid: VId,
    in_deg: u32,
    out_deg: u32,
    num_vlabels: u16,
) {
    unsafe {
        mm.copy_from_slice(
            pos,
            &[VertexHeader {
                num_bytes,
                vid,
                in_deg,
                out_deg,
                num_vlabels,
            }],
        );
    }
}

#[cfg(test)]
mod tests {
    use super::{super::DataGraph, *};
    use crate::data::{info_edges::mm_from_iter, Graph, GraphView};

    #[test]
    fn test_mm_from_info_edges() {
        let vertices = vec![(1, 10), (2, 20), (3, 30)];
        let edges = vec![(1, 2, 12), (1, 3, 13), (2, 3, 23), (3, 2, 32)];
        let mut info_edges_mm = MemoryManager::new_mem(0);
        mm_from_iter(
            &mut info_edges_mm,
            vertices.clone().into_iter(),
            edges.clone().into_iter(),
        );
        let mut mm = MemoryManager::new_mem(0);
        mm_from_info_edges(&mut mm, &info_edges_mm);
        assert_eq!(DataGraph::new(&mm).view(), GraphView::new(vertices, edges));
    }
}
