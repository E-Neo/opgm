use super::types::{NeighborHeader, VLabelPosLen, VertexHeader};
use crate::{
    memory_manager::MemoryManager,
    tools::GroupBy,
    types::{ELabel, VId, VLabel},
};
use std::{
    collections::{HashMap, HashSet},
    mem::size_of,
};

type Vertex = (VId, VLabel);
type Edge = (VId, VId, ELabel);
type InfoEdge = (VLabel, VId, VLabel, VId, bool, ELabel);

pub fn mm_from_iter<V, E>(
    mm: &mut MemoryManager,
    temp_mm: &mut MemoryManager,
    vertices: V,
    edges: E,
) where
    V: IntoIterator<Item = Vertex>,
    E: IntoIterator<Item = Edge>,
{
    let (num_vlabels, info_edges) = create_info_edges(temp_mm, vertices, edges);
    unsafe {
        mm.copy_from_slice(0, &[num_vlabels]);
    }
    let mut pos = size_of::<usize>() + num_vlabels * size_of::<VLabelPosLen>();
    for (i, (vlabel, vlabel_group)) in GroupBy::new(info_edges, |e| e.0).enumerate() {
        let (new_pos, len) = write_vertices(mm, pos, vlabel_group);
        write_index_entry(
            mm,
            size_of::<usize>() + i * size_of::<VLabelPosLen>(),
            vlabel,
            pos,
            len,
        );
        pos = new_pos;
    }
    mm.resize(pos);
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
    let mut new_pos = pos + size_of::<VertexHeader>() + num_nlabels * size_of::<VLabelPosLen>();
    for (i, (nlabel, nlabel_group)) in GroupBy::new(edges, |e| e.2).enumerate() {
        let (new_neighbors_pos, neighbors_len, neighbors_in_deg, neighbors_out_deg) =
            write_neighbors(mm, new_pos, nlabel_group);
        write_index_entry(
            mm,
            pos + size_of::<VertexHeader>() + i * size_of::<VLabelPosLen>(),
            nlabel,
            new_pos,
            neighbors_len,
        );
        new_pos = new_neighbors_pos;
        in_deg += neighbors_in_deg;
        out_deg += neighbors_out_deg;
    }
    write_vertex_header(
        mm,
        pos,
        new_pos - pos,
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
        let (new_neighbor_pos, num_n_to_v, num_v_to_n) =
            write_elabels(mm, pos + size_of::<NeighborHeader>(), nid_group);
        write_neighbor_header(mm, pos, nid, num_n_to_v, num_v_to_n);
        pos = new_neighbor_pos;
        in_deg += num_n_to_v as u32;
        out_deg += num_v_to_n as u32;
        neighbors_len += 1;
    }
    (pos, neighbors_len, in_deg, out_deg)
}

fn write_index_entry(mm: &mut MemoryManager, mm_pos: usize, vlabel: VLabel, pos: usize, len: u32) {
    unsafe {
        mm.copy_from_slice(mm_pos, &[VLabelPosLen { vlabel, pos, len }]);
    }
}

fn write_vertex_header(
    mm: &mut MemoryManager,
    pos: usize,
    num_bytes: usize,
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

fn write_neighbor_header(
    mm: &mut MemoryManager,
    pos: usize,
    nid: VId,
    num_n_to_v: u16,
    num_v_to_n: u16,
) {
    unsafe {
        mm.copy_from_slice(
            pos,
            &[NeighborHeader {
                nid,
                num_n_to_v,
                num_v_to_n,
            }],
        )
    }
}

fn write_elabels(mm: &mut MemoryManager, pos: usize, edges: &[InfoEdge]) -> (usize, u16, u16) {
    let (mut num_n_to_v, mut num_v_to_n) = (0, 0);
    let mut offset = 0;
    for (i, &(_, _, _, _, direction, elabel)) in edges.iter().enumerate() {
        unsafe {
            mm.copy_from_slice(pos + i * size_of::<ELabel>(), &[elabel]);
        }
        if direction {
            num_v_to_n += 1;
        } else {
            num_n_to_v += 1;
        }
        offset += 1;
    }
    (pos + offset * size_of::<ELabel>(), num_n_to_v, num_v_to_n)
}

fn create_vid_vlabel_map<V>(vertices: V) -> (HashMap<VId, VLabel>, usize)
where
    V: IntoIterator<Item = Vertex>,
{
    let mut label_set = HashSet::new();
    let mut vid_vlabel_map = HashMap::new();
    for (vid, vlabel) in vertices {
        label_set.insert(vlabel);
        vid_vlabel_map.insert(vid, vlabel);
    }
    (vid_vlabel_map, label_set.len())
}

fn create_info_edges<V, E>(
    temp_mm: &mut MemoryManager,
    vertices: V,
    edges: E,
) -> (usize, &[InfoEdge])
where
    V: IntoIterator<Item = Vertex>,
    E: IntoIterator<Item = Edge>,
{
    let (vid_vlabel_map, num_vlabels) = create_vid_vlabel_map(vertices);
    let mut pos = 0;
    for (src, dst, elabel) in edges {
        unsafe {
            temp_mm.copy_from_slice(
                pos,
                &[
                    (
                        *vid_vlabel_map.get(&dst).unwrap(),
                        dst,
                        *vid_vlabel_map.get(&src).unwrap(),
                        src,
                        false,
                        elabel,
                    ),
                    (
                        *vid_vlabel_map.get(&src).unwrap(),
                        src,
                        *vid_vlabel_map.get(&dst).unwrap(),
                        dst,
                        true,
                        elabel,
                    ),
                ],
            );
        }
        pos += 2 * size_of::<InfoEdge>();
    }
    temp_mm.resize(pos);
    let data: &mut [InfoEdge] = unsafe { temp_mm.as_mut_slice(0, pos / size_of::<InfoEdge>()) };
    data.sort();
    (num_vlabels, data)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_info_edges() {
        let mut temp_mm = MemoryManager::new_mem(6 * size_of::<InfoEdge>());
        let info_edges: &[InfoEdge] = &[
            (1, 1, 2, 2, true, 12),
            (1, 1, 2, 3, true, 13),
            (2, 2, 1, 1, false, 12),
            (2, 2, 2, 3, true, 23),
            (2, 3, 1, 1, false, 13),
            (2, 3, 2, 2, false, 23),
        ];
        assert_eq!(
            create_info_edges(
                &mut temp_mm,
                vec![(1, 1), (2, 2), (3, 2)],
                vec![(1, 2, 12), (1, 3, 13), (2, 3, 23)]
            ),
            (2, info_edges)
        );
    }
}
