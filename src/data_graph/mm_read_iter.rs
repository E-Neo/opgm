use crate::{
    memory_manager::{MemoryManager, MmapMutFile},
    types::{ELabel, NeighborHeader, VId, VLabel, VLabelPosLen, VertexHeader},
};
use itertools::Itertools;
use std::mem::size_of;

type Vertex = (VId, VLabel);
type Edge = (VId, VId, ELabel);
type EdgeWithVLabel = (VLabel, VId, VLabel, VId, ELabel);
type InfoEdge = (VLabel, VId, VLabel, VId, bool, ELabel);

fn create_vertices_mmap<V>(num_vertices: usize, vertices: V) -> MmapMutFile
where
    V: IntoIterator<Item = Vertex>,
{
    let file = tempfile::tempfile().unwrap();
    file.set_len((size_of::<Vertex>() * num_vertices) as u64)
        .unwrap();
    let mut mm = MmapMutFile::from_file(file);
    for (i, vertex) in vertices.into_iter().enumerate() {
        unsafe {
            *((mm.as_mut_ptr() as *mut Vertex).add(i)) = vertex;
        }
    }
    unsafe {
        std::slice::from_raw_parts_mut(mm.as_mut_ptr() as *mut Vertex, num_vertices).sort();
    }
    mm
}

fn create_vertices_index(mm: &MmapMutFile) -> &[Vertex] {
    unsafe {
        std::slice::from_raw_parts(mm.as_ptr() as *const Vertex, mm.len() / size_of::<Vertex>())
    }
}

fn search_vlabel_by_vid(vertices_index: &[Vertex], vid: VId) -> Option<VLabel> {
    if let Result::Ok(i) = vertices_index.binary_search_by_key(&vid, |&(vid, _)| vid) {
        Some(vertices_index[i].1)
    } else {
        None
    }
}

fn create_edges_with_vlabel_mmap<E>(
    vertices_index: &[Vertex],
    num_edges: usize,
    edges: E,
) -> MmapMutFile
where
    E: IntoIterator<Item = Edge>,
{
    let file = tempfile::tempfile().unwrap();
    file.set_len((size_of::<EdgeWithVLabel>() * num_edges) as u64)
        .unwrap();
    let mut mm = MmapMutFile::from_file(file);
    for (i, (src_vid, dst_vid, elabel)) in edges.into_iter().enumerate() {
        unsafe {
            *((mm.as_mut_ptr() as *mut EdgeWithVLabel).add(i)) = (
                search_vlabel_by_vid(vertices_index, src_vid).unwrap(),
                src_vid,
                search_vlabel_by_vid(vertices_index, dst_vid).unwrap(),
                dst_vid,
                elabel,
            );
        }
    }
    mm
}

fn create_edges_with_vlabel(mm: &MmapMutFile) -> &[EdgeWithVLabel] {
    unsafe {
        std::slice::from_raw_parts(
            mm.as_ptr() as *const EdgeWithVLabel,
            mm.len() / size_of::<EdgeWithVLabel>(),
        )
    }
}

fn create_edges_with_vlabel_mut(mm: &mut MmapMutFile) -> &mut [EdgeWithVLabel] {
    unsafe {
        std::slice::from_raw_parts_mut(
            mm.as_mut_ptr() as *mut EdgeWithVLabel,
            mm.len() / size_of::<EdgeWithVLabel>(),
        )
    }
}

fn copy_mmapfile(mm: &MmapMutFile) -> MmapMutFile {
    let file = tempfile::tempfile().unwrap();
    file.set_len(mm.len() as u64).unwrap();
    let mut new_mm = MmapMutFile::from_file(file);
    unsafe {
        std::ptr::copy(mm.as_ptr(), new_mm.as_mut_ptr(), new_mm.len());
    }
    new_mm
}

fn create_in_out_edges_mmap<E>(
    vertices_index: &[Vertex],
    num_edges: usize,
    edges: E,
) -> (MmapMutFile, MmapMutFile)
where
    E: IntoIterator<Item = Edge>,
{
    let mut out_edges_mm = create_edges_with_vlabel_mmap(vertices_index, num_edges, edges);
    let mut in_edges_mm = copy_mmapfile(&out_edges_mm);
    let in_edges = create_edges_with_vlabel_mut(&mut in_edges_mm);
    in_edges.sort_by_key(|&(src_vlabel, src_vid, dst_vlabel, dst_vid, elabel)| {
        (dst_vlabel, dst_vid, src_vlabel, src_vid, elabel)
    });
    for e in in_edges.iter_mut() {
        *e = (e.2, e.3, e.0, e.1, e.4);
    }
    let out_edges = create_edges_with_vlabel_mut(&mut out_edges_mm);
    out_edges.sort();
    (in_edges_mm, out_edges_mm)
}

fn create_info_edges_mmap<V, E>(
    num_vertices: usize,
    num_edges: usize,
    vertices: V,
    edges: E,
) -> MmapMutFile
where
    V: IntoIterator<Item = Vertex>,
    E: IntoIterator<Item = Edge>,
{
    let vertices_mm = create_vertices_mmap(num_vertices, vertices);
    let vertices_index = create_vertices_index(&vertices_mm);
    let (in_edges_mm, out_edges_mm) = create_in_out_edges_mmap(vertices_index, num_edges, edges);
    let (in_edges, out_edges) = (
        create_edges_with_vlabel(&in_edges_mm),
        create_edges_with_vlabel(&out_edges_mm),
    );
    let info_edges = in_edges
        .iter()
        .map(|(dst_vlabel, dst_vid, src_vlabel, src_vid, elabel)| {
            (dst_vlabel, dst_vid, src_vlabel, src_vid, false, elabel)
        })
        .merge(
            out_edges
                .iter()
                .map(|(src_vlabel, src_vid, dst_vlabel, dst_vid, elabel)| {
                    (src_vlabel, src_vid, dst_vlabel, dst_vid, true, elabel)
                }),
        );
    let file = tempfile::tempfile().unwrap();
    file.set_len((size_of::<InfoEdge>() * num_edges * 2) as u64)
        .unwrap();
    let mut mm = MmapMutFile::from_file(file);
    for (i, (&v_vlabel, &v_vid, &n_vlabel, &n_vid, direction, &elabel)) in info_edges.enumerate() {
        unsafe {
            *((mm.as_mut_ptr() as *mut InfoEdge).add(i)) =
                (v_vlabel, v_vid, n_vlabel, n_vid, direction, elabel);
        }
    }
    mm
}

fn create_info_edges(mm: &MmapMutFile) -> &[InfoEdge] {
    unsafe {
        std::slice::from_raw_parts(
            mm.as_ptr() as *const InfoEdge,
            mm.len() / size_of::<InfoEdge>(),
        )
    }
}

fn estimate_datagraph_size(num_vlabels: usize, num_vertices: usize, num_edges: usize) -> usize {
    let vlabel_pos_len_size = size_of::<VLabelPosLen>() * num_vlabels;
    let header_size = size_of::<usize>() + vlabel_pos_len_size;
    let vertex_header_size = size_of::<VertexHeader>() * num_vertices;
    let vertex_vlabel_pos_len_size = vlabel_pos_len_size * num_vertices;
    let neighbor_header_size = size_of::<NeighborHeader>() * num_edges * 2;
    let elabel_size = size_of::<ELabel>() * num_edges * 2;
    header_size
        + vertex_header_size
        + vertex_vlabel_pos_len_size
        + neighbor_header_size
        + elabel_size
}

fn write_num_vlabels(mm: &mut MemoryManager, num_vlabels: usize) {
    mm.write(0, &num_vlabels as *const usize, 1);
}

fn write_vlabel_pos_len(
    mm: &mut MemoryManager,
    addr: usize,
    index: usize,
    vlabel: VLabel,
    pos: usize,
    len: usize,
) {
    mm.write(
        addr + size_of::<VLabelPosLen>() * index,
        &VLabelPosLen {
            vlabel,
            pos,
            len: len as u32,
        } as *const VLabelPosLen,
        1,
    );
}

fn write_vertex_header(
    mm: &mut MemoryManager,
    pos: usize,
    new_pos: usize,
    vid: VId,
    in_deg: usize,
    out_deg: usize,
    num_vlabels: usize,
) {
    mm.write(
        pos,
        &VertexHeader {
            num_bytes: new_pos - pos,
            vid,
            in_deg: in_deg as u32,
            out_deg: out_deg as u32,
            num_vlabels: num_vlabels as u16,
        } as *const VertexHeader,
        1,
    );
}

fn write_elabels<'a, E>(mm: &mut MemoryManager, pos: usize, edges: E) -> (usize, usize, usize)
where
    E: IntoIterator<Item = &'a InfoEdge>,
{
    let mut num_n_to_v = 0;
    let mut num_v_to_n = 0;
    let mut index = 0;
    for (i, &(_, _, _, _, direction, elabel)) in edges.into_iter().enumerate() {
        mm.write(pos + size_of::<ELabel>() * i, &elabel as *const ELabel, 1);
        if direction {
            num_v_to_n += 1;
        } else {
            num_n_to_v += 1;
        }
        index += 1;
    }
    (pos + size_of::<ELabel>() * index, num_n_to_v, num_v_to_n)
}

fn write_neighbor_header(
    mm: &mut MemoryManager,
    pos: usize,
    nid: VId,
    num_n_to_v: usize,
    num_v_to_n: usize,
) {
    mm.write(
        pos,
        &NeighborHeader {
            nid,
            num_n_to_v: num_n_to_v as u16,
            num_v_to_n: num_v_to_n as u16,
        } as *const NeighborHeader,
        1,
    );
}

fn write_neighbors<'a, E>(
    mm: &mut MemoryManager,
    pos: usize,
    edges: E,
) -> (usize, usize, usize, usize)
where
    E: IntoIterator<Item = &'a InfoEdge>,
{
    let mut in_deg = 0;
    let mut out_deg = 0;
    let mut pos = pos;
    let mut neighbors_len = 0;
    for (n_vid, n_vid_group) in &edges.into_iter().group_by(|e| e.3) {
        let (new_neighbor_pos, num_n_to_v, num_v_to_n) =
            write_elabels(mm, pos + size_of::<NeighborHeader>(), n_vid_group);
        write_neighbor_header(mm, pos, n_vid, num_n_to_v, num_v_to_n);
        pos = new_neighbor_pos;
        in_deg += num_n_to_v;
        out_deg += num_v_to_n;
        neighbors_len += 1;
    }
    (pos, neighbors_len, in_deg, out_deg)
}

fn write_vertex<'a, E>(mm: &mut MemoryManager, pos: usize, vid: VId, edges: E) -> usize
where
    E: IntoIterator<Item = &'a InfoEdge>,
{
    let mut in_deg = 0;
    let mut out_deg = 0;
    let (edges1, edges2) = edges.into_iter().tee();
    let num_vlabels = edges1.into_iter().group_by(|e| e.2).into_iter().count();
    let mut new_pos = pos + size_of::<VertexHeader>() + size_of::<VLabelPosLen>() * num_vlabels;
    for (i, (n_vlabel, n_vlabel_group)) in
        edges2.into_iter().group_by(|e| e.2).into_iter().enumerate()
    {
        let (new_neighbors_pos, neighbors_len, neighbors_in_deg, neighbors_out_deg) =
            write_neighbors(mm, new_pos, n_vlabel_group);
        write_vlabel_pos_len(
            mm,
            pos + size_of::<VertexHeader>(),
            i,
            n_vlabel,
            new_pos,
            neighbors_len,
        );
        new_pos = new_neighbors_pos;
        in_deg += neighbors_in_deg;
        out_deg += neighbors_out_deg;
    }
    write_vertex_header(mm, pos, new_pos, vid, in_deg, out_deg, num_vlabels);
    new_pos
}

fn write_vertices<'a, E>(mm: &mut MemoryManager, pos: usize, edges: E) -> (usize, usize)
where
    E: IntoIterator<Item = &'a InfoEdge>,
{
    let mut pos = pos;
    let mut vertices_len = 0;
    for (v_vid, v_vid_group) in &edges.into_iter().group_by(|e| e.1) {
        pos = write_vertex(mm, pos, v_vid, v_vid_group);
        vertices_len += 1;
    }
    (pos, vertices_len)
}

/// Reads the data graph by vertices and edges.
pub fn mm_read_iter<V, E>(
    mm: &mut MemoryManager,
    num_vlabels: usize,
    num_vertices: usize,
    num_edges: usize,
    vertices: V,
    edges: E,
) where
    V: IntoIterator<Item = Vertex>,
    E: IntoIterator<Item = Edge>,
{
    mm.resize(0);
    mm.resize(estimate_datagraph_size(
        num_vlabels,
        num_vertices,
        num_edges,
    ));
    write_num_vlabels(mm, num_vlabels);
    let info_edges_mmap = create_info_edges_mmap(num_vertices, num_edges, vertices, edges);
    let mut pos = size_of::<usize>() + size_of::<VLabelPosLen>() * num_vlabels;
    for (i, (v_vlabel, v_vlabel_group)) in create_info_edges(&info_edges_mmap)
        .into_iter()
        .group_by(|e| e.0)
        .into_iter()
        .enumerate()
    {
        let (new_pos, vertices_len) = write_vertices(mm, pos, v_vlabel_group);
        write_vlabel_pos_len(mm, size_of::<usize>(), i, v_vlabel, pos, vertices_len);
        pos = new_pos;
    }
    mm.resize(pos);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_info_edges() {
        let vertices = vec![(1, 10), (2, 20), (3, 30)];
        let edges = vec![(1, 2, 12), (1, 3, 13), (2, 3, 23)];
        let info_edges_mm = create_info_edges_mmap(vertices.len(), edges.len(), vertices, edges);
        assert_eq!(
            [
                (10, 1, 20, 2, true, 12),
                (10, 1, 30, 3, true, 13),
                (20, 2, 10, 1, false, 12),
                (20, 2, 30, 3, true, 23),
                (30, 3, 10, 1, false, 13),
                (30, 3, 20, 2, false, 23)
            ],
            create_info_edges(&info_edges_mm)
        );
    }

    #[test]
    fn test_write_elabels() {
        let mut mm = MemoryManager::Mem(vec![]);
        let size = 10 + size_of::<ELabel>() * 2;
        mm.resize(size);
        assert_eq!(
            write_elabels(
                &mut mm,
                10,
                vec![&(20, 2, 20, 3, false, 32), &(20, 2, 20, 3, true, 23)]
            ),
            (size, 1, 1)
        );
    }

    #[test]
    fn test_write_neighbors() {
        let mut mm = MemoryManager::Mem(vec![]);
        let size = 10 + size_of::<NeighborHeader>() * 2 + size_of::<ELabel>() * 3;
        mm.resize(size);
        assert_eq!(
            write_neighbors(
                &mut mm,
                10,
                vec![
                    &(20, 2, 10, 1, false, 12),
                    &(20, 2, 20, 3, false, 32),
                    &(20, 2, 20, 3, true, 23)
                ]
            ),
            (size, 2, 2, 1)
        );
    }

    #[test]
    fn test_write_vertex_1() {
        let mut mm = MemoryManager::Mem(vec![]);
        let size = size_of::<VertexHeader>()
            + size_of::<VLabelPosLen>()
            + size_of::<NeighborHeader>() * 2
            + size_of::<ELabel>() * 2;
        mm.resize(size);
        assert_eq!(
            write_vertex(
                &mut mm,
                0,
                1,
                vec![&(10, 1, 20, 2, true, 12), &(10, 1, 20, 3, true, 13)]
            ),
            size
        );
    }

    #[test]
    fn test_write_vertex_2() {
        let mut mm = MemoryManager::Mem(vec![]);
        let size = 10
            + size_of::<VertexHeader>()
            + size_of::<VLabelPosLen>() * 2
            + size_of::<NeighborHeader>() * 2
            + size_of::<ELabel>() * 3;
        mm.resize(size);
        assert_eq!(
            write_vertex(
                &mut mm,
                10,
                2,
                vec![
                    &(20, 2, 10, 1, false, 12),
                    &(20, 2, 20, 3, false, 32),
                    &(20, 2, 20, 3, true, 23)
                ]
            ),
            size
        );
    }

    #[test]
    fn test_write_vertices_1() {
        let mut mm = MemoryManager::Mem(vec![]);
        let size = size_of::<VertexHeader>()
            + size_of::<VLabelPosLen>()
            + size_of::<NeighborHeader>() * 2
            + size_of::<ELabel>() * 2;
        mm.resize(size);
        assert_eq!(
            write_vertices(
                &mut mm,
                0,
                vec![&(10, 1, 20, 2, true, 12), &(10, 1, 20, 3, true, 13)]
            ),
            (size, 1)
        );
    }

    #[test]
    fn test_write_vertices_2() {
        let mut mm = MemoryManager::Mem(vec![]);
        let size = size_of::<VertexHeader>() * 2
            + size_of::<VLabelPosLen>() * 4
            + size_of::<NeighborHeader>() * 4
            + size_of::<ELabel>() * 6;
        mm.resize(size);
        assert_eq!(
            write_vertices(
                &mut mm,
                0,
                vec![
                    &(20, 2, 10, 1, false, 12),
                    &(20, 2, 20, 3, true, 23),
                    &(20, 2, 20, 3, false, 32),
                    &(20, 3, 10, 1, true, 13),
                    &(20, 3, 20, 2, false, 23),
                    &(20, 3, 20, 2, true, 32),
                ]
            ),
            (size, 2)
        )
    }
}
