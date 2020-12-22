use crate::{
    data_graph::{DataGraph, DataNeighbor, DataVertex},
    executor::{write_index, write_num_bytes, write_pos_len, write_super_row_header, write_vid},
    memory_manager::MemoryManager,
    pattern_graph::NeighborInfo,
    planner::CharacteristicInfo,
    types::{PosLen, SuperRowHeader, VId, VIdPos, VLabel},
};
use std::cmp::Ordering;
use std::collections::HashSet;
use std::mem::size_of;

/// Match the stars by matching the `characteristic`s.
///
/// The `infos`, `super_row_mms` and `index_mms` are calculated by the planner,
/// and we access to the specific parts by `ids`.
/// In order to scan the `data_graph` only once, we group stars (characteristics) with the
/// same root label (`vlabel`) together.
pub fn match_characteristics(
    data_graph: &DataGraph,
    vlabel: VLabel,
    infos: &[CharacteristicInfo],
    super_row_mms: &mut [MemoryManager],
    index_mms: &mut [MemoryManager],
) {
    let (vertices_len, vertices) = data_graph.vertices(vlabel);
    initialize_results(infos, super_row_mms, index_mms, vertices_len);
    let sr_pos_idx_poses = scan_data_vertices(vertices, infos, super_row_mms, index_mms);
    finish_results(infos, super_row_mms, index_mms, &sr_pos_idx_poses);
}

/// Allocate space for `SuperRowHeader` and the indices.
///
/// For the `super_row_mm`, we only allocate space to store the header.
/// One have to allocate enough space to store the new `SuperRow` manually.
/// For the `index_mm`, we could obtain the upper bound
/// by [`vertices()`](../data_graph/struct.DataGraph.html#method.vertices),
/// so it could be allocate once for all.
fn initialize_results(
    infos: &[CharacteristicInfo],
    super_row_mms: &mut [MemoryManager],
    index_mms: &mut [MemoryManager],
    vertices_len: usize,
) {
    infos.iter().map(|info| info.id()).for_each(|id| {
        super_row_mms[id].resize(size_of::<SuperRowHeader>());
        index_mms[id].resize(vertices_len * size_of::<VIdPos>());
    });
}

/// Returns `(sr_pos, idx_pos)` for `super_row_mms`.
///
/// Please note that the result of this function has the same length as `index_mms`,
/// but only the `id`s correspond to `infos` are valid.
fn scan_data_vertices<'a, VS>(
    vertices: VS,
    infos: &[CharacteristicInfo],
    super_row_mms: &mut [MemoryManager],
    index_mms: &mut [MemoryManager],
) -> Vec<(usize, usize)>
where
    VS: IntoIterator<Item = DataVertex<'a>>,
{
    let mut sr_pos_idx_poses = vec![(size_of::<SuperRowHeader>(), 0); index_mms.len()];
    for vertex in vertices {
        match_data_vertex(
            &vertex,
            infos,
            super_row_mms,
            index_mms,
            &mut sr_pos_idx_poses,
        );
    }
    sr_pos_idx_poses
}

/// Scan the `vertex` and its neighbors to match characteristics.
fn match_data_vertex(
    vertex: &DataVertex,
    infos: &[CharacteristicInfo],
    super_row_mms: &mut [MemoryManager],
    index_mms: &mut [MemoryManager],
    sr_pos_idx_poses: &mut [(usize, usize)],
) {
    infos.iter().for_each(|info| {
        match_data_vertex_for_one_info(
            vertex,
            info,
            &mut super_row_mms[info.id()],
            &mut index_mms[info.id()],
            &mut sr_pos_idx_poses[info.id()],
        );
    });
}

fn match_data_vertex_for_one_info(
    vertex: &DataVertex,
    info: &CharacteristicInfo,
    super_row_mm: &mut MemoryManager,
    index_mm: &mut MemoryManager,
    sr_pos_idx_pos: &mut (usize, usize),
) {
    if let Some(vc) = info.characteristic().root_constraint() {
        if !vc.f()(vertex.id()) {
            return;
        }
    }
    let &mut (sr_pos, idx_pos) = sr_pos_idx_pos;
    let root_pos = sr_pos
        + size_of::<usize>()
        + (1 + info.characteristic().info_nums().len()) * size_of::<PosLen>();
    let mut pos = root_pos + size_of::<VId>();
    if allocate(super_row_mm, sr_pos, vertex, info) {
        let (mut left_iter, mut right_iter) =
            (vertex.vlabels(), info.nlabel_ninfo_num_eqvs().iter());
        let (mut left, mut right) = (left_iter.next(), right_iter.next());
        while let (Some((x, _, neighbors)), Some((y, ninfo_num_eqvs))) = (&left, right) {
            match (*x).cmp(y) {
                Ordering::Less => {
                    left = left_iter.next();
                }
                Ordering::Equal => {
                    for &(ninfo, num, eqv) in ninfo_num_eqvs {
                        let num_wrote = match_neighbors(
                            vertex,
                            neighbors.clone(),
                            ninfo,
                            super_row_mm,
                            sr_pos,
                            eqv,
                            pos,
                        );
                        if num_wrote < num {
                            return;
                        } else {
                            pos += num_wrote * size_of::<VId>();
                        }
                    }
                    left = left_iter.next();
                    right = right_iter.next();
                }
                Ordering::Greater => {
                    break;
                }
            }
        }
        write_num_bytes(super_row_mm, sr_pos, pos - sr_pos);
        write_pos_len(super_row_mm, sr_pos, 0, root_pos, 1);
        write_vid(super_row_mm, root_pos, vertex.id());
        write_index(index_mm, idx_pos, vertex.id(), sr_pos);
        *sr_pos_idx_pos = (pos, idx_pos + size_of::<VIdPos>());
    }
}

/// Scan the neighbors and write them to the SuperRow file if matched.
///
/// Returns the number of vertices written to file.
fn match_neighbors<'a, N>(
    vertex: &DataVertex,
    neighbors: N,
    ninfo: &NeighborInfo,
    super_row_mm: &mut MemoryManager,
    sr_pos: usize,
    eqv: usize,
    pos: usize,
) -> usize
where
    N: IntoIterator<Item = DataNeighbor<'a>>,
{
    let mut new_pos = pos;
    neighbors
        .into_iter()
        .filter(|neighbor| check_neighbor_constraints(vertex, neighbor, ninfo))
        .filter(|neighbor| check_neighbor_degrees(neighbor, ninfo))
        .filter(|neighbor| check_neighbor_edges(neighbor, ninfo))
        .for_each(|neighbor| {
            write_vid(super_row_mm, new_pos, neighbor.id());
            new_pos += size_of::<VId>();
        });
    let len = (new_pos - pos) / size_of::<VId>();
    write_pos_len(super_row_mm, sr_pos, eqv, pos, len);
    len
}

/// Checks the *vertex constraint* and the *edge constraint* of `neighbor`.
fn check_neighbor_constraints(
    vertex: &DataVertex,
    neighbor: &DataNeighbor,
    info: &NeighborInfo,
) -> bool {
    if let Some(vc) = info.neighbor_constraint() {
        if !vc.f()(neighbor.id()) {
            return false;
        }
    }
    if let Some(ec) = info.edge_constraint() {
        if !ec.f()(vertex.id(), neighbor.id()) {
            return false;
        }
    }
    true
}

/// Checks the number of edges between the `vertex` and the `neighbor`.
fn check_neighbor_degrees(neighbor: &DataNeighbor, info: &NeighborInfo) -> bool {
    neighbor.n_to_v_elabels().len() >= info.n_to_v_elabels().len()
        && neighbor.v_to_n_elabels().len() >= info.v_to_n_elabels().len()
        && neighbor.n_to_v_elabels().len() + neighbor.v_to_n_elabels().len()
            >= info.n_to_v_elabels().len()
                + info.v_to_n_elabels().len()
                + info.undirected_elabels().len()
}

/// Checks whether the edges could match.
fn check_neighbor_edges(neighbor: &DataNeighbor, info: &NeighborInfo) -> bool {
    let mut n_to_v_elabels: HashSet<_> =
        info.n_to_v_elabels().iter().map(|&elabel| elabel).collect();
    let mut v_to_n_elabels: HashSet<_> =
        info.v_to_n_elabels().iter().map(|&elabel| elabel).collect();
    let mut undirected_elabels: HashSet<_> = info
        .undirected_elabels()
        .iter()
        .map(|&elabel| elabel)
        .collect();
    for elabel in neighbor.n_to_v_elabels() {
        if !n_to_v_elabels.remove(elabel) {
            undirected_elabels.remove(elabel);
        }
    }
    for elabel in neighbor.v_to_n_elabels() {
        if !v_to_n_elabels.remove(elabel) {
            undirected_elabels.remove(elabel);
        }
    }
    n_to_v_elabels.len() == 0 && v_to_n_elabels.len() == 0 && undirected_elabels.len() == 0
}

/// Tries to allocate space for a SuperRow.
///
/// It returns whether the `vertex` may match the `Characteristic`.
fn allocate(
    super_row_mm: &mut MemoryManager,
    sr_pos: usize,
    vertex: &DataVertex,
    info: &CharacteristicInfo,
) -> bool {
    let mut num_vids = 1;
    let mut left_iter = vertex.vlabels();
    let mut right_iter = info
        .nlabel_ninfo_num_eqvs()
        .iter()
        .map(|(&nlabel, ninfo_eqvs)| (nlabel, ninfo_eqvs.len()));
    let (mut left, mut right) = (left_iter.next(), right_iter.next());
    while let (Some((x, xlen, _)), Some((y, ylen))) = (left, right) {
        match x.cmp(&y) {
            Ordering::Less => {
                left = left_iter.next();
            }
            Ordering::Equal => {
                if ylen == 1 {
                    num_vids += xlen;
                } else if xlen >= ylen {
                    num_vids += xlen * ylen;
                } else {
                    return false;
                }
                left = left_iter.next();
                right = right_iter.next();
            }
            Ordering::Greater => {
                break;
            }
        }
    }
    if let Some(_) = right {
        false
    } else {
        super_row_mm.resize(
            sr_pos
                + size_of::<usize>()
                + (1 + info.characteristic().info_nums().len()) * size_of::<PosLen>()
                + num_vids * size_of::<VId>(),
        );
        true
    }
}

/// Update the `SuperRowHeader` and truncate `super_row_mm` and `index_mm` in `ids`.
fn finish_results(
    infos: &[CharacteristicInfo],
    super_row_mms: &mut [MemoryManager],
    index_mms: &mut [MemoryManager],
    sr_pos_idx_poses: &[(usize, usize)],
) {
    infos.iter().for_each(|info| {
        let (sr_pos, idx_pos) = sr_pos_idx_poses[info.id()];
        write_super_row_header(
            &mut super_row_mms[info.id()],
            idx_pos / size_of::<VIdPos>(),
            info.characteristic().info_nums().len() + 1,
            1,
        );
        super_row_mms[info.id()].resize(sr_pos);
        index_mms[info.id()].resize(idx_pos);
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        data_graph::mm_read_iter,
        executor::{add_super_row_and_index, empty_super_row_mm},
        pattern_graph::{Characteristic, PatternGraph},
    };
    use std::collections::HashSet;

    fn create_super_row_mm(num_eqvs: usize, num_cover: usize) -> MemoryManager {
        let mut mm = MemoryManager::Mem(vec![]);
        empty_super_row_mm(&mut mm, num_eqvs, num_cover);
        mm
    }

    fn create_stars() -> DataGraph {
        let mut mm = MemoryManager::Mem(vec![]);
        let vertices = vec![
            (1, 0),
            (2, 1),
            (3, 1),
            (4, 2),
            (5, 0),
            (6, 1),
            (7, 1),
            (8, 2),
        ];
        let edges = vec![
            (1, 2, 0),
            (1, 3, 0),
            (1, 4, 0),
            (5, 6, 0),
            (5, 7, 0),
            (5, 8, 0),
        ];
        mm_read_iter(
            &mut mm,
            vertices
                .iter()
                .map(|&(_, vlabel)| vlabel)
                .collect::<HashSet<_>>()
                .len(),
            vertices.len(),
            edges.len(),
            vertices,
            edges,
        );
        DataGraph::new(mm)
    }

    fn create_pattern_graph<'a>() -> PatternGraph<'a> {
        let mut p = PatternGraph::new();
        for (vid, vlabel) in vec![(1, 0), (2, 1), (3, 2), (4, 1), (5, 0)] {
            p.add_vertex(vid, vlabel);
        }
        for (u1, u2, elabel) in vec![(1, 2, 0), (1, 3, 0), (5, 2, 0), (5, 4, 0)] {
            p.add_arc(u1, u2, elabel);
        }
        p
    }

    #[test]
    fn test_super_row_index() {
        let data_graph = create_stars();
        let pattern_graph = create_pattern_graph();
        let infos = vec![
            CharacteristicInfo::new(Characteristic::new(&pattern_graph, 1), 0),
            CharacteristicInfo::new(Characteristic::new(&pattern_graph, 5), 1),
        ];
        let mut super_row_mms = vec![MemoryManager::Mem(vec![]), MemoryManager::Mem(vec![])];
        let mut index_mms = vec![MemoryManager::Mem(vec![]), MemoryManager::Mem(vec![])];
        match_characteristics(
            &data_graph,
            0,
            infos.as_slice(),
            super_row_mms.as_mut_slice(),
            index_mms.as_mut_slice(),
        );
        let mut super_row_mm0 = create_super_row_mm(3, 1);
        let mut index_mm0 = MemoryManager::Mem(vec![]);
        add_super_row_and_index(
            &mut super_row_mm0,
            &mut index_mm0,
            &[1, 2, 1],
            &[&[1], &[2, 3], &[4]],
        );
        add_super_row_and_index(
            &mut super_row_mm0,
            &mut index_mm0,
            &[1, 2, 1],
            &[&[5], &[6, 7], &[8]],
        );
        let mut super_row_mm1 = create_super_row_mm(2, 1);
        let mut index_mm1 = MemoryManager::Mem(vec![]);
        add_super_row_and_index(
            &mut super_row_mm1,
            &mut index_mm1,
            &[1, 2],
            &[&[1], &[2, 3]],
        );
        add_super_row_and_index(
            &mut super_row_mm1,
            &mut index_mm1,
            &[1, 2],
            &[&[5], &[6, 7]],
        );
        assert_eq!(
            super_row_mms
                .iter()
                .map(|mm| mm.read_slice::<u8>(0, mm.len()))
                .collect::<Vec<_>>(),
            vec![super_row_mm0, super_row_mm1]
                .iter()
                .map(|mm| mm.read_slice::<u8>(0, mm.len()))
                .collect::<Vec<_>>()
        );
        assert_eq!(
            index_mms
                .iter()
                .map(|mm| mm.read_slice::<u8>(0, mm.len()))
                .collect::<Vec<_>>(),
            vec![index_mm0, index_mm1]
                .iter()
                .map(|mm| mm.read_slice::<u8>(0, mm.len()))
                .collect::<Vec<_>>()
        );
    }

    fn create_data_graph_q02() -> DataGraph {
        let vertices = vec![(1, 1), (2, 2), (3, 3), (4, 4), (11, 1), (14, 4)];
        let arcs = vec![(1, 2, 0), (2, 3, 0), (3, 4, 0), (3, 14, 0), (11, 2, 0)];
        let mut mm = MemoryManager::Mem(vec![]);
        mm_read_iter(
            &mut mm,
            vertices
                .iter()
                .map(|&(_, l)| l)
                .collect::<HashSet<_>>()
                .len(),
            vertices.len(),
            arcs.len(),
            vertices,
            arcs,
        );
        DataGraph::new(mm)
    }

    fn create_pattern_graph_q02<'a>() -> PatternGraph<'a> {
        let mut pattern = PatternGraph::new();
        vec![(1, 1), (2, 2), (3, 3), (4, 4)]
            .into_iter()
            .for_each(|(v, l)| pattern.add_vertex(v, l));
        vec![(1, 2, 0), (2, 3, 0), (3, 4, 0)]
            .into_iter()
            .for_each(|(u1, u2, l)| {
                pattern.add_arc(u1, u2, l);
            });
        pattern
    }

    fn assert_mm_eq(mms1: &[MemoryManager], mms2: &[MemoryManager]) {
        assert_eq!(
            mms1.iter()
                .map(|mm| mm.read_slice::<u8>(0, mm.len()))
                .collect::<Vec<_>>(),
            mms2.iter()
                .map(|mm| mm.read_slice::<u8>(0, mm.len()))
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn test_q02() {
        let data_graph = create_data_graph_q02();
        let pattern_graph = create_pattern_graph_q02();
        let mut super_row_mms = vec![
            MemoryManager::Mem(vec![]),
            MemoryManager::Mem(vec![]),
            MemoryManager::Mem(vec![]),
        ];
        let mut index_mms = vec![MemoryManager::Mem(vec![]), MemoryManager::Mem(vec![])];
        let mut sr_mms = vec![
            MemoryManager::Mem(vec![]),
            MemoryManager::Mem(vec![]),
            MemoryManager::Mem(vec![]),
        ];
        let mut idx_mms = vec![MemoryManager::Mem(vec![]), MemoryManager::Mem(vec![])];
        match_characteristics(
            &data_graph,
            2,
            &[CharacteristicInfo::new(
                Characteristic::new(&pattern_graph, 2),
                0,
            )],
            &mut super_row_mms,
            &mut index_mms,
        );
        empty_super_row_mm(&mut sr_mms[0], 3, 1);
        add_super_row_and_index(
            &mut sr_mms[0],
            &mut idx_mms[0],
            &[1, 2, 1],
            &[&[2], &[1, 11], &[3]],
        );
        assert_mm_eq(&super_row_mms, &sr_mms);
        assert_mm_eq(&index_mms, &idx_mms);
        match_characteristics(
            &data_graph,
            3,
            &[CharacteristicInfo::new(
                Characteristic::new(&pattern_graph, 3),
                1,
            )],
            &mut super_row_mms,
            &mut index_mms,
        );
        empty_super_row_mm(&mut sr_mms[1], 3, 1);
        add_super_row_and_index(
            &mut sr_mms[1],
            &mut idx_mms[1],
            &[1, 1, 2],
            &[&[3], &[2], &[4, 14]],
        );
        assert_mm_eq(&super_row_mms, &sr_mms);
        assert_mm_eq(&index_mms, &idx_mms);
    }
}
