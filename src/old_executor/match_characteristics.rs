use crate::{
    data_graph::{DataGraph, DataNeighbor, DataVertex},
    memory_manager::MemoryManager,
    old_executor::{
        write_index, write_num_bytes, write_pos_len, write_super_row_header, write_vid,
    },
    old_planner::old_types::CharacteristicInfo,
    pattern_graph::NeighborInfo,
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
    let mut num_vertices = vec![0; index_mms.len()];
    let sr_pos_idx_poses =
        scan_data_vertices(vertices, infos, super_row_mms, index_mms, &mut num_vertices);
    finish_results(
        infos,
        super_row_mms,
        index_mms,
        &sr_pos_idx_poses,
        &num_vertices,
    );
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
    num_vertices: &mut [usize],
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
            num_vertices,
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
    num_vertices: &mut [usize],
) {
    infos.iter().for_each(|info| {
        num_vertices[info.id()] += match_data_vertex_for_one_info(
            vertex,
            info,
            &mut super_row_mms[info.id()],
            &mut index_mms[info.id()],
            &mut sr_pos_idx_poses[info.id()],
        );
    });
}

/// Returns number of neighbors wrote to file.
fn match_data_vertex_for_one_info(
    vertex: &DataVertex,
    info: &CharacteristicInfo,
    super_row_mm: &mut MemoryManager,
    index_mm: &mut MemoryManager,
    sr_pos_idx_pos: &mut (usize, usize),
) -> usize {
    if let Some(vc) = info.characteristic().root_constraint() {
        if !vc.f()(vertex.id()) {
            return 0;
        }
    }
    let mut num_vids = 0;
    let &mut (sr_pos, idx_pos) = sr_pos_idx_pos;
    if let Some(mut pos_lens) = allocate(super_row_mm, sr_pos, vertex, info) {
        let (mut left_iter, mut right_iter) = (vertex.vlabels(), info.nlabel_ninfo_eqvs().iter());
        let (mut left, mut right) = (left_iter.next(), right_iter.next());
        while let (Some((x, _, neighbors)), Some((y, ninfo_eqvs))) = (left, right) {
            match x.cmp(y) {
                Ordering::Less => {
                    left = left_iter.next();
                }
                Ordering::Equal => {
                    let num_wrote =
                        match_neighbors(super_row_mm, &mut pos_lens, vertex, neighbors, ninfo_eqvs);
                    if num_wrote == 0 {
                        return 0;
                    }
                    num_vids += num_wrote;
                    left = left_iter.next();
                    right = right_iter.next();
                }
                Ordering::Greater => {
                    break;
                }
            }
        }
        let &(pos, len) = pos_lens.last().unwrap();
        let pos = pos + len * size_of::<VId>();
        write_num_bytes(super_row_mm, sr_pos, pos - sr_pos);
        for (eqv, &(pos, len)) in pos_lens.iter().enumerate() {
            write_pos_len(super_row_mm, sr_pos, eqv, pos, len);
        }
        write_vid(super_row_mm, pos_lens[0].0, vertex.id());
        write_index(index_mm, idx_pos, vertex.id(), sr_pos);
        *sr_pos_idx_pos = (pos, idx_pos + size_of::<VIdPos>());
    }
    num_vids + 1
}

fn match_neighbors<'a, N: IntoIterator<Item = DataNeighbor<'a>>>(
    super_row_mm: &mut MemoryManager,
    pos_lens: &mut [(usize, usize)],
    vertex: &DataVertex,
    neighbors: N,
    ninfo_eqvs: &[(&NeighborInfo, usize)],
) -> usize {
    for neighbor in neighbors {
        for &(ninfo, eqv) in ninfo_eqvs {
            if check_neighbor_constraints(vertex, &neighbor, ninfo)
                && check_neighbor_degrees(&neighbor, ninfo)
                && check_neighbor_edges(&neighbor, ninfo)
            {
                let pos_len = &mut pos_lens[eqv];
                let &mut (pos, len) = pos_len;
                let pos = pos + len * size_of::<VId>();
                write_vid(super_row_mm, pos, neighbor.id());
                pos_len.1 += 1;
            }
        }
    }
    let mut num_vids = 0;
    for &(_, eqv) in ninfo_eqvs {
        let (_, len) = pos_lens[eqv];
        if len == 0 {
            return 0;
        } else {
            num_vids += len;
        }
    }
    num_vids
}

/// Returns PosLen for each eqv if the vertex may match.
fn allocate(
    super_row_mm: &mut MemoryManager,
    sr_pos: usize,
    vertex: &DataVertex,
    info: &CharacteristicInfo,
) -> Option<Vec<(usize, usize)>> {
    let mut pos = sr_pos
        + size_of::<usize>()
        + (1 + info.characteristic().infos().len()) * size_of::<PosLen>();
    let mut pos_lens = vec![(0, 0); info.characteristic().infos().len() + 1];
    pos_lens[0] = (pos, 1);
    pos += size_of::<VId>();
    let mut left_iter = vertex.vlabels();
    let mut right_iter = info.nlabel_ninfo_eqvs().iter();
    let (mut left, mut right) = (left_iter.next(), right_iter.next());
    while let (Some((x, xlen, _)), Some((y, ninfo_eqvs))) = (left, right) {
        match x.cmp(y) {
            Ordering::Less => {
                left = left_iter.next();
            }
            Ordering::Equal => {
                for &(_, eqv) in ninfo_eqvs {
                    pos_lens[eqv].0 = pos;
                    pos += xlen * size_of::<VId>();
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
        None
    } else {
        super_row_mm.resize(pos);
        Some(pos_lens)
    }
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

/// Update the `SuperRowHeader` and truncate `super_row_mm` and `index_mm` in `ids`.
fn finish_results(
    infos: &[CharacteristicInfo],
    super_row_mms: &mut [MemoryManager],
    index_mms: &mut [MemoryManager],
    sr_pos_idx_poses: &[(usize, usize)],
    num_vertices: &[usize],
) {
    infos.iter().for_each(|info| {
        let (sr_pos, idx_pos) = sr_pos_idx_poses[info.id()];
        write_super_row_header(
            &mut super_row_mms[info.id()],
            idx_pos / size_of::<VIdPos>(),
            info.characteristic().infos().len() + 1,
            num_vertices[info.id()],
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
        executor::{add_super_row_and_index, empty_super_row_mm, SuperRowIndexView, SuperRowsView},
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
        let mut super_row_mm0 = create_super_row_mm(3, 8);
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
        let mut super_row_mm1 = create_super_row_mm(2, 6);
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
                .map(|mm| SuperRowsView::new(mm))
                .collect::<Vec<_>>(),
            vec![super_row_mm0, super_row_mm1]
                .iter()
                .map(|mm| SuperRowsView::new(mm))
                .collect::<Vec<_>>()
        );
        assert_eq!(
            index_mms
                .iter()
                .map(|mm| SuperRowIndexView::new(mm))
                .collect::<Vec<_>>(),
            vec![index_mm0, index_mm1]
                .iter()
                .map(|mm| SuperRowIndexView::new(mm))
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
        empty_super_row_mm(&mut sr_mms[0], 3, 4);
        add_super_row_and_index(
            &mut sr_mms[0],
            &mut idx_mms[0],
            &[1, 2, 1],
            &[&[2], &[1, 11], &[3]],
        );
        assert_eq!(
            super_row_mms
                .iter()
                .map(|mm| SuperRowsView::new(mm))
                .collect::<Vec<_>>(),
            sr_mms
                .iter()
                .map(|mm| SuperRowsView::new(mm))
                .collect::<Vec<_>>()
        );
        assert_eq!(
            index_mms
                .iter()
                .map(|mm| SuperRowIndexView::new(mm))
                .collect::<Vec<_>>(),
            idx_mms
                .iter()
                .map(|mm| SuperRowIndexView::new(mm))
                .collect::<Vec<_>>()
        );
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
        empty_super_row_mm(&mut sr_mms[1], 3, 4);
        add_super_row_and_index(
            &mut sr_mms[1],
            &mut idx_mms[1],
            &[1, 1, 2],
            &[&[3], &[2], &[4, 14]],
        );
        assert_eq!(
            super_row_mms
                .iter()
                .map(|mm| SuperRowsView::new(mm))
                .collect::<Vec<_>>(),
            sr_mms
                .iter()
                .map(|mm| SuperRowsView::new(mm))
                .collect::<Vec<_>>()
        );
        assert_eq!(
            index_mms
                .iter()
                .map(|mm| SuperRowIndexView::new(mm))
                .collect::<Vec<_>>(),
            idx_mms
                .iter()
                .map(|mm| SuperRowIndexView::new(mm))
                .collect::<Vec<_>>()
        );
    }
}
