use crate::{
    data_graph::{DataGraph, DataNeighbor, DataVertex},
    memory_manager::MemoryManager,
    pattern_graph::{Characteristic, NeighborInfo},
    planner::CharacteristicInfo,
    types::{ELabel, PosLen, SuperRowHeader, VId, VIdPos, VLabel},
};
use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap, HashSet};
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
    ids: &[usize],
    infos: &[CharacteristicInfo],
    super_row_mms: &mut [MemoryManager],
    index_mms: &mut [MemoryManager],
) {
    let (vertices_len, vertices) = data_graph.vertices(vlabel);
    initialize_results(ids, super_row_mms, index_mms, vertices_len);
    let sr_pos_idx_poses = scan_data_vertices(vertices, ids, infos, super_row_mms, index_mms);
    finish_results(ids, infos, super_row_mms, index_mms, &sr_pos_idx_poses);
}

/// Allocate space for `SuperRowHeader` and the indices.
///
/// For the `super_row_mm`, we only allocate space to store the header.
/// One have to allocate enough space to store the new `SuperRow` manually.
/// For the `index_mm`, we could obtain the upper bound
/// by [`vertices()`](../data_graph/struct.DataGraph.html#method.vertices),
/// so it could be allocate once for all.
fn initialize_results(
    ids: &[usize],
    super_row_mms: &mut [MemoryManager],
    index_mms: &mut [MemoryManager],
    vertices_len: usize,
) {
    ids.iter().for_each(|&id| unsafe {
        super_row_mms
            .get_unchecked_mut(id)
            .resize(size_of::<SuperRowHeader>());
        index_mms
            .get_unchecked_mut(id)
            .resize(vertices_len * size_of::<VIdPos>());
    });
}

/// Returns `(sr_pos, idx_pos)` for each `characteristic` in `characteristics`.
///
/// Please note that the result of this function has the same length as `infos`,
/// `super_row_mms` and `index_mms`.
/// That is to say, we use the vector as an ad-hoc hash table and the elements
/// not in `ids` don't have any meanings.
/// Though it is a little bit dirty, it works fine as an internal interface.
fn scan_data_vertices<'a, VS>(
    vertices: VS,
    ids: &[usize],
    infos: &[CharacteristicInfo],
    super_row_mms: &mut [MemoryManager],
    index_mms: &mut [MemoryManager],
) -> Vec<(usize, usize)>
where
    VS: IntoIterator<Item = DataVertex<'a>>,
{
    let mut sr_pos_idx_poses = vec![(size_of::<SuperRowHeader>(), 0); infos.len()];
    for vertex in vertices {
        match_data_vertex(
            &vertex,
            ids,
            infos,
            super_row_mms,
            index_mms,
            &mut sr_pos_idx_poses,
        );
    }
    sr_pos_idx_poses
}

/// Scan the `vertex` and its neighbors to match characteristics.
///
/// We first select characteristic candidates by early filters,
/// i.e., root's vertex constraint and neighbors' labels.
/// And then group the candidates by neighbors' label such that the neighbors could be scanned
/// sequentially only once.
fn match_data_vertex(
    vertex: &DataVertex,
    ids: &[usize],
    infos: &[CharacteristicInfo],
    super_row_mms: &mut [MemoryManager],
    index_mms: &mut [MemoryManager],
    sr_pos_idx_poses: &mut [(usize, usize)],
) {
    let id_sr_pos_eqv_poses = get_characteristic_candidates(
        vertex,
        ids,
        infos,
        super_row_mms,
        sr_pos_idx_poses,
        index_mms,
    );
    let nlabel_ids =
        group_characteristics_by_nlabel(id_sr_pos_eqv_poses.keys().map(|&id| id), infos);
    let (mut left_iter, mut right_iter) = (vertex.vlabels(), nlabel_ids.iter());
    let (mut left, mut right) = (left_iter.next(), right_iter.next());
    while let (Some(x), Some(y)) = (left, right) {
        match x.0.cmp(&y.0) {
            Ordering::Less => left = left_iter.next(),
            Ordering::Equal => {
                match_neighbors(
                    vertex,
                    x.0,
                    x.2,
                    y.1.as_slice(),
                    infos,
                    super_row_mms,
                    &id_sr_pos_eqv_poses,
                    sr_pos_idx_poses,
                );
                left = left_iter.next();
                right = right_iter.next();
            }
            Ordering::Greater => panic!("match_data_vertex"),
        }
    }
}

/// Scan the neighbors and write them to the SuperRow files if matched.
fn match_neighbors<'a, N>(
    vertex: &DataVertex,
    nlabel: VLabel,
    neighbors: N,
    ids: &[usize],
    infos: &[CharacteristicInfo],
    super_row_mms: &mut [MemoryManager],
    id_sr_pos_eqv_poses: &HashMap<usize, (usize, Vec<usize>)>,
    sr_pos_eqv_poses: &[(usize, usize)],
) where
    N: IntoIterator<Item = DataNeighbor<'a>>,
{
    let id_sr_pos_eqv_ninfo_poses =
        get_id_sr_pos_eqv_info_poses(nlabel, ids, infos, id_sr_pos_eqv_poses);
    let mut id_lens: Vec<Vec<usize>> = id_sr_pos_eqv_ninfo_poses
        .iter()
        .map(|(_, (_, ninfo_poses))| ninfo_poses.iter().map(|_| 0).collect())
        .collect();
    for neighbor in neighbors {
        for (i, (id, (_, eqv_ninfo_poses))) in id_sr_pos_eqv_ninfo_poses.iter().enumerate() {
            for (j, (_, ninfo, pos)) in eqv_ninfo_poses.iter().enumerate() {
                if check_neighbor_constraints(vertex, &neighbor, ninfo)
                    && check_neighbor_degrees(&neighbor, ninfo)
                    && check_neighbor_edges(&neighbor, ninfo)
                {
                    write_vid(
                        unsafe { super_row_mms.get_unchecked_mut(*id) },
                        pos + unsafe { id_lens.get_unchecked(i).get_unchecked(j) }
                            * size_of::<VId>(),
                        neighbor.id(),
                    );
                    *unsafe { id_lens.get_unchecked_mut(i).get_unchecked_mut(j) } += 1;
                }
            }
        }
    }
    for ((id, (sr_pos, eqv_ninfo_poses)), lens) in
        id_sr_pos_eqv_ninfo_poses.iter().zip(id_lens.iter())
    {
        for (&(eqv, _, pos), &len) in eqv_ninfo_poses.iter().zip(lens) {
            write_pos_len(
                unsafe { super_row_mms.get_unchecked_mut(*id) },
                *sr_pos,
                eqv,
                pos,
                len,
            );
        }
    }
}

/// Get necessary static information to match neighbors.
fn get_id_sr_pos_eqv_info_poses<'a, 'b>(
    nlabel: VLabel,
    ids: &[usize],
    infos: &[CharacteristicInfo<'a, 'b>],
    id_sr_pos_eqv_poses: &HashMap<usize, (usize, Vec<usize>)>,
) -> Vec<(usize, (usize, Vec<(usize, &'a NeighborInfo<'b>, usize)>))> {
    ids.iter()
        .map(|&id| {
            let (sr_pos, eqv_poses) = id_sr_pos_eqv_poses.get(&id).unwrap();
            (
                id,
                (
                    *sr_pos,
                    unsafe { infos.get_unchecked(id) }
                        .nlabel_ninfo_eqv()
                        .get(&nlabel)
                        .unwrap()
                        .iter()
                        .map(|&(ninfo, eqv)| (eqv, ninfo, *unsafe { eqv_poses.get_unchecked(eqv) }))
                        .collect(),
                ),
            )
        })
        .collect()
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

/// Gets characteristic candidates and allocates space.
///
/// Returns the candidate `id` together with original `sr_pos`
/// and the allocated starting position for each equivalence class in the `SuperRow`.
/// The index is also updated by this function.
fn get_characteristic_candidates(
    vertex: &DataVertex,
    ids: &[usize],
    infos: &[CharacteristicInfo],
    super_row_mms: &mut [MemoryManager],
    sr_pos_idx_poses: &mut [(usize, usize)],
    index_mms: &mut [MemoryManager],
) -> HashMap<usize, (usize, Vec<usize>)> {
    ids.iter()
        .filter_map(|&id| {
            unsafe { infos.get_unchecked(id) }
                .characteristic()
                .root_constraint()
                .map_or(
                    Some(id),
                    |vc| if vc.f()(vertex.id()) { Some(id) } else { None },
                )
        })
        .filter_map(|id| {
            unsafe {
                allocate(
                    super_row_mms.get_unchecked_mut(id),
                    sr_pos_idx_poses.get_unchecked(id).0,
                    vertex,
                    infos.get_unchecked(id),
                )
            }
            .map(|(new_sr_pos, eqv_poses)| {
                let sr_pos_idx_pos = unsafe { sr_pos_idx_poses.get_unchecked_mut(id) };
                let sr_pos = sr_pos_idx_pos.0;
                write_index(
                    unsafe { index_mms.get_unchecked_mut(id) },
                    sr_pos_idx_pos.1,
                    vertex.id(),
                    sr_pos_idx_pos.0,
                );
                sr_pos_idx_pos.0 = new_sr_pos;
                sr_pos_idx_pos.1 += size_of::<VIdPos>();
                (id, (sr_pos, eqv_poses))
            })
        })
        .collect()
}

/// Groups the characteristics by neighbors' label.
fn group_characteristics_by_nlabel<ID>(
    ids: ID,
    infos: &[CharacteristicInfo],
) -> BTreeMap<VLabel, Vec<usize>>
where
    ID: IntoIterator<Item = usize>,
{
    let mut nlabel_ids: BTreeMap<VLabel, Vec<usize>> = BTreeMap::new();
    ids.into_iter().for_each(|id| {
        unsafe { infos.get_unchecked(id) }
            .nlabel_ninfo_eqv()
            .keys()
            .for_each(|&nlabel| nlabel_ids.entry(nlabel).or_default().push(id))
    });
    nlabel_ids
}

/// Tries to allocate space for a SuperRow.
///
/// If the `vertex` could match the `Characteristic`,
/// it returns the new `sr_pos` and the starting position for each matching result set.
/// Otherwise, it returns `None`.
/// The matched root is also written by this function.
fn allocate(
    super_row_mm: &mut MemoryManager,
    sr_pos: usize,
    vertex: &DataVertex,
    info: &CharacteristicInfo,
) -> Option<(usize, Vec<usize>)> {
    let mut new_sr_pos = sr_pos
        + size_of::<usize>()
        + (1 + info.characteristic().infos().len()) * size_of::<PosLen>();
    let mut eqv_poses = Vec::with_capacity(1 + info.characteristic().infos().len());
    eqv_poses.push(new_sr_pos);
    new_sr_pos += size_of::<VId>();
    let mut left_iter = vertex.vlabels();
    let mut right_iter = info
        .characteristic()
        .infos()
        .iter()
        .map(|&info| info.vlabel());
    let (mut left, mut right) = (left_iter.next(), right_iter.next());
    while let (Some((x, len, _)), Some(y)) = (&left, right) {
        match x.cmp(&y) {
            Ordering::Less => {
                left = left_iter.next();
            }
            Ordering::Equal => {
                eqv_poses.push(new_sr_pos);
                new_sr_pos += len * size_of::<VId>();
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
        let root_pos = *unsafe { eqv_poses.get_unchecked(0) };
        super_row_mm.resize(new_sr_pos);
        write_num_bytes(super_row_mm, sr_pos, new_sr_pos - sr_pos);
        write_pos_len(super_row_mm, sr_pos, 0, root_pos, 1);
        write_vid(super_row_mm, root_pos, vertex.id());
        Some((new_sr_pos, eqv_poses))
    }
}

/// Update the `SuperRowHeader` and truncate `super_row_mm` and `index_mm` in `ids`.
fn finish_results(
    ids: &[usize],
    infos: &[CharacteristicInfo],
    super_row_mms: &mut [MemoryManager],
    index_mms: &mut [MemoryManager],
    sr_pos_idx_poses: &[(usize, usize)],
) {
    ids.iter().for_each(|&id| unsafe {
        let &(sr_pos, idx_pos) = sr_pos_idx_poses.get_unchecked(id);
        write_super_row_header(
            super_row_mms.get_unchecked_mut(id),
            idx_pos / size_of::<VIdPos>(),
            infos.get_unchecked(id).characteristic().infos().len() + 1,
            1,
        );
        super_row_mms.get_unchecked_mut(id).resize(sr_pos);
        index_mms.get_unchecked_mut(id).resize(idx_pos);
    });
}

fn write_super_row_header(
    super_row_mm: &mut MemoryManager,
    num_rows: usize,
    num_eqvs: usize,
    num_cover: usize,
) {
    super_row_mm.write(
        0,
        &SuperRowHeader {
            num_rows,
            num_eqvs,
            num_cover,
        } as *const SuperRowHeader,
        1,
    );
}

fn write_num_bytes(super_row_mm: &mut MemoryManager, sr_pos: usize, num_bytes: usize) {
    super_row_mm.write(sr_pos, &num_bytes as *const _, 1);
}

fn write_pos_len(
    super_row_mm: &mut MemoryManager,
    sr_pos: usize,
    eqv: usize,
    pos: usize,
    len: usize,
) {
    super_row_mm.write(
        sr_pos + size_of::<usize>() + eqv * size_of::<PosLen>(),
        &PosLen { pos, len } as *const _,
        1,
    );
}

fn write_vid(super_row_mm: &mut MemoryManager, pos: usize, vid: VId) {
    super_row_mm.write(pos, &vid as *const _, 1);
}

fn write_index(index_mm: &mut MemoryManager, idx_pos: usize, vid: VId, pos: usize) {
    index_mm.write(idx_pos, &VIdPos { vid, pos } as *const _, 1);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{data_graph::mm_read_iter, pattern_graph::PatternGraph};
    use std::collections::HashSet;

    fn create_super_row_mm(num_eqvs: usize, num_cover: usize) -> MemoryManager {
        let mut mm = MemoryManager::Mem(vec![]);
        mm.resize(size_of::<SuperRowHeader>());
        mm.write(
            0,
            &SuperRowHeader {
                num_rows: 0,
                num_eqvs,
                num_cover,
            } as *const _,
            1,
        );
        mm
    }

    fn add_super_row(
        super_row_mm: &mut MemoryManager,
        index_mm: &mut MemoryManager,
        bounds: &[usize],
        imgs: &[&[VId]],
    ) {
        let sr_header = super_row_mm.read::<SuperRowHeader>(0);
        let (num_rows, num_eqvs, num_cover) = unsafe {
            (
                (*sr_header).num_rows,
                (*sr_header).num_eqvs,
                (*sr_header).num_cover,
            )
        };
        let sr_pos = super_row_mm.len();
        let new_sr_pos = sr_pos
            + size_of::<usize>()
            + num_eqvs * size_of::<PosLen>()
            + bounds.iter().sum::<usize>() * size_of::<VId>();
        super_row_mm.resize(new_sr_pos);
        super_row_mm.write(sr_pos, &(new_sr_pos - sr_pos) as *const _, 1);
        let idx_pos = index_mm.len();
        index_mm.resize(idx_pos + size_of::<VIdPos>());
        let mut pos = sr_pos + size_of::<usize>() + num_eqvs * size_of::<PosLen>();
        index_mm.write(
            idx_pos,
            &VIdPos {
                vid: *imgs.get(0).unwrap().get(0).unwrap(),
                pos: sr_pos,
            },
            1,
        );
        for (eqv, (&bound, &img)) in bounds.iter().zip(imgs).enumerate() {
            super_row_mm.write(
                sr_pos + size_of::<usize>() + eqv * size_of::<PosLen>(),
                &PosLen {
                    pos,
                    len: img.len(),
                } as *const _,
                1,
            );
            super_row_mm.write(pos, img.as_ptr(), img.len());
            pos += bound * size_of::<VId>();
        }
        super_row_mm.write(
            0,
            &SuperRowHeader {
                num_rows: num_rows + 1,
                num_eqvs,
                num_cover,
            } as *const _,
            1,
        );
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
            CharacteristicInfo::new(Characteristic::new(&pattern_graph, 5), 2),
        ];
        let mut super_row_mms = vec![MemoryManager::Mem(vec![]), MemoryManager::Mem(vec![])];
        let mut index_mms = vec![MemoryManager::Mem(vec![]), MemoryManager::Mem(vec![])];
        match_characteristics(
            &data_graph,
            0,
            &[0, 1],
            infos.as_slice(),
            super_row_mms.as_mut_slice(),
            index_mms.as_mut_slice(),
        );
        let mut super_row_mm0 = create_super_row_mm(3, 1);
        let mut index_mm0 = MemoryManager::Mem(vec![]);
        add_super_row(
            &mut super_row_mm0,
            &mut index_mm0,
            &[1, 2, 1],
            &[&[1], &[2, 3], &[4]],
        );
        add_super_row(
            &mut super_row_mm0,
            &mut index_mm0,
            &[1, 2, 1],
            &[&[5], &[6, 7], &[8]],
        );
        let mut super_row_mm1 = create_super_row_mm(2, 1);
        let mut index_mm1 = MemoryManager::Mem(vec![]);
        add_super_row(
            &mut super_row_mm1,
            &mut index_mm1,
            &[1, 2],
            &[&[1], &[2, 3]],
        );
        add_super_row(
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
}
