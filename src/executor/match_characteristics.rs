use crate::{
    data_graph::{DataGraph, DataVertex},
    memory_manager::MemoryManager,
    pattern_graph::NeighborInfo,
    planner::CharacteristicInfo,
    types::{SuperRowHeader, VIdPos, VLabel},
};
use std::collections::{BTreeMap, HashSet};
use std::mem::size_of;

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

fn initialize_results(
    infos: &[CharacteristicInfo],
    super_row_mms: &mut [MemoryManager],
    index_mms: &mut [MemoryManager],
    vertices_len: usize,
) {
    infos.iter().map(|info| info.id()).for_each(|id| unsafe {
        super_row_mms
            .get_unchecked_mut(id)
            .resize(size_of::<SuperRowHeader>());
        index_mms
            .get_unchecked_mut(id)
            .resize(vertices_len * size_of::<VIdPos>());
    });
}

/// Returns `(sr_pos, idx_pos)` for each `characteristic` in `characteristics`.
fn scan_data_vertices<'a, VS>(
    vertices: VS,
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
            infos,
            super_row_mms,
            index_mms,
            &mut sr_pos_idx_poses,
        );
    }
    sr_pos_idx_poses
}

fn match_data_vertex(
    vertex: &DataVertex,
    infos: &[CharacteristicInfo],
    super_row_mms: &mut [MemoryManager],
    index_mms: &mut [MemoryManager],
    sr_pos_idx_poses: &mut [(usize, usize)],
) {
}

fn finish_results(
    infos: &[CharacteristicInfo],
    super_row_mms: &mut [MemoryManager],
    index_mms: &mut [MemoryManager],
    sr_pos_idx_poses: &[(usize, usize)],
) {
    infos
        .iter()
        .map(|info| (info.id(), info.characteristic().infos().len() + 1))
        .for_each(|(id, num_eqvs)| {
            let (sr_pos, idx_pos) = sr_pos_idx_poses[id];
            write_super_row_header(
                &mut super_row_mms[id],
                idx_pos / size_of::<VIdPos>(),
                num_eqvs,
                1,
            );
            unsafe {
                super_row_mms.get_unchecked_mut(id).resize(sr_pos);
                index_mms.get_unchecked_mut(id).resize(idx_pos);
            }
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
