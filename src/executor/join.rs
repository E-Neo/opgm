use crate::{
    executor::{read_super_row_header, write_super_row_header, SuperRow, SuperRows},
    memory_manager::MemoryManager,
    planner::JoinInfo,
    types::{SuperRowHeader, VId, VIdPos},
};
use std::mem::size_of;

/// Join two `SuperRow` files.
///
/// We adopt a simple index-based nested-loop join to join two `SuperRow` files,
/// Since the data are compressed, the test equality process in the conventional join process is
/// replaced by the set intersection operation.
/// And we could intersect two sets sequentially for free, since the elements are always sorted
/// in a `SuperRow` as a consequence of our well-designed `DataGraph` format.
/// The join process is always boosted by the index because the image of a vertex cover always
/// contains only one item, which could then be used as the key to run the binary search.
pub fn join(
    super_row_mm: &mut MemoryManager,
    left_super_row_mm: &MemoryManager,
    right_super_row_mm: &MemoryManager,
    index_mm: &MemoryManager,
    num_eqvs: usize,
    num_cover: usize,
    indexed_intersection: usize,
    sequential_intersection: &[(usize, usize)],
    left_keep: &[usize],
    right_keep: &[usize],
) {
    let (_, right_num_eqvs, _) = read_super_row_header(right_super_row_mm);
    let index = index_mm.read_slice::<VIdPos>(0, index_mm.len() / size_of::<VIdPos>());
    initialize_result(super_row_mm);
    let mut pos_lens = vec![(0, 0); num_eqvs];
    let num_rows = 0;
    for left_sr in SuperRows::new(left_super_row_mm) {
        for &vid in left_sr.images()[indexed_intersection] {
            if let Some(right_sr_pos) = index_get_pos(index, vid) {
                let right_sr = SuperRow::new(right_super_row_mm, right_sr_pos, right_num_eqvs);
                todo!()
            }
        }
    }
    finish_result(super_row_mm, num_rows, num_eqvs, num_cover);
}

fn initialize_result(super_row_mm: &mut MemoryManager) {
    super_row_mm.resize(size_of::<SuperRowHeader>());
}

fn finish_result(
    super_row_mm: &mut MemoryManager,
    num_rows: usize,
    num_eqvs: usize,
    num_cover: usize,
) {
    write_super_row_header(super_row_mm, num_rows, num_eqvs, num_cover);
}

fn index_get_pos(index: &[VIdPos], root: VId) -> Option<usize> {
    index
        .binary_search_by_key(&root, |vid_pos| vid_pos.vid)
        .ok()
}
