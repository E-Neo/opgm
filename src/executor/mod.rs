//! The executor.

pub use count::{count_rows, count_rows_slow};
pub use decompress::{decompress, write_results};
pub use enumerate::enumerate;
pub use join::{
    create_indices, join, Index, Intersection, JoinedSuperRow, JoinedSuperRowIter, JoinedSuperRows,
    OneJoin,
};
pub use match_characteristics::match_characteristics;
pub use super_row::{
    add_super_row, add_super_row_and_index, add_super_row_and_index_compact, empty_super_row_mm,
    SuperRow, SuperRowIter, SuperRows, SuperRowsInfo,
};
pub use view::{SuperRowIndexView, SuperRowsView};

pub(crate) use super_row::{
    read_super_row_header, write_index, write_num_bytes, write_pos_len, write_super_row_header,
    write_vid,
};

mod count;
mod decompress;
mod enumerate;
mod join;
mod match_characteristics;
mod super_row;
mod view;

use crate::types::VId;

pub(crate) fn count_rows_slow_helper(mappings: &[&[VId]]) -> usize {
    let mut num_rows = 0;
    let mut offsets = vec![0; mappings.len()];
    let mut row = Vec::with_capacity(mappings.len());
    loop {
        let col = row.len();
        if col == mappings.len() {
            num_rows += 1;
            row.pop();
        } else if offsets[col] < mappings[col].len() {
            row.push(mappings[col][offsets[col]]);
            offsets[col] += 1;
        } else {
            if row.pop().is_none() {
                break;
            }
            offsets[col] = 0;
        }
    }
    num_rows
}
