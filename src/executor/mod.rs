//! The executor.

pub use decompress::{decompress, write_results};
pub use join::join;
pub use match_characteristics::match_characteristics;
pub use super_row::{
    add_super_row, add_super_row_and_index, empty_super_row_mm, SuperRow, SuperRowIter, SuperRows,
};
pub(crate) use super_row::{
    read_super_row_header, write_index, write_num_bytes, write_pos_len, write_super_row_header,
    write_vid,
};

mod decompress;
mod join;
mod match_characteristics;
mod super_row;
