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
