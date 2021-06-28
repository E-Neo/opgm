//! Various types related to graph matching.

pub use constraints::{EdgeConstraint, GlobalConstraint, VertexConstraint, VertexCoverConstraint};

mod constraints;

/// The vertex id type.
pub type VId = i32;

/// The vertex label type.
pub type VLabel = i16;

/// The edge label type.
pub type ELabel = i16;

pub(crate) struct SuperRowHeader {
    pub num_rows: u32,
    pub num_eqvs: u32,
    pub num_vertices: u32,
}

pub(crate) struct PosLen {
    pub pos: usize,
    pub len: usize,
}

pub(crate) struct VIdPos {
    pub vid: VId,
    pub pos: usize,
}
