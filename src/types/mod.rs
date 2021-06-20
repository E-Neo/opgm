//! Various types related to graph matching.

pub use constraints::{EdgeConstraint, GlobalConstraint, VertexConstraint, VertexCoverConstraint};

mod constraints;

/// The vertex id type.
pub type VId = i64;

/// The vertex label type.
pub type VLabel = i64;

/// The edge label type.
pub type ELabel = i64;

pub(crate) struct VLabelPosLen {
    pub vlabel: VLabel,
    pub pos: usize,
    pub len: usize,
}

pub(crate) struct VertexHeader {
    pub num_bytes: usize,
    pub vid: VId,
    pub in_deg: usize,
    pub out_deg: usize,
    pub num_vlabels: usize,
}

pub(crate) struct NeighborHeader {
    pub nid: VId,
    pub num_n_to_v: usize,
    pub num_v_to_n: usize,
}

pub(crate) struct SuperRowHeader {
    pub num_rows: usize,
    pub num_eqvs: usize,
    pub num_vertices: usize,
}

pub(crate) struct PosLen {
    pub pos: usize,
    pub len: usize,
}

pub(crate) struct VIdPos {
    pub vid: VId,
    pub pos: usize,
}
