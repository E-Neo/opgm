//! Various types related to graph matching.

/// The vertex id type.
pub type VId = i64;

/// The vertex label type.
pub type VLabel = i32;

/// The edge label type.
pub type ELabel = i32;

#[repr(C, packed)]
pub(crate) struct VLabelPosLen {
    pub vlabel: VLabel,
    pub pos: usize,
    pub len: usize,
}

#[repr(C, packed)]
pub(crate) struct VertexHeader {
    pub num_bytes: usize,
    pub vid: VId,
    pub in_deg: usize,
    pub out_deg: usize,
    pub num_vlabels: usize,
}

#[repr(C, packed)]
pub(crate) struct NeighborHeader {
    pub nid: VId,
    pub num_n_to_v: usize,
    pub num_v_to_n: usize,
}

#[repr(C, packed)]
pub(crate) struct SuperRowHeader {
    pub num_rows: usize,
    pub num_eqvs: usize,
    pub num_cover: usize,
}

#[repr(C, packed)]
pub(crate) struct PosLen {
    pub pos: usize,
    pub len: usize,
}

#[repr(C, packed)]
pub(crate) struct VIdPos {
    pub vid: VId,
    pub pos: usize,
}

/// Vertex constraint type.
pub type VertexConstraint = dyn Fn(VId) -> bool;

/// Edge constraint type.
pub type EdgeConstraint = dyn Fn(VId, VId) -> bool;

/// Global constraint type.
pub type GlobalConstraint = dyn Fn(&[VId]) -> bool;
