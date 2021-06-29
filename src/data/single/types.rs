use crate::types::{ELabel, VId, VLabel};

pub struct IndexEntry {
    pub vlabel: VLabel,
    pub pos: u64,
    pub len: u32,
}

pub struct VertexHeader {
    pub num_bytes: u64,
    pub vid: VId,
    pub in_deg: u32,
    pub out_deg: u32,
    pub num_vlabels: u16,
}

pub struct NeighborEntry {
    pub nid: VId,
    pub n_to_v_elabel: ELabel,
    pub v_to_n_elabel: ELabel,
}
