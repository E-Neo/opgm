use crate::types::{VId, VLabel};

pub struct VLabelPosLen {
    pub vlabel: VLabel,
    pub pos: usize,
    pub len: u32,
}

pub struct VertexHeader {
    pub num_bytes: usize,
    pub vid: VId,
    pub in_deg: u32,
    pub out_deg: u32,
    pub num_vlabels: u16,
}

pub struct NeighborHeader {
    pub nid: VId,
    pub num_n_to_v: u16,
    pub num_v_to_n: u16,
}
