use crate::types::{ELabel, VId, VLabel};

#[derive(Debug)]
pub struct InfoEdgeHeader {
    pub num_vertices: u32,
    pub num_edges: u64,
    pub num_vlabels: u16,
    pub num_elabels: u16,
    pub vid_max: VId,
}

pub type InfoEdge = (VLabel, VId, VLabel, VId, bool, ELabel);
