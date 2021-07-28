use crate::types::{ELabel, VId, VLabel};

pub struct Header {
    pub num_vertices: u32,
    pub num_edges: u64,
    pub num_vlabels: u16,
    pub num_elabels: u16,
    pub vids_pos: u64,
    pub vertices_pos: u64,
    pub edges_pos: u64,
}

pub struct IndexEntry {
    pub vlabel: VLabel,
    pub offset: u64,
    pub len: u32,
}

pub struct VertexHeader {
    pub in_deg: u32,
    pub out_deg: u32,
    pub index_offset: u64,
    pub num_vlabels: u16,
}

pub struct ArcEntry {
    pub nid: VId,
    pub elabel: ELabel,
}
