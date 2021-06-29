use crate::types::VId;

pub struct SuperRowHeader {
    pub num_rows: u32,
    pub num_eqvs: u32,
    pub num_vertices: u32,
}

pub struct PosLen {
    pub pos: usize,
    pub len: usize,
}

pub struct VIdPos {
    pub vid: VId,
    pub pos: usize,
}
