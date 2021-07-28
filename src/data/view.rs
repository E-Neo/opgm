use crate::types::{ELabel, VId, VLabel};

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct GraphView {
    vertices: Vec<(VId, VLabel)>,
    edges: Vec<(VId, VId, ELabel)>,
}

impl GraphView {
    pub fn new(vertices: Vec<(VId, VLabel)>, edges: Vec<(VId, VId, ELabel)>) -> Self {
        Self { vertices, edges }
    }
}
