use derive_more::Display;

#[derive(Debug, Display)]
#[display(fmt = "{} {} {} {}", num_vertices, num_edges, num_vlabels, num_elabels)]
pub struct GraphInfo {
    num_vertices: usize,
    num_edges: usize,
    num_vlabels: usize,
    num_elabels: usize,
}

impl GraphInfo {
    pub fn new(
        num_vertices: usize,
        num_edges: usize,
        num_vlabels: usize,
        num_elabels: usize,
    ) -> Self {
        Self {
            num_vertices,
            num_edges,
            num_vlabels,
            num_elabels,
        }
    }
}
