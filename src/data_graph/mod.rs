//! The data graph.

pub use data_graph::{
    DataGraph, DataGraphInfo, DataNeighbor, DataNeighborIter, DataVLabelNeighborIter, DataVertex,
    DataVertexIter,
};
pub use mm_read_iter::mm_read_iter;
pub use mm_read_sqlite3::mm_read_sqlite3;
pub use view::{DataGraphView, NeighborView, VertexView};

pub(crate) use display::display;

mod data_graph;
mod display;
mod mm_read_iter;
mod mm_read_sqlite3;
mod view;
