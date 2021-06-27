//! The data graph.

pub use data_graph::{
    DataGraph, DataGraphInfo, DataNeighbor, DataNeighborIter, DataVLabelNeighborIter, DataVertex,
    DataVertexIter,
};
pub use info::GraphInfo;
pub use mm_read_iter::mm_read_iter;
pub use view::{GraphView, NeighborView, VertexView};

pub(crate) use display::display;

use crate::{
    pattern::NeighborInfo,
    types::{VId, VLabel},
};

pub mod multiple;

mod data_graph;
mod display;
mod info;
mod mm_read_iter;
mod view;

pub trait Graph<'a, I> {
    fn index(&'a self) -> I;

    fn info(&self) -> GraphInfo;

    fn view(&self) -> GraphView;
}

pub trait Index<I>: IntoIterator<Item = (VLabel, I)> {
    fn len(&self) -> usize;

    fn get(&self, label: VLabel) -> I;
}

pub trait VertexIter<V>: ExactSizeIterator<Item = V> {}

pub trait NeighborIter<N>: ExactSizeIterator<Item = N> {}

pub trait Vertex<I> {
    fn id(&self) -> VId;

    fn in_deg(&self) -> usize;

    fn out_deg(&self) -> usize;

    fn index(&self) -> I;
}

pub trait Neighbor {
    fn id(&self) -> VId;

    fn topology_will_match(&self, info: &NeighborInfo) -> bool;
}
