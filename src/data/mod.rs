//! The data graph.

pub use info::GraphInfo;
pub use view::GraphView;

use crate::{
    pattern::NeighborInfo,
    types::{VId, VLabel},
};

pub mod csr;
pub mod info_edges;
pub mod multiple;
pub mod single;
pub mod types;

mod info;
mod view;

pub trait Graph<I> {
    fn index(&self) -> I;

    fn count(&self, label: VLabel) -> usize;

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
