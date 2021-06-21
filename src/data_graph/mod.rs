//! The data graph.

pub use data_graph::{
    DataGraph, DataGraphInfo, DataNeighbor, DataNeighborIter, DataVLabelNeighborIter, DataVertex,
    DataVertexIter,
};
pub use mm_read_iter::mm_read_iter;
pub use mm_read_sqlite3::mm_read_sqlite3;
pub use view::{DataGraphView, NeighborView, VertexView};

pub(crate) use display::display;

use crate::types::{VId, VLabel};

pub mod multiple;

mod data_graph;
mod display;
mod mm_read_iter;
mod mm_read_sqlite3;
mod view;

pub trait Graph<'a, I> {
    fn index(&'a self) -> I;

    // fn info(&self) -> DataGraphInfo;

    // fn view(&self) -> DataGraphView;
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

pub trait Neighbor<C> {
    fn id(&self) -> VId;

    fn connection(&self) -> C;
}

pub trait Connection {
    fn will_match(&self, other: &Self) -> bool;
}
