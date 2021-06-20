use crate::types::{ELabel, VId, VLabel};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct DataGraphView {
    index: BTreeMap<VLabel, BTreeSet<VertexView>>,
}

impl DataGraphView {
    pub fn new<I, V>(index: I) -> Self
    where
        I: IntoIterator<Item = (VLabel, V)>,
        V: IntoIterator<Item = VertexView>,
    {
        Self {
            index: index
                .into_iter()
                .map(|(l, vs)| (l, vs.into_iter().collect()))
                .collect(),
        }
    }
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct VertexView {
    id: VId,
    index: BTreeMap<VLabel, BTreeSet<NeighborView>>,
}

impl VertexView {
    pub fn new<V, E>(id: VId, index: V) -> Self
    where
        V: IntoIterator<Item = (VLabel, E)>,
        E: IntoIterator<Item = NeighborView>,
    {
        Self {
            id,
            index: index
                .into_iter()
                .map(|(l, ns)| (l, ns.into_iter().collect()))
                .collect(),
        }
    }
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct NeighborView {
    id: VId,
    n_to_v_elabels: BTreeSet<ELabel>,
    v_to_n_elabels: BTreeSet<ELabel>,
}

impl NeighborView {
    pub fn new<I, O>(id: VId, n_to_v_elabels: I, v_to_n_elabels: O) -> Self
    where
        I: IntoIterator<Item = ELabel>,
        O: IntoIterator<Item = ELabel>,
    {
        Self {
            id,
            n_to_v_elabels: n_to_v_elabels.into_iter().collect(),
            v_to_n_elabels: v_to_n_elabels.into_iter().collect(),
        }
    }
}
