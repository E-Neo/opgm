use crate::{
    front_end::types::VertexConstraint,
    pattern::{NeighborInfo, PatternGraph},
    types::{VId, VLabel},
};
use std::collections::BTreeSet;

/// The characteristic of a star graph.
///
/// Isomorphic stars have the same Characteristic.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Characteristic {
    root_vlabel: VLabel,
    root_constraint: Option<VertexConstraint>,
    infos: BTreeSet<NeighborInfo>,
}

impl Characteristic {
    pub fn new(pattern_graph: &PatternGraph, root: VId) -> Self {
        let root_vlabel = pattern_graph.vlabel(root).unwrap();
        let root_constraint = pattern_graph
            .vertex_constraint(root)
            .unwrap()
            .map(|x| x.clone());
        let mut infos: BTreeSet<_> = BTreeSet::new();
        for (_, info) in pattern_graph.neighbors(root).unwrap() {
            infos.insert(info.clone());
        }
        Self {
            root_vlabel,
            root_constraint,
            infos,
        }
    }

    pub fn root_vlabel(&self) -> VLabel {
        self.root_vlabel
    }

    pub fn root_constraint(&self) -> Option<&VertexConstraint> {
        self.root_constraint.as_ref()
    }

    pub fn infos(&self) -> &BTreeSet<NeighborInfo> {
        &self.infos
    }
}
