use crate::{
    pattern_graph::{NeighborInfo, PatternGraph},
    types::{VId, VLabel, VertexConstraint},
};
use std::collections::BTreeSet;

/// The characteristic of a star graph.
///
/// Isomorphic stars have the same Characteristic.
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Characteristic<'a, 'g> {
    root_vlabel: VLabel,
    root_constraint: Option<&'g VertexConstraint>,
    infos: BTreeSet<&'a NeighborInfo<'g>>,
}

impl<'a, 'g> Characteristic<'a, 'g> {
    pub fn new(pattern_graph: &'a PatternGraph<'g>, root: VId) -> Self {
        let root_vlabel = pattern_graph.vlabel(root).unwrap();
        let root_constraint = pattern_graph.vertex_constraint(root).unwrap();
        let infos: BTreeSet<_> = pattern_graph
            .neighbors(root)
            .unwrap()
            .iter()
            .map(|(_, info)| info)
            .collect();
        Self {
            root_vlabel,
            root_constraint,
            infos,
        }
    }

    pub fn root_vlabel(&self) -> VLabel {
        self.root_vlabel
    }

    pub fn root_constraint(&self) -> Option<&'g VertexConstraint> {
        self.root_constraint
    }

    pub fn infos(&self) -> &BTreeSet<&'a NeighborInfo<'g>> {
        &self.infos
    }
}
