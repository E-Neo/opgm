use crate::{
    pattern_graph::{NeighborInfo, PatternGraph},
    types::{VId, VLabel, VertexConstraint},
};
use std::collections::BTreeMap;

/// The characteristic of a star graph.
///
/// Isomorphic stars have the same Characteristic.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Characteristic<'a, 'g> {
    root_vlabel: VLabel,
    root_constraint: Option<&'g VertexConstraint>,
    info_nums: BTreeMap<&'a NeighborInfo<'g>, usize>,
}

impl<'a, 'g> Characteristic<'a, 'g> {
    pub fn new(pattern_graph: &'a PatternGraph<'g>, root: VId) -> Self {
        let root_vlabel = pattern_graph.vlabel(root).unwrap();
        let root_constraint = pattern_graph.vertex_constraint(root).unwrap();
        let mut info_nums: BTreeMap<_, usize> = BTreeMap::new();
        for (_, info) in pattern_graph.neighbors(root).unwrap() {
            *info_nums.entry(info).or_default() += 1;
        }
        Self {
            root_vlabel,
            root_constraint,
            info_nums,
        }
    }

    pub fn root_vlabel(&self) -> VLabel {
        self.root_vlabel
    }

    pub fn root_constraint(&self) -> Option<&'g VertexConstraint> {
        self.root_constraint
    }

    pub fn info_nums(&self) -> &BTreeMap<&'a NeighborInfo<'g>, usize> {
        &self.info_nums
    }
}
