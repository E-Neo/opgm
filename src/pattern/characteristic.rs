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
    in_deg: usize,
    out_deg: usize,
    undirected_deg: usize,
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
        let (mut in_deg, mut out_deg, mut undirected_deg) = (0, 0, 0);
        for (_, info) in pattern_graph.neighbors(root).unwrap() {
            infos.insert(info.clone());
            in_deg += info.n_to_v_elabels().len();
            out_deg += info.v_to_n_elabels().len();
            undirected_deg += info.undirected_elabels().len();
        }
        Self {
            root_vlabel,
            root_constraint,
            infos,
            in_deg,
            out_deg,
            undirected_deg,
        }
    }

    pub fn root_vlabel(&self) -> VLabel {
        self.root_vlabel
    }

    pub fn root_constraint(&self) -> Option<&VertexConstraint> {
        self.root_constraint.as_ref()
    }

    pub fn in_deg(&self) -> usize {
        self.in_deg
    }

    pub fn out_deg(&self) -> usize {
        self.out_deg
    }

    pub fn undirected_deg(&self) -> usize {
        self.undirected_deg
    }

    pub fn infos(&self) -> &BTreeSet<NeighborInfo> {
        &self.infos
    }
}
