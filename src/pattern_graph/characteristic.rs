use crate::{
    pattern_graph::{NeighborInfo, PatternGraph},
    types::{VId, VLabel, VertexConstraint},
};
use std::collections::HashSet;
use std::hash::{Hash, Hasher};

/// The characteristic of a star graph.
///
/// Isomorphic stars have the same Characteristic.
pub struct Characteristic<'a, 'g> {
    hash: u64,
    root_vlabel: VLabel,
    root_constraint: Option<&'g VertexConstraint>,
    infos: HashSet<&'a NeighborInfo<'g>>,
}

impl<'a, 'g> Characteristic<'a, 'g> {
    pub fn new(pattern_graph: &'a PatternGraph<'g>, root: VId) -> Self {
        let root_vlabel = pattern_graph.vlabel(root).unwrap();
        let root_constraint = pattern_graph.vertex_constraint(root).unwrap();
        let infos: HashSet<_> = pattern_graph
            .neighbors(root)
            .unwrap()
            .iter()
            .map(|(_, info)| info)
            .collect();
        Self {
            hash: ((root_vlabel as u64) << 32) + (infos.len() as u64),
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

    pub fn infos(&self) -> &HashSet<&'a NeighborInfo<'g>> {
        &self.infos
    }
}

// // private methods
// impl<'a, 'g> Characteristic<'a, 'g> {
//     fn new(
//         root_vlabel: VLabel,
//         root_constraint: Option<&'g VertexConstraint>,
//         infos: HashSet<&'a NeighborInfo<'g>>,
//     ) -> Self {
//         Self {
//             hash: ((root_vlabel as u64) << 32) + (infos.len() as u64),
//             root_vlabel,
//             root_constraint,
//             infos,
//         }
//     }
// }

impl<'a, 'g> std::fmt::Debug for Characteristic<'a, 'g> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Characteristic {{...}}")
    }
}

impl<'a, 'g> PartialEq for Characteristic<'a, 'g> {
    fn eq(&self, other: &Self) -> bool {
        self.root_vlabel == other.root_vlabel
            && match (self.root_constraint, other.root_constraint) {
                (Some(f), Some(g)) => std::ptr::eq(f, g),
                (None, None) => true,
                _ => false,
            }
            && self.infos == other.infos
    }
}

impl<'a, 'g> Eq for Characteristic<'a, 'g> {}

impl<'a, 'g> Hash for Characteristic<'a, 'g> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.hash.hash(state)
    }
}
