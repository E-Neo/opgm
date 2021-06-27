use crate::{
    front_end::types::{EdgeConstraint, VertexConstraint},
    types::{ELabel, VId, VLabel},
};
use std::collections::{BTreeSet, HashMap};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct NeighborInfo {
    vlabel: VLabel,
    n_to_v_arcs: BTreeSet<ELabel>,
    v_to_n_arcs: BTreeSet<ELabel>,
    undirected_edges: BTreeSet<ELabel>,
    neighbor_constraint: Option<VertexConstraint>,
    edge_constraint: Option<EdgeConstraint>,
}

impl NeighborInfo {
    pub fn vlabel(&self) -> VLabel {
        self.vlabel
    }

    pub fn n_to_v_elabels(&self) -> &BTreeSet<ELabel> {
        &self.n_to_v_arcs
    }

    pub fn v_to_n_elabels(&self) -> &BTreeSet<ELabel> {
        &self.v_to_n_arcs
    }

    pub fn undirected_elabels(&self) -> &BTreeSet<ELabel> {
        &self.undirected_edges
    }

    pub fn neighbor_constraint(&self) -> Option<&VertexConstraint> {
        self.neighbor_constraint.as_ref()
    }

    pub fn edge_constraint(&self) -> Option<&EdgeConstraint> {
        self.edge_constraint.as_ref()
    }
}

// private methods
impl NeighborInfo {
    fn new(vlabel: VLabel) -> Self {
        Self {
            vlabel,
            n_to_v_arcs: BTreeSet::new(),
            v_to_n_arcs: BTreeSet::new(),
            undirected_edges: BTreeSet::new(),
            neighbor_constraint: None,
            edge_constraint: None,
        }
    }

    fn add_predecessor(&mut self, elabel: ELabel) {
        self.n_to_v_arcs.insert(elabel);
    }

    fn add_successor(&mut self, elabel: ELabel) {
        self.v_to_n_arcs.insert(elabel);
    }

    fn add_neighbor(&mut self, elabel: ELabel) {
        self.undirected_edges.insert(elabel);
    }

    fn set_neighbor_constraint(&mut self, constraint: Option<VertexConstraint>) {
        self.neighbor_constraint = constraint;
    }

    fn set_edge_constraint(&mut self, constraint: Option<EdgeConstraint>) {
        self.edge_constraint = constraint;
    }
}

struct GraphNode {
    vlabel: VLabel,
    in_deg: usize,
    out_deg: usize,
    undirected_deg: usize,
    neighbors: HashMap<VId, NeighborInfo>,
    constraint: Option<VertexConstraint>,
}

impl GraphNode {
    fn new(vlabel: VLabel) -> GraphNode {
        GraphNode {
            vlabel,
            in_deg: 0,
            out_deg: 0,
            undirected_deg: 0,
            neighbors: HashMap::new(),
            constraint: None,
        }
    }

    fn add_predecessor(&mut self, u1: VId, vlabel: VLabel, elabel: ELabel) {
        self.neighbors
            .entry(u1)
            .or_insert(NeighborInfo::new(vlabel))
            .add_predecessor(elabel);
        self.in_deg += 1;
    }

    fn add_successor(&mut self, u2: VId, vlabel: VLabel, elabel: ELabel) {
        self.neighbors
            .entry(u2)
            .or_insert(NeighborInfo::new(vlabel))
            .add_successor(elabel);
        self.out_deg += 1;
    }

    fn add_neighbor(&mut self, n: VId, vlabel: VLabel, elabel: ELabel) {
        self.neighbors
            .entry(n)
            .or_insert(NeighborInfo::new(vlabel))
            .add_neighbor(elabel);
        self.undirected_deg += 1;
    }

    fn remove_neighbor(&mut self, n: VId) {
        let info = self.neighbors.remove(&n).unwrap();
        self.in_deg -= info.n_to_v_elabels().len();
        self.out_deg -= info.v_to_n_elabels().len();
        self.undirected_deg -= info.undirected_elabels().len();
    }
}

pub struct PatternGraph {
    vertices: HashMap<VId, GraphNode>,
}

impl PatternGraph {
    /// Create a new empty pattern graph.
    pub fn new() -> Self {
        Self {
            vertices: HashMap::new(),
        }
    }

    pub fn add_vertex(&mut self, vid: VId, vlabel: VLabel) {
        self.vertices
            .entry(vid)
            .and_modify(|v| v.vlabel = vlabel)
            .or_insert(GraphNode::new(vlabel));
    }

    pub fn add_arc(&mut self, u1: VId, u2: VId, elabel: ELabel) -> bool {
        if let (Some(u1_node), Some(u2_node)) = (self.vertices.get(&u1), self.vertices.get(&u2)) {
            let (u1_vlabel, u2_vlabel) = (u1_node.vlabel, u2_node.vlabel);
            self.vertices
                .get_mut(&u1)
                .unwrap()
                .add_successor(u2, u2_vlabel, elabel);
            self.vertices
                .get_mut(&u2)
                .unwrap()
                .add_predecessor(u1, u1_vlabel, elabel);
            self.update_vertex_constraint(
                u1,
                self.vertex_constraint(u1).unwrap().map(|x| x.clone()),
            );
            self.update_vertex_constraint(
                u2,
                self.vertex_constraint(u2).unwrap().map(|x| x.clone()),
            );
            true
        } else {
            false
        }
    }

    pub fn add_edge(&mut self, u1: VId, u2: VId, elabel: ELabel) -> bool {
        if let (Some(u1_node), Some(u2_node)) = (self.vertices.get(&u1), self.vertices.get(&u2)) {
            let (u1_vlabel, u2_vlabel) = (u1_node.vlabel, u2_node.vlabel);
            self.vertices
                .get_mut(&u1)
                .unwrap()
                .add_neighbor(u2, u2_vlabel, elabel);
            self.vertices
                .get_mut(&u2)
                .unwrap()
                .add_neighbor(u1, u1_vlabel, elabel);
            self.update_vertex_constraint(
                u1,
                self.vertex_constraint(u1).unwrap().map(|x| x.clone()),
            );
            self.update_vertex_constraint(
                u2,
                self.vertex_constraint(u2).unwrap().map(|x| x.clone()),
            );
            true
        } else {
            false
        }
    }

    /// Add a vertex constraint.
    pub fn add_vertex_constraint(
        &mut self,
        vid: VId,
        constraint: Option<VertexConstraint>,
    ) -> bool {
        if let Some(node) = self.vertices.get_mut(&vid) {
            node.constraint = constraint.clone();
            self.update_vertex_constraint(vid, constraint);
            true
        } else {
            false
        }
    }

    /// Add an edge constraint.
    ///
    /// Note that we have to provide two edge constraints here: *f(u1, u2)* and *f(u2, u1)*.
    pub fn add_edge_constraint(
        &mut self,
        u1: VId,
        u2: VId,
        constraint: Option<(EdgeConstraint, EdgeConstraint)>,
    ) -> bool {
        if self.vertices.contains_key(&u1) && self.vertices.contains_key(&u2) {
            if self.neighbors(u1).unwrap().contains_key(&u2) {
                let (f12, f21) =
                    constraint.map_or((None, None), |(f12, f21)| (Some(f12), Some(f21)));
                self.vertices
                    .get_mut(&u1)
                    .unwrap()
                    .neighbors
                    .entry(u2)
                    .and_modify(|info| info.set_edge_constraint(f12));
                self.vertices
                    .get_mut(&u2)
                    .unwrap()
                    .neighbors
                    .entry(u1)
                    .and_modify(|info| info.set_edge_constraint(f21));
                true
            } else {
                false
            }
        } else {
            false
        }
    }

    pub fn vlabel(&self, vid: VId) -> Option<VLabel> {
        self.vertices.get(&vid).map(|node| node.vlabel)
    }

    pub fn in_deg(&self, vid: VId) -> Option<usize> {
        self.vertices.get(&vid).map(|node| node.in_deg)
    }

    pub fn out_deg(&self, vid: VId) -> Option<usize> {
        self.vertices.get(&vid).map(|node| node.out_deg)
    }

    pub fn undirected_deg(&self, vid: VId) -> Option<usize> {
        self.vertices.get(&vid).map(|node| node.undirected_deg)
    }

    pub fn vertex_constraint(&self, vid: VId) -> Option<Option<&VertexConstraint>> {
        self.vertices.get(&vid).map(|node| node.constraint.as_ref())
    }

    pub fn edge_constraint(&self, u1: VId, u2: VId) -> Option<Option<&EdgeConstraint>> {
        self.vertices.get(&u1).and_then(|node| {
            node.neighbors
                .get(&u2)
                .map(|info| info.edge_constraint.as_ref())
        })
    }

    pub fn neighbors(&self, vid: VId) -> Option<&HashMap<VId, NeighborInfo>> {
        self.vertices.get(&vid).map(|node| &node.neighbors)
    }

    pub fn vertices(&self) -> Vec<(VId, VLabel)> {
        let mut vertices: Vec<_> = self
            .vertices
            .iter()
            .map(|(&v, node)| (v, node.vlabel))
            .collect();
        vertices.sort();
        vertices
    }

    pub fn arcs(&self) -> Vec<(VId, VId, ELabel)> {
        let mut arcs = vec![];
        for (&v, node) in &self.vertices {
            for (&n, info) in &node.neighbors {
                for &e in &info.v_to_n_arcs {
                    arcs.push((v, n, e))
                }
            }
        }
        arcs.sort();
        arcs
    }

    pub fn edges(&self) -> Vec<(VId, VId, ELabel)> {
        let mut edges = vec![];
        for (&v, node) in &self.vertices {
            for (&n, info) in &node.neighbors {
                for &e in &info.undirected_edges {
                    edges.push((v, n, e))
                }
            }
        }
        edges.sort();
        edges
    }

    pub fn remove_vertex(&mut self, vid: VId) {
        let neighbors: Vec<VId> = self.neighbors(vid).unwrap().keys().map(|&n| n).collect();
        for n in neighbors {
            self.vertices
                .entry(n)
                .and_modify(|node| node.remove_neighbor(vid));
        }
        self.vertices.remove(&vid);
    }
}

// private methods.
impl PatternGraph {
    fn update_vertex_constraint(&mut self, n: VId, constraint: Option<VertexConstraint>) {
        let n_neighbors: Vec<VId> = self.neighbors(n).unwrap().iter().map(|(&v, _)| v).collect();
        for v in n_neighbors {
            self.vertices
                .get_mut(&v)
                .unwrap()
                .neighbors
                .get_mut(&n)
                .unwrap()
                .set_neighbor_constraint(constraint.clone());
        }
    }
}
