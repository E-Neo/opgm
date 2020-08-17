use crate::types::{ELabel, EdgeConstraint, VId, VLabel, VertexConstraint};
use std::cmp::Ordering;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::hash::{Hash, Hasher};

/// The neighbor's information of a vertex.
///
/// It stores the connection detail between a vertex and the neighbor of the vertex.
/// Neighbors with the same [`NeighborInfo`](struct.NeighborInfo.html) construct the
/// neighborhood equivalence class.
#[derive(Clone)]
pub struct NeighborInfo<'a> {
    vlabel: VLabel,
    n_to_v_arcs: BTreeSet<ELabel>,
    v_to_n_arcs: BTreeSet<ELabel>,
    undirected_edges: BTreeSet<ELabel>,
    neighbor_constraint: Option<&'a VertexConstraint>,
    edge_constraint: Option<&'a EdgeConstraint>,
    hash: u64,
}

impl<'a> NeighborInfo<'a> {
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

    pub fn neighbor_constraint(&self) -> &Option<&'a VertexConstraint> {
        &self.neighbor_constraint
    }

    pub fn edge_constraint(&self) -> &Option<&'a EdgeConstraint> {
        &self.edge_constraint
    }
}

// private methods
impl<'a> NeighborInfo<'a> {
    fn new(vlabel: VLabel) -> Self {
        Self {
            vlabel,
            n_to_v_arcs: BTreeSet::new(),
            v_to_n_arcs: BTreeSet::new(),
            undirected_edges: BTreeSet::new(),
            neighbor_constraint: None,
            edge_constraint: None,
            hash: vlabel as u64,
        }
    }

    fn add_predecessor(&mut self, elabel: ELabel) {
        self.n_to_v_arcs.insert(elabel);
        self.hash += (elabel as u64) << 8;
    }

    fn add_successor(&mut self, elabel: ELabel) {
        self.v_to_n_arcs.insert(elabel);
        self.hash += (elabel as u64) << 16;
    }

    fn add_neighbor(&mut self, elabel: ELabel) {
        self.undirected_edges.insert(elabel);
        self.hash += (elabel as u64) << 32;
    }

    fn set_neighbor_constraint(&mut self, constraint: Option<&'a VertexConstraint>) {
        self.neighbor_constraint = constraint;
    }

    fn set_edge_constraint(&mut self, constraint: Option<&'a EdgeConstraint>) {
        self.edge_constraint = constraint;
    }
}

impl<'a> std::fmt::Debug for NeighborInfo<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "NeighborInfo {{...}}")
    }
}

impl<'a> PartialEq for NeighborInfo<'a> {
    fn eq(&self, other: &Self) -> bool {
        self.hash == other.hash
            && self.vlabel == other.vlabel
            && self.n_to_v_arcs == other.n_to_v_arcs
            && self.v_to_n_arcs == other.v_to_n_arcs
            && self.undirected_edges == other.undirected_edges
            && match (self.neighbor_constraint, other.neighbor_constraint) {
                (Some(f), Some(g)) => std::ptr::eq(f, g),
                (None, None) => true,
                _ => false,
            }
            && match (self.edge_constraint, other.edge_constraint) {
                (Some(f), Some(g)) => std::ptr::eq(f, g),
                (None, None) => true,
                _ => false,
            }
    }
}

impl<'a> Eq for NeighborInfo<'a> {}

impl<'a> Hash for NeighborInfo<'a> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.hash.hash(state);
    }
}

#[derive(Clone)]
struct GraphNode<'a> {
    vlabel: VLabel,
    in_deg: usize,
    out_deg: usize,
    undirected_deg: usize,
    neighbors: HashMap<VId, NeighborInfo<'a>>,
    constraint: Option<&'a VertexConstraint>,
}

impl<'a> GraphNode<'a> {
    fn new(vlabel: VLabel) -> GraphNode<'a> {
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

/// An iterator over the vertices of a pattern graph.
pub struct Vertices<'a> {
    vertices: std::collections::hash_map::Iter<'a, VId, GraphNode<'a>>,
}

impl<'a> Iterator for Vertices<'a> {
    type Item = (VId, VLabel);

    fn next(&mut self) -> Option<Self::Item> {
        self.vertices.next().map(|(&vid, node)| (vid, node.vlabel))
    }
}

/// An iterator over the arcs of a pattern graph.
pub struct Arcs {
    arcs: Vec<(VId, VId, ELabel)>,
    offset: usize,
}

impl Arcs {
    fn new<'a>(vertices: &HashMap<VId, GraphNode<'a>>) -> Self {
        let mut arcs = Vec::new();
        for (&vid, node) in vertices {
            for (&nid, info) in &node.neighbors {
                for &elabel in &info.v_to_n_arcs {
                    arcs.push((vid, nid, elabel))
                }
            }
        }
        Self { arcs, offset: 0 }
    }
}

impl Iterator for Arcs {
    type Item = (VId, VId, ELabel);

    fn next(&mut self) -> Option<Self::Item> {
        if self.offset == self.arcs.len() {
            None
        } else {
            let res = self.arcs[self.offset];
            self.offset += 1;
            Some(res)
        }
    }
}

/// An iterator over the undirected edges of a pattern graph.
pub struct Edges {
    edges: Vec<(VId, VId, ELabel)>,
    offset: usize,
}

impl Edges {
    fn new<'a>(vertices: &HashMap<VId, GraphNode<'a>>) -> Self {
        let mut edges = Vec::new();
        for (&vid, node) in vertices {
            for (&nid, info) in &node.neighbors {
                if vid <= nid {
                    for &elabel in &info.undirected_edges {
                        edges.push((vid, nid, elabel))
                    }
                }
            }
        }
        Self { edges, offset: 0 }
    }
}

impl Iterator for Edges {
    type Item = (VId, VId, ELabel);

    fn next(&mut self) -> Option<Self::Item> {
        if self.offset == self.edges.len() {
            None
        } else {
            let res = self.edges[self.offset];
            self.offset += 1;
            Some(res)
        }
    }
}

/// The pattern graph type.
///
/// The pattern graph is a property graph except that it also support undirected edges,
/// because users may want to ignore the directions of some edges.
/// The *vertex constraints* and *edge constraints* are also pushed down to the pattern graph,
/// so we can filter out useless matching results as much as possible.
#[derive(Clone)]
pub struct PatternGraph<'a> {
    vertices: HashMap<VId, GraphNode<'a>>,
}

impl<'a> PatternGraph<'a> {
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
            self.update_vertex_constraint(u1, self.vertex_constraint(u1).unwrap());
            self.update_vertex_constraint(u2, self.vertex_constraint(u2).unwrap());
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
            self.update_vertex_constraint(u1, self.vertex_constraint(u1).unwrap());
            self.update_vertex_constraint(u2, self.vertex_constraint(u2).unwrap());
            true
        } else {
            false
        }
    }

    /// Add a vertex constraint.
    pub fn add_vertex_constraint(
        &mut self,
        vid: VId,
        constraint: Option<&'a VertexConstraint>,
    ) -> bool {
        if let Some(node) = self.vertices.get_mut(&vid) {
            node.constraint = constraint;
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
        constraint: Option<(&'a EdgeConstraint, &'a EdgeConstraint)>,
    ) -> bool {
        if self.vertices.contains_key(&u1) && self.vertices.contains_key(&u2) {
            if self.neighbors(u1).unwrap().contains_key(&u2) {
                self.vertices
                    .get_mut(&u1)
                    .unwrap()
                    .neighbors
                    .entry(u2)
                    .and_modify(|info| info.set_edge_constraint(constraint.map(|x| x.0)));
                self.vertices
                    .get_mut(&u2)
                    .unwrap()
                    .neighbors
                    .entry(u1)
                    .and_modify(|info| info.set_edge_constraint(constraint.map(|x| x.1)));
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

    pub fn vertex_constraint(&self, vid: VId) -> Option<Option<&'a VertexConstraint>> {
        self.vertices.get(&vid).map(|node| node.constraint)
    }

    pub fn edge_constraint(&self, u1: VId, u2: VId) -> Option<Option<&'a EdgeConstraint>> {
        self.vertices
            .get(&u1)
            .and_then(|node| node.neighbors.get(&u2).map(|info| info.edge_constraint))
    }

    pub fn neighbors(&self, vid: VId) -> Option<&HashMap<VId, NeighborInfo<'a>>> {
        self.vertices.get(&vid).map(|node| &node.neighbors)
    }

    pub fn vertices(&self) -> Vertices {
        Vertices {
            vertices: self.vertices.iter(),
        }
    }

    pub fn arcs(&self) -> Arcs {
        Arcs::new(&self.vertices)
    }

    pub fn edges(&self) -> Edges {
        Edges::new(&self.vertices)
    }
}

// pub(crate) methods.
impl<'a> PatternGraph<'a> {
    pub(crate) fn remove_vertex(&mut self, vid: VId) {
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
impl<'a> PatternGraph<'a> {
    fn update_vertex_constraint(&mut self, n: VId, constraint: Option<&'a VertexConstraint>) {
        let n_neighbors: Vec<VId> = self.neighbors(n).unwrap().iter().map(|(&v, _)| v).collect();
        for v in n_neighbors {
            self.vertices
                .get_mut(&v)
                .unwrap()
                .neighbors
                .get_mut(&n)
                .unwrap()
                .set_neighbor_constraint(constraint);
        }
    }
}

impl<'a> std::fmt::Debug for PatternGraph<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "PatternGraph")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_triangle<'a>() -> PatternGraph<'a> {
        let mut g = PatternGraph::new();
        g.add_vertex(1, 10);
        g.add_vertex(2, 20);
        g.add_vertex(3, 20);
        g.add_arc(1, 2, 12);
        g.add_arc(1, 3, 13);
        g.add_edge(2, 3, 23);
        g
    }

    #[test]
    fn test_triangle() {
        let p = create_triangle();
        let mut vertices = p.vertices().collect::<Vec<_>>();
        vertices.sort();
        assert_eq!(vertices, vec![(1, 10), (2, 20), (3, 20)]);
        let mut arcs = p.arcs().collect::<Vec<_>>();
        arcs.sort();
        assert_eq!(arcs, vec![(1, 2, 12), (1, 3, 13)]);
        let mut edges = p.edges().collect::<Vec<_>>();
        edges.sort();
        assert_eq!(edges, vec![(2, 3, 23)]);
    }

    #[test]
    fn test_create_triangle() {
        let mut g = PatternGraph::new();
        g.add_vertex(1, 1);
        g.add_vertex(2, 20);
        g.add_vertex(3, 30);
        assert_eq!(g.add_arc(1, 2, 12), true);
        assert_eq!(g.add_arc(1, 4, 14), false);
        assert_eq!(g.vlabel(1), Some(1));
        g.add_vertex(1, 10);
        assert_eq!(g.vlabel(1), Some(10));
        assert_eq!(g.add_arc(1, 3, 13), true);
        assert_eq!(g.out_deg(1), Some(2));
        assert_eq!(g.add_edge(2, 3, 23), true);
    }

    #[test]
    fn test_vlabel() {
        let g = create_triangle();
        assert_eq!(g.vlabel(1), Some(10));
        assert_eq!(g.vlabel(2), Some(20));
        assert_eq!(g.vlabel(3), Some(20));
        assert_eq!(g.vlabel(4), None);
    }

    #[test]
    fn test_elabel() {
        let mut g = PatternGraph::new();
        g.add_vertex(1, 1);
        g.add_vertex(2, 2);
        g.add_vertex(3, 3);
        g.add_arc(1, 2, 1);
        g.add_arc(2, 1, 1);
        g.add_arc(1, 3, 2);
        let info2 = g.neighbors(1).unwrap().get(&2).unwrap();
        let info2_n_to_v: Vec<_> = info2.n_to_v_elabels().iter().map(|&e| e).collect();
        let info2_v_to_n: Vec<_> = info2.v_to_n_elabels().iter().map(|&e| e).collect();
        assert_eq!(info2_n_to_v, [1]);
        assert_eq!(info2_v_to_n, [1]);
    }

    #[test]
    fn test_degrees() {
        let g = create_triangle();
        assert_eq!(g.in_deg(1), Some(0));
        assert_eq!(g.in_deg(2), Some(1));
        assert_eq!(g.in_deg(3), Some(1));
        assert_eq!(g.in_deg(4), None);
        assert_eq!(g.out_deg(1), Some(2));
        assert_eq!(g.out_deg(2), Some(0));
        assert_eq!(g.out_deg(3), Some(0));
        assert_eq!(g.out_deg(4), None);
        assert_eq!(g.undirected_deg(1), Some(0));
        assert_eq!(g.undirected_deg(2), Some(1));
        assert_eq!(g.undirected_deg(3), Some(1));
        assert_eq!(g.undirected_deg(4), None);
    }

    #[test]
    fn test_vertex_constraint() {
        let f = |u| u <= 10;
        let (f0n, fn0) = (|u0, un| u0 < un, |un, u0| u0 < un);
        let mut g = PatternGraph::new();
        g.add_vertex(0, 0);
        g.add_vertex(1, 1);
        g.add_vertex_constraint(1, Some(&f));
        g.add_vertex(2, 1);
        g.add_vertex_constraint(2, Some(&f));
        g.add_arc(0, 1, 10);
        g.add_edge_constraint(0, 1, Some((&f0n, &fn0)));
        g.add_arc(0, 2, 10);
        g.add_edge_constraint(0, 2, Some((&f0n, &fn0)));
        assert_eq!(
            g.neighbors(0).unwrap().get(&1).unwrap(),
            g.neighbors(0).unwrap().get(&2).unwrap()
        );
        assert_eq!(g.vertex_constraint(1).unwrap().unwrap()(10), true);
        assert_eq!(g.vertex_constraint(2).unwrap().unwrap()(11), false);
    }

    #[test]
    fn test_edge_constraint() {
        let (f12, f21) = (|u1, u2| u1 < u2, |u2, u1| u1 < u2);
        let mut g = PatternGraph::new();
        g.add_vertex(0, 0);
        g.add_vertex(1, 1);
        g.add_vertex(2, 1);
        g.add_edge(0, 1, 10);
        g.add_edge(1, 2, 10);
        assert_eq!(g.add_edge_constraint(1, 2, Some((&f12, &f21))), true);
        assert_eq!(g.add_edge_constraint(0, 2, Some((&f12, &f21))), false);
        assert_eq!(g.edge_constraint(1, 2).unwrap().unwrap()(1, 2), true);
        assert_eq!(g.edge_constraint(1, 2).unwrap().unwrap()(2, 1), false);
        assert_eq!(g.edge_constraint(2, 1).unwrap().unwrap()(2, 1), true);
        assert_eq!(g.edge_constraint(2, 1).unwrap().unwrap()(1, 2), false);
    }

    #[test]
    fn test_remove_vertex() {
        let mut g = PatternGraph::new();
        g.add_vertex(1, 0);
        g.add_vertex(2, 0);
        g.add_vertex(3, 0);
        g.add_arc(1, 2, 12);
        g.add_arc(1, 3, 12);
        g.add_edge(2, 3, 0);
        assert_eq!(g.in_deg(2), Some(1));
        assert_eq!(g.in_deg(3), Some(1));
        g.remove_vertex(1);
        assert_eq!(g.in_deg(2), Some(0));
        assert_eq!(g.in_deg(3), Some(0));
        assert_eq!(g.undirected_deg(2), Some(1));
        assert_eq!(g.undirected_deg(3), Some(1));
    }

    #[test]
    fn test_constraint() {
        fn lt_vid_num(n: i64) -> Box<dyn Fn(VId) -> bool> {
            Box::new(move |vid| vid < n)
        }
        let vertex_constraints: HashMap<VId, Box<dyn Fn(VId) -> bool>> =
            vec![(1, lt_vid_num(10))].into_iter().collect();
        let mut p = PatternGraph::new();
        p.add_vertex(1, 0);
        p.add_vertex(2, 0);
        p.add_edge(1, 2, 0);
        p.add_vertex_constraint(1, vertex_constraints.get(&1).map(|f| &**f));
        assert_eq!(p.vertex_constraint(1).unwrap().unwrap()(9), true);
        assert_eq!(p.vertex_constraint(1).unwrap().unwrap()(10), false);
    }
}
