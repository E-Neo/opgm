use crate::{data_graph::DataGraph, pattern_graph::PatternGraph, types::VId};
use std::collections::HashMap;

/// The selection value of a vertex.
///
/// Higher value will be selected first.
#[derive(PartialEq, Eq, PartialOrd, Ord)]
struct VertexValue {
    deg: i64,
    num_constraints: i64,
    neg_vlabel_freq: i64,
    neg_vid: i64,
}

impl VertexValue {
    fn new(d: &DataGraph, p: &PatternGraph, u: VId) -> Self {
        let (mut deg, mut num_constraints) = (0, p.vertex_constraint(u).unwrap().map_or(0, |_| 1));
        for (_, info) in p.neighbors(u).unwrap() {
            deg += info.v_to_n_elabels().len()
                + info.n_to_v_elabels().len()
                + info.undirected_elabels().len();
            num_constraints += info.neighbor_constraint().map_or(0, |_| 1)
                + info.edge_constraint().map_or(0, |_| 1);
        }
        Self {
            deg: deg as i64,
            num_constraints,
            neg_vlabel_freq: -(d.frequency(p.vlabel(u).unwrap()) as i64),
            neg_vid: -u,
        }
    }

    fn vid(&self) -> VId {
        -self.neg_vid
    }
}

struct Candidates<'a, 'b> {
    d: &'a DataGraph,
    p: &'a PatternGraph<'b>,
    graph: PatternGraph<'b>,
    vid_values: HashMap<VId, VertexValue>,
}

impl<'a, 'b> Candidates<'a, 'b> {
    /// Selects one vertex in `p` and makes it the only item in the new candidates.
    fn new(d: &'a DataGraph, p: &'a PatternGraph<'b>) -> Self {
        Self {
            d,
            p,
            graph: p.clone(),
            vid_values: std::iter::once(
                p.vertices()
                    .map(|(u, _)| (VertexValue::new(d, p, u), u))
                    .max()
                    .map(|(val, key)| (key, val))
                    .unwrap(),
            )
            .collect(),
        }
    }

    fn is_empty(&self) -> bool {
        self.vid_values.is_empty()
    }

    fn select_root(&self) -> VId {
        self.vid_values.values().max().map(|v| v.vid()).unwrap()
    }

    fn add_candidates(&mut self, vid: VId) {
        for &n in self.graph.neighbors(vid).unwrap().keys() {
            self.vid_values
                .insert(n, VertexValue::new(self.d, self.p, n));
        }
    }

    fn remove_useless_candidates(&mut self) {
        let vertices: Vec<VId> = self.vid_values.keys().map(|&vid| vid).collect();
        for vid in vertices {
            if self.graph.in_deg(vid).unwrap()
                + self.graph.out_deg(vid).unwrap()
                + self.graph.undirected_deg(vid).unwrap()
                == 0
            {
                self.vid_values.remove(&vid);
            }
        }
    }

    fn elect(&mut self) -> VId {
        let root = self.select_root();
        self.vid_values.remove(&root);
        self.add_candidates(root);
        self.graph.remove_vertex(root);
        self.remove_useless_candidates();
        root
    }
}

/// Returns a vector of roots that can be used to create stars.
///
/// The order of the returning vector matters for the join operation.
pub fn decompose_stars<'a>(d: &DataGraph, p: &PatternGraph<'a>) -> Vec<VId> {
    let mut roots = Vec::new();
    let mut candidates = Candidates::new(d, p);
    while !candidates.is_empty() {
        roots.push(candidates.elect());
    }
    roots
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data_graph::mm_read_iter;
    use crate::memory_manager::MemoryManager;

    fn create_empty_data_graph() -> DataGraph {
        let mut mm = MemoryManager::Mem(vec![]);
        mm_read_iter(&mut mm, 0, 0, 0, vec![], vec![]);
        DataGraph::new(mm)
    }

    fn create_rectangle() -> PatternGraph<'static> {
        let mut p = PatternGraph::new();
        p.add_vertex(1, 0);
        p.add_vertex(2, 0);
        p.add_vertex(3, 0);
        p.add_vertex(4, 0);
        p.add_edge(1, 2, 0);
        p.add_edge(2, 3, 0);
        p.add_edge(3, 4, 0);
        p.add_edge(4, 1, 0);
        p
    }

    fn create_diamond() -> PatternGraph<'static> {
        let mut p = PatternGraph::new();
        p.add_vertex(1, 0);
        p.add_vertex(2, 0);
        p.add_vertex(3, 0);
        p.add_vertex(4, 0);
        p.add_edge(1, 2, 0);
        p.add_edge(2, 3, 0);
        p.add_edge(3, 4, 0);
        p.add_edge(4, 1, 0);
        p.add_edge(1, 3, 0);
        p
    }

    fn create_diamond2() -> PatternGraph<'static> {
        let mut p = PatternGraph::new();
        p.add_vertex(1, 1);
        p.add_vertex(2, 1);
        p.add_vertex(3, 1);
        p.add_vertex(4, 2);
        p.add_arc(1, 2, 10);
        p.add_arc(1, 3, 10);
        p.add_arc(1, 4, 20);
        p.add_arc(1, 4, 30);
        p.add_arc(2, 1, 10);
        p.add_arc(2, 4, 20);
        p.add_arc(3, 1, 10);
        p.add_arc(3, 4, 20);
        p
    }

    #[test]
    fn test_decompose_rectangle() {
        let d = create_empty_data_graph();
        let p = create_rectangle();
        let roots = decompose_stars(&d, &p);
        assert_eq!(roots, [1, 2, 3]);
    }

    #[test]
    fn test_decompose_diamond() {
        let d = create_empty_data_graph();
        let p = create_diamond();
        assert_eq!(decompose_stars(&d, &p), &[1, 3]);
    }

    #[test]
    fn test_decompose_diamond2() {
        let d = create_empty_data_graph();
        let p = create_diamond2();
        assert_eq!(decompose_stars(&d, &p), &[1, 4]);
    }
}
