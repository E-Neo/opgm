use super::types::{NeighborHeader, VLabelPosLen, VertexHeader};
use crate::{
    data::{
        Graph, GraphInfo, GraphView, Index, Neighbor, NeighborIter, NeighborView, Vertex,
        VertexIter, VertexView,
    },
    memory_manager::MemoryManager,
    pattern::NeighborInfo,
    types::{ELabel, VId, VLabel},
};
use std::{collections::HashSet, mem::size_of};

/// The data graph type under the property graph model.
///
/// The underlying format of the data graph is:
///
/// ```text
/// +-----------------------------------------+
/// |             num_vlabels: k              |
/// +-----------------------------------------+
/// +-------------+-------------+-------------+
/// |   vlabel    |     pos     |     len     |<-+
/// +-------------+-------------+-------------+  |
/// |   vlabel    |     pos     |     len     |  |
/// +-------------+-------------+-------------+  |- k rows
///                     ...                      |
/// +-------------+-------------+-------------+  |
/// |   vlabel    |     pos     |     len     |<-+
/// +-------------+-------------+-------------+
/// +--------------------+--------------------+
/// |     num_bytes      |         v          |<-----------------------------+
/// +--------------------+--------------------+                              |
/// |      in_deg        |      out_deg       |                              |
/// +--------------------+--------------------+                              |
/// |               num_vlabels               |                              |
/// +-----------------------------------------+                              |
/// +-------------+-------------+-------------+                              |
/// |   vlabel    |     pos     |     len     |<-+                           |
/// +-------------+-------------+-------------+  |                           |
/// |   vlabel    |     pos     |     len     |  |                           |
/// +-------------+-------------+-------------+  |- num_vlabels rows         |
///                     ...                      |                           |
/// +-------------+-------------+-------------+  |                           |
/// |   vlabel    |     pos     |     len     |<-+                           |
/// +-------------+-------------+-------------+                              |     One
/// +-----------------------------------------+                              |- VertexNode
/// |                    n                    |                              |
/// +--------------------+--------------------+                              |
/// |    num_n_to_v      |    num_v_to n      |                              |
/// +--------------------+--------------------+                              |
/// +-----------------------------------------+                              |
/// |                 elabel                  |<-+                           |
/// +-----------------------------------------+  |                           |
/// |                 elabel                  |  |  num_n_to_v + num_v_to_n  |
/// +-----------------------------------------+  |-           rows           |
///                     ...                      |                           |
/// +-----------------------------------------+  |                           |
/// |                 elabel                  |<-+                           |
/// +-----------------------------------------+                              |
///                     ...                    <-----------------------------+
/// ```
pub struct DataGraph<'a> {
    mm: &'a MemoryManager,
}

impl<'a> DataGraph<'a> {
    pub fn new(mm: &'a MemoryManager) -> Self {
        DataGraph { mm }
    }
}

impl<'a> Graph<GlobalIndex<'a>> for DataGraph<'a> {
    fn index(&self) -> GlobalIndex<'a> {
        let &num_vlabels = unsafe { self.mm.as_ref::<usize>(0) };
        GlobalIndex {
            mm: &self.mm,
            index: unsafe { self.mm.as_slice(size_of::<usize>(), num_vlabels) },
        }
    }

    fn count(&self, label: VLabel) -> usize {
        self.index().get(label).len()
    }

    fn info(&self) -> GraphInfo {
        let (mut num_vertices, mut num_edges) = (0, 0);
        let mut elabels = HashSet::new();
        for (_, vertices) in self.index() {
            num_vertices += vertices.len();
            for vertex in vertices {
                for (_, neighbors) in vertex.index() {
                    for neighbor in neighbors {
                        num_edges += neighbor.num_v_to_n() + neighbor.num_n_to_v();
                        neighbor.n_to_v_elabels().iter().for_each(|&elabel| {
                            elabels.insert(elabel);
                        });
                        neighbor.v_to_n_elabels().iter().for_each(|&elabel| {
                            elabels.insert(elabel);
                        });
                    }
                }
            }
        }
        num_edges /= 2;
        GraphInfo::new(num_vertices, num_edges, self.index().len(), elabels.len())
    }

    fn view(&self) -> GraphView {
        let global_index = self.index();
        GraphView::new(self.index().into_iter().map(|(vlabel, _)| {
            (
                vlabel,
                global_index.get(vlabel).map(|vertex| {
                    VertexView::new(
                        vertex.id(),
                        vertex.index().into_iter().map(|(nlabel, neighbors)| {
                            (
                                nlabel,
                                neighbors.map(|neighbor| {
                                    NeighborView::new(
                                        neighbor.id(),
                                        neighbor.n_to_v_elabels().iter().map(|&e| e),
                                        neighbor.v_to_n_elabels().iter().map(|&e| e),
                                    )
                                }),
                            )
                        }),
                    )
                }),
            )
        }))
    }
}

pub struct GlobalIndex<'a> {
    mm: &'a MemoryManager,
    index: &'a [VLabelPosLen],
}

impl<'a> IntoIterator for GlobalIndex<'a> {
    type Item = (VLabel, DataVertexIter<'a>);
    type IntoIter = GlobalIndexIntoIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        GlobalIndexIntoIter {
            mm: self.mm,
            iter: self.index.into_iter(),
        }
    }
}

impl<'a> Index<DataVertexIter<'a>> for GlobalIndex<'a> {
    fn len(&self) -> usize {
        self.index.len()
    }

    fn get(&self, label: VLabel) -> DataVertexIter<'a> {
        self.index
            .binary_search_by_key(&label, |&VLabelPosLen { vlabel, .. }| vlabel)
            .map_or(DataVertexIter::new(self.mm, 0, 0), |offset| {
                let &VLabelPosLen { pos, len, .. } = &self.index[offset];
                DataVertexIter::new(self.mm, pos, len as usize)
            })
    }
}

pub struct GlobalIndexIntoIter<'a> {
    mm: &'a MemoryManager,
    iter: std::slice::Iter<'a, VLabelPosLen>,
}

impl<'a> Iterator for GlobalIndexIntoIter<'a> {
    type Item = (VLabel, DataVertexIter<'a>);

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(|x| {
            let &VLabelPosLen { vlabel, pos, len } = x;
            (vlabel, DataVertexIter::new(self.mm, pos, len as usize))
        })
    }
}

pub struct LocalIndex<'a> {
    mm: &'a MemoryManager,
    index: &'a [VLabelPosLen],
}

impl<'a> IntoIterator for LocalIndex<'a> {
    type Item = (VLabel, DataNeighborIter<'a>);
    type IntoIter = LocalIndexIntoIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        LocalIndexIntoIter {
            mm: self.mm,
            iter: self.index.into_iter(),
        }
    }
}

impl<'a> Index<DataNeighborIter<'a>> for LocalIndex<'a> {
    fn len(&self) -> usize {
        self.index.len()
    }

    fn get(&self, label: VLabel) -> DataNeighborIter<'a> {
        self.index
            .binary_search_by_key(&label, |&VLabelPosLen { vlabel, .. }| vlabel)
            .map_or(DataNeighborIter::new(self.mm, 0, 0), |offset| {
                let &VLabelPosLen { pos, len, .. } = &self.index[offset];
                DataNeighborIter::new(self.mm, pos, len as usize)
            })
    }
}

pub struct LocalIndexIntoIter<'a> {
    mm: &'a MemoryManager,
    iter: std::slice::Iter<'a, VLabelPosLen>,
}

impl<'a> Iterator for LocalIndexIntoIter<'a> {
    type Item = (VLabel, DataNeighborIter<'a>);

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(|x| {
            let &VLabelPosLen { vlabel, pos, len } = x;
            (vlabel, DataNeighborIter::new(self.mm, pos, len as usize))
        })
    }
}

/// The data vertex type stored in the data graph.
pub struct DataVertex<'a> {
    mm: &'a MemoryManager,
    pos: usize,
}

impl<'a> Vertex<LocalIndex<'a>> for DataVertex<'a> {
    fn id(&self) -> VId {
        unsafe { (*self.mm.read::<VertexHeader>(self.pos)).vid }
    }

    fn in_deg(&self) -> usize {
        unsafe { (*self.mm.read::<VertexHeader>(self.pos)).in_deg as usize }
    }

    fn out_deg(&self) -> usize {
        unsafe { (*self.mm.read::<VertexHeader>(self.pos)).out_deg as usize }
    }

    fn index(&self) -> LocalIndex<'a> {
        LocalIndex {
            mm: self.mm,
            index: unsafe {
                self.mm.as_slice(
                    self.pos + size_of::<VertexHeader>(),
                    (*self.mm.read::<VertexHeader>(self.pos)).num_vlabels as usize,
                )
            },
        }
    }
}

/// An iterator visiting the data vertices with the same vertex label.
pub struct DataVertexIter<'a> {
    mm: &'a MemoryManager,
    pos: usize,
    offset: usize,
    len: usize,
}

impl<'a> DataVertexIter<'a> {
    fn new(mm: &'a MemoryManager, pos: usize, len: usize) -> Self {
        let offset = 0;
        Self {
            mm,
            pos,
            offset,
            len,
        }
    }
}

impl<'a> Iterator for DataVertexIter<'a> {
    type Item = DataVertex<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.offset < self.len {
            let pos = self.pos;
            self.pos += unsafe { (*self.mm.read::<VertexHeader>(pos)).num_bytes };
            self.offset += 1;
            Some(DataVertex { mm: self.mm, pos })
        } else {
            None
        }
    }
}

impl<'a> ExactSizeIterator for DataVertexIter<'a> {
    fn len(&self) -> usize {
        self.len
    }
}

impl<'a> VertexIter<DataVertex<'a>> for DataVertexIter<'a> {}

/// The neighbor `n` of a data vertex `v`.
pub struct DataNeighbor<'a> {
    mm: &'a MemoryManager,
    pos: usize,
}

impl<'a> DataNeighbor<'a> {
    fn num_n_to_v(&self) -> usize {
        unsafe { (*self.mm.read::<NeighborHeader>(self.pos)).num_n_to_v as usize }
    }

    fn num_v_to_n(&self) -> usize {
        unsafe { (*self.mm.read::<NeighborHeader>(self.pos)).num_v_to_n as usize }
    }

    pub fn n_to_v_elabels(&self) -> &'a [ELabel] {
        unsafe {
            self.mm
                .as_slice(self.pos + size_of::<NeighborHeader>(), self.num_n_to_v())
        }
    }

    pub fn v_to_n_elabels(&self) -> &'a [ELabel] {
        unsafe {
            self.mm.as_slice(
                self.pos + size_of::<NeighborHeader>() + size_of::<ELabel>() * self.num_n_to_v(),
                self.num_v_to_n(),
            )
        }
    }
}

impl<'a> Neighbor for DataNeighbor<'a> {
    fn id(&self) -> VId {
        unsafe { (*self.mm.read::<NeighborHeader>(self.pos)).nid }
    }

    fn topology_will_match(&self, info: &NeighborInfo) -> bool {
        check_degree(self, info) && check_edges(self, info)
    }
}

fn check_degree(neighbor: &DataNeighbor, info: &NeighborInfo) -> bool {
    neighbor.n_to_v_elabels().len() >= info.n_to_v_elabels().len()
        && neighbor.v_to_n_elabels().len() >= info.v_to_n_elabels().len()
        && neighbor.n_to_v_elabels().len() + neighbor.v_to_n_elabels().len()
            >= info.n_to_v_elabels().len()
                + info.v_to_n_elabels().len()
                + info.undirected_elabels().len()
}

fn check_edges(neighbor: &DataNeighbor, info: &NeighborInfo) -> bool {
    let mut n_to_v_elabels: HashSet<_> =
        info.n_to_v_elabels().iter().map(|&elabel| elabel).collect();
    let mut v_to_n_elabels: HashSet<_> =
        info.v_to_n_elabels().iter().map(|&elabel| elabel).collect();
    let mut undirected_elabels: HashSet<_> = info
        .undirected_elabels()
        .iter()
        .map(|&elabel| elabel)
        .collect();
    for elabel in neighbor.n_to_v_elabels() {
        if !n_to_v_elabels.remove(elabel) {
            undirected_elabels.remove(elabel);
        }
    }
    for elabel in neighbor.v_to_n_elabels() {
        if !v_to_n_elabels.remove(elabel) {
            undirected_elabels.remove(elabel);
        }
    }
    n_to_v_elabels.len() == 0 && v_to_n_elabels.len() == 0 && undirected_elabels.len() == 0
}

/// An iterator visiting the neighbors of a data vertex.
#[derive(Clone)]
pub struct DataNeighborIter<'a> {
    mm: &'a MemoryManager,
    pos: usize,
    offset: usize,
    len: usize,
}

impl<'a> DataNeighborIter<'a> {
    fn new(mm: &'a MemoryManager, pos: usize, len: usize) -> Self {
        Self {
            mm,
            pos,
            offset: 0,
            len,
        }
    }
}

impl<'a> Iterator for DataNeighborIter<'a> {
    type Item = DataNeighbor<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.offset < self.len {
            let data_neighbor = DataNeighbor {
                mm: self.mm,
                pos: self.pos,
            };
            self.pos += size_of::<NeighborHeader>()
                + size_of::<ELabel>() * (data_neighbor.num_n_to_v() + data_neighbor.num_v_to_n());
            self.offset += 1;
            Some(data_neighbor)
        } else {
            None
        }
    }
}

impl<'a> ExactSizeIterator for DataNeighborIter<'a> {
    fn len(&self) -> usize {
        self.len
    }
}

impl<'a> NeighborIter<DataNeighbor<'a>> for DataNeighborIter<'a> {}

#[derive(Debug)]
pub struct DataGraphInfo {
    num_vertices: usize,
    num_edges: usize,
    num_vlabels: usize,
    num_elabels: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::multiple::create::mm_from_iter;

    fn create_triangle_mm() -> MemoryManager {
        let mut mm = MemoryManager::Mem(vec![]);
        mm_from_iter(
            &mut mm,
            vec![(1, 10), (2, 20), (3, 20)].into_iter(),
            vec![(1, 2, 12), (1, 3, 13), (2, 3, 23), (3, 2, 32)].into_iter(),
        );
        mm
    }

    fn create_star_mm() -> MemoryManager {
        let mut mm = MemoryManager::Mem(vec![]);
        let vertices = vec![(1, 1), (3, 2), (4, 2), (5, 3), (6, 3)];
        let edges = vec![
            (1, 3, 2),
            (1, 3, 1),
            (1, 3, 3),
            (3, 1, 1),
            (1, 4, 1),
            (1, 5, 2),
            (1, 6, 2),
        ];
        mm_from_iter(&mut mm, vertices.into_iter(), edges.into_iter());
        mm
    }

    #[test]
    fn test_vertices() {
        let data_graph_mm = create_triangle_mm();
        let data_graph = DataGraph::new(&data_graph_mm);
        let global_index = data_graph.index();
        let vertices = global_index.get(10);
        assert_eq!(vertices.len(), 1);
        assert_eq!(
            vertices
                .map(|v| (v.id(), v.in_deg(), v.out_deg()))
                .collect::<Vec<_>>(),
            [(1, 0, 2)]
        );
        let vertices = global_index.get(20);
        assert_eq!(vertices.len(), 2);
        assert_eq!(
            vertices
                .map(|v| (v.id(), v.in_deg(), v.out_deg()))
                .collect::<Vec<_>>(),
            [(2, 2, 1), (3, 2, 1)]
        );
        let vertices = global_index.get(0);
        assert_eq!(vertices.len(), 0);
        assert_eq!(vertices.map(|v| v.id()).collect::<Vec<_>>(), []);
    }

    #[test]
    fn test_frequency() {
        let data_graph_mm = create_triangle_mm();
        let data_graph = DataGraph::new(&data_graph_mm);
        let global_index = data_graph.index();
        assert_eq!(global_index.get(0).len(), 0);
        assert_eq!(global_index.get(10).len(), 1);
        assert_eq!(global_index.get(20).len(), 2);
    }

    #[test]
    fn test_neighbor_elabels() {
        let data_graph_mm = create_star_mm();
        let data_graph = DataGraph::new(&data_graph_mm);
        let n3 = data_graph
            .index()
            .get(1)
            .next()
            .unwrap()
            .index()
            .into_iter()
            .next()
            .unwrap()
            .1
            .next()
            .unwrap();
        assert_eq!(n3.n_to_v_elabels(), [1]);
        assert_eq!(n3.v_to_n_elabels(), [1, 2, 3]);
    }

    #[test]
    fn test_neighbors() {
        let data_graph_mm = create_star_mm();
        let data_graph = DataGraph::new(&data_graph_mm);
        let neighbor_vids: Vec<_> = data_graph
            .index()
            .get(1)
            .next()
            .unwrap()
            .index()
            .into_iter()
            .map(|(_, neighbors)| neighbors.map(|n| n.id()))
            .flatten()
            .collect();
        assert_eq!(neighbor_vids, [3, 4, 5, 6]);
    }
}
