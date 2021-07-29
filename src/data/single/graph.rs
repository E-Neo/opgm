use super::types::{Header, IndexEntry, NeighborEntry, VertexHeader};
use crate::{
    data::{Graph, GraphInfo, GraphView, Index, Neighbor, NeighborIter, Vertex, VertexIter},
    memory_manager::MemoryManager,
    pattern::NeighborInfo,
    types::{ELabel, VId, VLabel},
};
use std::mem::size_of;

/// The data graph type under the property graph model.
///
/// The underlying format of the data graph is:
///
/// ```text
/// +--------------------+--------------------+
/// |       magic        |   num_vlabels: k   |
/// +--------------------+--------------------+
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
/// |    n_to_v_elabel   |    v_to n_elabel   |                              |
/// +--------------------+--------------------+                              |
/// |                    n                    |                              |
/// +--------------------+--------------------+                              |
/// |    n_to_v_elabel   |    v_to n_elabel   |                              |
/// +--------------------+--------------------+                              |
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
        let &Header { num_vlabels, .. } = unsafe { self.mm.as_ref(size_of::<u64>()) };
        GlobalIndex {
            mm: &self.mm,
            index: unsafe {
                self.mm
                    .as_slice(size_of::<u64>() + size_of::<Header>(), num_vlabels as usize)
            },
        }
    }

    fn count(&self, label: VLabel) -> usize {
        self.index().get(label).len()
    }

    fn info(&self) -> GraphInfo {
        let &Header {
            num_vertices,
            num_edges,
            num_vlabels,
            num_elabels,
        } = unsafe { self.mm.as_ref(size_of::<u64>()) };
        GraphInfo::new(
            num_vertices as usize,
            num_edges as usize,
            num_vlabels as usize,
            num_elabels as usize,
        )
    }

    fn view(&self) -> GraphView {
        let (mut vertices, mut edges) = (vec![], vec![]);
        for (vlabel, vs) in self.index() {
            for v in vs {
                vertices.push((v.id(), vlabel));
                for (_, ns) in v.index() {
                    for n in ns {
                        let elabel = n.v_to_n_elabel();
                        if elabel >= 0 {
                            edges.push((v.id(), n.id(), elabel));
                        }
                    }
                }
            }
        }
        vertices.sort();
        edges.sort();
        GraphView::new(vertices, edges)
    }
}

pub struct GlobalIndex<'a> {
    mm: &'a MemoryManager,
    index: &'a [IndexEntry],
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
            .binary_search_by_key(&label, |&IndexEntry { vlabel, .. }| vlabel)
            .map_or(DataVertexIter::new(self.mm, 0, 0), |offset| {
                let &IndexEntry { pos, len, .. } = &self.index[offset];
                DataVertexIter::new(self.mm, pos as usize, len as usize)
            })
    }
}

pub struct GlobalIndexIntoIter<'a> {
    mm: &'a MemoryManager,
    iter: std::slice::Iter<'a, IndexEntry>,
}

impl<'a> Iterator for GlobalIndexIntoIter<'a> {
    type Item = (VLabel, DataVertexIter<'a>);

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(|x| {
            let &IndexEntry { vlabel, pos, len } = x;
            (
                vlabel,
                DataVertexIter::new(self.mm, pos as usize, len as usize),
            )
        })
    }
}

pub struct LocalIndex<'a> {
    mm: &'a MemoryManager,
    index: &'a [IndexEntry],
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
            .binary_search_by_key(&label, |&IndexEntry { vlabel, .. }| vlabel)
            .map_or(DataNeighborIter::new(self.mm, 0, 0), |offset| {
                let &IndexEntry { pos, len, .. } = &self.index[offset];
                DataNeighborIter::new(self.mm, pos as usize, len as usize)
            })
    }
}

pub struct LocalIndexIntoIter<'a> {
    mm: &'a MemoryManager,
    iter: std::slice::Iter<'a, IndexEntry>,
}

impl<'a> Iterator for LocalIndexIntoIter<'a> {
    type Item = (VLabel, DataNeighborIter<'a>);

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(|x| {
            let &IndexEntry { vlabel, pos, len } = x;
            (
                vlabel,
                DataNeighborIter::new(self.mm, pos as usize, len as usize),
            )
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
        unsafe { self.mm.as_ref::<VertexHeader>(self.pos).vid }
    }

    fn in_deg(&self) -> usize {
        unsafe { self.mm.as_ref::<VertexHeader>(self.pos).in_deg as usize }
    }

    fn out_deg(&self) -> usize {
        unsafe { self.mm.as_ref::<VertexHeader>(self.pos).out_deg as usize }
    }

    fn index(&self) -> LocalIndex<'a> {
        LocalIndex {
            mm: self.mm,
            index: unsafe {
                self.mm.as_slice(
                    self.pos + size_of::<VertexHeader>(),
                    self.mm.as_ref::<VertexHeader>(self.pos).num_vlabels as usize,
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
            self.pos += unsafe { self.mm.as_ref::<VertexHeader>(pos).num_bytes } as usize;
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
    fn n_to_v_elabel(&self) -> ELabel {
        unsafe { self.mm.as_ref::<NeighborEntry>(self.pos).n_to_v_elabel }
    }

    fn v_to_n_elabel(&self) -> ELabel {
        unsafe { self.mm.as_ref::<NeighborEntry>(self.pos).v_to_n_elabel }
    }
}

impl<'a> Neighbor for DataNeighbor<'a> {
    fn id(&self) -> VId {
        unsafe { self.mm.as_ref::<NeighborEntry>(self.pos).nid }
    }

    fn topology_will_match(&self, info: &NeighborInfo) -> bool {
        info.n_to_v_elabels().contains(&self.n_to_v_elabel()) as usize
            + info.v_to_n_elabels().contains(&self.v_to_n_elabel()) as usize
            + info.undirected_elabels().contains(&self.n_to_v_elabel()) as usize
            == info.n_to_v_elabels().len()
                + info.v_to_n_elabels().len()
                + info.undirected_elabels().len()
    }
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
            self.pos += size_of::<NeighborEntry>();
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::{info_edges::mm_from_iter, single::create::mm_from_info_edges};

    fn create_triangle_mm() -> MemoryManager {
        let mut info_edges_mm = MemoryManager::Mem(vec![]);
        mm_from_iter(
            &mut info_edges_mm,
            vec![(1, 10), (2, 20), (3, 20)].into_iter(),
            vec![(1, 2, 12), (1, 3, 13), (2, 3, 23), (3, 2, 32)].into_iter(),
        );
        let mut mm = MemoryManager::new_mem(0);
        mm_from_info_edges(&mut mm, &info_edges_mm);
        mm
    }

    fn create_star_mm() -> MemoryManager {
        let mut info_edges_mm = MemoryManager::new_mem(0);
        let vertices = vec![(1, 1), (3, 2), (4, 2), (5, 3), (6, 3)];
        let edges = vec![(1, 3, 2), (3, 1, 1), (1, 4, 1), (1, 5, 2), (1, 6, 2)];
        mm_from_iter(&mut info_edges_mm, vertices.into_iter(), edges.into_iter());
        let mut mm = MemoryManager::new_mem(0);
        mm_from_info_edges(&mut mm, &info_edges_mm);
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
        assert_eq!(n3.n_to_v_elabel(), 1);
        assert_eq!(n3.v_to_n_elabel(), 2);
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
