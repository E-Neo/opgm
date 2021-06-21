use crate::{
    data_graph::{Graph, Index, Vertex, VertexIter},
    memory_manager::MemoryManager,
    types::{ELabel, NeighborHeader, VId, VLabel, VLabelPosLen, VertexHeader},
};
use std::mem::size_of;

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
pub struct DataGraph {
    mm: MemoryManager,
}

impl DataGraph {
    pub fn new(mm: MemoryManager) -> Self {
        DataGraph { mm }
    }
}

impl<'a> Graph<'a, GlobalIndex<'a>> for DataGraph {
    fn index(&'a self) -> GlobalIndex<'a> {
        let num_vlabels = unsafe { *self.mm.read::<usize>(0) };
        GlobalIndex {
            mm: &self.mm,
            index: self.mm.read_slice(size_of::<usize>(), num_vlabels),
        }
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
            index: self
                .mm
                .read_slice(self.pos + size_of::<VertexHeader>(), unsafe {
                    (*self.mm.read::<VertexHeader>(self.pos)).num_vlabels as usize
                }),
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
    pub fn id(&self) -> VId {
        unsafe { (*self.mm.read::<NeighborHeader>(self.pos)).nid }
    }

    fn num_n_to_v(&self) -> usize {
        unsafe { (*self.mm.read::<NeighborHeader>(self.pos)).num_n_to_v as usize }
    }

    fn num_v_to_n(&self) -> usize {
        unsafe { (*self.mm.read::<NeighborHeader>(self.pos)).num_v_to_n as usize }
    }

    pub fn n_to_v_elabels(&self) -> &'a [ELabel] {
        self.mm
            .read_slice(self.pos + size_of::<NeighborHeader>(), self.num_n_to_v())
    }

    pub fn v_to_n_elabels(&self) -> &'a [ELabel] {
        self.mm.read_slice(
            self.pos + size_of::<NeighborHeader>() + size_of::<ELabel>() * self.num_n_to_v(),
            self.num_v_to_n(),
        )
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
    use crate::data_graph::mm_read_iter;

    fn create_triangle() -> DataGraph {
        let mut mm = MemoryManager::Mem(vec![]);
        mm_read_iter(
            &mut mm,
            2,
            3,
            4,
            vec![(1, 10), (2, 20), (3, 20)],
            vec![(1, 2, 12), (1, 3, 13), (2, 3, 23), (3, 2, 32)],
        );
        DataGraph::new(mm)
    }

    fn create_star() -> DataGraph {
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
        mm_read_iter(&mut mm, 3, vertices.len(), edges.len(), vertices, edges);
        DataGraph::new(mm)
    }

    #[test]
    fn test_vertices() {
        let data_graph = create_triangle();
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
        let data_graph = create_triangle();
        let global_index = data_graph.index();
        assert_eq!(global_index.get(0).len(), 0);
        assert_eq!(global_index.get(10).len(), 1);
        assert_eq!(global_index.get(20).len(), 2);
    }

    #[test]
    fn test_neighbor_elabels() {
        let data_graph = create_star();
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
        let data_graph = create_star();
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
