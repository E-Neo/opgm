use crate::{
    data_graph::display,
    memory_manager::MemoryManager,
    types::{ELabel, NeighborHeader, VId, VLabel, VLabelPosLen, VertexHeader},
};
use std::collections::{HashMap, HashSet};
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
    index: HashMap<VLabel, (usize, usize)>, // (pos, len)
}

impl DataGraph {
    pub fn new(mm: MemoryManager) -> DataGraph {
        let index = DataGraph::create_index(&mm);
        DataGraph { mm, index }
    }

    /// Returns the size of data vertices with `vlabel` and an iterator visiting these vertices.
    pub fn vertices(&self, vlabel: VLabel) -> (usize, DataVertexIter) {
        if let Some(&(pos, len)) = self.index.get(&vlabel) {
            (len, DataVertexIter::new(&self.mm, pos, len))
        } else {
            (0, DataVertexIter::new(&self.mm, 0, 0))
        }
    }

    /// Returns the occurrence of vertices with `vlabel`.
    pub fn frequency(&self, vlabel: VLabel) -> usize {
        self.index.get(&vlabel).map_or(0, |&(_, len)| len)
    }
}

// Private methods.
impl DataGraph {
    fn create_index(mm: &MemoryManager) -> HashMap<VLabel, (usize, usize)> {
        let num_vlabels = unsafe { *mm.read::<usize>(0) };
        let mut index = HashMap::with_capacity(num_vlabels);
        for vpl in mm.read_slice::<VLabelPosLen>(size_of::<usize>(), num_vlabels) {
            index.insert(vpl.vlabel, (vpl.pos, vpl.len));
        }
        index
    }
}

impl std::fmt::Display for DataGraph {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        display(&self.mm, f)
    }
}

/// The data vertex type stored in the data graph.
pub struct DataVertex<'a> {
    mm: &'a MemoryManager,
    pos: usize,
}

impl<'a> DataVertex<'a> {
    pub fn id(&self) -> VId {
        unsafe { (*self.mm.read::<VertexHeader>(self.pos)).vid }
    }

    pub fn in_deg(&self) -> usize {
        unsafe { (*self.mm.read::<VertexHeader>(self.pos)).in_deg }
    }

    pub fn out_deg(&self) -> usize {
        unsafe { (*self.mm.read::<VertexHeader>(self.pos)).out_deg }
    }

    pub fn num_vlabels(&self) -> usize {
        unsafe { (*self.mm.read::<VertexHeader>(self.pos)).num_vlabels }
    }

    /// Returns an iterator visiting the neighbors
    /// (grouped by [`VLabel`](../types/type.VLabel.html)) of the data vertex.
    pub fn vlabels(&self) -> DataVLabelNeighborIter<'a> {
        DataVLabelNeighborIter::new(self.mm, self.pos)
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
        unsafe { (*self.mm.read::<NeighborHeader>(self.pos)).num_n_to_v }
    }

    fn num_v_to_n(&self) -> usize {
        unsafe { (*self.mm.read::<NeighborHeader>(self.pos)).num_v_to_n }
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

/// An iterator visiting the neighbors (grouped by [`VLabel`](../types/type.VLabel.html)) of a data
/// vertex.
///
/// The element of this iterator is `(vlabel, size_of_neighbors, neighbor_iter)`.
pub struct DataVLabelNeighborIter<'a> {
    mm: &'a MemoryManager,
    vlabel_pos_lens: &'a [VLabelPosLen],
    offset: usize,
}

impl<'a> DataVLabelNeighborIter<'a> {
    fn new(mm: &'a MemoryManager, pos: usize) -> Self {
        let len = unsafe { (*mm.read::<VertexHeader>(pos)).num_vlabels };
        let vlabel_pos_lens = mm.read_slice::<VLabelPosLen>(pos + size_of::<VertexHeader>(), len);
        let offset = 0;
        Self {
            mm,
            vlabel_pos_lens,
            offset,
        }
    }
}

impl<'a> Iterator for DataVLabelNeighborIter<'a> {
    type Item = (VLabel, usize, DataNeighborIter<'a>);

    fn next(&mut self) -> Option<Self::Item> {
        if self.offset < self.vlabel_pos_lens.len() {
            let vpl = &self.vlabel_pos_lens[self.offset];
            self.offset += 1;
            Some((
                vpl.vlabel,
                vpl.len,
                DataNeighborIter::new(self.mm, vpl.pos, vpl.len),
            ))
        } else {
            None
        }
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

#[derive(Debug)]
pub struct DataGraphInfo {
    num_vertices: usize,
    num_edges: usize,
    num_vlabels: usize,
    num_elabels: usize,
}

impl DataGraphInfo {
    pub fn new(data_graph: &DataGraph) -> Self {
        let (mut num_vertices, mut num_edges) = (0, 0);
        let mut elabels = HashSet::new();
        for &vlabel in data_graph.index.keys() {
            let (num, vertices) = data_graph.vertices(vlabel);
            num_vertices += num;
            for vertex in vertices {
                for (_, _, neighbors) in vertex.vlabels() {
                    for neighbor in neighbors {
                        num_edges += neighbor.num_v_to_n() + neighbor.num_n_to_v();
                        neighbor.v_to_n_elabels().iter().for_each(|&elabel| {
                            elabels.insert(elabel);
                        });
                        neighbor.n_to_v_elabels().iter().for_each(|&elabel| {
                            elabels.insert(elabel);
                        });
                    }
                }
            }
        }
        Self {
            num_vertices,
            num_edges,
            num_vlabels: data_graph.index.len(),
            num_elabels: elabels.len(),
        }
    }
}

impl std::fmt::Display for DataGraphInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "num_vertices: {}", self.num_vertices)?;
        writeln!(f, "num_edges: {}", self.num_edges)?;
        writeln!(f, "num_vlabels: {}", self.num_vlabels)?;
        writeln!(f, "num_elabels: {}", self.num_elabels)?;
        Ok(())
    }
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
        let (num_vertices, vertices) = data_graph.vertices(10);
        assert_eq!(num_vertices, 1);
        assert_eq!(
            vertices
                .map(|v| (v.id(), v.in_deg(), v.out_deg()))
                .collect::<Vec<_>>(),
            [(1, 0, 2)]
        );
        let (num_vertices, vertices) = data_graph.vertices(20);
        assert_eq!(num_vertices, 2);
        assert_eq!(
            vertices
                .map(|v| (v.id(), v.in_deg(), v.out_deg()))
                .collect::<Vec<_>>(),
            [(2, 2, 1), (3, 2, 1)]
        );
        let (num_vertices, vertices) = data_graph.vertices(0);
        assert_eq!(num_vertices, 0);
        assert_eq!(vertices.map(|v| v.id()).collect::<Vec<_>>(), []);
    }

    #[test]
    fn test_frequency() {
        let data_graph = create_triangle();
        assert_eq!(data_graph.frequency(0), 0);
        assert_eq!(data_graph.frequency(10), 1);
        assert_eq!(data_graph.frequency(20), 2);
    }

    #[test]
    fn test_neighbor_elabels() {
        let data_graph = create_star();
        let n3 = data_graph
            .vertices(1)
            .1
            .next()
            .unwrap()
            .vlabels()
            .next()
            .unwrap()
            .2
            .next()
            .unwrap();
        assert_eq!(n3.n_to_v_elabels(), [1]);
        assert_eq!(n3.v_to_n_elabels(), [1, 2, 3]);
    }

    #[test]
    fn test_neighbors() {
        let data_graph = create_star();
        let neighbor_vids: Vec<_> = data_graph
            .vertices(1)
            .1
            .next()
            .unwrap()
            .vlabels()
            .map(|(_, _, neighbors)| neighbors.map(|n| n.id()))
            .flatten()
            .collect();
        assert_eq!(neighbor_vids, [3, 4, 5, 6]);
    }
}
