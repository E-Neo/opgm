use super::types::{ArcEntry, Header, IndexEntry, VertexHeader};
use crate::{
    data::{Graph, GraphInfo, GraphView, Index, Neighbor, NeighborIter, Vertex, VertexIter},
    memory_manager::MemoryManager,
    pattern::NeighborInfo,
    types::{VId, VLabel},
};
use std::mem::size_of;

/// The CSR data graph.
///
/// ```text
/// +--------------------+--------------------+
/// |       magic        |       Header       |
/// +--------------------+--------------------+
/// +--------------------+
/// |     IndexEntry     |   ...
/// +--------------------+
/// +--------------------+
/// |        VId         |   ...
/// +--------------------+
/// +--------------------+
/// |    VertexHeader    |   ...
/// +--------------------+
/// +--------------------+
/// |      EdgeEntry     |   ...
/// +--------------------+
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
        GlobalIndex {
            mm: self.mm,
            index: unsafe {
                self.mm.as_slice(
                    size_of::<u64>() + size_of::<Header>(),
                    get_header(self.mm).num_vlabels as usize,
                )
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
            ..
        } = get_header(self.mm);
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
                        let elabel = n.arc.elabel;
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
            .map_or(DataVertexIter::new(self.mm, label, 0, 0), |idx| {
                let &IndexEntry { offset, len, .. } = &self.index[idx];
                DataVertexIter::new(self.mm, label, offset as usize, len as usize)
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
            let &IndexEntry {
                vlabel,
                offset,
                len,
            } = x;
            (
                vlabel,
                DataVertexIter::new(self.mm, vlabel, offset as usize, len as usize),
            )
        })
    }
}

pub struct LocalIndex<'a> {
    mm: &'a MemoryManager,
    vlabel: VLabel,
    vid: VId,
    index: &'a [IndexEntry],
}

impl<'a> IntoIterator for LocalIndex<'a> {
    type Item = (VLabel, DataNeighborIter<'a>);
    type IntoIter = LocalIndexIntoIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        LocalIndexIntoIter {
            mm: self.mm,
            vlabel: self.vlabel,
            vid: self.vid,
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
            .map_or(
                DataNeighborIter::new(self.mm, self.vlabel, self.vid, label, 0, 0),
                |idx| {
                    let &IndexEntry { offset, len, .. } = &self.index[idx];
                    DataNeighborIter::new(
                        self.mm,
                        self.vlabel,
                        self.vid,
                        label,
                        offset as usize,
                        len as usize,
                    )
                },
            )
    }
}

pub struct LocalIndexIntoIter<'a> {
    mm: &'a MemoryManager,
    vlabel: VLabel,
    vid: VId,
    iter: std::slice::Iter<'a, IndexEntry>,
}

impl<'a> Iterator for LocalIndexIntoIter<'a> {
    type Item = (VLabel, DataNeighborIter<'a>);

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(|x| {
            let &IndexEntry {
                vlabel,
                offset,
                len,
            } = x;
            (
                vlabel,
                DataNeighborIter::new(
                    self.mm,
                    self.vlabel,
                    self.vid,
                    vlabel,
                    offset as usize,
                    len as usize,
                ),
            )
        })
    }
}

pub struct DataVertex<'a> {
    mm: &'a MemoryManager,
    pos: usize,
    vlabel: VLabel,
    vid: VId,
}

impl<'a> Vertex<LocalIndex<'a>> for DataVertex<'a> {
    fn id(&self) -> VId {
        self.vid
    }

    fn in_deg(&self) -> usize {
        unsafe { self.mm.as_ref::<VertexHeader>(self.pos as usize).in_deg as usize }
    }

    fn out_deg(&self) -> usize {
        unsafe { self.mm.as_ref::<VertexHeader>(self.pos as usize).out_deg as usize }
    }

    fn index(&self) -> LocalIndex<'a> {
        let vheader = unsafe { self.mm.as_ref::<VertexHeader>(self.pos) };
        LocalIndex {
            mm: self.mm,
            vlabel: self.vlabel,
            vid: self.vid,
            index: unsafe {
                self.mm.as_slice(
                    size_of::<u64>()
                        + size_of::<Header>()
                        + vheader.index_offset as usize * size_of::<IndexEntry>(),
                    vheader.num_vlabels as usize,
                )
            },
        }
    }
}

pub struct DataVertexIter<'a> {
    mm: &'a MemoryManager,
    vlabel: VLabel,
    iter: std::slice::Iter<'a, VId>,
}

impl<'a> DataVertexIter<'a> {
    fn new(mm: &'a MemoryManager, vlabel: VLabel, offset: usize, len: usize) -> Self {
        Self {
            mm,
            vlabel,
            iter: unsafe {
                mm.as_slice(
                    get_header(mm).vids_pos as usize + offset * size_of::<VId>(),
                    len,
                )
            }
            .iter(),
        }
    }
}

impl<'a> Iterator for DataVertexIter<'a> {
    type Item = DataVertex<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(&vid) = self.iter.next() {
            Some(DataVertex {
                mm: self.mm,
                vlabel: self.vlabel,
                pos: get_header(self.mm).vertices_pos as usize
                    + vid as usize * size_of::<VertexHeader>(),
                vid,
            })
        } else {
            None
        }
    }
}

impl<'a> ExactSizeIterator for DataVertexIter<'a> {
    fn len(&self) -> usize {
        self.iter.len()
    }
}

impl<'a> VertexIter<DataVertex<'a>> for DataVertexIter<'a> {}

pub struct DataNeighbor<'a> {
    mm: &'a MemoryManager,
    vlabel: VLabel,
    vid: VId,
    nlabel: VLabel,
    arc: &'a ArcEntry,
}

impl<'a> Neighbor for DataNeighbor<'a> {
    fn id(&self) -> VId {
        self.arc.nid
    }

    fn topology_will_match(&self, info: &NeighborInfo) -> bool {
        // assert!(
        //     info.v_to_n_elabels().len() <= 1
        //         && info.n_to_v_elabels().len() <= 1
        //         && info.undirected_elabels().len() <= 1
        // );
        info.v_to_n_elabels().contains(&self.arc.elabel)
            && if let Some(&elabel) = info.n_to_v_elabels().iter().next() {
                let nid = self.arc.nid;
                DataVertex {
                    mm: self.mm,
                    vlabel: self.nlabel,
                    pos: get_header(self.mm).vertices_pos as usize
                        + nid as usize * size_of::<VertexHeader>(),
                    vid: nid,
                }
                .index()
                .get(self.vlabel)
                .find(|v| v.arc.nid == self.vid && v.arc.elabel == elabel)
                .is_some()
            } else {
                true
            }
    }
}

pub struct DataNeighborIter<'a> {
    mm: &'a MemoryManager,
    vlabel: VLabel,
    vid: VId,
    nlabel: VLabel,
    iter: std::slice::Iter<'a, ArcEntry>,
}

impl<'a> DataNeighborIter<'a> {
    fn new(
        mm: &'a MemoryManager,
        vlabel: VLabel,
        vid: VId,
        nlabel: VLabel,
        offset: usize,
        len: usize,
    ) -> Self {
        Self {
            mm,
            vlabel,
            vid,
            nlabel,
            iter: unsafe {
                mm.as_slice(
                    get_header(mm).vertices_pos as usize + offset * size_of::<ArcEntry>(),
                    len,
                )
            }
            .iter(),
        }
    }
}

impl<'a> Iterator for DataNeighborIter<'a> {
    type Item = DataNeighbor<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(arc) = self.iter.next() {
            Some(DataNeighbor {
                mm: self.mm,
                vlabel: self.vlabel,
                vid: self.vid,
                nlabel: self.nlabel,
                arc,
            })
        } else {
            None
        }
    }
}

impl<'a> ExactSizeIterator for DataNeighborIter<'a> {
    fn len(&self) -> usize {
        self.iter.len()
    }
}

impl<'a> NeighborIter<DataNeighbor<'a>> for DataNeighborIter<'a> {}

fn get_header(mm: &MemoryManager) -> &Header {
    unsafe { mm.as_ref(size_of::<u64>()) }
}
