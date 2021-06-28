use crate::{
    tools::GroupBy,
    types::{ELabel, VId, VLabel},
};
use std::collections::{BTreeMap, BTreeSet, HashMap};

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct GraphView {
    index: BTreeMap<VLabel, BTreeSet<VertexView>>,
}

impl GraphView {
    pub fn new<I, V>(index: I) -> Self
    where
        I: IntoIterator<Item = (VLabel, V)>,
        V: IntoIterator<Item = VertexView>,
    {
        Self {
            index: index
                .into_iter()
                .map(|(l, vs)| (l, vs.into_iter().collect()))
                .collect(),
        }
    }

    pub fn from_iter<V, E>(vertices: V, edges: E) -> Self
    where
        V: IntoIterator<Item = (VId, VLabel)>,
        E: IntoIterator<Item = (VId, VId, ELabel)>,
    {
        let vid_vlabel_map: HashMap<VId, VLabel> = vertices.into_iter().collect();
        let mut info_edges: Vec<(VLabel, VId, VLabel, VId, bool, ELabel)> = edges
            .into_iter()
            .flat_map(|(src, dst, elabel)| {
                let (src_label, dst_label) = (
                    *vid_vlabel_map.get(&src).unwrap(),
                    *vid_vlabel_map.get(&dst).unwrap(),
                );
                vec![
                    (dst_label, dst, src_label, src, false, elabel),
                    (src_label, src, dst_label, dst, true, elabel),
                ]
            })
            .collect();
        info_edges.sort();
        // This is crazy:
        Self {
            index: GroupBy::new(&info_edges, |e| e.0)
                .map(|(vlabel, vlabel_group)| {
                    (
                        vlabel,
                        GroupBy::new(vlabel_group, |e| e.1)
                            .map(|(vid, vid_group)| {
                                VertexView::new(
                                    vid,
                                    GroupBy::new(vid_group, |e| e.2).map(
                                        |(nlabel, nlabel_group)| {
                                            (
                                                nlabel,
                                                GroupBy::new(nlabel_group, |e| e.3).map(
                                                    |(nid, nid_group)| {
                                                        let (mut n_to_v, mut v_to_n) =
                                                            (vec![], vec![]);
                                                        for &(_, _, _, _, dir, e) in nid_group {
                                                            if dir {
                                                                v_to_n.push(e);
                                                            } else {
                                                                n_to_v.push(e);
                                                            }
                                                        }
                                                        NeighborView::new(nid, n_to_v, v_to_n)
                                                    },
                                                ),
                                            )
                                        },
                                    ),
                                )
                            })
                            .collect(),
                    )
                })
                .collect(),
        }
    }
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct VertexView {
    id: VId,
    index: BTreeMap<VLabel, BTreeSet<NeighborView>>,
}

impl VertexView {
    pub fn new<V, E>(id: VId, index: V) -> Self
    where
        V: IntoIterator<Item = (VLabel, E)>,
        E: IntoIterator<Item = NeighborView>,
    {
        Self {
            id,
            index: index
                .into_iter()
                .map(|(l, ns)| (l, ns.into_iter().collect()))
                .collect(),
        }
    }
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct NeighborView {
    id: VId,
    n_to_v_elabels: BTreeSet<ELabel>,
    v_to_n_elabels: BTreeSet<ELabel>,
}

impl NeighborView {
    pub fn new<I, O>(id: VId, n_to_v_elabels: I, v_to_n_elabels: O) -> Self
    where
        I: IntoIterator<Item = ELabel>,
        O: IntoIterator<Item = ELabel>,
    {
        Self {
            id,
            n_to_v_elabels: n_to_v_elabels.into_iter().collect(),
            v_to_n_elabels: v_to_n_elabels.into_iter().collect(),
        }
    }
}
