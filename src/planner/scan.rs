use crate::{
    pattern_graph::{Characteristic, NeighborInfo, PatternGraph},
    types::{VId, VLabel},
};
use std::collections::{BTreeMap, BTreeSet, HashMap};

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct CharacteristicInfo<'a, 'b> {
    id: usize,
    characteristic: Characteristic<'a, 'b>,
    nlabel_ninfo_eqvs: BTreeMap<VLabel, Vec<(&'a NeighborInfo<'b>, usize)>>,
}

impl<'a, 'b> CharacteristicInfo<'a, 'b> {
    pub fn id(&self) -> usize {
        self.id
    }

    pub fn characteristic(&self) -> &Characteristic<'a, 'b> {
        &self.characteristic
    }

    pub fn nlabel_ninfo_eqvs(&self) -> &BTreeMap<VLabel, Vec<(&'a NeighborInfo<'b>, usize)>> {
        &self.nlabel_ninfo_eqvs
    }
}

impl<'a, 'b> CharacteristicInfo<'a, 'b> {
    pub fn new(characteristic: Characteristic<'a, 'b>, id: usize) -> Self {
        let mut nlabel_ninfo_eqvs: BTreeMap<VLabel, Vec<(&'a NeighborInfo<'b>, usize)>> =
            BTreeMap::new();
        characteristic
            .infos()
            .iter()
            .enumerate()
            .for_each(|(i, &ninfo)| {
                nlabel_ninfo_eqvs
                    .entry(ninfo.vlabel())
                    .or_default()
                    .push((ninfo, i + 1));
            });
        Self {
            id,
            characteristic,
            nlabel_ninfo_eqvs,
        }
    }
}

#[derive(Debug, PartialEq)]
pub struct StarInfo<'a, 'b> {
    root: VId,
    vertex_cover: Vec<VId>,
    characteristic_info: CharacteristicInfo<'a, 'b>,
    vertex_eqv: HashMap<VId, usize>,
}

impl<'a, 'b> StarInfo<'a, 'b> {
    pub fn root(&self) -> VId {
        self.root
    }

    pub fn id(&self) -> usize {
        self.characteristic_info.id()
    }

    pub fn characteristic(&self) -> &Characteristic<'a, 'b> {
        self.characteristic_info.characteristic()
    }

    pub fn vertex_eqv(&self) -> &HashMap<VId, usize> {
        &self.vertex_eqv
    }
}

impl<'a, 'b> StarInfo<'a, 'b> {
    pub fn new(pattern_graph: &'a PatternGraph<'b>, root: VId, id: usize) -> Self {
        let characteristic = Characteristic::new(&pattern_graph, root);
        let mut vertex_eqv: HashMap<VId, usize> = HashMap::new();
        vertex_eqv.insert(root, 0);
        let neighbor_info_offset: HashMap<&NeighborInfo, usize> = characteristic
            .infos()
            .iter()
            .enumerate()
            .map(|(i, &info)| (info, i + 1))
            .collect();
        for (&n, info) in pattern_graph.neighbors(root).unwrap() {
            vertex_eqv.insert(n, *neighbor_info_offset.get(info).unwrap());
        }
        Self {
            root,
            vertex_eqv,
            vertex_cover: vec![root],
            characteristic_info: CharacteristicInfo::new(characteristic, id),
        }
    }
}

pub struct ScanPlan<'a, 'b> {
    plan: Vec<(VLabel, Vec<CharacteristicInfo<'a, 'b>>)>,
}

impl<'a, 'b> ScanPlan<'a, 'b> {
    pub fn new<I: IntoIterator<Item = VId>>(pattern_graph: &'b PatternGraph, roots: I) -> Self {
        let mut id = 0;
        let mut characteristic_ids = HashMap::new();
        for root in roots {
            characteristic_ids
                .entry(Characteristic::new(pattern_graph, root))
                .or_insert_with(|| {
                    let myid = id;
                    id += 1;
                    myid
                });
        }
        let mut vlabel_characteristics: BTreeMap<VLabel, BTreeSet<CharacteristicInfo>> =
            BTreeMap::new();
        for x in characteristic_ids.keys() {
            vlabel_characteristics
                .entry(x.root_vlabel())
                .or_default()
                .insert(CharacteristicInfo::new(
                    x.clone(),
                    *characteristic_ids.get(x).unwrap(),
                ));
        }
        Self {
            plan: vlabel_characteristics
                .into_iter()
                .map(|(vlabel, xs)| (vlabel, xs.into_iter().collect::<Vec<_>>()))
                .collect(),
        }
    }

    pub fn plan(&self) -> &[(VLabel, Vec<CharacteristicInfo<'a, 'b>>)] {
        &self.plan
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_pattern_graph<'a>() -> PatternGraph<'a> {
        let mut p = PatternGraph::new();
        for (u, l) in vec![(1, 1), (2, 2), (3, 2)] {
            p.add_vertex(u, l);
        }
        for (u1, u2, l) in vec![(1, 2, 0), (1, 3, 0)] {
            p.add_arc(u1, u2, l);
        }
        p
    }

    fn create_pattern_graph2<'a>() -> PatternGraph<'a> {
        let mut p = PatternGraph::new();
        for (vid, vlabel) in vec![(1, 0), (2, 1), (3, 2), (4, 1), (5, 0)] {
            p.add_vertex(vid, vlabel);
        }
        for (u1, u2, elabel) in vec![(1, 2, 0), (1, 3, 0), (5, 2, 0), (5, 4, 0)] {
            p.add_arc(u1, u2, elabel);
        }
        p
    }

    #[test]
    fn test_scan_plan1() {
        let pattern_graph = create_pattern_graph();
        let scan_plan = ScanPlan::new(&pattern_graph, vec![1]);
        assert_eq!(
            scan_plan.plan(),
            &[(
                1,
                vec![CharacteristicInfo::new(
                    Characteristic::new(&pattern_graph, 1),
                    0
                )]
            )]
        );
    }

    #[test]
    fn test_scan_plan2() {
        let pattern_graph = create_pattern_graph2();
        let scan_plan = ScanPlan::new(&pattern_graph, vec![1, 2, 5]);
        assert_eq!(
            scan_plan.plan(),
            &[
                (
                    0,
                    vec![
                        CharacteristicInfo::new(Characteristic::new(&pattern_graph, 1), 0),
                        CharacteristicInfo::new(Characteristic::new(&pattern_graph, 5), 2)
                    ]
                ),
                (
                    1,
                    vec![CharacteristicInfo::new(
                        Characteristic::new(&pattern_graph, 2),
                        1
                    )]
                )
            ]
        );
    }
}
