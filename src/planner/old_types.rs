use crate::{
    pattern_graph::{Characteristic, NeighborInfo, PatternGraph},
    types::{VId, VLabel},
};
use std::collections::{BTreeMap, HashMap};

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

#[derive(Clone)]
pub enum IndexType {
    Sorted,
    Hash,
}
