//! The planner.

use crate::{
    compiler::ast::Expr,
    data_graph::DataGraph,
    memory_manager::{MemoryManager, MmapFile},
    pattern_graph::{Characteristic, NeighborInfo, PatternGraph},
    planner::decompose_stars,
    types::{VId, VLabel},
};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::PathBuf;

pub enum MemoryManagerType {
    Mem,
    Mmap(PathBuf),
    Sink,
}

pub struct Planner<'a, 'b> {
    data_graph: &'a DataGraph,
    pattern_graph: &'a PatternGraph<'b>,
    global_constraints: Vec<Expr>,
    super_row_mm_type: MemoryManagerType,
    index_mm_type: MemoryManagerType,
}

impl<'a, 'b> Planner<'a, 'b> {
    pub fn new(
        data_graph: &'a DataGraph,
        pattern_graph: &'a PatternGraph<'b>,
        global_constraints: Vec<Expr>,
    ) -> Self {
        Self {
            data_graph,
            pattern_graph,
            global_constraints,
            super_row_mm_type: MemoryManagerType::Mem,
            index_mm_type: MemoryManagerType::Mem,
        }
    }

    pub fn super_row_mm_type(&mut self, mm_type: MemoryManagerType) -> &mut Self {
        self.super_row_mm_type = mm_type;
        self
    }

    pub fn index_mm_type(&mut self, mm_type: MemoryManagerType) -> &mut Self {
        self.index_mm_type = mm_type;
        self
    }

    pub fn plan(&self) -> Plan<'a, 'b> {
        let roots = decompose_stars(self.data_graph, self.pattern_graph);
        let mut characteristic_roots: BTreeMap<Characteristic, Vec<VId>> = BTreeMap::new();
        for &root in roots.iter() {
            characteristic_roots
                .entry(Characteristic::new(self.pattern_graph, root))
                .and_modify(|xs| xs.push(root))
                .or_insert(vec![root]);
        }
        let mut vlabel_characteristics: BTreeMap<VLabel, Vec<&Characteristic>> = BTreeMap::new();
        for x in characteristic_roots.keys() {
            vlabel_characteristics
                .entry(x.root_vlabel())
                .and_modify(|xs| xs.push(x))
                .or_insert(vec![x]);
        }
        let mut id = 0;
        let stars_plan: Vec<(VLabel, Vec<(Characteristic, CharacteristicPlan)>)> =
            vlabel_characteristics
                .iter()
                .map(|(&vlabel, xs)| {
                    (
                        vlabel,
                        xs.iter()
                            .map(|&x| {
                                let res = (
                                    x.clone(),
                                    CharacteristicPlan::new(
                                        self.pattern_graph,
                                        characteristic_roots.get(x).unwrap(),
                                        id,
                                    ),
                                );
                                id += 1;
                                res
                            })
                            .collect(),
                    )
                })
                .collect();
        let super_row_mms: Vec<MemoryManager> = (0..2 * roots.len() - 1)
            .map(|id| match &self.super_row_mm_type {
                MemoryManagerType::Mem => MemoryManager::Mem(vec![]),
                MemoryManagerType::Mmap(path) => {
                    MemoryManager::Mmap(MmapFile::new(path.join(format!("{}.sr", id))))
                }
                MemoryManagerType::Sink => MemoryManager::Sink,
            })
            .collect();
        let index_mms: Vec<MemoryManager> = (0..roots.len())
            .map(|id| match &self.index_mm_type {
                MemoryManagerType::Mem => MemoryManager::Mem(vec![]),
                MemoryManagerType::Mmap(path) => {
                    MemoryManager::Mmap(MmapFile::new(path.join(format!("{}.idx", id))))
                }
                MemoryManagerType::Sink => MemoryManager::Sink,
            })
            .collect();
        Plan {
            data_graph: self.data_graph,
            pattern_graph: self.pattern_graph,
            roots,
            super_row_mms,
            index_mms,
            stars_plan,
        }
    }
}

#[derive(Debug, PartialEq)]
pub struct StarInfo {
    root: VId,
    vertex_eqv: BTreeMap<VId, usize>,
}

impl StarInfo {
    fn new(pattern_graph: &PatternGraph, root: VId) -> Self {
        let mut vertex_eqv: BTreeMap<VId, usize> = BTreeMap::new();
        vertex_eqv.insert(root, 0);
        let neighbors = pattern_graph.neighbors(root).unwrap();
        let neighbor_infos: BTreeSet<&NeighborInfo> =
            neighbors.iter().map(|(_, info)| info).collect();
        let neighbor_info_offset: HashMap<&NeighborInfo, usize> = neighbor_infos
            .iter()
            .enumerate()
            .map(|(i, &info)| (info, i + 1))
            .collect();
        for (&n, info) in neighbors {
            vertex_eqv.insert(n, *neighbor_info_offset.get(info).unwrap());
        }
        Self { root, vertex_eqv }
    }
}

#[derive(Debug, PartialEq)]
pub struct CharacteristicPlan {
    id: usize,
    stars: BTreeMap<VId, StarInfo>,
}

impl CharacteristicPlan {
    pub fn id(&self) -> usize {
        self.id
    }

    pub fn stars(&self) -> &BTreeMap<VId, StarInfo> {
        &self.stars
    }
}

// private methods.
impl CharacteristicPlan {
    fn new(pattern_graph: &PatternGraph, roots: &[VId], id: usize) -> Self {
        let stars: BTreeMap<VId, StarInfo> = roots
            .iter()
            .map(|&root| (root, StarInfo::new(pattern_graph, root)))
            .collect();
        Self { id, stars }
    }
}

pub struct Plan<'a, 'b> {
    data_graph: &'a DataGraph,
    pattern_graph: &'a PatternGraph<'b>,
    roots: Vec<VId>,
    super_row_mms: Vec<MemoryManager>,
    index_mms: Vec<MemoryManager>,
    stars_plan: Vec<(VLabel, Vec<(Characteristic<'a, 'b>, CharacteristicPlan)>)>,
}

impl<'a, 'b> Plan<'a, 'b> {
    pub fn data_graph(&self) -> &'a DataGraph {
        self.data_graph
    }

    pub fn pattern_graph(&self) -> &'a PatternGraph<'b> {
        self.pattern_graph
    }

    pub fn stars_plan(&self) -> &Vec<(VLabel, Vec<(Characteristic<'a, 'b>, CharacteristicPlan)>)> {
        &self.stars_plan
    }

    pub fn super_row_mms(&mut self) -> &mut Vec<MemoryManager> {
        &mut self.super_row_mms
    }

    pub fn index_mms(&mut self) -> &mut Vec<MemoryManager> {
        &mut self.index_mms
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data_graph::mm_read_iter;
    use std::collections::HashSet;

    fn create_data_graph() -> DataGraph {
        let mut mm = MemoryManager::Mem(vec![]);
        let vertices = vec![(1, 1), (2, 2), (3, 2), (4, 2)];
        let edges = vec![(1, 2, 0), (1, 3, 0), (1, 4, 0)];
        mm_read_iter(
            &mut mm,
            vertices
                .iter()
                .map(|(_, l)| l)
                .collect::<HashSet<_>>()
                .len(),
            vertices.len(),
            edges.len(),
            vertices,
            edges,
        );
        DataGraph::new(mm)
    }

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

    #[test]
    fn test_stars_plan() {
        let data_graph = create_data_graph();
        let pattern_graph = create_pattern_graph();
        let mut plan = Planner::new(&data_graph, &pattern_graph, vec![]).plan();
        let data: &[u8] = &[1, 2, 3, 4];
        plan.super_row_mms()[0].resize(data.len());
        plan.super_row_mms()[0].write(0, data.as_ptr(), data.len());
        plan.index_mms()[0].resize(data.len());
        plan.index_mms()[0].write(0, data.as_ptr(), data.len());
        assert_eq!(
            plan.stars_plan(),
            &vec![(
                1,
                vec![(
                    Characteristic::new(&pattern_graph, 1),
                    CharacteristicPlan::new(&pattern_graph, &[1], 0)
                )]
            )]
        );
    }
}
