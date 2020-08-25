//! The planner.

use crate::{
    compiler::ast::Expr,
    data_graph::DataGraph,
    executor::match_characteristics,
    memory_manager::{MemoryManager, MmapFile},
    pattern_graph::{Characteristic, NeighborInfo, PatternGraph},
    planner::decompose_stars,
    types::{GlobalConstraint, VId, VLabel},
};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::path::PathBuf;
use std::rc::Rc;

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

    pub fn plan(self) -> Plan<'a, 'b> {
        let roots = decompose_stars(self.data_graph, self.pattern_graph);
        let characteristic_ids = self.create_characteristic_ids(&roots);
        let stars = self.create_stars(&roots, &characteristic_ids);
        let stars_plan = self.create_stars_plan(&characteristic_ids);
        let join_plan = self.create_join_plan(&stars, characteristic_ids.len());
        Plan {
            data_graph: self.data_graph,
            pattern_graph: self.pattern_graph,
            super_row_mm_type: self.super_row_mm_type,
            super_row_mm_len: characteristic_ids.len() + roots.len() - 1,
            index_mm_type: self.index_mm_type,
            index_mm_len: characteristic_ids.len(),
            stars,
            stars_plan,
            join_plan,
        }
    }
}

// private methods.
impl<'a, 'b> Planner<'a, 'b> {
    fn create_characteristic_ids(&self, roots: &[VId]) -> HashMap<Characteristic<'a, 'b>, usize> {
        roots
            .iter()
            .map(|&root| Characteristic::new(self.pattern_graph, root))
            .into_iter()
            .enumerate()
            .map(|(i, x)| (x, i))
            .collect()
    }

    fn create_stars(
        &self,
        roots: &[VId],
        characteristic_ids: &HashMap<Characteristic<'a, 'b>, usize>,
    ) -> Vec<StarInfo<'a, 'b>> {
        roots
            .iter()
            .map(|&root| {
                StarInfo::new(
                    self.pattern_graph,
                    root,
                    *characteristic_ids
                        .get(&Characteristic::new(self.pattern_graph, root))
                        .unwrap(),
                )
            })
            .collect()
    }

    fn create_stars_plan(
        &self,
        characteristic_ids: &HashMap<Characteristic<'a, 'b>, usize>,
    ) -> Vec<(VLabel, Vec<CharacteristicInfo<'a, 'b>>)> {
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
        vlabel_characteristics
            .into_iter()
            .map(|(vlabel, xs)| (vlabel, xs.into_iter().collect()))
            .collect()
    }

    fn create_join_plan(&self, stars: &[StarInfo], id: usize) -> Vec<JoinInfo> {
        if stars.len() < 2 {
            vec![]
        } else {
            vec![JoinInfo::new(&stars[0], &stars[1], id)]
        }
    }
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct CharacteristicInfo<'a, 'b> {
    id: usize,
    characteristic: Characteristic<'a, 'b>,
    nlabel_ninfo_eqv: BTreeMap<VLabel, Vec<(&'a NeighborInfo<'b>, usize)>>,
}

impl<'a, 'b> CharacteristicInfo<'a, 'b> {
    pub fn id(&self) -> usize {
        self.id
    }

    pub fn characteristic(&self) -> &Characteristic<'a, 'b> {
        &self.characteristic
    }

    pub fn nlabel_ninfo_eqv(&self) -> &BTreeMap<VLabel, Vec<(&'a NeighborInfo<'b>, usize)>> {
        &self.nlabel_ninfo_eqv
    }
}

// private methods.
impl<'a, 'b> CharacteristicInfo<'a, 'b> {
    fn new(characteristic: Characteristic<'a, 'b>, id: usize) -> Self {
        let mut nlabel_ninfo_eqv: BTreeMap<VLabel, Vec<(&'a NeighborInfo<'b>, usize)>> =
            BTreeMap::new();
        characteristic
            .infos()
            .iter()
            .enumerate()
            .for_each(|(eqv, &ninfo)| {
                nlabel_ninfo_eqv
                    .entry(ninfo.vlabel())
                    .or_default()
                    .push((ninfo, eqv));
            });
        Self {
            id,
            characteristic,
            nlabel_ninfo_eqv,
        }
    }
}

#[derive(Debug, PartialEq)]
pub struct StarInfo<'a, 'b> {
    root: VId,
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

// private methods.
impl<'a, 'b> StarInfo<'a, 'b> {
    fn new(pattern_graph: &'a PatternGraph<'b>, root: VId, id: usize) -> Self {
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
            characteristic_info: CharacteristicInfo::new(characteristic, id),
        }
    }
}

#[derive(Debug, PartialEq)]
pub struct JoinInfo {
    id: usize,
    left_id: usize,
    right_id: usize,
    vertex_cover: Vec<VId>,
    vertex_eqv: HashMap<VId, usize>,
    global_constraint: Option<GlobalConstraint>,
    indexed_intersection: usize,                  // left_eqv
    sequential_intersection: Vec<(usize, usize)>, // (left_eqv, right_eqv)
    left_keep: Vec<usize>,                        // left_eqv
    right_keep: Vec<usize>,                       // right_eqv
}

impl JoinInfo {
    fn new(left: &StarInfo, right: &StarInfo, id: usize) -> Self {
        let (vertex_eqv, indexed_intersection, sequential_intersection, left_keep, right_keep) =
            Self::create_eqv_intersection_keep(left, right);
        Self {
            id,
            left_id: left.id(),
            right_id: right.id(),
            vertex_cover: vec![left.root(), right.root()],
            vertex_eqv,
            global_constraint: None, // TODO!!!
            indexed_intersection,
            sequential_intersection,
            left_keep,
            right_keep,
        }
    }

    fn create_eqv_intersection_keep(
        left: &StarInfo,
        right: &StarInfo,
    ) -> (
        HashMap<VId, usize>,
        usize,
        Vec<(usize, usize)>,
        Vec<usize>,
        Vec<usize>,
    ) {
        let left_vertices: HashSet<VId> = left.vertex_eqv().keys().map(|&x| x).collect();
        let right_vertices: HashSet<VId> = right.vertex_eqv().keys().map(|&x| x).collect();
        let mut vertex_eqv: HashMap<VId, usize> = vec![(left.root(), 0), (right.root(), 1)]
            .into_iter()
            .collect();
        let mut eqv = 2;
        let indexed_intersection = Self::create_indexed_intersection(left, right);
        let sequential_intersection = Self::create_sequential_intersection(
            &mut vertex_eqv,
            &mut eqv,
            left,
            right,
            &left_vertices,
            &right_vertices,
        );
        let left_keep = Self::create_left_keep(
            &mut vertex_eqv,
            &mut eqv,
            left,
            &left_vertices,
            &right_vertices,
        );
        let right_keep = Self::create_right_keep(
            &mut vertex_eqv,
            &mut eqv,
            right,
            &left_vertices,
            &right_vertices,
        );
        (
            vertex_eqv,
            indexed_intersection,
            sequential_intersection,
            left_keep,
            right_keep,
        )
    }

    fn create_indexed_intersection(left: &StarInfo, right: &StarInfo) -> usize {
        *left.vertex_eqv().get(&right.root()).unwrap()
    }

    fn create_sequential_intersection_info(
        left: &StarInfo,
        right: &StarInfo,
        left_vertices: &HashSet<VId>,
        right_vertices: &HashSet<VId>,
    ) -> BTreeMap<(usize, usize), Vec<VId>> {
        let mut vertices: HashSet<VId> = left_vertices
            .intersection(&right_vertices)
            .map(|&x| x)
            .collect();
        vertices.remove(&left.root());
        vertices.remove(&right.root());
        let mut info: BTreeMap<(usize, usize), Vec<VId>> = BTreeMap::new();
        for v in vertices {
            info.entry((
                *left.vertex_eqv().get(&v).unwrap(),
                *right.vertex_eqv().get(&v).unwrap(),
            ))
            .or_default()
            .push(v);
        }
        info
    }

    fn create_sequential_intersection(
        vertex_eqv: &mut HashMap<VId, usize>,
        eqv: &mut usize,
        left: &StarInfo,
        right: &StarInfo,
        left_vertices: &HashSet<VId>,
        right_vertices: &HashSet<VId>,
    ) -> Vec<(usize, usize)> {
        Self::create_sequential_intersection_info(left, right, &left_vertices, &right_vertices)
            .into_iter()
            .fold(vec![], |mut xs, (l_r_eqv, vertices)| {
                vertices.into_iter().for_each(|v| {
                    vertex_eqv.insert(v, *eqv);
                });
                *eqv += 1;
                xs.push(l_r_eqv);
                xs
            })
    }

    fn create_left_keep_info(
        left: &StarInfo,
        left_vertices: &HashSet<VId>,
        right_vertices: &HashSet<VId>,
    ) -> BTreeMap<usize, Vec<VId>> {
        let mut vertices: HashSet<VId> = left_vertices
            .difference(&right_vertices)
            .map(|&x| x)
            .collect();
        vertices.remove(&left.root());
        let mut info: BTreeMap<usize, Vec<VId>> = BTreeMap::new();
        for v in vertices {
            info.entry(*left.vertex_eqv().get(&v).unwrap())
                .or_default()
                .push(v);
        }
        info
    }

    fn create_left_keep(
        vertex_eqv: &mut HashMap<VId, usize>,
        eqv: &mut usize,
        left: &StarInfo,
        left_vertices: &HashSet<VId>,
        right_vertices: &HashSet<VId>,
    ) -> Vec<usize> {
        Self::create_left_keep_info(left, left_vertices, right_vertices)
            .into_iter()
            .fold(vec![], |mut xs, (left_eqv, vertices)| {
                vertices.into_iter().for_each(|v| {
                    vertex_eqv.insert(v, *eqv);
                });
                *eqv += 1;
                xs.push(left_eqv);
                xs
            })
    }

    fn create_right_keep_info(
        right: &StarInfo,
        left_vertices: &HashSet<VId>,
        right_vertices: &HashSet<VId>,
    ) -> BTreeMap<usize, Vec<VId>> {
        let mut vertices: HashSet<VId> = left_vertices
            .difference(&right_vertices)
            .map(|&x| x)
            .collect();
        vertices.remove(&right.root());
        let mut info: BTreeMap<usize, Vec<VId>> = BTreeMap::new();
        for v in vertices {
            info.entry(*right.vertex_eqv().get(&v).unwrap())
                .or_default()
                .push(v);
        }
        info
    }

    fn create_right_keep(
        vertex_eqv: &mut HashMap<VId, usize>,
        eqv: &mut usize,
        right: &StarInfo,
        left_vertices: &HashSet<VId>,
        right_vertices: &HashSet<VId>,
    ) -> Vec<usize> {
        Self::create_right_keep_info(right, left_vertices, right_vertices)
            .into_iter()
            .fold(vec![], |mut xs, (left_eqv, vertices)| {
                vertices.into_iter().for_each(|v| {
                    vertex_eqv.insert(v, *eqv);
                });
                *eqv += 1;
                xs.push(left_eqv);
                xs
            })
    }
}

/// The plan to match the `pattern_graph` in `data_graph`.
pub struct Plan<'a, 'b> {
    data_graph: &'a DataGraph,
    pattern_graph: &'a PatternGraph<'b>,
    super_row_mm_type: MemoryManagerType,
    super_row_mm_len: usize,
    index_mm_type: MemoryManagerType,
    index_mm_len: usize,
    stars: Vec<StarInfo<'a, 'b>>,
    stars_plan: Vec<(VLabel, Vec<CharacteristicInfo<'a, 'b>>)>,
    join_plan: Vec<JoinInfo>,
}

impl<'a, 'b> Plan<'a, 'b> {
    pub fn data_graph(&self) -> &'a DataGraph {
        self.data_graph
    }

    pub fn pattern_graph(&self) -> &'a PatternGraph<'b> {
        self.pattern_graph
    }

    /// Returns the stars decomposed from the `pattern_graph`.
    ///
    /// The order is the join order.
    pub fn stars(&self) -> &[StarInfo<'a, 'b>] {
        &self.stars
    }

    /// Returns the stars matching plan.
    ///
    /// The `infos` are grouped by the `VLabel` and they are sorted by the `VLabel` such that
    /// the huge data graph file could be scanned only once sequentially.
    /// For each group, the `infos` are sorted by the `id`.
    pub fn stars_plan(&self) -> &[(VLabel, Vec<CharacteristicInfo<'a, 'b>>)] {
        &self.stars_plan
    }

    pub fn join_plan(&self) -> &[JoinInfo] {
        &self.join_plan
    }

    /// Execute the plan.
    pub fn execute(&self) {}
}

// private methods.
impl<'a, 'b> Plan<'a, 'b> {
    fn create_super_row_mms(&self, count: usize) -> Vec<MemoryManager> {
        (0..count)
            .map(|id| match &self.super_row_mm_type {
                MemoryManagerType::Mem => MemoryManager::Mem(vec![]),
                MemoryManagerType::Mmap(path) => {
                    MemoryManager::Mmap(MmapFile::new(path.join(format!("{}.sr", id))))
                }
                MemoryManagerType::Sink => MemoryManager::Sink,
            })
            .collect()
    }

    fn create_index_mms(&self, count: usize) -> Vec<MemoryManager> {
        (0..count)
            .map(|id| match &self.index_mm_type {
                MemoryManagerType::Mem => MemoryManager::Mem(vec![]),
                MemoryManagerType::Mmap(path) => {
                    MemoryManager::Mmap(MmapFile::new(path.join(format!("{}.idx", id))))
                }
                MemoryManagerType::Sink => MemoryManager::Sink,
            })
            .collect()
    }

    /// Returns `(super_row_mms, index_mms)`.
    fn allocate(&self) -> (Vec<MemoryManager>, Vec<MemoryManager>) {
        (
            self.create_super_row_mms(self.super_row_mm_len),
            self.create_index_mms(self.index_mm_len),
        )
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
        let plan = Planner::new(&data_graph, &pattern_graph, vec![]).plan();
        assert_eq!(
            plan.stars_plan(),
            &[(
                1,
                vec![CharacteristicInfo::new(
                    Characteristic::new(&pattern_graph, 1),
                    0
                )]
            )]
        );
    }
}
