//! The planner.

use crate::{
    compiler::{
        ast::Expr,
        generator::{
            extract_global_constraint, extract_vertex_cover_constraint, extract_vertices,
            EdgeConstraintsInfo, VertexConstraintsInfo,
        },
    },
    data_graph::DataGraph,
    executor::{decompress, join, match_characteristics, write_results},
    memory_manager::{MemoryManager, MmapFile},
    pattern_graph::{Characteristic, NeighborInfo, PatternGraph},
    planner::decompose_stars,
    types::{GlobalConstraint, VId, VLabel, VertexCoverConstraint},
};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::io::Write;
use std::path::PathBuf;

pub enum MemoryManagerType<'a> {
    Mem,
    Mmap(PathBuf, &'a str), // (directory, name)
    Sink,
}

pub struct Task<'a, 'b> {
    data_graph: &'a DataGraph,
    pattern_graph: PatternGraph<'b>,
    gcs: Vec<Expr>,
}

impl<'a, 'b> Task<'a, 'b> {
    pub fn new(
        data_graph: &'a DataGraph,
        mut pattern_graph: PatternGraph<'b>,
        vc_info: &'b VertexConstraintsInfo,
        ec_info: &'b EdgeConstraintsInfo,
        gcs: Vec<Expr>,
    ) -> Self {
        vc_info.add_to_graph(&mut pattern_graph);
        ec_info.add_to_graph(&mut pattern_graph);
        Self {
            data_graph,
            pattern_graph,
            gcs,
        }
    }

    pub fn prepare<'c>(&'a mut self) -> Planner<'a, 'b, 'c> {
        Planner::new(self.data_graph, &self.pattern_graph, &mut self.gcs)
    }
}

pub struct Planner<'a, 'b, 'c> {
    data_graph: &'a DataGraph,
    pattern_graph: &'a PatternGraph<'b>,
    global_constraints: &'a mut Vec<Expr>,
    star_sr_mm_type: MemoryManagerType<'c>,
    join_sr_mm_type: MemoryManagerType<'c>,
    index_mm_type: MemoryManagerType<'c>,
}

impl<'a, 'b, 'c> Planner<'a, 'b, 'c> {
    pub fn new(
        data_graph: &'a DataGraph,
        pattern_graph: &'a PatternGraph<'b>,
        global_constraints: &'a mut Vec<Expr>,
    ) -> Self {
        Self {
            data_graph,
            pattern_graph,
            global_constraints,
            star_sr_mm_type: MemoryManagerType::Mem,
            join_sr_mm_type: MemoryManagerType::Mem,
            index_mm_type: MemoryManagerType::Mem,
        }
    }

    pub fn star_sr_mm_type(mut self, mm_type: MemoryManagerType<'c>) -> Self {
        self.star_sr_mm_type = mm_type;
        self
    }

    pub fn join_sr_mm_type(mut self, mm_type: MemoryManagerType<'c>) -> Self {
        self.join_sr_mm_type = mm_type;
        self
    }

    pub fn index_mm_type(mut self, mm_type: MemoryManagerType<'c>) -> Self {
        self.index_mm_type = mm_type;
        self
    }

    pub fn plan(self) -> Plan<'a, 'b, 'c> {
        let roots = decompose_stars(self.data_graph, self.pattern_graph);
        let characteristic_ids = self.create_characteristic_ids(&roots);
        let stars = self.create_stars(&roots, &characteristic_ids);
        let stars_plan = self.create_stars_plan(&characteristic_ids);
        let mut global_constraints = std::mem::replace(self.global_constraints, vec![]);
        let join_plan =
            self.create_join_plan(&stars, characteristic_ids.len(), &mut global_constraints);
        let global_constraint = if stars.is_empty() {
            None
        } else {
            extract_global_constraint(
                &mut global_constraints,
                if join_plan.is_empty() {
                    stars.last().unwrap().vertex_eqv()
                } else {
                    join_plan.last().unwrap().vertex_eqv()
                },
            )
        };
        Plan {
            data_graph: self.data_graph,
            pattern_graph: self.pattern_graph,
            star_sr_mm_type: self.star_sr_mm_type,
            join_sr_mm_type: self.join_sr_mm_type,
            star_sr_mms_len: characteristic_ids.len(),
            join_sr_mms_len: roots.len() - 1,
            index_mm_type: self.index_mm_type,
            index_mm_len: characteristic_ids.len(),
            stars,
            stars_plan,
            join_plan,
            global_constraint,
        }
    }
}

// private methods.
impl<'a, 'b, 'c> Planner<'a, 'b, 'c> {
    fn create_characteristic_ids(&self, roots: &[VId]) -> HashMap<Characteristic<'a, 'b>, usize> {
        let mut id = 0;
        let mut characteristic_ids = HashMap::with_capacity(roots.len());
        for &root in roots {
            characteristic_ids
                .entry(Characteristic::new(self.pattern_graph, root))
                .or_insert_with(|| {
                    let myid = id;
                    id += 1;
                    myid
                });
        }
        characteristic_ids
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

    fn create_join_plan(
        &self,
        stars: &[StarInfo],
        mut id: usize,
        global_constraints: &mut Vec<Expr>,
    ) -> Vec<JoinInfo> {
        if stars.len() < 2 {
            vec![]
        } else {
            let mut join_infos = vec![JoinInfo::new(global_constraints, &stars[0], &stars[1], id)];
            id += 1;
            for right in stars.iter().skip(2) {
                join_infos.push(JoinInfo::new(
                    global_constraints,
                    join_infos.last().unwrap(),
                    right,
                    id,
                ));
                id += 1;
            }
            let mut gc_verticeses: Vec<(&Expr, HashSet<VId>)> = global_constraints
                .iter()
                .map(|gc| (gc, extract_vertices(gc)))
                .collect();
            for info in join_infos.iter_mut() {
                let info_vertices: HashSet<VId> = info.vertex_eqv().keys().map(|&x| x).collect();
                let info_cover: HashSet<VId> = info.vertex_cover().iter().map(|&x| x).collect();
                let mut info_gcs = Vec::with_capacity(global_constraints.len());
                gc_verticeses =
                    gc_verticeses
                        .into_iter()
                        .fold(vec![], |mut gcvs, (gc, gc_vertices)| {
                            if gc_vertices.is_subset(&info_vertices)
                                && gc_vertices.difference(&info_cover).count() <= 1
                            {
                                info_gcs.push(gc);
                            } else {
                                gcvs.push((gc, gc_vertices));
                            }
                            gcvs
                        });
            }
            join_infos
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
    pub(crate) fn new(characteristic: Characteristic<'a, 'b>, id: usize) -> Self {
        let mut nlabel_ninfo_eqv: BTreeMap<VLabel, Vec<(&'a NeighborInfo<'b>, usize)>> =
            BTreeMap::new();
        characteristic
            .infos()
            .iter()
            .enumerate()
            .for_each(|(i, &ninfo)| {
                nlabel_ninfo_eqv
                    .entry(ninfo.vlabel())
                    .or_default()
                    .push((ninfo, i + 1));
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
            vertex_cover: vec![root],
            characteristic_info: CharacteristicInfo::new(characteristic, id),
        }
    }

    fn vertex_cover(&self) -> &[VId] {
        self.vertex_cover.as_slice()
    }
}

/// The `JoinableInfo` can be binary joined by a `StarInfo`.
pub trait JoinableInfo {
    fn id(&self) -> usize;

    fn vertex_cover(&self) -> &[VId];

    fn vertex_eqv(&self) -> &HashMap<VId, usize>;
}

impl<'a, 'b> JoinableInfo for StarInfo<'a, 'b> {
    fn id(&self) -> usize {
        self.id()
    }

    fn vertex_cover(&self) -> &[VId] {
        self.vertex_cover()
    }

    fn vertex_eqv(&self) -> &HashMap<VId, usize> {
        self.vertex_eqv()
    }
}

impl JoinableInfo for JoinInfo {
    fn id(&self) -> usize {
        self.id()
    }

    fn vertex_cover(&self) -> &[VId] {
        self.vertex_cover()
    }

    fn vertex_eqv(&self) -> &HashMap<VId, usize> {
        self.vertex_eqv()
    }
}

#[derive(Debug, PartialEq)]
pub struct JoinInfo {
    id: usize,
    left_id: usize,
    right_id: usize,
    vertex_cover: Vec<VId>,
    num_eqvs: usize,
    vertex_eqv: HashMap<VId, usize>,
    /// left_eqv
    indexed_intersection: usize,
    /// (left_eqv, right_eqv, constraint)
    sequential_intersections: Vec<(usize, usize, Option<VertexCoverConstraint>)>,
    /// (left_eqv, constraint)
    left_keeps: Vec<(usize, Option<VertexCoverConstraint>)>,
    /// (right_eqv, constraint)
    right_keeps: Vec<(usize, Option<VertexCoverConstraint>)>,
}

impl JoinInfo {
    /// The `id` of the `SuperRow` file for the join result.
    pub fn id(&self) -> usize {
        self.id
    }

    /// The `id` of the left join operand.
    pub fn left_id(&self) -> usize {
        self.left_id
    }

    /// The `id` of the right join operand.
    pub fn right_id(&self) -> usize {
        self.right_id
    }

    /// The vertex cover of the joined graph.
    pub fn vertex_cover(&self) -> &[VId] {
        self.vertex_cover.as_slice()
    }

    /// Number of equivalence classes.
    pub fn num_eqvs(&self) -> usize {
        self.num_eqvs
    }

    /// The mapping from the vertex to the equivalence class id `eqv`.
    pub fn vertex_eqv(&self) -> &HashMap<VId, usize> {
        &self.vertex_eqv
    }

    /// The `left_eqv` to apply the index-based join.
    pub fn indexed_intersection(&self) -> usize {
        self.indexed_intersection
    }

    /// The `(left_eqv, right_eqv, constraint)`s to apply the sequential set intersection.
    pub fn sequential_intersections(&self) -> &[(usize, usize, Option<VertexCoverConstraint>)] {
        self.sequential_intersections.as_slice()
    }

    /// The `(left_eqv, constraint)`s to write down directly.
    pub fn left_keeps(&self) -> &[(usize, Option<VertexCoverConstraint>)] {
        self.left_keeps.as_slice()
    }

    /// The `(right_eqv, constraint)`s to write down directly.
    pub fn right_keeps(&self) -> &[(usize, Option<VertexCoverConstraint>)] {
        self.right_keeps.as_slice()
    }
}

// private methods.
impl JoinInfo {
    fn new<L: JoinableInfo>(
        global_constraints: &mut Vec<Expr>,
        left: &L,
        right: &StarInfo,
        id: usize,
    ) -> Self {
        let (
            vertex_eqv,
            num_eqvs,
            indexed_intersection,
            sequential_intersections,
            left_keeps,
            right_keeps,
        ) = Self::create_eqv_intersection_keep(global_constraints, left, right);
        Self {
            id,
            left_id: left.id(),
            right_id: right.id(),
            vertex_cover: left
                .vertex_cover()
                .iter()
                .map(|&vid| vid)
                .chain(std::iter::once(right.root()))
                .collect(),
            vertex_eqv,
            num_eqvs,
            indexed_intersection,
            sequential_intersections,
            left_keeps,
            right_keeps,
        }
    }

    fn create_eqv_intersection_keep<L: JoinableInfo>(
        global_constraints: &mut Vec<Expr>,
        left: &L,
        right: &StarInfo,
    ) -> (
        HashMap<VId, usize>,
        usize,
        usize,
        Vec<(usize, usize, Option<VertexCoverConstraint>)>,
        Vec<(usize, Option<VertexCoverConstraint>)>,
        Vec<(usize, Option<VertexCoverConstraint>)>,
    ) {
        let left_vertices: HashSet<VId> = left.vertex_eqv().keys().map(|&x| x).collect();
        let right_vertices: HashSet<VId> = right.vertex_eqv().keys().map(|&x| x).collect();
        let mut vertex_eqv: HashMap<VId, usize> = left
            .vertex_cover()
            .iter()
            .map(|&vid| vid)
            .enumerate()
            .map(|(eqv, vid)| (vid, eqv))
            .collect();
        vertex_eqv.insert(right.root(), vertex_eqv.len());
        let vertex_cover_eqv = vertex_eqv.clone();
        let mut eqv = vertex_eqv.len(); // eqv == vertex_eqv.len() only at this point.
        let indexed_intersection = Self::create_indexed_intersection(left, right);
        let sequential_intersection = Self::create_sequential_intersections(
            global_constraints,
            &mut vertex_eqv,
            &mut eqv,
            &vertex_cover_eqv,
            left,
            right,
            &left_vertices,
            &right_vertices,
        );
        let left_keep = Self::create_left_keeps(
            global_constraints,
            &mut vertex_eqv,
            &mut eqv,
            &vertex_cover_eqv,
            left,
            &left_vertices,
            &right_vertices,
        );
        let right_keep = Self::create_right_keeps(
            global_constraints,
            &mut vertex_eqv,
            &mut eqv,
            &vertex_cover_eqv,
            right,
            &left_vertices,
            &right_vertices,
        );
        (
            vertex_eqv,
            eqv,
            indexed_intersection,
            sequential_intersection,
            left_keep,
            right_keep,
        )
    }

    fn create_indexed_intersection<L: JoinableInfo>(left: &L, right: &StarInfo) -> usize {
        *left.vertex_eqv().get(&right.root()).unwrap()
    }

    fn create_sequential_intersections_info<L: JoinableInfo>(
        global_constraints: &mut Vec<Expr>,
        vertex_cover_eqv: &HashMap<VId, usize>,
        left: &L,
        right: &StarInfo,
        left_vertices: &HashSet<VId>,
        right_vertices: &HashSet<VId>,
    ) -> BTreeMap<(usize, usize, Option<VertexCoverConstraint>), Vec<VId>> {
        let mut vertices: HashSet<VId> = left_vertices
            .intersection(&right_vertices)
            .map(|&x| x)
            .collect();
        for vid in left.vertex_cover() {
            vertices.remove(vid);
        }
        vertices.remove(&right.root());
        let mut info: BTreeMap<(usize, usize, Option<VertexCoverConstraint>), Vec<VId>> =
            BTreeMap::new();
        for v in vertices {
            info.entry((
                *left.vertex_eqv().get(&v).unwrap(),
                *right.vertex_eqv().get(&v).unwrap(),
                extract_vertex_cover_constraint(global_constraints, vertex_cover_eqv, v),
            ))
            .or_default()
            .push(v);
        }
        info
    }

    fn create_sequential_intersections<L: JoinableInfo>(
        global_constraints: &mut Vec<Expr>,
        vertex_eqv: &mut HashMap<VId, usize>,
        eqv: &mut usize,
        vertex_cover_eqv: &HashMap<VId, usize>,
        left: &L,
        right: &StarInfo,
        left_vertices: &HashSet<VId>,
        right_vertices: &HashSet<VId>,
    ) -> Vec<(usize, usize, Option<VertexCoverConstraint>)> {
        Self::create_sequential_intersections_info(
            global_constraints,
            vertex_cover_eqv,
            left,
            right,
            &left_vertices,
            &right_vertices,
        )
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

    fn create_left_keeps_info<L: JoinableInfo>(
        global_constraints: &mut Vec<Expr>,
        vertex_cover_eqv: &HashMap<VId, usize>,
        left: &L,
        left_vertices: &HashSet<VId>,
        right_vertices: &HashSet<VId>,
    ) -> BTreeMap<(usize, Option<VertexCoverConstraint>), Vec<VId>> {
        let mut vertices: HashSet<VId> = left_vertices
            .difference(&right_vertices)
            .map(|&x| x)
            .collect();
        for vid in left.vertex_cover() {
            vertices.remove(vid);
        }
        let mut info: BTreeMap<(usize, Option<VertexCoverConstraint>), Vec<VId>> = BTreeMap::new();
        for v in vertices {
            info.entry((
                *left.vertex_eqv().get(&v).unwrap(),
                extract_vertex_cover_constraint(global_constraints, vertex_cover_eqv, v),
            ))
            .or_default()
            .push(v);
        }
        info
    }

    fn create_left_keeps<L: JoinableInfo>(
        global_constraints: &mut Vec<Expr>,
        vertex_eqv: &mut HashMap<VId, usize>,
        eqv: &mut usize,
        vertex_cover_eqv: &HashMap<VId, usize>,
        left: &L,
        left_vertices: &HashSet<VId>,
        right_vertices: &HashSet<VId>,
    ) -> Vec<(usize, Option<VertexCoverConstraint>)> {
        Self::create_left_keeps_info(
            global_constraints,
            vertex_cover_eqv,
            left,
            left_vertices,
            right_vertices,
        )
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

    fn create_right_keeps_info(
        global_constraints: &mut Vec<Expr>,
        vertex_cover_eqv: &HashMap<VId, usize>,
        right: &StarInfo,
        left_vertices: &HashSet<VId>,
        right_vertices: &HashSet<VId>,
    ) -> BTreeMap<(usize, Option<VertexCoverConstraint>), Vec<VId>> {
        let mut vertices: HashSet<VId> = right_vertices
            .difference(&left_vertices)
            .map(|&x| x)
            .collect();
        vertices.remove(&right.root());
        let mut info: BTreeMap<(usize, Option<VertexCoverConstraint>), Vec<VId>> = BTreeMap::new();
        for v in vertices {
            info.entry((
                *right.vertex_eqv().get(&v).unwrap(),
                extract_vertex_cover_constraint(global_constraints, vertex_cover_eqv, v),
            ))
            .or_default()
            .push(v);
        }
        info
    }

    fn create_right_keeps(
        global_constraints: &mut Vec<Expr>,
        vertex_eqv: &mut HashMap<VId, usize>,
        eqv: &mut usize,
        vertex_cover_eqv: &HashMap<VId, usize>,
        right: &StarInfo,
        left_vertices: &HashSet<VId>,
        right_vertices: &HashSet<VId>,
    ) -> Vec<(usize, Option<VertexCoverConstraint>)> {
        Self::create_right_keeps_info(
            global_constraints,
            vertex_cover_eqv,
            right,
            left_vertices,
            right_vertices,
        )
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
pub struct Plan<'a, 'b, 'c> {
    data_graph: &'a DataGraph,
    pattern_graph: &'a PatternGraph<'b>,
    star_sr_mm_type: MemoryManagerType<'c>,
    join_sr_mm_type: MemoryManagerType<'c>,
    star_sr_mms_len: usize,
    join_sr_mms_len: usize,
    index_mm_type: MemoryManagerType<'c>,
    index_mm_len: usize,
    stars: Vec<StarInfo<'a, 'b>>,
    stars_plan: Vec<(VLabel, Vec<CharacteristicInfo<'a, 'b>>)>,
    join_plan: Vec<JoinInfo>,
    global_constraint: Option<GlobalConstraint>,
}

impl<'a, 'b, 'c> Plan<'a, 'b, 'c> {
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

    pub fn global_constraint(&self) -> &Option<GlobalConstraint> {
        &self.global_constraint
    }

    /// Returns `(super_row_mms, index_mms)`.
    pub fn allocate(&self) -> (Vec<MemoryManager>, Vec<MemoryManager>) {
        (
            self.create_super_row_mms(self.star_sr_mms_len + self.join_sr_mms_len),
            self.create_index_mms(self.index_mm_len),
        )
    }

    pub fn execute_stars_matching(
        &self,
        super_row_mms: &mut [MemoryManager],
        index_mms: &mut [MemoryManager],
    ) {
        self.stars_plan().iter().for_each(|(vlabel, infos)| {
            match_characteristics(self.data_graph, *vlabel, infos, super_row_mms, index_mms)
        });
    }

    pub fn execute_join(&self, super_row_mms: &mut [MemoryManager], index_mms: &[MemoryManager]) {
        self.join_plan().iter().for_each(|info| {
            let (wrote_mms, mms) = super_row_mms.split_at_mut(info.id());
            join(
                &mut mms[0],
                info.num_eqvs(),
                info.vertex_cover().len(),
                &wrote_mms[info.left_id()],
                &wrote_mms[info.right_id()],
                &index_mms[info.right_id()],
                info.indexed_intersection(),
                info.sequential_intersections(),
                info.left_keeps(),
                info.right_keeps(),
            );
        });
    }

    pub fn execute_write_results(
        &self,
        writer: &mut dyn Write,
        super_row_mms: &[MemoryManager],
    ) -> std::io::Result<usize> {
        let vertex_eqv = if self.stars().is_empty() {
            return Ok(0);
        } else if self.join_plan().is_empty() {
            self.stars().last().unwrap().vertex_eqv()
        } else {
            self.join_plan().last().unwrap().vertex_eqv()
        };
        let super_row_mm = super_row_mms.last().unwrap();
        let mut vertex_eqv: Vec<(VId, usize)> = vertex_eqv
            .iter()
            .map(|(&vertex, &eqv)| (vertex, eqv))
            .collect();
        vertex_eqv.sort();
        let rows = decompress(super_row_mm, &vertex_eqv);
        write_results(writer, rows, &vertex_eqv, self.global_constraint())
    }

    pub fn execute(
        &self,
        writer: &mut dyn Write,
        super_row_mms: &mut [MemoryManager],
        index_mms: &mut [MemoryManager],
    ) -> std::io::Result<usize> {
        self.execute_stars_matching(super_row_mms, index_mms);
        self.execute_join(super_row_mms, index_mms);
        self.execute_write_results(writer, super_row_mms)
    }
}

// private methods.
impl<'a, 'b, 'c> Plan<'a, 'b, 'c> {
    fn create_super_row_mms(&self, count: usize) -> Vec<MemoryManager> {
        (0..count)
            .map(|id| {
                match if id < self.star_sr_mms_len {
                    &self.star_sr_mm_type
                } else {
                    &self.join_sr_mm_type
                } {
                    MemoryManagerType::Mem => MemoryManager::Mem(vec![]),
                    MemoryManagerType::Mmap(path, name) => {
                        MemoryManager::Mmap(MmapFile::new(path.join(format!("{}{}.sr", name, id))))
                    }
                    MemoryManagerType::Sink => MemoryManager::Sink,
                }
            })
            .collect()
    }

    fn create_index_mms(&self, count: usize) -> Vec<MemoryManager> {
        (0..count)
            .map(|id| match &self.index_mm_type {
                MemoryManagerType::Mem => MemoryManager::Mem(vec![]),
                MemoryManagerType::Mmap(path, name) => {
                    MemoryManager::Mmap(MmapFile::new(path.join(format!("{}{}.idx", name, id))))
                }
                MemoryManagerType::Sink => MemoryManager::Sink,
            })
            .collect()
    }

    fn fmt_stars(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if !self.stars().is_empty() {
            writeln!(f, "Stars")?;
            for (star_id, star) in self.stars().iter().enumerate() {
                writeln!(f, "  Star{} [{}]", star_id, star.id())?;
                let mut eqv_vertices: BTreeMap<usize, BTreeSet<VId>> = BTreeMap::new();
                for (&v, &eqv) in star.vertex_eqv() {
                    eqv_vertices.entry(eqv).or_default().insert(v);
                }
                for (&eqv, vertices) in eqv_vertices.iter().take(1) {
                    writeln!(
                        f,
                        "    eqv{} {:?} ({}){}",
                        eqv,
                        vertices
                            .iter()
                            .map(|v| format!("u{}", v))
                            .collect::<Vec<_>>(),
                        star.characteristic().root_vlabel(),
                        if let Some(vc) = star.characteristic().root_constraint() {
                            format!(" {:?}", vc)
                        } else {
                            String::from("")
                        }
                    )?;
                }
                for ((&eqv, vertices), &info) in eqv_vertices
                    .iter()
                    .skip(1)
                    .zip(star.characteristic().infos())
                {
                    writeln!(
                        f,
                        "    eqv{} {:?} ({}){}{}",
                        eqv,
                        vertices
                            .iter()
                            .map(|v| format!("u{}", v))
                            .collect::<Vec<_>>(),
                        info.vlabel(),
                        if let Some(vc) = info.neighbor_constraint() {
                            format!(" {:?}", vc)
                        } else {
                            String::from("")
                        },
                        if let Some(ec) = info.edge_constraint() {
                            format!(" {:?}", ec)
                        } else {
                            String::from("")
                        }
                    )?;
                    if !info.v_to_n_elabels().is_empty() {
                        writeln!(f, "      v->n {:?}", info.v_to_n_elabels())?;
                    }
                    if !info.n_to_v_elabels().is_empty() {
                        writeln!(f, "      v<-n {:?}", info.n_to_v_elabels())?;
                    }
                    if !info.undirected_elabels().is_empty() {
                        writeln!(f, "      v--n {:?}", info.undirected_elabels())?;
                    }
                }
            }
        }
        Ok(())
    }

    fn fmt_stars_plan(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if !self.stars_plan().is_empty() {
            writeln!(f, "Stars Plan")?;
            for (vlabel, cinfos) in self.stars_plan() {
                writeln!(f, "  ({})", vlabel)?;
                for cinfo in cinfos {
                    writeln!(f, "    Characteristic [{}]", cinfo.id())?;
                    writeln!(
                        f,
                        "      eqv0 ({}){}",
                        cinfo.characteristic().root_vlabel(),
                        if let Some(vc) = cinfo.characteristic().root_constraint() {
                            format!(" {:?}", vc)
                        } else {
                            String::from("")
                        }
                    )?;
                    for (i, ninfo) in cinfo.characteristic().infos().iter().enumerate() {
                        writeln!(
                            f,
                            "      eqv{} ({}){}{}",
                            i + 1,
                            ninfo.vlabel(),
                            if let Some(vc) = ninfo.neighbor_constraint() {
                                format!(" {:?}", vc)
                            } else {
                                String::from("")
                            },
                            if let Some(ec) = ninfo.edge_constraint() {
                                format!(" {:?}", ec)
                            } else {
                                String::from("")
                            }
                        )?;
                    }
                }
            }
        }
        Ok(())
    }

    fn fmt_join_plan(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if !self.join_plan().is_empty() {
            writeln!(f, "Join Plan")?;
            for info in self.join_plan() {
                writeln!(
                    f,
                    "  Join [{}] = [{}] x [{}]",
                    info.id(),
                    info.left_id(),
                    info.right_id()
                )?;
                let mut eqv_vertices = vec![BTreeSet::new(); info.num_eqvs()];
                for (&v, &eqv) in info.vertex_eqv() {
                    eqv_vertices[eqv].insert(v);
                }
                let mut new_eqv = info.vertex_cover().len() - 1;
                writeln!(
                    f,
                    "    indexed intersection {:?} = [{}]eqv{} x [{}]eqv0",
                    eqv_vertices[new_eqv]
                        .iter()
                        .map(|v| format!("u{}", v))
                        .collect::<Vec<_>>(),
                    info.left_id(),
                    info.indexed_intersection(),
                    info.right_id()
                )?;
                new_eqv += 1;
                writeln!(
                    f,
                    "    vertex cover {:?}",
                    info.vertex_cover()
                        .iter()
                        .map(|v| format!("u{}", v))
                        .collect::<Vec<_>>(),
                )?;
                if !info.sequential_intersections().is_empty() {
                    writeln!(f, "    sequential intersections:")?;
                    for (left, right, vcc) in info.sequential_intersections() {
                        writeln!(
                            f,
                            "      {:?} = [{}]eqv{} x [{}]eqv{}{}",
                            eqv_vertices[new_eqv]
                                .iter()
                                .map(|v| format!("u{}", v))
                                .collect::<Vec<_>>(),
                            info.left_id(),
                            left,
                            info.right_id(),
                            right,
                            if let Some(vcc) = vcc {
                                format!(" {:?}", vcc)
                            } else {
                                String::from("")
                            }
                        )?;
                        new_eqv += 1;
                    }
                }
                if !info.left_keeps().is_empty() {
                    writeln!(f, "    left keeps")?;
                    for (eqv, vcc) in info.left_keeps() {
                        writeln!(
                            f,
                            "      {:?} = [{}]eqv{}{}",
                            eqv_vertices[new_eqv]
                                .iter()
                                .map(|v| format!("u{}", v))
                                .collect::<Vec<_>>(),
                            info.left_id(),
                            eqv,
                            if let Some(vcc) = vcc {
                                format!(" {:?}", vcc)
                            } else {
                                String::from("")
                            }
                        )?;
                        new_eqv += 1;
                    }
                }
                if !info.right_keeps().is_empty() {
                    writeln!(f, "    right keeps")?;
                    for (eqv, vcc) in info.right_keeps() {
                        writeln!(
                            f,
                            "      {:?} = [{}]eqv{}{}",
                            eqv_vertices[new_eqv]
                                .iter()
                                .map(|v| format!("u{}", v))
                                .collect::<Vec<_>>(),
                            info.left_id(),
                            eqv,
                            if let Some(vcc) = vcc {
                                format!(" {:?}", vcc)
                            } else {
                                String::from("")
                            }
                        )?;
                    }
                }
            }
        }
        Ok(())
    }
}

impl<'a, 'b, 'c> std::fmt::Display for Plan<'a, 'b, 'c> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.fmt_stars(f)?;
        self.fmt_stars_plan(f)?;
        self.fmt_join_plan(f)?;
        Ok(())
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

    fn create_pattern_graph1<'a>() -> PatternGraph<'a> {
        let mut p = PatternGraph::new();
        for (u, l) in vec![(0, 0), (1, 1), (2, 2), (3, 2)] {
            p.add_vertex(u, l);
        }
        for (u1, u2, l) in vec![(0, 1, 0), (0, 2, 0), (1, 2, 0), (1, 3, 0)] {
            p.add_edge(u1, u2, l);
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
    fn test_stars_plan() {
        let data_graph = create_data_graph();
        let pattern_graph = create_pattern_graph();
        let mut global_constraints = vec![];
        let plan = Planner::new(&data_graph, &pattern_graph, &mut global_constraints).plan();
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

    #[test]
    fn test_stars_plan2() {
        let data_graph = create_data_graph();
        let pattern_graph = create_pattern_graph2();
        let mut global_constraints = vec![];
        let plan = Planner::new(&data_graph, &pattern_graph, &mut global_constraints).plan();
        assert_eq!(
            plan.stars(),
            &[
                StarInfo::new(&pattern_graph, 1, 0),
                StarInfo::new(&pattern_graph, 2, 1),
                StarInfo::new(&pattern_graph, 5, 2)
            ]
        );
        assert_eq!(
            plan.stars_plan(),
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

    #[test]
    fn test_join_plan() {
        let pattern_graph = create_pattern_graph1();
        let star_info1 = StarInfo::new(&pattern_graph, 0, 0);
        let star_info2 = StarInfo::new(&pattern_graph, 1, 1);
        let join_info = JoinInfo::new(&mut vec![], &star_info1, &star_info2, 2);
        assert_eq!(join_info.vertex_cover(), &[0, 1]);
        assert_eq!(join_info.num_eqvs(), 4);
        assert_eq!(
            join_info.vertex_eqv(),
            &vec![(0, 0), (1, 1), (2, 2), (3, 3)]
                .into_iter()
                .collect::<HashMap<_, _>>()
        );
        assert_eq!(join_info.indexed_intersection(), 1);
        assert_eq!(join_info.sequential_intersections(), &[(2, 2, None)]);
        assert_eq!(join_info.left_keeps(), &[]);
        assert_eq!(join_info.right_keeps(), &[(2, None)]);
    }
}
