//! The planner.

use crate::{
    compiler::{
        ast::Expr,
        generator::{extract_global_constraint, EdgeConstraintsInfo, VertexConstraintsInfo},
    },
    data_graph::DataGraph,
    executor::{join, match_characteristics, read_super_row_header, JoinedSuperRows},
    memory_manager::{MemoryManager, MmapMutFile},
    pattern_graph::{Characteristic, PatternGraph},
    planner::{decompose_stars, CharacteristicInfo, IndexType, StarInfo},
    types::{GlobalConstraint, VId, VLabel},
};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
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
    index_mm_type: MemoryManagerType<'c>,
    index_type: IndexType,
    roots: Option<Vec<VId>>,
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
            index_mm_type: MemoryManagerType::Mem,
            index_type: IndexType::Sorted,
            roots: None,
        }
    }

    pub fn star_sr_mm_type(mut self, mm_type: MemoryManagerType<'c>) -> Self {
        self.star_sr_mm_type = mm_type;
        self
    }

    pub fn index_mm_type(mut self, mm_type: MemoryManagerType<'c>) -> Self {
        self.index_mm_type = mm_type;
        self
    }

    pub fn index_type(mut self, index_type: IndexType) -> Self {
        self.index_type = index_type;
        self
    }

    pub fn roots(mut self, roots: Vec<VId>) -> Self {
        self.roots = Some(roots);
        self
    }

    pub fn plan(mut self) -> Plan<'a, 'b, 'c> {
        let roots = if let Some(roots) = &self.roots {
            roots.clone()
        } else {
            decompose_stars(self.data_graph, self.pattern_graph)
        };
        let characteristic_ids = self.create_characteristic_ids(&roots);
        let stars = self.create_stars(&roots, &characteristic_ids);
        let stars_plan = self.create_stars_plan(&characteristic_ids);
        let join_plan = create_join_plan(self.pattern_graph, &stars, self.index_type);
        let global_constraint = if stars.is_empty() {
            None
        } else {
            extract_global_constraint(
                &mut self.global_constraints,
                if let Some(join_plan) = &join_plan {
                    join_plan.vertex_eqv()
                } else {
                    stars.last().unwrap().vertex_eqv()
                },
            )
        };
        Plan {
            data_graph: self.data_graph,
            pattern_graph: self.pattern_graph,
            star_sr_mm_type: self.star_sr_mm_type,
            star_sr_mms_len: characteristic_ids.len(),
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
}

fn create_join_plan(
    pattern_graph: &PatternGraph,
    stars: &[StarInfo],
    index_type: IndexType,
) -> Option<JoinPlan> {
    if stars.len() < 2 {
        None
    } else {
        let mut uid_sr_eqvs: HashMap<VId, BTreeSet<(usize, usize)>> = HashMap::new();
        for (star_id, star) in stars.iter().enumerate() {
            for (&uid, &eqv) in star.vertex_eqv() {
                uid_sr_eqvs.entry(uid).or_default().insert((star_id, eqv));
            }
        }
        let leaves = get_leaves(pattern_graph, stars);
        let vertex_eqv = create_vertex_eqv(stars, &leaves, &uid_sr_eqvs);
        let eqv_pivots: BTreeMap<usize, VId> =
            vertex_eqv.iter().map(|(&uid, &eqv)| (eqv, uid)).collect();
        Some(JoinPlan::new(
            vertex_eqv,
            stars.len(),
            index_type,
            create_indexed_joins(stars, &uid_sr_eqvs),
            eqv_pivots
                .iter()
                .skip(stars.len())
                .map(|(_, pivot)| {
                    IntersectionPlan::new(
                        uid_sr_eqvs.get(pivot).unwrap().iter().map(|&x| x).collect(),
                    )
                })
                .collect(),
        ))
    }
}

fn get_leaves(pattern_graph: &PatternGraph, stars: &[StarInfo]) -> BTreeSet<VId> {
    pattern_graph
        .vertices()
        .map(|(uid, _)| uid)
        .collect::<BTreeSet<VId>>()
        .difference(&stars.iter().map(|star| star.root()).collect())
        .map(|&uid| uid)
        .collect()
}

fn create_vertex_eqv(
    stars: &[StarInfo],
    leaves: &BTreeSet<VId>,
    uid_sr_eqvs: &HashMap<VId, BTreeSet<(usize, usize)>>,
) -> HashMap<VId, usize> {
    let mut sr_eqvs_leaves: HashMap<&BTreeSet<(usize, usize)>, BTreeSet<VId>> =
        HashMap::with_capacity(leaves.len());
    leaves.iter().for_each(|&leaf| {
        sr_eqvs_leaves
            .entry(uid_sr_eqvs.get(&leaf).unwrap())
            .or_default()
            .insert(leaf);
    });
    let mut vertex_eqv: HashMap<VId, usize> = stars
        .iter()
        .enumerate()
        .map(|(eqv, star)| (star.root(), eqv))
        .collect();
    let mut eqv = stars.len();
    let mut visited = HashSet::new();
    for leaf in leaves {
        if !visited.contains(leaf) {
            for &uid in sr_eqvs_leaves.get(uid_sr_eqvs.get(leaf).unwrap()).unwrap() {
                vertex_eqv.insert(uid, eqv);
                visited.insert(uid);
            }
            eqv += 1;
        }
    }
    vertex_eqv
}

fn create_indexed_joins(
    stars: &[StarInfo],
    uid_sr_eqvs: &HashMap<VId, BTreeSet<(usize, usize)>>,
) -> Vec<IndexedJoinPlan> {
    stars
        .iter()
        .enumerate()
        .skip(1)
        .map(|(i, star)| {
            IndexedJoinPlan::new(
                uid_sr_eqvs
                    .get(&star.root())
                    .unwrap()
                    .iter()
                    .filter_map(|&(sr, eqv)| if sr < i { Some((sr, eqv)) } else { None })
                    .collect(),
                star.id(),
            )
        })
        .collect()
}

#[derive(Debug, PartialEq)]
pub struct IndexedJoinPlan {
    scan: Vec<(usize, usize)>, // (sr, eqv)
    index_id: usize,
}

impl IndexedJoinPlan {
    pub fn new(scan: Vec<(usize, usize)>, index_id: usize) -> Self {
        Self { scan, index_id }
    }

    pub fn scan(&self) -> &[(usize, usize)] {
        &self.scan
    }

    pub fn index_id(&self) -> usize {
        self.index_id
    }
}

#[derive(Debug, PartialEq)]
pub struct IntersectionPlan {
    intersection: Vec<(usize, usize)>, // (sr, eqv)
}

impl IntersectionPlan {
    pub fn new(intersection: Vec<(usize, usize)>) -> Self {
        Self { intersection }
    }

    pub fn intersection(&self) -> &[(usize, usize)] {
        &self.intersection
    }
}

pub struct JoinPlan {
    vertex_eqv: HashMap<VId, usize>,
    sorted_vertex_eqv: Vec<(VId, usize)>,
    num_cover: usize,
    index_type: IndexType,
    indexed_joins: Vec<IndexedJoinPlan>,
    intersections: Vec<IntersectionPlan>,
}

impl JoinPlan {
    pub fn new(
        vertex_eqv: HashMap<VId, usize>,
        num_cover: usize,
        index_type: IndexType,
        indexed_joins: Vec<IndexedJoinPlan>,
        intersections: Vec<IntersectionPlan>,
    ) -> Self {
        let mut sorted_vertex_eqv: Vec<_> =
            vertex_eqv.iter().map(|(&uid, &eqv)| (uid, eqv)).collect();
        sorted_vertex_eqv.sort();
        Self {
            vertex_eqv,
            sorted_vertex_eqv,
            num_cover,
            index_type,
            indexed_joins,
            intersections,
        }
    }

    pub fn vertex_eqv(&self) -> &HashMap<VId, usize> {
        &self.vertex_eqv
    }

    pub fn sorted_vertex_eqv(&self) -> &[(VId, usize)] {
        &self.sorted_vertex_eqv
    }

    pub fn num_cover(&self) -> usize {
        self.num_cover
    }

    pub fn index_type(&self) -> &IndexType {
        &self.index_type
    }

    pub fn indexed_joins(&self) -> &[IndexedJoinPlan] {
        &self.indexed_joins
    }

    pub fn intersections(&self) -> &[IntersectionPlan] {
        &self.intersections
    }
}

/// The plan to match the `pattern_graph` in `data_graph`.
pub struct Plan<'a, 'b, 'c> {
    data_graph: &'a DataGraph,
    pattern_graph: &'a PatternGraph<'b>,
    star_sr_mm_type: MemoryManagerType<'c>,
    star_sr_mms_len: usize,
    index_mm_type: MemoryManagerType<'c>,
    index_mm_len: usize,
    stars: Vec<StarInfo<'a, 'b>>,
    stars_plan: Vec<(VLabel, Vec<CharacteristicInfo<'a, 'b>>)>,
    join_plan: Option<JoinPlan>,
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

    pub fn join_plan(&self) -> Option<&JoinPlan> {
        self.join_plan.as_ref()
    }

    pub fn global_constraint(&self) -> &Option<GlobalConstraint> {
        &self.global_constraint
    }

    /// Returns `(super_row_mms, index_mms)`.
    pub fn allocate(&self) -> (Vec<MemoryManager>, Vec<MemoryManager>) {
        (
            self.create_super_row_mms(self.star_sr_mms_len),
            self.create_index_mms(self.index_mm_len),
        )
    }

    pub fn execute_stars_plan(
        &self,
        super_row_mms: &mut [MemoryManager],
        index_mms: &mut [MemoryManager],
    ) {
        self.stars_plan().iter().for_each(|(vlabel, infos)| {
            match_characteristics(self.data_graph, *vlabel, infos, super_row_mms, index_mms)
        });
        for (id, sr) in super_row_mms.iter().enumerate() {
            let (num_rows, num_eqvs, num_vertices) = read_super_row_header(sr);
            eprintln!(
                "characteristic[{}]: ({}, {}, {})",
                id, num_rows, num_eqvs, num_vertices
            );
        }
    }

    pub fn execute_join_plan<'s, 'm>(
        &'s self,
        super_row_mms: &'m [MemoryManager],
        index_mms: &'m [MemoryManager],
    ) -> Option<JoinedSuperRows<'m, 's>> {
        self.join_plan()
            .map(|join_plan| join(super_row_mms, index_mms, join_plan))
    }
}

// private methods.
impl<'a, 'b, 'c> Plan<'a, 'b, 'c> {
    fn create_super_row_mms(&self, count: usize) -> Vec<MemoryManager> {
        (0..count)
            .map(|id| match &self.star_sr_mm_type {
                MemoryManagerType::Mem => MemoryManager::Mem(vec![]),
                MemoryManagerType::Mmap(path, name) => MemoryManager::MmapMut(MmapMutFile::new(
                    path.join(format!("{}{}.sr", name, id)),
                )),
                MemoryManagerType::Sink => MemoryManager::Sink,
            })
            .collect()
    }

    fn create_index_mms(&self, count: usize) -> Vec<MemoryManager> {
        (0..count)
            .map(|id| match &self.index_mm_type {
                MemoryManagerType::Mem => MemoryManager::Mem(vec![]),
                MemoryManagerType::Mmap(path, name) => MemoryManager::MmapMut(MmapMutFile::new(
                    path.join(format!("{}{}.idx", name, id)),
                )),
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
        if let Some(join_plan) = self.join_plan() {
            for indexed_join in join_plan.indexed_joins() {
                writeln!(f, "{:?}", indexed_join)?;
            }
            for intersection in join_plan.intersections() {
                writeln!(f, "{:?}", intersection)?;
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
    fn test_q02_join() {
        let mut p = PatternGraph::new();
        for (vid, vlabel) in vec![(1, 0), (2, 0), (3, 0), (4, 0)] {
            p.add_vertex(vid, vlabel);
        }
        for (src, dst, elabel) in vec![(1, 2, 0), (1, 3, 0), (2, 4, 0), (3, 4, 0)] {
            p.add_arc(src, dst, elabel);
        }
        let stars = vec![
            StarInfo::new(&p, 1, 0),
            StarInfo::new(&p, 2, 1),
            StarInfo::new(&p, 3, 1),
        ];
        let join_plan = create_join_plan(&p, &stars, IndexType::Hash).unwrap();
        assert_eq!(
            join_plan.indexed_joins(),
            &[
                IndexedJoinPlan::new(vec![(0, 1)], 1),
                IndexedJoinPlan::new(vec![(0, 1)], 1)
            ]
        );
        assert_eq!(
            join_plan.intersections(),
            &[IntersectionPlan::new(vec![(1, 1), (2, 1)])]
        );
    }

    #[test]
    fn test_q06_join() {
        let mut p = PatternGraph::new();
        for (vid, vlabel) in vec![(1, 0), (2, 0), (3, 0), (4, 0)] {
            p.add_vertex(vid, vlabel);
        }
        for (u1, u2, elabel) in vec![
            (1, 2, 0),
            (1, 3, 0),
            (1, 4, 0),
            (2, 3, 0),
            (2, 4, 0),
            (3, 4, 0),
        ] {
            p.add_arc(u1, u2, elabel);
        }
        let stars = vec![
            StarInfo::new(&p, 1, 0),
            StarInfo::new(&p, 2, 1),
            StarInfo::new(&p, 3, 2),
        ];
        let join_plan = create_join_plan(&p, &stars, IndexType::Hash).unwrap();
        assert_eq!(
            join_plan.indexed_joins(),
            vec![
                IndexedJoinPlan::new(vec![(0, 1)], 1),
                IndexedJoinPlan::new(vec![(0, 1), (1, 1)], 2)
            ]
        );
        assert_eq!(
            join_plan.intersections(),
            vec![IntersectionPlan::new(vec![(0, 1), (1, 1), (2, 1)])]
        );
    }
}
