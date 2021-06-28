use crate::{
    old_planner::{IndexType, StarInfo},
    pattern::PatternGraph,
    types::VId,
};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

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
    pub fn new(pattern_graph: &PatternGraph, index_type: IndexType, stars: &[StarInfo]) -> Self {
        let mut uid_sr_eqvs: HashMap<VId, BTreeSet<(usize, usize)>> = HashMap::new();
        for (star_id, star) in stars.iter().enumerate() {
            for (&uid, &eqv) in star.vertex_eqv() {
                uid_sr_eqvs.entry(uid).or_default().insert((star_id, eqv));
            }
        }
        let leaves = get_leaves(pattern_graph, &stars);
        let vertex_eqv = create_vertex_eqv(stars, &leaves, &uid_sr_eqvs);
        let sorted_vertex_eqv: Vec<_> = vertex_eqv
            .iter()
            .map(|(&uid, &eqv)| (uid, eqv))
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        let eqv_pivots: BTreeMap<usize, VId> =
            vertex_eqv.iter().map(|(&uid, &eqv)| (eqv, uid)).collect();
        Self {
            vertex_eqv,
            sorted_vertex_eqv,
            num_cover: stars.len(),
            index_type,
            indexed_joins: create_indexed_joins(&stars, &uid_sr_eqvs),
            intersections: eqv_pivots
                .iter()
                .skip(stars.len())
                .map(|(_, pivot)| {
                    IntersectionPlan::new(
                        uid_sr_eqvs.get(pivot).unwrap().iter().map(|&x| x).collect(),
                    )
                })
                .collect(),
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

fn get_leaves(pattern_graph: &PatternGraph, stars: &[StarInfo]) -> BTreeSet<VId> {
    pattern_graph
        .vertices()
        .into_iter()
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_join1() {
        let mut p = PatternGraph::new();
        for (vid, vlabel) in vec![(1, 0), (2, 0), (3, 0), (4, 0)] {
            p.add_vertex(vid, vlabel);
        }
        for (src, dst, elabel) in vec![(1, 2, 0), (1, 3, 0), (2, 4, 0), (3, 4, 0)] {
            p.add_arc(src, dst, elabel);
        }
        let join_plan = JoinPlan::new(
            &p,
            IndexType::Hash,
            &[
                StarInfo::new(&p, 1, 0),
                StarInfo::new(&p, 2, 1),
                StarInfo::new(&p, 3, 1),
            ],
        );
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
    fn test_join2() {
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
        let join_plan = JoinPlan::new(
            &p,
            IndexType::Hash,
            &[
                StarInfo::new(&p, 1, 0),
                StarInfo::new(&p, 2, 1),
                StarInfo::new(&p, 3, 2),
            ],
        );
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
