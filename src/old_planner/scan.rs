use crate::{
    old_planner::CharacteristicInfo,
    pattern::{Characteristic, PatternGraph},
    types::{VId, VLabel},
};
use std::collections::{BTreeMap, BTreeSet, HashMap};

pub struct ScanPlan {
    plan: Vec<(VLabel, Vec<CharacteristicInfo>)>,
}

impl ScanPlan {
    pub fn new(pattern_graph: &PatternGraph, roots: &[VId]) -> Self {
        let characteristic_id_map = create_characteristic_id_map(pattern_graph, roots);
        let mut vlabel_characteristics: BTreeMap<VLabel, BTreeSet<CharacteristicInfo>> =
            BTreeMap::new();
        for x in characteristic_id_map.keys() {
            vlabel_characteristics
                .entry(x.root_vlabel())
                .or_default()
                .insert(CharacteristicInfo::new(
                    x.clone(),
                    *characteristic_id_map.get(x).unwrap(),
                ));
        }
        Self {
            plan: vlabel_characteristics
                .into_iter()
                .map(|(vlabel, xs)| (vlabel, xs.into_iter().collect::<Vec<_>>()))
                .collect(),
        }
    }

    pub fn plan(&self) -> &[(VLabel, Vec<CharacteristicInfo>)] {
        &self.plan
    }
}

fn create_characteristic_id_map(
    pattern_graph: &PatternGraph,
    roots: &[VId],
) -> HashMap<Characteristic, usize> {
    let mut id = 0;
    let mut characteristic_id_map = HashMap::with_capacity(roots.len());
    for &root in roots {
        characteristic_id_map
            .entry(Characteristic::new(pattern_graph, root))
            .or_insert_with(|| {
                let myid = id;
                id += 1;
                myid
            });
    }
    characteristic_id_map
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_pattern_graph() -> PatternGraph {
        let mut p = PatternGraph::new();
        for (u, l) in vec![(1, 1), (2, 2), (3, 2)] {
            p.add_vertex(u, l);
        }
        for (u1, u2, l) in vec![(1, 2, 0), (1, 3, 0)] {
            p.add_arc(u1, u2, l);
        }
        p
    }

    fn create_pattern_graph2() -> PatternGraph {
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
        let scan_plan = ScanPlan::new(&pattern_graph, &[1]);
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
        let scan_plan = ScanPlan::new(&pattern_graph, &[1, 2, 5]);
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
