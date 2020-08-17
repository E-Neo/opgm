//! The planner.

use crate::{
    compiler::ast::Expr,
    data_graph::DataGraph,
    pattern_graph::{Characteristic, PatternGraph},
    planner::decompose_stars,
    types::VId,
};
use std::collections::HashMap;

pub struct Planner<'a, 'b> {
    data_graph: &'a DataGraph,
    pattern_graph: &'a PatternGraph<'b>,
    global_constraints: Vec<Expr>,
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
        }
    }

    pub fn plan(&self) -> Plan {
        let roots = decompose_stars(self.data_graph, self.pattern_graph);
        let star_plans: HashMap<_, _> = roots
            .iter()
            .map(|&root| {
                (
                    Characteristic::new(self.pattern_graph, root),
                    StarMatchingPlan,
                )
            })
            .collect();
        Plan { roots, star_plans }
    }
}

pub struct StarMatchingPlan;

pub struct Plan<'a, 'b> {
    roots: Vec<VId>,
    star_plans: HashMap<Characteristic<'a, 'b>, StarMatchingPlan>,
}
