use crate::types::VId;
use std::collections::HashMap;

#[derive(Clone)]
pub enum IndexType {
    Sorted,
    Hash,
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
