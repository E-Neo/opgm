use super::codegen::{emit_edge_constraint, emit_vertex_constraint};
use crate::{front_end::Expr, types::VId};
use derive_more::AsRef;

#[derive(Debug)]
pub struct VertexConstraint {
    expr: Expr,
    f: VertexConstraintF,
}

impl VertexConstraint {
    pub fn new(expr: Expr, f: Box<dyn Fn(VId) -> bool>) -> Self {
        Self {
            expr,
            f: VertexConstraintF(f),
        }
    }

    pub fn f(&self) -> &dyn Fn(VId) -> bool {
        self.f.as_ref()
    }
}

impl Clone for VertexConstraint {
    fn clone(&self) -> Self {
        emit_vertex_constraint(&self.expr)
    }
}

impl PartialEq for VertexConstraint {
    fn eq(&self, other: &Self) -> bool {
        self.expr == other.expr
    }
}

impl Eq for VertexConstraint {}

impl PartialOrd for VertexConstraint {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.expr.partial_cmp(&other.expr)
    }
}

impl Ord for VertexConstraint {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.expr.cmp(&other.expr)
    }
}

impl std::hash::Hash for VertexConstraint {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.expr.hash(state)
    }
}

#[derive(Debug)]
pub struct EdgeConstraint {
    expr: Expr,
    f: EdgeConstraintF,
}

impl EdgeConstraint {
    pub fn new(expr: Expr, f: Box<dyn Fn(VId, VId) -> bool>) -> Self {
        Self {
            expr,
            f: EdgeConstraintF(f),
        }
    }

    pub fn f(&self) -> &dyn Fn(VId, VId) -> bool {
        self.f.as_ref()
    }
}

impl Clone for EdgeConstraint {
    fn clone(&self) -> Self {
        emit_edge_constraint(&self.expr)
    }
}

impl PartialEq for EdgeConstraint {
    fn eq(&self, other: &Self) -> bool {
        self.expr == other.expr
    }
}

impl Eq for EdgeConstraint {}

impl PartialOrd for EdgeConstraint {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.expr.partial_cmp(&other.expr)
    }
}

impl Ord for EdgeConstraint {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.expr.cmp(&other.expr)
    }
}

impl std::hash::Hash for EdgeConstraint {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.expr.hash(state)
    }
}

#[derive(AsRef)]
struct VertexConstraintF(Box<dyn Fn(VId) -> bool>);

impl std::fmt::Debug for VertexConstraintF {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:p}", self.0)
    }
}

#[derive(AsRef)]
struct EdgeConstraintF(Box<dyn Fn(VId, VId) -> bool>);

impl std::fmt::Debug for EdgeConstraintF {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:p}", self.0)
    }
}
