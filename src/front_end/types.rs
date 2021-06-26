use super::codegen::emit_vertex_constraint;
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

impl PartialEq for EdgeConstraint {
    fn eq(&self, other: &Self) -> bool {
        self.expr == other.expr
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
