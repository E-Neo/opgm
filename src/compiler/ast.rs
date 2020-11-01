//! Abstract Syntax Tree

use crate::types::{ELabel, VId, VLabel};

/// Abstract Syntax Tree of the graph query language.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub struct Ast {
    vertices: Vec<(VId, VLabel)>,
    arcs: Vec<(VId, VId, ELabel)>,
    edges: Vec<(VId, VId, ELabel)>,
    constraint: Option<Expr>,
}

impl Ast {
    pub fn new(
        vertices: Vec<(VId, VLabel)>,
        arcs: Vec<(VId, VId, ELabel)>,
        edges: Vec<(VId, VId, ELabel)>,
        constraint: Option<Expr>,
    ) -> Self {
        Self {
            vertices,
            arcs,
            edges,
            constraint,
        }
    }

    pub fn vertices(&self) -> &[(VId, VLabel)] {
        &self.vertices
    }

    pub fn arcs(&self) -> &[(VId, VId, ELabel)] {
        &self.arcs
    }

    pub fn edges(&self) -> &[(VId, VId, ELabel)] {
        &self.edges
    }

    pub fn constraint(&self) -> &Option<Expr> {
        &self.constraint
    }
}

/// Built-in functions.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Hash)]
pub enum BuiltIn {
    And,
    Or,
    Not,
    Lt,
    Ge,
    Eq,
    Mod,
}

/// The atom.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Hash)]
pub enum Atom {
    VId(VId),
    Num(i64),
    Boolean(bool),
    BuiltIn(BuiltIn),
}

/// Constraint Expression.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Hash)]
pub enum Expr {
    Constant(Atom),
    Application(Box<Expr>, Vec<Expr>),
}
