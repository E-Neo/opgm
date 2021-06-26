use crate::types::{ELabel, VId, VLabel};

#[derive(Debug, PartialEq, Default)]
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

    pub fn set_vertices(&mut self, vertices: Vec<(VId, VLabel)>) {
        self.vertices = vertices;
    }

    pub fn set_arcs(&mut self, arcs: Vec<(VId, VId, VLabel)>) {
        self.arcs = arcs;
    }

    pub fn set_edges(&mut self, edges: Vec<(VId, VId, VLabel)>) {
        self.edges = edges;
    }

    pub fn set_constraint(&mut self, constraint: Option<Expr>) {
        self.constraint = constraint;
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

    pub fn constraint(&self) -> Option<&Expr> {
        self.constraint.as_ref()
    }
}

#[derive(Debug, PartialEq, Clone)]
pub enum Expr {
    VId(VId),
    Int(VId),
    Bool(bool),
    And(Box<Expr>, Box<Expr>),
    Or(Box<Expr>, Box<Expr>),
    Not(Box<Expr>),
    Lt(Box<Expr>, Box<Expr>),
    Ge(Box<Expr>, Box<Expr>),
    Eq(Box<Expr>, Box<Expr>),
    Neq(Box<Expr>, Box<Expr>),
    Mod(Box<Expr>, Box<Expr>),
}
