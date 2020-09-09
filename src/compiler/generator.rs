//! Code generator.

use crate::{
    compiler::ast::{Ast, Atom, BuiltIn, Expr},
    pattern_graph::PatternGraph,
    types::{EdgeConstraint, GlobalConstraint, VId, VertexConstraint},
};
use std::collections::{hash_map::Entry, HashMap, HashSet};

pub struct Edges {
    vertices: HashMap<VId, HashSet<VId>>,
}

impl Edges {
    pub fn new(ast: &Ast) -> Self {
        let mut vertices: HashMap<VId, HashSet<VId>> = HashMap::new();
        ast.arcs()
            .iter()
            .chain(ast.edges())
            .for_each(|&(u1, u2, _)| {
                vertices
                    .entry(u1)
                    .and_modify(|neighbors| {
                        neighbors.insert(u2);
                    })
                    .or_insert(vec![u2].into_iter().collect());
                vertices
                    .entry(u2)
                    .and_modify(|neighbors| {
                        neighbors.insert(u1);
                    })
                    .or_insert(vec![u1].into_iter().collect());
            });
        Self { vertices }
    }

    fn contains(&self, u1: VId, u2: VId) -> bool {
        self.vertices
            .get(&u1)
            .map_or(false, |neighbors| neighbors.contains(&u2))
    }
}

fn extract_vertices_aux(expr: &Expr, vertices: &mut HashSet<VId>) {
    match expr {
        Expr::Constant(atom) => {
            if let Atom::VId(vid) = atom {
                vertices.insert(*vid);
            }
        }
        Expr::Application(_, args) => {
            for arg in args {
                extract_vertices_aux(arg, vertices);
            }
        }
    }
}

/// Extracts vertices from an `expr`.
fn extract_vertices(expr: &Expr) -> HashSet<VId> {
    let mut vertices = HashSet::new();
    extract_vertices_aux(expr, &mut vertices);
    vertices
}

/// Renames the `VId` in the `expr`.
///
/// For every key-value pair in `rules`, the key is the old `VId` and the value is the new one.
pub fn rename_vid(expr: &mut Expr, rules: &HashMap<VId, VId>) {
    match expr {
        Expr::Constant(atom) => {
            if let Atom::VId(old) = atom {
                *old = *rules.get(old).unwrap();
            }
        }
        Expr::Application(_, args) => {
            for arg in args {
                rename_vid(arg, rules);
            }
        }
    }
}

/// Create a new expression by concatenating the `exprs` with AND.
///
/// Duplicate constraints will also be removed by this function,
/// and the length of `exprs` will never be 0.
fn merge_exprs(mut exprs: Vec<Expr>) -> Expr {
    exprs.sort();
    exprs.dedup();
    if exprs.len() >= 2 {
        Expr::Application(Box::new(Expr::Constant(Atom::BuiltIn(BuiltIn::And))), exprs)
    } else {
        exprs.pop().unwrap()
    }
}

fn emit_vertex_constraint_and(args: &[Expr]) -> VertexConstraint {
    let args: Vec<_> = args.iter().map(emit_vertex_constraint).collect();
    VertexConstraint::new(Box::new(move |vid| args.iter().all(|f| f.f()(vid))))
}

fn emit_vertex_constraint_or(args: &[Expr]) -> VertexConstraint {
    let args: Vec<_> = args.iter().map(emit_vertex_constraint).collect();
    VertexConstraint::new(Box::new(move |vid| args.iter().any(|f| f.f()(vid))))
}

fn emit_vertex_constraint_not(arg: &Expr) -> VertexConstraint {
    let arg = emit_vertex_constraint(arg);
    VertexConstraint::new(Box::new(move |vid| !arg.f()(vid)))
}

fn emit_vertex_constraint_lt(left: &Expr, right: &Expr) -> VertexConstraint {
    match (left, right) {
        (Expr::Constant(Atom::VId(_)), Expr::Constant(Atom::Num(n))) => {
            let n = *n;
            VertexConstraint::new(Box::new(move |vid| vid < n))
        }
        (Expr::Constant(Atom::Num(n)), Expr::Constant(Atom::VId(_))) => {
            let n = *n;
            VertexConstraint::new(Box::new(move |vid| n < vid))
        }
        _ => panic!("Invalid vertex constraint LT"),
    }
}

fn emit_vertex_constraint_ge(left: &Expr, right: &Expr) -> VertexConstraint {
    match (left, right) {
        (Expr::Constant(Atom::VId(_)), Expr::Constant(Atom::Num(n))) => {
            let n = *n;
            VertexConstraint::new(Box::new(move |vid| vid >= n))
        }
        (Expr::Constant(Atom::Num(n)), Expr::Constant(Atom::VId(_))) => {
            let n = *n;
            VertexConstraint::new(Box::new(move |vid| n >= vid))
        }
        _ => panic!("Invalid vertex constraint GE"),
    }
}

fn emit_vertex_constraint(expr: &Expr) -> VertexConstraint {
    match expr {
        Expr::Constant(_) => panic!("Vertex constraint should not be Constant"),
        Expr::Application(fun, args) => {
            if let Expr::Constant(Atom::BuiltIn(builtin)) = &**fun {
                match builtin {
                    BuiltIn::And => emit_vertex_constraint_and(args),
                    BuiltIn::Or => emit_vertex_constraint_or(args),
                    BuiltIn::Not => emit_vertex_constraint_not(&args[0]),
                    BuiltIn::LT => emit_vertex_constraint_lt(&args[0], &args[1]),
                    BuiltIn::GE => emit_vertex_constraint_ge(&args[0], &args[1]),
                }
            } else {
                panic!("Invalid application: {:?}", &**fun)
            }
        }
    }
}

fn emit_edge_constraint_and(args: &[Expr]) -> EdgeConstraint {
    let args: Vec<_> = args.iter().map(emit_edge_constraint).collect();
    EdgeConstraint::new(Box::new(move |u1, u2| args.iter().all(|f| f.f()(u1, u2))))
}

fn emit_edge_constraint_or(args: &[Expr]) -> EdgeConstraint {
    let args: Vec<_> = args.iter().map(emit_edge_constraint).collect();
    EdgeConstraint::new(Box::new(move |u1, u2| args.iter().any(|f| f.f()(u1, u2))))
}

fn emit_edge_constraint_not(arg: &Expr) -> EdgeConstraint {
    let arg = emit_edge_constraint(arg);
    EdgeConstraint::new(Box::new(move |u1, u2| !arg.f()(u1, u2)))
}

fn emit_edge_constraint_lt(left: &Expr, right: &Expr) -> EdgeConstraint {
    match (left, right) {
        (Expr::Constant(Atom::VId(1)), Expr::Constant(Atom::VId(2))) => {
            EdgeConstraint::new(Box::new(|u1, u2| u1 < u2))
        }
        (Expr::Constant(Atom::VId(2)), Expr::Constant(Atom::VId(1))) => {
            EdgeConstraint::new(Box::new(|u1, u2| u2 < u1))
        }
        (Expr::Constant(Atom::VId(1)), Expr::Constant(Atom::Num(n))) => {
            let n = *n;
            EdgeConstraint::new(Box::new(move |u1, _| u1 < n))
        }
        (Expr::Constant(Atom::Num(n)), Expr::Constant(Atom::VId(1))) => {
            let n = *n;
            EdgeConstraint::new(Box::new(move |u1, _| n < u1))
        }
        (Expr::Constant(Atom::VId(2)), Expr::Constant(Atom::Num(n))) => {
            let n = *n;
            EdgeConstraint::new(Box::new(move |_, u2| u2 < n))
        }
        (Expr::Constant(Atom::Num(n)), Expr::Constant(Atom::VId(2))) => {
            let n = *n;
            EdgeConstraint::new(Box::new(move |_, u2| n < u2))
        }
        _ => panic!("Invalid edge constraint LT"),
    }
}

fn emit_edge_constraint_ge(left: &Expr, right: &Expr) -> EdgeConstraint {
    match (left, right) {
        (Expr::Constant(Atom::VId(1)), Expr::Constant(Atom::VId(2))) => {
            EdgeConstraint::new(Box::new(|u1, u2| u1 >= u2))
        }
        (Expr::Constant(Atom::VId(2)), Expr::Constant(Atom::VId(1))) => {
            EdgeConstraint::new(Box::new(|u1, u2| u2 >= u1))
        }
        (Expr::Constant(Atom::VId(1)), Expr::Constant(Atom::Num(n))) => {
            let n = *n;
            EdgeConstraint::new(Box::new(move |u1, _| u1 >= n))
        }
        (Expr::Constant(Atom::Num(n)), Expr::Constant(Atom::VId(1))) => {
            let n = *n;
            EdgeConstraint::new(Box::new(move |u1, _| n >= u1))
        }
        (Expr::Constant(Atom::VId(2)), Expr::Constant(Atom::Num(n))) => {
            let n = *n;
            EdgeConstraint::new(Box::new(move |_, u2| u2 >= n))
        }
        (Expr::Constant(Atom::Num(n)), Expr::Constant(Atom::VId(2))) => {
            let n = *n;
            EdgeConstraint::new(Box::new(move |_, u2| n >= u2))
        }
        _ => panic!("Invalid edge constraint GE"),
    }
}

fn emit_edge_constraint(expr: &Expr) -> EdgeConstraint {
    match expr {
        Expr::Constant(_) => panic!("Edge constraint should not be Constant"),
        Expr::Application(fun, args) => {
            if let Expr::Constant(Atom::BuiltIn(builtin)) = &**fun {
                match builtin {
                    BuiltIn::And => emit_edge_constraint_and(args),
                    BuiltIn::Or => emit_edge_constraint_or(args),
                    BuiltIn::Not => emit_edge_constraint_not(&args[0]),
                    BuiltIn::LT => emit_edge_constraint_lt(&args[0], &args[1]),
                    BuiltIn::GE => emit_edge_constraint_ge(&args[0], &args[1]),
                }
            } else {
                panic!("Invalid application: {:?}", &**fun)
            }
        }
    }
}

fn emit_flip_edge_constraint(expr: &Expr) -> EdgeConstraint {
    let f = emit_edge_constraint(expr);
    EdgeConstraint::new(Box::new(move |u2, u1| f.f()(u1, u2)))
}

fn emit_global_constraint_and(args: &[Expr]) -> GlobalConstraint {
    let args: Vec<_> = args.iter().map(emit_global_constraint).collect();
    GlobalConstraint::new(Box::new(move |eqvs| args.iter().all(|f| f.f()(eqvs))))
}

fn emit_global_constraint_or(args: &[Expr]) -> GlobalConstraint {
    let args: Vec<_> = args.iter().map(emit_global_constraint).collect();
    GlobalConstraint::new(Box::new(move |eqvs| args.iter().any(|f| f.f()(eqvs))))
}

fn emit_global_constraint_not(arg: &Expr) -> GlobalConstraint {
    let arg = emit_global_constraint(arg);
    GlobalConstraint::new(Box::new(move |eqvs| !arg.f()(eqvs)))
}

fn emit_global_constraint_lt(left: &Expr, right: &Expr) -> GlobalConstraint {
    match (left, right) {
        (Expr::Constant(Atom::VId(l)), Expr::Constant(Atom::VId(r))) => {
            let (l, r) = (*l as usize, *r as usize);
            GlobalConstraint::new(Box::new(move |eqvs| unsafe {
                eqvs.get_unchecked(l) < eqvs.get_unchecked(r)
            }))
        }
        (Expr::Constant(Atom::VId(l)), Expr::Constant(Atom::Num(n))) => {
            let (l, n) = (*l as usize, *n);
            GlobalConstraint::new(Box::new(move |eqvs| *unsafe { eqvs.get_unchecked(l) } < n))
        }
        (Expr::Constant(Atom::Num(n)), Expr::Constant(Atom::VId(r))) => {
            let (n, r) = (*n, *r as usize);
            GlobalConstraint::new(Box::new(move |eqvs| n < *unsafe { eqvs.get_unchecked(r) }))
        }
        _ => panic!("Invalid global constraint (LT {:?} {:?})", left, right),
    }
}

fn emit_global_constraint_ge(left: &Expr, right: &Expr) -> GlobalConstraint {
    match (left, right) {
        (Expr::Constant(Atom::VId(l)), Expr::Constant(Atom::VId(r))) => {
            let (l, r) = (*l as usize, *r as usize);
            GlobalConstraint::new(Box::new(move |eqvs| unsafe {
                eqvs.get_unchecked(l) >= eqvs.get_unchecked(r)
            }))
        }
        (Expr::Constant(Atom::VId(l)), Expr::Constant(Atom::Num(n))) => {
            let (l, n) = (*l as usize, *n);
            GlobalConstraint::new(Box::new(move |eqvs| *unsafe { eqvs.get_unchecked(l) } >= n))
        }
        (Expr::Constant(Atom::Num(n)), Expr::Constant(Atom::VId(r))) => {
            let (n, r) = (*n, *r as usize);
            GlobalConstraint::new(Box::new(move |eqvs| n >= *unsafe { eqvs.get_unchecked(r) }))
        }
        _ => panic!("Invalid global constraint (LT {:?} {:?})", left, right),
    }
}

fn emit_global_constraint(expr: &Expr) -> GlobalConstraint {
    match expr {
        Expr::Constant(_) => panic!("Global constraint should not be Constant"),
        Expr::Application(fun, args) => {
            if let Expr::Constant(Atom::BuiltIn(builtin)) = &**fun {
                match builtin {
                    BuiltIn::And => emit_global_constraint_and(args),
                    BuiltIn::Or => emit_global_constraint_or(args),
                    BuiltIn::Not => emit_global_constraint_not(&args[0]),
                    BuiltIn::LT => emit_global_constraint_lt(&args[0], &args[1]),
                    BuiltIn::GE => emit_global_constraint_ge(&args[0], &args[1]),
                }
            } else {
                panic!("Invalid application: {:?}", &**fun)
            }
        }
    }
}

/// Compile the global constraint.
///
/// This function will not check whether `expr` is a valid global constraint,
/// be careful to call this function.
pub unsafe fn compile_global_constraint(
    expr: &Expr,
    vertex_eqv: &HashMap<VId, usize>,
) -> GlobalConstraint {
    let rules: HashMap<VId, VId> = vertex_eqv
        .iter()
        .map(|(&vid, &eqv)| (vid, eqv as VId))
        .collect();
    let mut expr = expr.clone();
    rename_vid(&mut expr, &rules);
    emit_global_constraint(&expr)
}

/// Splits the constraints into `(vertex constraints, edge constraints, global constraints)`.
///
/// For vertex constraints, the vertex ID is renamed to 0.
/// For edges constraints, the key is a sorted pair and is renamed to (1, 2).
/// The global constraints are not renamed.
pub fn split_constraints(
    edges: &Edges,
    exprs: Vec<Expr>,
) -> (HashMap<VId, Expr>, HashMap<(VId, VId), Expr>, Vec<Expr>) {
    let mut vcs: HashMap<VId, Vec<Expr>> = HashMap::new();
    let mut ecs: HashMap<(VId, VId), Vec<Expr>> = HashMap::new();
    let mut gcs: Vec<Expr> = Vec::new();
    for expr in exprs {
        let mut vertices: Vec<VId> = extract_vertices(&expr).into_iter().collect();
        vertices.sort();
        match vertices.len() {
            1 => {
                let mut expr = expr;
                rename_vid(&mut expr, &vec![(vertices[0], 0)].into_iter().collect());
                let entry = vcs.entry(vertices[0]);
                match entry {
                    Entry::Occupied(_) => {
                        entry.and_modify(|exprs| exprs.push(expr));
                    }
                    Entry::Vacant(_) => {
                        entry.or_insert(vec![expr]);
                    }
                }
            }
            2 => {
                if edges.contains(vertices[0], vertices[1]) {
                    let mut expr = expr;
                    rename_vid(
                        &mut expr,
                        &vec![(vertices[0], 1), (vertices[1], 2)]
                            .into_iter()
                            .collect(),
                    );
                    let entry = ecs.entry((vertices[0], vertices[1]));
                    match entry {
                        Entry::Occupied(_) => {
                            entry.and_modify(|exprs| exprs.push(expr));
                        }
                        Entry::Vacant(_) => {
                            entry.or_insert(vec![expr]);
                        }
                    }
                } else {
                    gcs.push(expr);
                }
            }
            _ => gcs.push(expr),
        }
    }
    (
        vcs.into_iter()
            .map(|(vid, exprs)| (vid, merge_exprs(exprs)))
            .collect(),
        ecs.into_iter()
            .map(|(key, exprs)| (key, merge_exprs(exprs)))
            .collect(),
        gcs,
    )
}

pub struct VertexConstraintsInfo {
    constraints: Vec<VertexConstraint>,
    vid_offsets: Vec<(VId, usize)>,
}

impl VertexConstraintsInfo {
    pub fn empty() -> Self {
        Self {
            constraints: vec![],
            vid_offsets: vec![],
        }
    }

    pub fn add_to_graph<'a>(&'a self, p: &mut PatternGraph<'a>) {
        for &(vid, offset) in &self.vid_offsets {
            p.add_vertex_constraint(vid, Some(&self.constraints[offset]));
        }
    }
}

// private methods
impl VertexConstraintsInfo {
    fn new(constraints: Vec<VertexConstraint>, vid_offsets: Vec<(VId, usize)>) -> Self {
        Self {
            constraints,
            vid_offsets,
        }
    }
}

/// Generates vertex constraints.
///
/// Returns a vector of vertex constraints, and a vector stores the mapping from `VId` to the offset
/// in the first vector.
pub fn generate_vertex_constraints(vid_exprs: &HashMap<VId, Expr>) -> VertexConstraintsInfo {
    let mut expr_vids: HashMap<&Expr, Vec<VId>> = HashMap::with_capacity(vid_exprs.len());
    for (&vid, expr) in vid_exprs {
        expr_vids
            .entry(expr)
            .and_modify(|vids| vids.push(vid))
            .or_insert(vec![vid]);
    }
    let mut constraints = Vec::with_capacity(expr_vids.len());
    let mut vid_offsets = Vec::with_capacity(vid_exprs.len());
    for (expr, vids) in expr_vids {
        let offset = constraints.len();
        constraints.push(emit_vertex_constraint(expr));
        for vid in vids {
            vid_offsets.push((vid, offset));
        }
    }
    VertexConstraintsInfo::new(constraints, vid_offsets)
}

pub struct EdgeConstraintsInfo {
    constraints: Vec<(EdgeConstraint, EdgeConstraint)>,
    edge_offsets: Vec<((VId, VId), usize)>,
}

impl EdgeConstraintsInfo {
    pub fn empty() -> Self {
        Self {
            constraints: vec![],
            edge_offsets: vec![],
        }
    }

    pub fn add_to_graph<'a>(&'a self, p: &mut PatternGraph<'a>) {
        for &((u1, u2), offset) in &self.edge_offsets {
            let (f12, f21) = &self.constraints[offset];
            p.add_edge_constraint(u1, u2, Some((f12, f21)));
        }
    }
}

// private methods.
impl EdgeConstraintsInfo {
    fn new(
        constraints: Vec<(EdgeConstraint, EdgeConstraint)>,
        edge_offsets: Vec<((VId, VId), usize)>,
    ) -> Self {
        Self {
            constraints,
            edge_offsets,
        }
    }
}

/// Generate edge constraints.
///
/// Returns a vector of edge constraints where each item contains *(f(u1, u2), f(u2, u1))*,
/// and a vector indicates the mapping from *(u1, u2)* to the offset of the first vector.
pub fn generate_edge_constraints(edge_exprs: &HashMap<(VId, VId), Expr>) -> EdgeConstraintsInfo {
    let mut expr_edges: HashMap<&Expr, Vec<(VId, VId)>> = HashMap::with_capacity(edge_exprs.len());
    for (&(u1, u2), expr) in edge_exprs {
        expr_edges
            .entry(expr)
            .and_modify(|edges| edges.push((u1, u2)))
            .or_insert(vec![(u1, u2)]);
    }
    let mut f12_f21s = Vec::with_capacity(expr_edges.len());
    let mut edge_offsets = Vec::with_capacity(edge_exprs.len());
    for (expr, edges) in expr_edges {
        let offset = f12_f21s.len();
        f12_f21s.push((emit_edge_constraint(expr), emit_flip_edge_constraint(expr)));
        for edge in edges {
            edge_offsets.push((edge, offset));
        }
    }
    EdgeConstraintsInfo::new(f12_f21s, edge_offsets)
}

/// Generates pattern graph without constraints.
pub fn generate_pattern_graph<'a>(ast: &Ast) -> PatternGraph<'a> {
    let mut p = PatternGraph::new();
    for &(vid, vlabel) in ast.vertices() {
        p.add_vertex(vid, vlabel);
    }
    for &(u1, u2, elabel) in ast.arcs() {
        p.add_arc(u1, u2, elabel);
    }
    for &(u1, u2, elabel) in ast.edges() {
        p.add_edge(u1, u2, elabel);
    }
    p
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compiler::{
        parser::{expr_parse, parse},
        rewriter::rewrite,
    };

    #[test]
    fn test_extract_vertices() {
        let mut vertices: Vec<VId> =
            extract_vertices(&expr_parse("(and (< u1 u2) (not (< u1 u3)))").unwrap())
                .into_iter()
                .collect();
        vertices.sort();
        assert_eq!(vertices, &[1, 2, 3]);
    }

    #[test]
    fn test_emit_vertex_constraint() {
        let f = emit_vertex_constraint(&expr_parse("(and (< 3 u0) (< u0 6))").unwrap());
        assert_eq!(
            vec![3, 4, 5, 6].into_iter().map(f.f()).collect::<Vec<_>>(),
            vec![false, true, true, false]
        );
        let f = emit_vertex_constraint(&expr_parse("(not (and (< 3 u0) (< u0 6)))").unwrap());
        assert_eq!(
            vec![3, 4, 5, 6].into_iter().map(f.f()).collect::<Vec<_>>(),
            vec![true, false, false, true]
        );
        let f = emit_vertex_constraint(&expr_parse("(or (not (< 3 u0)) (>= u0 6))").unwrap());
        assert_eq!(
            vec![3, 4, 5, 6].into_iter().map(f.f()).collect::<Vec<_>>(),
            vec![true, false, false, true]
        );
    }

    #[test]
    fn test_emit_edge_constraint() {
        let f = emit_edge_constraint(&expr_parse("(and (< 3 u1) (< u1 u2))").unwrap());
        assert_eq!(
            vec![(3, 4), (4, 4), (4, 5), (5, 4)]
                .into_iter()
                .map(|(u1, u2)| f.f()(u1, u2))
                .collect::<Vec<_>>(),
            vec![false, false, true, false]
        );
        let f = emit_edge_constraint(&expr_parse("(or (< 5 u1) (< u1 u2))").unwrap());
        assert_eq!(
            vec![(3, 4), (4, 4), (4, 5), (5, 4)]
                .into_iter()
                .map(|(u1, u2)| f.f()(u1, u2))
                .collect::<Vec<_>>(),
            vec![true, false, true, false]
        );
        let f = emit_edge_constraint(&expr_parse("(not (or (< 5 u1) (< u1 u2)))").unwrap());
        assert_eq!(
            vec![(3, 4), (4, 4), (4, 5), (5, 4)]
                .into_iter()
                .map(|(u1, u2)| f.f()(u1, u2))
                .collect::<Vec<_>>(),
            vec![false, true, false, true]
        );
        let f = emit_edge_constraint(&expr_parse("(or (not (< 3 u1)) (>= u1 u2))").unwrap());
        assert_eq!(
            vec![(3, 4), (4, 4), (4, 5), (5, 4)]
                .into_iter()
                .map(|(u1, u2)| f.f()(u1, u2))
                .collect::<Vec<_>>(),
            vec![true, true, false, true]
        );
    }

    #[test]
    fn test_emit_flip_edge_constraint() {
        let f = emit_flip_edge_constraint(&expr_parse("(< u1 u2)").unwrap());
        assert_eq!(
            vec![(1, 2), (2, 2), (3, 2)]
                .into_iter()
                .map(|(u1, u2)| f.f()(u2, u1))
                .collect::<Vec<_>>(),
            vec![true, false, false]
        );
    }

    #[test]
    fn test_compile_global_constraint() {
        let f = unsafe {
            compile_global_constraint(
                &expr_parse("(or (< u1 u2) (< u1 u3))").unwrap(),
                &vec![(1, 0), (2, 1), (3, 2)].into_iter().collect(),
            )
        };
        assert_eq!(
            [&[1 as VId, 2, 3], &[2, 3, 1], &[3, 1, 2], &[3, 2, 1]]
                .iter()
                .map(|&eqvs| f.f()(eqvs))
                .collect::<Vec<_>>(),
            vec![true, true, false, false]
        );
    }

    #[test]
    fn test_split() {
        let edges = Edges::new(
            &parse(
                "\
(match (vertices (u1 0) (u2 0) (u3 0))
       (edges (u1 u2 0) (u1 u3 0)))",
            )
            .unwrap(),
        );
        assert_eq!(
            split_constraints(
                &edges,
                rewrite(
                    &expr_parse("(and (< 3 u1 10) (< 3 u2 10) (< u1 u2) (< u1 u2) (< u2 u3))")
                        .unwrap()
                )
            ),
            (
                vec![
                    (1, expr_parse("(and (< u0 10) (< 3 u0))").unwrap()),
                    (2, expr_parse("(and (< u0 10) (< 3 u0))").unwrap())
                ]
                .into_iter()
                .collect(),
                vec![((1, 2), expr_parse("(< u1 u2)").unwrap())]
                    .into_iter()
                    .collect(),
                vec![expr_parse("(< u2 u3)").unwrap()]
            )
        );
    }

    #[test]
    fn test_generate_pattern_graph() {
        let p = generate_pattern_graph(
            &parse(
                "\
(match (vertices (u1 1) (u2 1) (u3 1) (u4 2))
       (arcs (u1 u2 10) (u1 u3 10) (u1 u4 20) (u1 u4 30)
             (u2 u1 10) (u2 u4 20) (u3 u1 10) (u3 u4 20))
       (where (and (< u1 u2) (< u1 u3) (not (or (>= u2 u3) (>= u4 2020))))))",
            )
            .unwrap(),
        );
        let mut vertices: Vec<_> = p.vertices().collect();
        vertices.sort();
        assert_eq!(vertices, vec![(1, 1), (2, 1), (3, 1), (4, 2)]);
        let mut arcs: Vec<_> = p.arcs().collect();
        arcs.sort();
        assert_eq!(
            arcs,
            vec![
                (1, 2, 10),
                (1, 3, 10),
                (1, 4, 20),
                (1, 4, 30),
                (2, 1, 10),
                (2, 4, 20),
                (3, 1, 10),
                (3, 4, 20)
            ]
        );
    }
}
