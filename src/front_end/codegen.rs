use std::collections::{HashMap, HashSet};

use crate::{
    front_end::{
        types::{EdgeConstraint, VertexConstraint},
        Expr,
    },
    pattern::PatternGraph,
    types::{ELabel, VId, VLabel},
};

pub fn codegen(
    vertices: &[(VId, VLabel)],
    arcs: &[(VId, VId, ELabel)],
    edges: &[(VId, VId, ELabel)],
    constraints: Vec<Expr>,
) -> (PatternGraph, Vec<Expr>) {
    let mut p = emit_pattern_graph(vertices, arcs, edges);
    let mut gcs = vec![];
    for mut expr in constraints {
        let mut vertices: Vec<VId> = extract_vertices(&expr).into_iter().collect();
        vertices.sort();
        match vertices.as_slice() {
            &[u1] => {
                rename_vid(&mut expr, &vec![(u1, 0)].into_iter().collect());
                p.add_vertex_constraint(u1, Some(emit_vertex_constraint(&expr)));
            }
            &[u1, u2] if p.neighbors(u1).unwrap().contains_key(&u2) => {
                rename_vid(&mut expr, &vec![(u1, 0), (u2, 1)].into_iter().collect());
                let f12 = emit_edge_constraint(&expr);
                rename_vid(&mut expr, &vec![(0, 1), (1, 0)].into_iter().collect());
                let f21 = emit_edge_constraint(&expr);
                p.add_edge_constraint(u1, u2, Some((f12, f21)));
            }
            _ => gcs.push(expr),
        }
    }
    (p, gcs)
}

fn emit_pattern_graph(
    vertices: &[(VId, VLabel)],
    arcs: &[(VId, VId, ELabel)],
    edges: &[(VId, VId, ELabel)],
) -> PatternGraph {
    let mut p = PatternGraph::new();
    for &(vid, vlabel) in vertices {
        p.add_vertex(vid, vlabel);
    }
    for &(src, dst, elabel) in arcs {
        p.add_arc(src, dst, elabel);
    }
    for &(src, dst, elabel) in edges {
        p.add_edge(src, dst, elabel);
    }
    p
}

fn extract_vertices_aux(expr: &Expr, vertices: &mut HashSet<VId>) {
    match expr {
        &Expr::VId(u) => {
            vertices.insert(u);
        }
        Expr::Int(_) | Expr::Bool(_) => (),
        Expr::Not(arg1) => extract_vertices_aux(arg1, vertices),
        Expr::And(arg1, arg2)
        | Expr::Or(arg1, arg2)
        | Expr::Lt(arg1, arg2)
        | Expr::Ge(arg1, arg2)
        | Expr::Eq(arg1, arg2)
        | Expr::Neq(arg1, arg2)
        | Expr::Mod(arg1, arg2) => {
            extract_vertices_aux(arg1, vertices);
            extract_vertices_aux(arg2, vertices);
        }
    }
}

fn extract_vertices(expr: &Expr) -> HashSet<VId> {
    let mut vertices = HashSet::new();
    extract_vertices_aux(expr, &mut vertices);
    vertices
}

/// Renames the `VId` in the `expr`.
///
/// For every key-value pair in `rules`, the key is the old `VId` and the value is the new one.
fn rename_vid(expr: &mut Expr, rules: &HashMap<VId, VId>) {
    match expr {
        Expr::VId(u) => *u = *rules.get(u).unwrap(),
        Expr::Int(_) | Expr::Bool(_) => (),
        Expr::Not(arg1) => {
            rename_vid(arg1, rules);
        }
        Expr::And(arg1, arg2)
        | Expr::Or(arg1, arg2)
        | Expr::Lt(arg1, arg2)
        | Expr::Ge(arg1, arg2)
        | Expr::Eq(arg1, arg2)
        | Expr::Neq(arg1, arg2)
        | Expr::Mod(arg1, arg2) => {
            rename_vid(arg1, rules);
            rename_vid(arg2, rules);
        }
    }
}

#[derive(PartialEq)]
enum Value {
    Bool(bool),
    Int(VId),
}

fn eval(expr: &Expr, env: &[VId]) -> Value {
    match expr {
        &Expr::VId(u) => Value::Int(env[u as usize]),
        &Expr::Int(x) => Value::Int(x),
        &Expr::Bool(x) => Value::Bool(x),
        Expr::And(arg1, arg2) => Value::Bool(
            eval(arg1, env) == Value::Bool(true) && eval(arg2, env) == Value::Bool(true),
        ),
        Expr::Or(arg1, arg2) => Value::Bool(
            eval(arg1, env) == Value::Bool(true) || eval(arg2, env) == Value::Bool(true),
        ),
        Expr::Not(arg1) => {
            if let Value::Bool(x) = eval(arg1, env) {
                Value::Bool(!x)
            } else {
                unreachable!()
            }
        }
        Expr::Lt(arg1, arg2) => {
            if let (Value::Int(x), Value::Int(y)) = (eval(arg1, env), eval(arg2, env)) {
                Value::Bool(x < y)
            } else {
                unreachable!()
            }
        }
        Expr::Ge(arg1, arg2) => {
            if let (Value::Int(x), Value::Int(y)) = (eval(arg1, env), eval(arg2, env)) {
                Value::Bool(x >= y)
            } else {
                unreachable!()
            }
        }
        Expr::Eq(arg1, arg2) => {
            if let (Value::Int(x), Value::Int(y)) = (eval(arg1, env), eval(arg2, env)) {
                Value::Bool(x == y)
            } else {
                unreachable!()
            }
        }
        Expr::Neq(arg1, arg2) => {
            if let (Value::Int(x), Value::Int(y)) = (eval(arg1, env), eval(arg2, env)) {
                Value::Bool(x != y)
            } else {
                unreachable!()
            }
        }
        Expr::Mod(arg1, arg2) => {
            if let (Value::Int(x), Value::Int(y)) = (eval(arg1, env), eval(arg2, env)) {
                Value::Int(x % y)
            } else {
                unreachable!()
            }
        }
    }
}

pub fn emit_vertex_constraint(expr: &Expr) -> VertexConstraint {
    let expr = expr.clone();
    VertexConstraint::new(
        expr.clone(),
        Box::new(move |v| {
            if let Value::Bool(x) = eval(&expr, &[v]) {
                x
            } else {
                unreachable!()
            }
        }),
    )
}

fn emit_edge_constraint(expr: &Expr) -> EdgeConstraint {
    let expr = expr.clone();
    EdgeConstraint::new(
        expr.clone(),
        Box::new(move |v0, v1| {
            if let Value::Bool(x) = eval(&expr, &[v0, v1]) {
                x
            } else {
                unreachable!()
            }
        }),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::front_end::{expr_parse, parse, rewrite};

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
        let f = emit_edge_constraint(&expr_parse("(and (< 3 u0) (< u0 u1))").unwrap());
        assert_eq!(
            vec![(3, 4), (4, 4), (4, 5), (5, 4)]
                .into_iter()
                .map(|(u1, u2)| f.f()(u1, u2))
                .collect::<Vec<_>>(),
            vec![false, false, true, false]
        );
        let f = emit_edge_constraint(&expr_parse("(or (< 5 u0) (< u0 u1))").unwrap());
        assert_eq!(
            vec![(3, 4), (4, 4), (4, 5), (5, 4)]
                .into_iter()
                .map(|(u1, u2)| f.f()(u1, u2))
                .collect::<Vec<_>>(),
            vec![true, false, true, false]
        );
        let f = emit_edge_constraint(&expr_parse("(not (or (< 5 u0) (< u0 u1)))").unwrap());
        assert_eq!(
            vec![(3, 4), (4, 4), (4, 5), (5, 4)]
                .into_iter()
                .map(|(u1, u2)| f.f()(u1, u2))
                .collect::<Vec<_>>(),
            vec![false, true, false, true]
        );
        let f = emit_edge_constraint(&expr_parse("(or (not (< 3 u0)) (>= u0 u1))").unwrap());
        assert_eq!(
            vec![(3, 4), (4, 4), (4, 5), (5, 4)]
                .into_iter()
                .map(|(u1, u2)| f.f()(u1, u2))
                .collect::<Vec<_>>(),
            vec![true, true, false, true]
        );
    }

    #[test]
    fn test_codegen() {
        let ast = parse(
            "\
    (match (vertices (u1 1) (u2 1) (u3 1) (u4 2))
           (arcs (u1 u2 10) (u1 u3 10) (u1 u4 20) (u1 u4 30)
                 (u2 u1 10) (u2 u4 20) (u3 u1 10) (u3 u4 20))
           (where (and (< u1 u2) (and (< u1 u3) (not (or (>= u2 u3) (>= u4 2020)))))))",
        )
        .unwrap();
        let (p, gcs) = codegen(
            ast.vertices(),
            ast.arcs(),
            ast.edges(),
            rewrite(ast.constraint().unwrap().clone()),
        );
        assert_eq!(p.vertices(), vec![(1, 1), (2, 1), (3, 1), (4, 2)]);
        assert_eq!(
            p.arcs(),
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
        assert_eq!(gcs, vec![expr_parse("(< u2 u3)").unwrap()]);
    }
}
