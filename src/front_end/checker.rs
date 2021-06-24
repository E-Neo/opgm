use super::{error::Result, GispRule};
use crate::{
    front_end::{Ast, Expr},
    types::{ELabel, VId, VLabel},
};
use std::collections::HashSet;

fn check_vertices(vertices: &[(VId, VLabel)]) -> Result<HashSet<VId>> {
    let mut vids = HashSet::with_capacity(vertices.len());
    for &(vid, _) in vertices {
        if !vids.insert(vid) {
            return Err(graph_error());
        }
    }
    Ok(vids)
}

fn check_arcs(vids: &HashSet<VId>, arcs: &[(VId, VId, ELabel)]) -> Result<()> {
    let mut arc_set = HashSet::new();
    for &(src, dst, elabel) in arcs {
        if !vids.contains(&src) || !vids.contains(&dst) || !arc_set.insert((src, dst, elabel)) {
            return Err(graph_error());
        }
    }
    Ok(())
}

fn check_edges(vids: &HashSet<VId>, edges: &[(VId, VId, ELabel)]) -> Result<()> {
    let mut edge_set = HashSet::new();
    for &(src, dst, elabel) in edges {
        if !vids.contains(&src)
            || !vids.contains(&dst)
            || !edge_set.insert((src, dst, elabel))
            || !edge_set.insert((dst, src, elabel))
        {
            return Err(graph_error());
        }
    }
    Ok(())
}

fn check_graph(ast: &Ast) -> Result<HashSet<VId>> {
    if ast.arcs().is_empty() && ast.edges().is_empty() {
        Err(graph_error())
    } else {
        let vids = check_vertices(ast.vertices())?;
        check_arcs(&vids, ast.arcs())?;
        check_edges(&vids, ast.edges())?;
        Ok(vids)
    }
}

#[derive(PartialEq)]
enum Type {
    Int,
    Bool,
}

fn type_of(vids: &HashSet<VId>, expr: &Expr) -> Result<Type> {
    match expr {
        Expr::VId(vid) => {
            if vids.contains(vid) {
                Ok(Type::Int)
            } else {
                Err(graph_error())
            }
        }
        Expr::Int(_) => Ok(Type::Int),
        Expr::Bool(_) => Ok(Type::Bool),
        Expr::And(arg1, arg2) | Expr::Or(arg1, arg2) => {
            if is_type(vids, arg1, Type::Bool)? && is_type(vids, arg2, Type::Bool)? {
                Ok(Type::Bool)
            } else {
                Err(type_error())
            }
        }
        Expr::Not(arg1) => {
            if is_type(vids, arg1, Type::Bool)? {
                Ok(Type::Bool)
            } else {
                Err(type_error())
            }
        }
        Expr::Lt(arg1, arg2)
        | Expr::Ge(arg1, arg2)
        | Expr::Eq(arg1, arg2)
        | Expr::Neq(arg1, arg2) => {
            if is_type(vids, arg1, Type::Int)? && is_type(vids, arg2, Type::Int)? {
                Ok(Type::Bool)
            } else {
                Err(type_error())
            }
        }
        Expr::Mod(arg1, arg2) => {
            if is_type(vids, arg1, Type::Int)? && is_type(vids, arg2, Type::Int)? {
                Ok(Type::Int)
            } else {
                Err(type_error())
            }
        }
    }
}

fn is_type(vids: &HashSet<VId>, expr: &Expr, typ: Type) -> Result<bool> {
    Ok(type_of(vids, expr)? == typ)
}

pub fn check(ast: &Ast) -> Result<()> {
    let vids = check_graph(ast)?;
    if let Some(expr) = ast.constraint() {
        if is_type(&vids, expr, Type::Bool)? {
            Ok(())
        } else {
            Err(type_error())
        }
    } else {
        Ok(())
    }
}

fn graph_error() -> pest::error::Error<GispRule> {
    pest::error::Error::new_from_span(
        pest::error::ErrorVariant::CustomError {
            message: String::from("graph"),
        },
        pest::Span::new("", 0, 0).unwrap(),
    )
}

fn type_error() -> pest::error::Error<GispRule> {
    pest::error::Error::new_from_span(
        pest::error::ErrorVariant::CustomError {
            message: String::from("type"),
        },
        pest::Span::new("", 0, 0).unwrap(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::front_end::parse;

    #[test]
    fn test_check_graph() {
        assert_eq!(
            check_graph(
                &parse("(match (vertices (u1 1) (u2 2) (u3 3)) (edges (u1 u2 12) (u2 u3 23)))")
                    .unwrap()
            ),
            Ok(vec![1, 2, 3].into_iter().collect())
        );
        assert_eq!(
            check_graph(
                &parse("(match (vertices (u1 1) (u1 2) (u3 3)) (edges (u1 u2 12) (u2 u3 23)))")
                    .unwrap()
            ),
            Err(graph_error())
        );
        assert_eq!(
            check_graph(&parse("(match (vertices (u1 1) (u2 2)))").unwrap()),
            Err(graph_error())
        );
        assert_eq!(
            check_graph(&parse("(match (vertices (u1 1) (u2 2)) (edges (u1 u3 12)))").unwrap()),
            Err(graph_error())
        );
        assert_eq!(
            check_graph(
                &parse("(match (vertices (u1 1) (u2 2)) (edges (u1 u2 12) (u1 u2 12)))").unwrap()
            ),
            Err(graph_error())
        );
    }

    #[test]
    fn test_check() {
        assert_eq!(
            check(
                &parse(
                    "\
(match (vertices (u1 0) (u2 0) (u3 0))
       (edges (u1 u2 0) (u2 u3 0))
       (where (< u1 u2)))"
                )
                .unwrap()
            ),
            Ok(())
        );
        assert_eq!(
            check(
                &parse(
                    "\
(match (vertices (u1 0) (u2 0) (u3 0))
       (edges (u1 u2 0) (u2 u3 0))
       (where (< u1 (< u1 u2))))"
                )
                .unwrap()
            ),
            Err(type_error())
        );
        assert_eq!(
            check(
                &parse(
                    "\
(match (vertices (u1 0) (u2 0) (u3 0))
       (arcs (u1 u2 0) (u1 u3 0))
       (edges (u2 u3 0))
       (where (and (< u1 u2) (< u1 u3))))"
                )
                .unwrap()
            ),
            Ok(())
        );
    }
}
