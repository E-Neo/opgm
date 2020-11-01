//! Semantic checker.

use crate::compiler::{
    ast::{Ast, Atom, BuiltIn, Expr},
    error,
};
use crate::types::{ELabel, VId, VLabel};
use std::collections::{HashMap, HashSet};

fn check_vertices<'a>(vertices: &[(VId, VLabel)]) -> Result<(), error::Err<'a>> {
    let vid_vlabels: HashMap<VId, VLabel> = vertices.iter().map(|&x| x).collect();
    if vid_vlabels.len() == vertices.len() {
        Ok(())
    } else {
        Err(error::Err::DuplicateVertex)
    }
}

fn check_arcs<'a>(
    vertices: &HashSet<VId>,
    arcs: &[(VId, VId, ELabel)],
) -> Result<(), error::Err<'a>> {
    let mut arcs_set = HashSet::new();
    for &(u1, u2, elabel) in arcs {
        if vertices.contains(&u1) && vertices.contains(&u2) {
            if !arcs_set.insert((u1, u2, elabel)) {
                return Err(error::Err::DuplicateArc);
            }
        } else {
            return Err(error::Err::InvalidVertex);
        }
    }
    Ok(())
}

fn check_edges<'a>(
    vertices: &HashSet<VId>,
    edges: &[(VId, VId, ELabel)],
) -> Result<(), error::Err<'a>> {
    let mut edges_set = HashSet::new();
    for &(u1, u2, elabel) in edges {
        if vertices.contains(&u1) && vertices.contains(&u2) {
            if !edges_set.insert((u1, u2, elabel)) {
                return Err(error::Err::DuplicateEdge);
            }
        } else {
            return Err(error::Err::InvalidVertex);
        }
    }
    Ok(())
}

fn check_graph<'a>(ast: &Ast) -> Result<HashSet<VId>, error::Err<'a>> {
    check_vertices(ast.vertices())?;
    let vertex_set = ast.vertices().iter().map(|&(vid, _)| vid).collect();
    check_arcs(&vertex_set, ast.arcs())?;
    check_edges(&vertex_set, ast.edges())?;
    Ok(vertex_set)
}

#[derive(PartialEq, Eq, Hash)]
enum TypeExpr {
    Integer,
    Boolean,
    Function,
}

fn is_type<'a>(
    vertices: &HashSet<VId>,
    expr: &Expr,
    type_expr: TypeExpr,
) -> Result<bool, error::Err<'a>> {
    Ok(type_of(vertices, expr)? == type_expr)
}

fn type_of_fun<'a>(
    vertices: &HashSet<VId>,
    fun: &BuiltIn,
    args: &[Expr],
) -> Result<TypeExpr, error::Err<'a>> {
    match fun {
        BuiltIn::And | BuiltIn::Or => {
            for arg in args {
                if !is_type(vertices, arg, TypeExpr::Boolean)? {
                    return Err(error::Err::TypeError);
                }
            }
            Ok(TypeExpr::Boolean)
        }
        BuiltIn::Not => {
            if args.len() == 1 {
                if is_type(vertices, &args[0], TypeExpr::Boolean)? {
                    Ok(TypeExpr::Boolean)
                } else {
                    Err(error::Err::TypeError)
                }
            } else {
                Err(error::Err::WrongNumOfArgs)
            }
        }
        BuiltIn::Lt | BuiltIn::Ge | BuiltIn::Eq => {
            if args.len() >= 1 {
                for arg in args {
                    if !is_type(vertices, arg, TypeExpr::Integer)? {
                        return Err(error::Err::TypeError);
                    }
                }
                Ok(TypeExpr::Boolean)
            } else {
                Err(error::Err::WrongNumOfArgs)
            }
        }
        BuiltIn::Mod => {
            if args.len() == 2 {
                for arg in args {
                    if !is_type(vertices, arg, TypeExpr::Integer)? {
                        return Err(error::Err::TypeError);
                    }
                }
                Ok(TypeExpr::Integer)
            } else {
                Err(error::Err::WrongNumOfArgs)
            }
        }
    }
}

fn type_of<'a>(vertices: &HashSet<VId>, expr: &Expr) -> Result<TypeExpr, error::Err<'a>> {
    match expr {
        Expr::Constant(atom) => match atom {
            Atom::VId(vid) => {
                if vertices.contains(vid) {
                    Ok(TypeExpr::Integer)
                } else {
                    Err(error::Err::InvalidVertex)
                }
            }
            Atom::Num(_) => Ok(TypeExpr::Integer),
            Atom::Boolean(_) => Ok(TypeExpr::Boolean),
            Atom::BuiltIn(_) => Ok(TypeExpr::Function),
        },
        Expr::Application(fun, args) => {
            if let Expr::Constant(Atom::BuiltIn(fun)) = &**fun {
                type_of_fun(vertices, fun, args)
            } else {
                Err(error::Err::InvalidFunction)
            }
        }
    }
}

fn check_constraint<'a>(vertices: &HashSet<VId>, ast: &Ast) -> Result<(), error::Err<'a>> {
    if let Some(expr) = ast.constraint() {
        if is_type(vertices, expr, TypeExpr::Boolean)? {
            Ok(())
        } else {
            Err(error::Err::InvalidConstraint)
        }
    } else {
        Ok(())
    }
}

/// Checks whether the `ast` is semantically correct.
pub fn check<'a>(ast: &Ast) -> Result<(), error::Err<'a>> {
    check_constraint(&check_graph(ast)?, ast)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compiler::parser::parse;

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
            check_graph(&parse("(match (vertices (u1 1) (u1 2)))").unwrap()),
            Err(error::Err::DuplicateVertex)
        );
        assert_eq!(
            check_graph(&parse("(match (vertices (u1 1) (u2 2)) (edges (u1 u3 12)))").unwrap()),
            Err(error::Err::InvalidVertex)
        );
        assert_eq!(
            check_graph(
                &parse("(match (vertices (u1 1) (u2 2)) (edges (u1 u2 12) (u1 u2 12)))").unwrap()
            ),
            Err(error::Err::DuplicateEdge)
        );
    }

    #[test]
    fn test_check_constraint() {
        assert_eq!(
            check_constraint(
                &vec![1, 2, 3].into_iter().collect(),
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
            check_constraint(
                &vec![1, 2, 3].into_iter().collect(),
                &parse(
                    "\
(match (vertices (u1 0) (u2 0) (u3 0))
       (edges (u1 u2 0) (u2 u3 0))
       (where (< u1 (< u1 u2))))"
                )
                .unwrap()
            ),
            Err(error::Err::TypeError)
        );
    }

    #[test]
    fn test_check() {
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
