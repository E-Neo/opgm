//! The compiler.

use crate::{
    compiler::{
        ast::{Atom, Expr},
        checker::check,
        error,
        generator::{
            generate_edge_constraints, generate_pattern_graph, generate_vertex_constraints,
            split_constraints, EdgeConstraintsInfo, Edges, VertexConstraintsInfo,
        },
        parser::parse,
        rewriter::rewrite,
    },
    pattern_graph::PatternGraph,
};

/// Compiles the graph matching query.
pub fn compile<'a>(
    input: &'a str,
) -> Result<
    (
        PatternGraph,
        VertexConstraintsInfo,
        EdgeConstraintsInfo,
        Vec<Expr>,
    ),
    error::Err<'a>,
> {
    let ast = parse(input)?;
    check(&ast)?;
    if let Some(expr) = &ast.constraint() {
        let exprs = rewrite(expr);
        match &exprs[0] {
            Expr::Constant(Atom::Boolean(true)) => (),
            Expr::Constant(Atom::Boolean(false)) => return Err(error::Err::WhereFalse),
            _ => {
                let (vcs, ecs, gcs) = split_constraints(&Edges::new(&ast), exprs);
                return Ok((
                    generate_pattern_graph(&ast),
                    generate_vertex_constraints(&vcs),
                    generate_edge_constraints(&ecs),
                    gcs,
                ));
            }
        }
    }
    Ok((
        generate_pattern_graph(&ast),
        VertexConstraintsInfo::empty(),
        EdgeConstraintsInfo::empty(),
        vec![],
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compiler::ast::BuiltIn;

    #[test]
    fn test_compile() {
        match compile(
            "\
(match (vertices (u1 0) (u2 0))
       (edges (u1 u2 0))
       (where (< 2 1 3)))",
        ) {
            Ok(_) => assert!(false),
            Err(e) => assert_eq!(e, error::Err::WhereFalse),
        }
        match compile(
            "\
(match (vertices (u1 0) (u2 0) (u3 0))
       (arcs (u1 u2 0) (u1 u3 0))
       (where (and (< u2 10) (< u3 10) (< u1 u2) (< u1 u3) (< u2 u3))))",
        ) {
            Ok((mut p, vc_info, ec_info, gcs)) => {
                vc_info.add_to_graph(&mut p);
                ec_info.add_to_graph(&mut p);
                assert_eq!(
                    p.neighbors(1).unwrap().get(&2),
                    p.neighbors(1).unwrap().get(&3)
                );
                assert_eq!(
                    vec![8, 9, 10, 11]
                        .into_iter()
                        .map(p.vertex_constraint(2).unwrap().unwrap().f())
                        .collect::<Vec<_>>(),
                    vec![true, true, false, false]
                );
                assert_eq!(
                    vec![(1, 1), (1, 2), (2, 3), (3, 2)]
                        .into_iter()
                        .map(|(u1, u2)| (
                            p.edge_constraint(1, 2).unwrap().unwrap().f()(u1, u2),
                            p.edge_constraint(2, 1).unwrap().unwrap().f()(u2, u1)
                        ))
                        .collect::<Vec<_>>(),
                    vec![false, true, true, false]
                        .into_iter()
                        .map(|x| (x, x))
                        .collect::<Vec<_>>()
                );
                assert_eq!(
                    gcs,
                    vec![Expr::Application(
                        Box::new(Expr::Constant(Atom::BuiltIn(BuiltIn::Lt))),
                        vec![Expr::Constant(Atom::VId(2)), Expr::Constant(Atom::VId(3))]
                    )]
                );
            }
            Err(_) => assert!(false),
        }
    }
}
