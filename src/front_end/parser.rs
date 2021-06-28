use super::error::Result;
use crate::{
    front_end::{Ast, Expr},
    types::{ELabel, VId, VLabel},
};
use itertools::Itertools;
use pest::Parser;
use pest_derive::Parser;

pub type GispRule = Rule;

#[derive(Parser)]
#[grammar = "front_end/grammar.pest"]
struct GispParser;

pub fn parse(input: &str) -> Result<Ast> {
    let mut ast = Ast::default();
    for pair in GispParser::parse(Rule::stat, input)? {
        match pair.as_rule() {
            Rule::vertices_stat => {
                ast.set_vertices(parse_vertices_stat(pair));
            }
            Rule::arcs_stat => {
                if ast.arcs().is_empty() {
                    ast.set_arcs(parse_arcs_or_edges_stat(pair));
                } else {
                    return Err(unexpected_error(pair));
                }
            }
            Rule::edges_stat => {
                if ast.edges().is_empty() {
                    ast.set_edges(parse_arcs_or_edges_stat(pair));
                } else {
                    return Err(unexpected_error(pair));
                }
            }
            Rule::where_stat => {
                if ast.arcs().is_empty() && ast.edges().is_empty() {
                    return Err(unexpected_error(pair));
                } else {
                    ast.set_constraint(Some(parse_expr(pair.into_inner().next().unwrap())?));
                }
            }
            Rule::EOI => {}
            _ => unreachable!(),
        }
    }
    Ok(ast)
}

pub fn expr_parse(input: &str) -> Result<Expr> {
    parse_expr(GispParser::parse(Rule::expr, input)?.next().unwrap())
}

fn unexpected_error(pair: pest::iterators::Pair<Rule>) -> pest::error::Error<GispRule> {
    pest::error::Error::new_from_span(
        pest::error::ErrorVariant::CustomError {
            message: String::from("unexpected"),
        },
        pair.as_span(),
    )
}

fn parse_vertices_stat(pair: pest::iterators::Pair<Rule>) -> Vec<(VId, VLabel)> {
    let mut vertices = vec![];
    for (vid, vlabel) in pair.into_inner().tuples() {
        match (vid.as_rule(), vlabel.as_rule()) {
            (Rule::ident, Rule::label) => {
                vertices.push((
                    vid.as_str()[1..].parse().unwrap(),
                    vlabel.as_str().parse().unwrap(),
                ));
            }
            _ => unreachable!(),
        }
    }
    vertices
}

fn parse_arcs_or_edges_stat(pair: pest::iterators::Pair<Rule>) -> Vec<(VId, VId, ELabel)> {
    let mut edges = vec![];
    for (src, dst, elabel) in pair.into_inner().tuples() {
        match (src.as_rule(), dst.as_rule(), elabel.as_rule()) {
            (Rule::ident, Rule::ident, Rule::label) => {
                edges.push((
                    src.as_str()[1..].parse().unwrap(),
                    dst.as_str()[1..].parse().unwrap(),
                    elabel.as_str().parse().unwrap(),
                ));
            }
            _ => unreachable!(),
        }
    }
    edges
}

fn parse_expr(pair: pest::iterators::Pair<Rule>) -> Result<Expr> {
    let pair = pair.into_inner().next().unwrap();
    match pair.as_rule() {
        Rule::ident => Ok(Expr::VId(pair.as_str()[1..].parse().unwrap())),
        Rule::int => Ok(Expr::Int(pair.as_str().parse().unwrap())),
        Rule::bool => Ok(Expr::Bool(match pair.as_str() {
            "#t" => true,
            "#f" => false,
            _ => unreachable!(),
        })),
        Rule::and_expr => {
            let mut pairs = pair.into_inner();
            Ok(Expr::And(
                Box::new(parse_expr(pairs.next().unwrap())?),
                Box::new(parse_expr(pairs.next().unwrap())?),
            ))
        }
        Rule::or_expr => {
            let mut pairs = pair.into_inner();
            Ok(Expr::Or(
                Box::new(parse_expr(pairs.next().unwrap())?),
                Box::new(parse_expr(pairs.next().unwrap())?),
            ))
        }
        Rule::not_expr => Ok(Expr::Not(Box::new(parse_expr(
            pair.into_inner().next().unwrap(),
        )?))),
        Rule::lt_expr => {
            let mut pairs = pair.into_inner();
            Ok(Expr::Lt(
                Box::new(parse_expr(pairs.next().unwrap())?),
                Box::new(parse_expr(pairs.next().unwrap())?),
            ))
        }
        Rule::ge_expr => {
            let mut pairs = pair.into_inner();
            Ok(Expr::Ge(
                Box::new(parse_expr(pairs.next().unwrap())?),
                Box::new(parse_expr(pairs.next().unwrap())?),
            ))
        }
        Rule::eq_expr => {
            let mut pairs = pair.into_inner();
            Ok(Expr::Eq(
                Box::new(parse_expr(pairs.next().unwrap())?),
                Box::new(parse_expr(pairs.next().unwrap())?),
            ))
        }
        Rule::neq_expr => {
            let mut pairs = pair.into_inner();
            Ok(Expr::Neq(
                Box::new(parse_expr(pairs.next().unwrap())?),
                Box::new(parse_expr(pairs.next().unwrap())?),
            ))
        }
        Rule::mod_expr => {
            let mut pairs = pair.into_inner();
            Ok(Expr::Mod(
                Box::new(parse_expr(pairs.next().unwrap())?),
                Box::new(parse_expr(pairs.next().unwrap())?),
            ))
        }
        _ => unreachable!(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_no_where() {
        assert_eq!(
            parse(
                "\
(match (vertices (u1 0) (u2 0))
       (arcs (u1 u2 0)))
"
            ),
            Ok(Ast::new(
                vec![(1, 0), (2, 0)],
                vec![(1, 2, 0)],
                vec![],
                None
            ))
        );
        assert_eq!(
            parse(
                "\
(match (vertices (u1 1) (u2 2) (u3 3))
       (arcs  (u1 u2 12) (u1 u3 13))
       (edges (u1 u3 13) (u2 u3 23)))
"
            ),
            Ok(Ast::new(
                vec![(1, 1), (2, 2), (3, 3)],
                vec![(1, 2, 12), (1, 3, 13)],
                vec![(1, 3, 13), (2, 3, 23)],
                None,
            ))
        );
    }

    #[test]
    fn test_where() {
        assert_eq!(
            parse(
                "\
(match (vertices (u1 1) (u2 2) (u3 3))
       (edges (u1 u2 0) (u1 u3 0) (u2 u3 0))
       (where (and (< u2 u1) (>= u3 8))))
"
            ),
            Ok(Ast::new(
                vec![(1, 1), (2, 2), (3, 3)],
                vec![],
                vec![(1, 2, 0), (1, 3, 0), (2, 3, 0)],
                Some(Expr::And(
                    Box::new(Expr::Lt(Box::new(Expr::VId(2)), Box::new(Expr::VId(1)))),
                    Box::new(Expr::Ge(Box::new(Expr::VId(3)), Box::new(Expr::Int(8))))
                )),
            ))
        );
    }
}
