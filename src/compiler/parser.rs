//! Parser.

use crate::compiler::{
    ast::{Ast, Atom, BuiltIn, Expr},
    error,
};
use crate::types::{ELabel, VId, VLabel};
use nom::{
    branch::alt,
    bytes::complete::tag,
    character::complete::{digit1, multispace0, multispace1},
    combinator::{map, map_opt, map_res},
    multi::many0,
    sequence::{delimited, preceded, terminated, tuple},
    IResult,
};

#[derive(Debug, PartialEq)]
enum Item {
    Vertices(Vec<(VId, VLabel)>),
    Arcs(Vec<(VId, VId, ELabel)>),
    Edges(Vec<(VId, VId, ELabel)>),
    Constraint(Expr),
}

fn non_neg(input: &str) -> IResult<&str, i64> {
    map_res(digit1, |digit_str: &str| digit_str.parse())(input)
}

fn num(input: &str) -> IResult<&str, i64> {
    alt((
        non_neg,
        map_opt(preceded(tag("-"), digit1), |digit_str: &str| {
            digit_str
                .parse::<i64>()
                .ok()
                .map(|n| n.checked_neg())
                .flatten()
        }),
    ))(input)
}

fn s_exp<'a, O, F>(inner: F) -> impl Fn(&'a str) -> IResult<&'a str, O>
where
    F: Fn(&'a str) -> IResult<&'a str, O>,
{
    delimited(
        tag("("),
        preceded(multispace0, inner),
        preceded(multispace0, tag(")")),
    )
}

fn vid(input: &str) -> IResult<&str, VId> {
    map(preceded(tag("u"), num), |vid| vid as VId)(input)
}

fn vlabel(input: &str) -> IResult<&str, VLabel> {
    map(num, |vlabel| vlabel as VLabel)(input)
}

fn elabel(input: &str) -> IResult<&str, ELabel> {
    map(num, |elabel| elabel as ELabel)(input)
}

fn vid_vlabel(input: &str) -> IResult<&str, (VId, VLabel)> {
    s_exp(tuple((
        preceded(multispace0, vid),
        preceded(multispace0, vlabel),
    )))(input)
}

fn vid_vid_elabel(input: &str) -> IResult<&str, (VId, VId, ELabel)> {
    s_exp(tuple((
        preceded(multispace0, vid),
        preceded(multispace0, vid),
        preceded(multispace0, elabel),
    )))(input)
}

fn parse_vertices(input: &str) -> IResult<&str, Item> {
    s_exp(preceded(
        multispace0,
        preceded(
            tag("vertices"),
            map(many0(preceded(multispace0, vid_vlabel)), Item::Vertices),
        ),
    ))(input)
}

fn parse_arcs(input: &str) -> IResult<&str, Item> {
    s_exp(preceded(
        multispace0,
        preceded(
            tag("arcs"),
            map(many0(preceded(multispace0, vid_vid_elabel)), Item::Arcs),
        ),
    ))(input)
}

fn parse_edges(input: &str) -> IResult<&str, Item> {
    s_exp(preceded(
        multispace0,
        preceded(
            tag("edges"),
            map(many0(preceded(multispace0, vid_vid_elabel)), Item::Edges),
        ),
    ))(input)
}

fn boolean(input: &str) -> IResult<&str, bool> {
    preceded(
        tag("#"),
        alt((map(tag("t"), |_| true), map(tag("f"), |_| false))),
    )(input)
}

fn builtin(input: &str) -> IResult<&str, BuiltIn> {
    alt((
        map(tag("and"), |_| BuiltIn::And),
        map(tag("or"), |_| BuiltIn::Or),
        map(tag("not"), |_| BuiltIn::Not),
        map(tag("<"), |_| BuiltIn::Lt),
        map(tag(">="), |_| BuiltIn::Ge),
        map(tag("="), |_| BuiltIn::Eq),
        map(tag("%"), |_| BuiltIn::Mod),
    ))(input)
}

fn atom(input: &str) -> IResult<&str, Atom> {
    alt((
        map(vid, Atom::VId),
        map(num, Atom::Num),
        map(boolean, Atom::Boolean),
        map(builtin, Atom::BuiltIn),
    ))(input)
}

fn parse_constant(input: &str) -> IResult<&str, Expr> {
    map(atom, Expr::Constant)(input)
}

fn parse_application(input: &str) -> IResult<&str, Expr> {
    s_exp(map(
        tuple((parse_expr, many0(parse_expr))),
        |(head, tail)| Expr::Application(Box::new(head), tail),
    ))(input)
}

fn parse_expr(input: &str) -> IResult<&str, Expr> {
    preceded(multispace0, alt((parse_constant, parse_application)))(input)
}

fn parse_constraint(input: &str) -> IResult<&str, Item> {
    s_exp(preceded(
        multispace0,
        preceded(
            tag("where"),
            alt((
                preceded(multispace1, map(parse_constant, Item::Constraint)),
                preceded(multispace0, map(parse_application, Item::Constraint)),
            )),
        ),
    ))(input)
}

fn items(input: &str) -> IResult<&str, Vec<Item>> {
    s_exp(preceded(
        multispace0,
        preceded(
            tag("match"),
            many0(preceded(
                multispace0,
                alt((parse_vertices, parse_arcs, parse_edges, parse_constraint)),
            )),
        ),
    ))(input)
}

fn create_ast<'a>(items: Vec<Item>) -> Result<Ast, error::Err<'a>> {
    let (mut vertices, mut arcs, mut edges, mut constraint) = (None, None, None, None);
    for item in items {
        match item {
            Item::Vertices(x) => {
                if let Some(_) = vertices {
                    return Err(error::Err::RedudantItems);
                } else {
                    vertices = Some(x);
                }
            }
            Item::Arcs(x) => {
                if let Some(_) = arcs {
                    return Err(error::Err::RedudantItems);
                } else {
                    arcs = Some(x);
                }
            }
            Item::Edges(x) => {
                if let Some(_) = edges {
                    return Err(error::Err::RedudantItems);
                } else {
                    edges = Some(x);
                }
            }
            Item::Constraint(x) => {
                if let Some(_) = constraint {
                    return Err(error::Err::RedudantItems);
                } else {
                    constraint = Some(x);
                }
            }
        }
    }
    Ok(Ast::new(
        vertices.unwrap_or(vec![]),
        arcs.unwrap_or(vec![]),
        edges.unwrap_or(vec![]),
        constraint,
    ))
}

/// Parses the constraint expression.
pub fn expr_parse<'a>(input: &'a str) -> Result<Expr, error::Err<'a>> {
    let (eof, expr) =
        terminated(parse_expr, multispace0)(input).map_err(|e| error::Err::ParseError(e))?;
    if eof.len() == 0 {
        Ok(expr)
    } else {
        Err(error::Err::RedudantTail)
    }
}

/// Parses the Lisp-like graph query language.
pub fn parse<'a>(input: &'a str) -> Result<Ast, error::Err<'a>> {
    let (eof, items) =
        terminated(items, multispace0)(input).map_err(|e| error::Err::ParseError(e))?;
    if eof.len() == 0 {
        create_ast(items)
    } else {
        Err(error::Err::RedudantTail)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_vid_vlabel() {
        assert_eq!(vid_vlabel("(u1 10)"), Ok(("", (1, 10))));
    }

    #[test]
    fn test_parse_vertices() {
        assert_eq!(
            parse_vertices("(vertices (u1 10) (u2 20) (u3 20))"),
            Ok(("", Item::Vertices(vec![(1, 10), (2, 20), (3, 20)])))
        );
        assert_eq!(
            parse_vertices("( vertices ( u1 10)(u2 20) (u3 20 ) )   "),
            Ok(("   ", Item::Vertices(vec![(1, 10), (2, 20), (3, 20)])))
        );
        assert_eq!(
            parse_vertices("(vertices)"),
            Ok(("", Item::Vertices(vec![])))
        );
    }

    #[test]
    fn test_parse_arcs() {
        assert_eq!(
            parse_arcs("(arcs (u1 u2 12) (u1 u3 12))"),
            Ok(("", Item::Arcs(vec![(1, 2, 12), (1, 3, 12)])))
        );
        assert_eq!(
            parse_arcs("( arcs (u1 u2 12 ) (u1 u3 12)  )   "),
            Ok(("   ", Item::Arcs(vec![(1, 2, 12), (1, 3, 12)])))
        );
        assert_eq!(parse_arcs("(arcs)"), Ok(("", Item::Arcs(vec![]))));
    }

    #[test]
    fn test_parse_edges() {
        assert_eq!(
            parse_edges("(edges (u1 u2 12) (u1 u3 12))"),
            Ok(("", Item::Edges(vec![(1, 2, 12), (1, 3, 12)])))
        );
        assert_eq!(
            parse_edges("( edges (u1 u2 12 ) (u1 u3 12)  )   "),
            Ok(("   ", Item::Edges(vec![(1, 2, 12), (1, 3, 12)])))
        );
        assert_eq!(parse_edges("(edges)"), Ok(("", Item::Edges(vec![]))));
    }

    #[test]
    fn test_parse_constraint() {
        assert_eq!(
            parse_constraint("(where #t)"),
            Ok(("", Item::Constraint(Expr::Constant(Atom::Boolean(true)))))
        );
        assert_eq!(
            parse_constraint("(where(not (or (< u1 u2) (< u1 u3))))"),
            Ok((
                "",
                Item::Constraint(Expr::Application(
                    Box::new(Expr::Constant(Atom::BuiltIn(BuiltIn::Not))),
                    vec![Expr::Application(
                        Box::new(Expr::Constant(Atom::BuiltIn(BuiltIn::Or))),
                        vec![
                            Expr::Application(
                                Box::new(Expr::Constant(Atom::BuiltIn(BuiltIn::Lt))),
                                vec![Expr::Constant(Atom::VId(1)), Expr::Constant(Atom::VId(2))]
                            ),
                            Expr::Application(
                                Box::new(Expr::Constant(Atom::BuiltIn(BuiltIn::Lt))),
                                vec![Expr::Constant(Atom::VId(1)), Expr::Constant(Atom::VId(3))]
                            )
                        ]
                    )]
                ))
            ))
        );
    }

    #[test]
    fn test_parser() {
        assert_eq!(
            parse("(match (vertices (u1 1) (u2 2)) (arcs (u1 u2 12)))"),
            Ok(Ast::new(
                vec![(1, 1), (2, 2)],
                vec![(1, 2, 12)],
                vec![],
                None
            ))
        );
        assert_eq!(
            parse(
                "\
(match
  (vertices
    (u1 1) (u2 2) (u3 2))
  (edges
    (u1 u2 0) (u1 u3 0) (u2 u3 0))
  (where
    (and (< u1 u2) (< u2 u3))))"
            ),
            Ok(Ast::new(
                vec![(1, 1), (2, 2), (3, 2)],
                vec![],
                vec![(1, 2, 0), (1, 3, 0), (2, 3, 0)],
                Some(Expr::Application(
                    Box::new(Expr::Constant(Atom::BuiltIn(BuiltIn::And))),
                    vec![
                        Expr::Application(
                            Box::new(Expr::Constant(Atom::BuiltIn(BuiltIn::Lt))),
                            vec![Expr::Constant(Atom::VId(1)), Expr::Constant(Atom::VId(2))]
                        ),
                        Expr::Application(
                            Box::new(Expr::Constant(Atom::BuiltIn(BuiltIn::Lt))),
                            vec![Expr::Constant(Atom::VId(2)), Expr::Constant(Atom::VId(3))]
                        )
                    ]
                ))
            ))
        );
    }
}
