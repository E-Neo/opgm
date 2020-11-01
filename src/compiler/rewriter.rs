//! Constraint rewriter.

use crate::compiler::ast::{Atom, BuiltIn, Expr};

fn builtin_box(builtin: BuiltIn) -> Box<Expr> {
    Box::new(Expr::Constant(Atom::BuiltIn(builtin)))
}

fn rewrite_and(args: &[Expr]) -> Expr {
    let mut new_args = Vec::with_capacity(args.len());
    for arg in args.iter().map(rewrite_aux) {
        match &arg {
            Expr::Constant(Atom::Boolean(false)) => return Expr::Constant(Atom::Boolean(false)),
            Expr::Constant(Atom::Boolean(true)) => (),
            Expr::Application(fun, args) => {
                if let Expr::Constant(Atom::BuiltIn(BuiltIn::And)) = &**fun {
                    new_args.append(&mut args.clone())
                } else {
                    new_args.push(arg);
                }
            }
            _ => new_args.push(arg),
        }
    }
    if new_args.len() == 0 {
        Expr::Constant(Atom::Boolean(true))
    } else if new_args.len() == 1 {
        new_args[0].clone()
    } else {
        Expr::Application(builtin_box(BuiltIn::And), new_args)
    }
}

fn rewrite_or(args: &[Expr]) -> Expr {
    let mut new_args = Vec::with_capacity(args.len());
    for arg in args.iter().map(rewrite_aux) {
        match &arg {
            Expr::Constant(Atom::Boolean(true)) => return Expr::Constant(Atom::Boolean(true)),
            Expr::Constant(Atom::Boolean(false)) => (),
            Expr::Application(fun, args) => {
                if let Expr::Constant(Atom::BuiltIn(BuiltIn::Or)) = &**fun {
                    new_args.append(&mut args.clone())
                } else {
                    new_args.push(arg);
                }
            }
            _ => new_args.push(arg),
        }
    }
    if new_args.len() == 0 {
        Expr::Constant(Atom::Boolean(false))
    } else if new_args.len() == 1 {
        new_args[0].clone()
    } else {
        Expr::Application(builtin_box(BuiltIn::Or), new_args)
    }
}

fn rewrite_not_lt(args: &[Expr]) -> Expr {
    match rewrite_lt(args) {
        Expr::Constant(Atom::Boolean(x)) => Expr::Constant(Atom::Boolean(!x)),
        Expr::Application(fun, args) => {
            if let Expr::Constant(Atom::BuiltIn(builtin)) = &*fun {
                match builtin {
                    BuiltIn::Lt => Expr::Application(builtin_box(BuiltIn::Ge), args),
                    BuiltIn::Ge => Expr::Application(builtin_box(BuiltIn::Lt), args),
                    _ => rewrite_aux(&Expr::Application(
                        builtin_box(BuiltIn::Not),
                        vec![Expr::Application(fun, args)],
                    )),
                }
            } else {
                panic!("Invalid application: {:?}", &*fun)
            }
        }
        _ => panic!("Invalid expression"),
    }
}

fn rewrite_not_ge(args: &[Expr]) -> Expr {
    let expr = rewrite_ge(args);
    match expr {
        Expr::Constant(Atom::Boolean(x)) => Expr::Constant(Atom::Boolean(!x)),
        Expr::Application(fun, args) => {
            if let Expr::Constant(Atom::BuiltIn(builtin)) = &*fun {
                match builtin {
                    BuiltIn::Lt => Expr::Application(builtin_box(BuiltIn::Ge), args),
                    BuiltIn::Ge => Expr::Application(builtin_box(BuiltIn::Lt), args),
                    _ => rewrite_aux(&Expr::Application(
                        builtin_box(BuiltIn::Not),
                        vec![Expr::Application(fun, args)],
                    )),
                }
            } else {
                panic!("Invalid application: {:?}", &*fun)
            }
        }
        _ => panic!("Invalid expression"),
    }
}

fn rewrite_not(args: &[Expr]) -> Expr {
    let arg = &args[0];
    match arg {
        Expr::Constant(Atom::Boolean(x)) => Expr::Constant(Atom::Boolean(!x)),
        Expr::Application(fun, args) => {
            if let Expr::Constant(Atom::BuiltIn(fun)) = &**fun {
                match fun {
                    BuiltIn::Not => rewrite_aux(&args[0]),
                    BuiltIn::Or => rewrite_aux(&Expr::Application(
                        builtin_box(BuiltIn::And),
                        args.iter()
                            .map(|arg| {
                                Expr::Application(builtin_box(BuiltIn::Not), vec![arg.clone()])
                            })
                            .collect(),
                    )),
                    BuiltIn::And => {
                        Expr::Application(builtin_box(BuiltIn::Not), vec![rewrite_aux(arg)])
                    }
                    BuiltIn::Lt => rewrite_not_lt(args),
                    BuiltIn::Ge => rewrite_not_ge(args),
                    BuiltIn::Eq => {
                        Expr::Application(builtin_box(BuiltIn::Not), vec![rewrite_aux(arg)])
                    }
                    _ => unreachable!(),
                }
            } else {
                panic!("Invalid application: {:?}", &**fun)
            }
        }
        _ => panic!("Invalid not expression"),
    }
}

fn rewrite_lt(args: &[Expr]) -> Expr {
    let mut pairs = Vec::with_capacity(args.len());
    for (left, right) in args.windows(2).map(|x| (&x[0], &x[1])) {
        let (left, right) = (rewrite_aux(left), rewrite_aux(right));
        match (&left, &right) {
            (Expr::Constant(Atom::Num(x)), Expr::Constant(Atom::Num(y))) => {
                if x >= y {
                    return Expr::Constant(Atom::Boolean(false));
                }
            }
            _ => pairs.push(Expr::Application(
                builtin_box(BuiltIn::Lt),
                vec![left, right],
            )),
        }
    }
    if pairs.len() == 0 {
        Expr::Constant(Atom::Boolean(true))
    } else if pairs.len() == 1 {
        pairs[0].clone()
    } else {
        rewrite_aux(&Expr::Application(builtin_box(BuiltIn::And), pairs))
    }
}

fn rewrite_ge(args: &[Expr]) -> Expr {
    let mut pairs = Vec::with_capacity(args.len());
    for (left, right) in args.windows(2).map(|x| (&x[0], &x[1])) {
        let (left, right) = (rewrite_aux(left), rewrite_aux(right));
        match (&left, &right) {
            (Expr::Constant(Atom::Num(x)), Expr::Constant(Atom::Num(y))) => {
                if x < y {
                    return Expr::Constant(Atom::Boolean(false));
                }
            }
            _ => pairs.push(Expr::Application(
                builtin_box(BuiltIn::Ge),
                vec![left, right],
            )),
        }
    }
    if pairs.len() == 0 {
        Expr::Constant(Atom::Boolean(true))
    } else if pairs.len() == 1 {
        pairs[0].clone()
    } else {
        rewrite_aux(&Expr::Application(builtin_box(BuiltIn::And), pairs))
    }
}

fn rewrite_eq(args: &[Expr]) -> Expr {
    let mut pairs = Vec::with_capacity(args.len());
    for (left, right) in args.windows(2).map(|x| (&x[0], &x[1])) {
        let (left, right) = (rewrite_aux(left), rewrite_aux(right));
        match (&left, &right) {
            (Expr::Constant(Atom::Num(x)), Expr::Constant(Atom::Num(y))) => {
                if x != y {
                    return Expr::Constant(Atom::Boolean(false));
                }
            }
            _ => pairs.push(Expr::Application(
                builtin_box(BuiltIn::Eq),
                vec![left, right],
            )),
        }
    }
    if pairs.len() == 0 {
        Expr::Constant(Atom::Boolean(true))
    } else if pairs.len() == 1 {
        pairs[0].clone()
    } else {
        rewrite_aux(&Expr::Application(builtin_box(BuiltIn::And), pairs))
    }
}

fn rewrite_mod(args: &[Expr]) -> Expr {
    let (left, right) = (rewrite_aux(&args[0]), rewrite_aux(&args[1]));
    match (&left, &right) {
        (Expr::Constant(Atom::Num(x)), Expr::Constant(Atom::Num(y))) => {
            Expr::Constant(Atom::Num(x % y))
        }
        _ => Expr::Application(builtin_box(BuiltIn::Mod), vec![left, right]),
    }
}

/// Rewrites the constraint to normal form.
fn rewrite_aux(expr: &Expr) -> Expr {
    match expr {
        Expr::Constant(_) => expr.clone(),
        Expr::Application(fun, args) => {
            if let Expr::Constant(Atom::BuiltIn(fun)) = &**fun {
                match fun {
                    BuiltIn::And => rewrite_and(args),
                    BuiltIn::Or => rewrite_or(args),
                    BuiltIn::Not => rewrite_not(args),
                    BuiltIn::Lt => rewrite_lt(args),
                    BuiltIn::Ge => rewrite_ge(args),
                    BuiltIn::Eq => rewrite_eq(args),
                    BuiltIn::Mod => rewrite_mod(args),
                }
            } else {
                panic!("Invalid application: {:?}", &**fun);
            }
        }
    }
}

/// rewrites the constraint to normal form.
///
/// A normal form is a sequence of constraints concatenated by the AND operator.
pub fn rewrite(expr: &Expr) -> Vec<Expr> {
    let expr = rewrite_aux(expr);
    match expr {
        Expr::Constant(_) => vec![expr],
        Expr::Application(fun, args) => {
            if let Expr::Constant(Atom::BuiltIn(BuiltIn::And)) = &*fun {
                args
            } else {
                vec![Expr::Application(fun, args)]
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compiler::parser::expr_parse;

    #[test]
    fn test_and() {
        assert_eq!(
            rewrite_aux(&expr_parse("(and (< u1 u2) (< u1 u3) #f)").unwrap()),
            expr_parse("#f").unwrap()
        );
        assert_eq!(
            rewrite_aux(&expr_parse("(and (and (< u1 u2) (< u2 u3)) #t (< u1 u3))").unwrap()),
            expr_parse("(and (< u1 u2) (< u2 u3) (< u1 u3))").unwrap()
        );
        assert_eq!(
            rewrite_aux(&expr_parse("(and #t #t #t)").unwrap()),
            expr_parse("#t").unwrap()
        );
    }

    #[test]
    fn test_or() {
        assert_eq!(
            rewrite_aux(&expr_parse("(or (< u1 u2) (< u1 u3) #t)").unwrap()),
            expr_parse("#t").unwrap()
        );
        assert_eq!(
            rewrite_aux(&expr_parse("(or (or (< u1 u2) (< u2 u3)) #f (< u1 u3))").unwrap()),
            expr_parse("(or (< u1 u2) (< u2 u3) (< u1 u3))").unwrap()
        );
        assert_eq!(
            rewrite_aux(&expr_parse("(or #f #f #f)").unwrap()),
            expr_parse("#f").unwrap()
        );
    }

    #[test]
    fn test_not_not() {
        assert_eq!(
            rewrite_aux(&expr_parse("(not (not #t))").unwrap()),
            expr_parse("#t").unwrap()
        );
        assert_eq!(
            rewrite_aux(&expr_parse("(not (not (< u1 u2)))").unwrap()),
            expr_parse("(< u1 u2)").unwrap()
        );
    }

    #[test]
    fn test_lt() {
        assert_eq!(
            rewrite_aux(&expr_parse("(< u1 3 2)").unwrap()),
            expr_parse("#f").unwrap()
        );
        assert_eq!(
            rewrite_aux(&expr_parse("(and (< 1 2) (< 1 2 3 u1 u2 9))").unwrap()),
            expr_parse("(and (< 3 u1) (< u1 u2) (< u2 9))").unwrap()
        );
    }

    #[test]
    fn test_not_lt() {
        assert_eq!(
            rewrite_aux(&expr_parse("(not (< u1 u2))").unwrap()),
            expr_parse("(>= u1 u2)").unwrap()
        );
        assert_eq!(
            rewrite_aux(&expr_parse("(not (< u1 u2 u3))").unwrap()),
            expr_parse("(not (and (< u1 u2) (< u2 u3)))").unwrap()
        );
    }

    #[test]
    fn test_rewrite() {
        assert_eq!(
            rewrite(
                &expr_parse(
                    "\
(and (< u1 u2)
     (< u1 u3)
     (not (or (>= u2 u3) (>= u4 2020))))"
                )
                .unwrap()
            ),
            vec!["(< u1 u2)", "(< u1 u3)", "(< u2 u3)", "(< u4 2020)"]
                .into_iter()
                .map(|input| expr_parse(input).unwrap())
                .collect::<Vec<_>>()
        );
    }
}
