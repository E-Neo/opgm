use crate::front_end::Expr;

fn rewrite_and(arg1: Expr, arg2: Expr) -> Expr {
    let arg1 = rewrite_aux(arg1);
    match arg1 {
        Expr::Bool(true) => rewrite_aux(arg2),
        Expr::Bool(false) => Expr::Bool(false),
        _ => {
            let arg2 = rewrite_aux(arg2);
            match arg2 {
                Expr::Bool(true) => arg1,
                Expr::Bool(false) => Expr::Bool(false),
                _ => Expr::And(Box::new(arg1), Box::new(arg2)),
            }
        }
    }
}

fn rewrite_or(arg1: Expr, arg2: Expr) -> Expr {
    let arg1 = rewrite_aux(arg1);
    match arg1 {
        Expr::Bool(true) => Expr::Bool(true),
        Expr::Bool(false) => rewrite_aux(arg2),
        _ => {
            let arg2 = rewrite_aux(arg2);
            match arg2 {
                Expr::Bool(true) => Expr::Bool(true),
                Expr::Bool(false) => arg1,
                _ => Expr::Or(Box::new(arg1), Box::new(arg2)),
            }
        }
    }
}

fn rewrite_not(arg1: Expr) -> Expr {
    let arg1 = rewrite_aux(arg1);
    match arg1 {
        Expr::Bool(x) => Expr::Bool(!x),
        Expr::And(_, _) => Expr::Not(Box::new(arg1)),
        Expr::Or(a, b) => rewrite_and(Expr::Not(a), Expr::Not(b)),
        Expr::Not(a) => rewrite_aux(*a),
        Expr::Lt(a, b) => rewrite_aux(Expr::Ge(a, b)),
        Expr::Ge(a, b) => rewrite_aux(Expr::Lt(a, b)),
        Expr::Eq(a, b) => rewrite_aux(Expr::Neq(a, b)),
        Expr::Neq(a, b) => rewrite_aux(Expr::Eq(a, b)),
        Expr::VId(_) | Expr::Int(_) | Expr::Mod(_, _) => unreachable!(),
    }
}

fn rewrite_lt(arg1: Expr, arg2: Expr) -> Expr {
    let (arg1, arg2) = (rewrite_aux(arg1), rewrite_aux(arg2));
    match (&arg1, &arg2) {
        (Expr::Int(x), Expr::Int(y)) => Expr::Bool(x < y),
        _ => Expr::Lt(Box::new(arg1), Box::new(arg2)),
    }
}

fn rewrite_ge(arg1: Expr, arg2: Expr) -> Expr {
    let (arg1, arg2) = (rewrite_aux(arg1), rewrite_aux(arg2));
    match (&arg1, &arg2) {
        (Expr::Int(x), Expr::Int(y)) => Expr::Bool(x >= y),
        _ => Expr::Ge(Box::new(arg1), Box::new(arg2)),
    }
}

fn rewrite_eq(arg1: Expr, arg2: Expr) -> Expr {
    let (arg1, arg2) = (rewrite_aux(arg1), rewrite_aux(arg2));
    match (&arg1, &arg2) {
        (Expr::VId(a), Expr::VId(b)) if a == b => Expr::Bool(true),
        (Expr::Int(x), Expr::Int(y)) => Expr::Bool(x == y),
        _ => Expr::Eq(Box::new(arg1), Box::new(arg2)),
    }
}

fn rewrite_neq(arg1: Expr, arg2: Expr) -> Expr {
    let (arg1, arg2) = (rewrite_aux(arg1), rewrite_aux(arg2));
    match (&arg1, &arg2) {
        (Expr::VId(a), Expr::VId(b)) if a == b => Expr::Bool(false),
        (Expr::Int(x), Expr::Int(y)) => Expr::Bool(x != y),
        _ => Expr::Neq(Box::new(arg1), Box::new(arg2)),
    }
}

fn rewrite_mod(arg1: Expr, arg2: Expr) -> Expr {
    let (arg1, arg2) = (rewrite_aux(arg1), rewrite_aux(arg2));
    match (&arg1, &arg2) {
        (Expr::Int(x), Expr::Int(y)) => Expr::Int(x % y),
        _ => Expr::Mod(Box::new(arg1), Box::new(arg2)),
    }
}

fn rewrite_aux(expr: Expr) -> Expr {
    match expr {
        Expr::VId(_) | Expr::Int(_) | Expr::Bool(_) => expr,
        Expr::And(arg1, arg2) => rewrite_and(*arg1, *arg2),
        Expr::Or(arg1, arg2) => rewrite_or(*arg1, *arg2),
        Expr::Not(arg1) => rewrite_not(*arg1),
        Expr::Lt(arg1, arg2) => rewrite_lt(*arg1, *arg2),
        Expr::Ge(arg1, arg2) => rewrite_ge(*arg1, *arg2),
        Expr::Eq(arg1, arg2) => rewrite_eq(*arg1, *arg2),
        Expr::Neq(arg1, arg2) => rewrite_neq(*arg1, *arg2),
        Expr::Mod(arg1, arg2) => rewrite_mod(*arg1, *arg2),
    }
}

fn flatten_and(expr: Expr, result: &mut Vec<Expr>) {
    if let Expr::And(arg1, arg2) = expr {
        flatten_and(*arg1, result);
        flatten_and(*arg2, result);
    } else {
        result.push(expr);
    }
}

pub fn rewrite(expr: Expr) -> Vec<Expr> {
    let mut result = Vec::new();
    flatten_and(rewrite_aux(expr), &mut result);
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::front_end::expr_parse;

    #[test]
    fn test_and() {
        assert_eq!(
            rewrite_aux(expr_parse("(and (< u1 u2) #f)").unwrap()),
            expr_parse("#f").unwrap()
        );
        assert_eq!(
            rewrite_aux(expr_parse("(and (and (< u1 u2) (< u2 u3)) (and #t (< u1 u3)))").unwrap()),
            expr_parse("(and (and (< u1 u2) (< u2 u3)) (< u1 u3))").unwrap()
        );
        assert_eq!(
            rewrite_aux(expr_parse("(and #t #t)").unwrap()),
            expr_parse("#t").unwrap()
        );
    }

    #[test]
    fn test_or() {
        assert_eq!(
            rewrite_aux(expr_parse("(or (or (< u1 u2) (< u1 u3)) #t)").unwrap()),
            expr_parse("#t").unwrap()
        );
        assert_eq!(
            rewrite_aux(expr_parse("(or (or (< u1 u2) (< u2 u3)) (or #f (< u1 u3)))").unwrap()),
            expr_parse("(or (or (< u1 u2) (< u2 u3)) (< u1 u3))").unwrap()
        );
        assert_eq!(
            rewrite_aux(expr_parse("(or #f #f)").unwrap()),
            expr_parse("#f").unwrap()
        );
    }

    #[test]
    fn test_not_not() {
        assert_eq!(
            rewrite_aux(expr_parse("(not (not #t))").unwrap()),
            expr_parse("#t").unwrap()
        );
        assert_eq!(
            rewrite_aux(expr_parse("(not (not (< u1 u2)))").unwrap()),
            expr_parse("(< u1 u2)").unwrap()
        );
    }

    #[test]
    fn test_lt() {
        assert_eq!(
            rewrite_aux(expr_parse("(and (< u1 3) (< 3 2))").unwrap()),
            expr_parse("#f").unwrap()
        );
        assert_eq!(
            rewrite_aux(expr_parse("(and (< 1 2) (< u1 u2))").unwrap()),
            expr_parse("(< u1 u2)").unwrap()
        );
    }

    #[test]
    fn test_not_lt() {
        assert_eq!(
            rewrite_aux(expr_parse("(not (< u1 u2))").unwrap()),
            expr_parse("(>= u1 u2)").unwrap()
        );
    }

    #[test]
    fn test_rewrite() {
        assert_eq!(
            rewrite(
                expr_parse(
                    "\
(and (and (< u1 u2) (< u1 u3))
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
