//! Error management.

use derive_more::Display;
use nom::error::ErrorKind;

#[derive(Debug, Display, PartialEq)]
pub enum Err<'a> {
    ParseError(nom::Err<(&'a str, ErrorKind)>),
    RedudantItems,
    RedudantTail,
    DuplicateVertex,
    DuplicateArc,
    DuplicateEdge,
    InvalidVertex,
    InvalidConstraint,
    InvalidFunction,
    TypeError,
    WrongNumOfArgs,
    WhereFalse,
}

impl<'a> std::error::Error for Err<'a> {}
