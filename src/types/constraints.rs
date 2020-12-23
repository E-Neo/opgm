//! Constraint types.

use crate::types::VId;
use std::{
    cmp::Ordering,
    hash::{Hash, Hasher},
};

/// Vertex constraint type.
pub struct VertexConstraint {
    f: Box<dyn Fn(VId) -> bool>,
}

impl VertexConstraint {
    pub fn new(f: Box<dyn Fn(VId) -> bool>) -> Self {
        Self { f }
    }

    pub fn f(&self) -> &dyn Fn(VId) -> bool {
        self.f.as_ref()
    }
}

impl std::fmt::Debug for VertexConstraint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "VertexConstraint({:p})", self.f)
    }
}

impl PartialEq for VertexConstraint {
    fn eq(&self, other: &Self) -> bool {
        std::ptr::eq(self.f.as_ref() as *const _, other.f.as_ref() as *const _)
    }
}

impl Eq for VertexConstraint {}

impl PartialOrd for VertexConstraint {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        (self.f.as_ref() as *const _ as *const u8 as usize)
            .partial_cmp(&(other.f.as_ref() as *const _ as *const u8 as usize))
    }
}

impl Ord for VertexConstraint {
    fn cmp(&self, other: &Self) -> Ordering {
        (self.f.as_ref() as *const _ as *const u8 as usize)
            .cmp(&(other.f.as_ref() as *const _ as *const u8 as usize))
    }
}

impl Hash for VertexConstraint {
    fn hash<H: Hasher>(&self, state: &mut H) {
        (self.f.as_ref() as *const _ as *const u8 as usize).hash(state);
    }
}

/// Edge constraint type.
pub struct EdgeConstraint {
    f: Box<dyn Fn(VId, VId) -> bool>,
}

impl EdgeConstraint {
    pub fn new(f: Box<dyn Fn(VId, VId) -> bool>) -> Self {
        Self { f }
    }

    pub fn f(&self) -> &dyn Fn(VId, VId) -> bool {
        self.f.as_ref()
    }
}

impl std::fmt::Debug for EdgeConstraint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "EdgeConstraint({:p})", self.f)
    }
}

impl PartialEq for EdgeConstraint {
    fn eq(&self, other: &Self) -> bool {
        std::ptr::eq(self.f.as_ref() as *const _, other.f.as_ref() as *const _)
    }
}

impl Eq for EdgeConstraint {}

impl PartialOrd for EdgeConstraint {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        (self.f.as_ref() as *const _ as *const u8 as usize)
            .partial_cmp(&(other.f.as_ref() as *const _ as *const u8 as usize))
    }
}

impl Ord for EdgeConstraint {
    fn cmp(&self, other: &Self) -> Ordering {
        (self.f.as_ref() as *const _ as *const u8 as usize)
            .cmp(&(other.f.as_ref() as *const _ as *const u8 as usize))
    }
}

impl Hash for EdgeConstraint {
    fn hash<H: Hasher>(&self, state: &mut H) {
        (self.f.as_ref() as *const _ as *const u8 as usize).hash(state);
    }
}

pub struct GlobalConstraint {
    f: Box<dyn Fn(&[VId]) -> bool>,
}

impl GlobalConstraint {
    pub fn new(f: Box<dyn Fn(&[VId]) -> bool>) -> Self {
        Self { f }
    }

    pub fn f(&self) -> &dyn Fn(&[VId]) -> bool {
        self.f.as_ref()
    }
}

impl std::fmt::Debug for GlobalConstraint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "GlobalConstraint({:p})", self.f)
    }
}

impl PartialEq for GlobalConstraint {
    fn eq(&self, other: &Self) -> bool {
        std::ptr::eq(self.f.as_ref() as *const _, other.f.as_ref() as *const _)
    }
}

impl Eq for GlobalConstraint {}

impl PartialOrd for GlobalConstraint {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        (self.f.as_ref() as *const _ as *const u8 as usize)
            .partial_cmp(&(other.f.as_ref() as *const _ as *const u8 as usize))
    }
}

impl Ord for GlobalConstraint {
    fn cmp(&self, other: &Self) -> Ordering {
        (self.f.as_ref() as *const _ as *const u8 as usize)
            .cmp(&(other.f.as_ref() as *const _ as *const u8 as usize))
    }
}

impl Hash for GlobalConstraint {
    fn hash<H: Hasher>(&self, state: &mut H) {
        (self.f.as_ref() as *const _ as *const u8 as usize).hash(state);
    }
}

/// Vertex cover constraint type.
pub struct VertexCoverConstraint {
    f: Box<dyn Fn(&[VId], VId) -> bool>,
}

impl VertexCoverConstraint {
    pub fn new(f: Box<dyn Fn(&[VId], VId) -> bool>) -> Self {
        Self { f }
    }

    pub fn f(&self) -> &dyn Fn(&[VId], VId) -> bool {
        self.f.as_ref()
    }
}

impl std::fmt::Debug for VertexCoverConstraint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "VertexCoverConstraint({:p})", self.f)
    }
}

impl PartialEq for VertexCoverConstraint {
    fn eq(&self, other: &Self) -> bool {
        std::ptr::eq(self.f.as_ref() as *const _, other.f.as_ref() as *const _)
    }
}

impl Eq for VertexCoverConstraint {}

impl PartialOrd for VertexCoverConstraint {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        (self.f.as_ref() as *const _ as *const u8 as usize)
            .partial_cmp(&(other.f.as_ref() as *const _ as *const u8 as usize))
    }
}

impl Ord for VertexCoverConstraint {
    fn cmp(&self, other: &Self) -> Ordering {
        (self.f.as_ref() as *const _ as *const u8 as usize)
            .cmp(&(other.f.as_ref() as *const _ as *const u8 as usize))
    }
}

impl Hash for VertexCoverConstraint {
    fn hash<H: Hasher>(&self, state: &mut H) {
        (self.f.as_ref() as *const _ as *const u8 as usize).hash(state);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_global_constraint() {
        let gc = GlobalConstraint::new(Box::new(|vs| vs[0] < vs[1] && vs[1] < vs[2]));
        assert_eq!(gc.f()(&[1, 2, 3]), true);
        assert_eq!(gc.f()(&[1, 3, 2]), false);
    }

    #[test]
    fn test_vertex_cover_constraint() {
        let vcc = VertexCoverConstraint::new(Box::new(|vc, v| vc[0] < v || vc[1] < v));
        assert_eq!(vcc.f()(&[1, 2], 3), true);
        assert_eq!(vcc.f()(&[2, 3], 1), false);
    }
}
