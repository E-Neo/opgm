use crate::{
    memory_manager::MemoryManager,
    old_executor::SuperRows,
    types::{VId, VIdPos},
};
use std::mem::size_of;

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct SuperRowIndexView(Vec<(VId, usize)>);

impl SuperRowIndexView {
    pub fn new(mm: &MemoryManager) -> Self {
        Self(
            unsafe { mm.as_slice::<VIdPos>(0, mm.len() / size_of::<VIdPos>()) }
                .iter()
                .map(|entry| (entry.vid, entry.pos))
                .collect(),
        )
    }
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct SuperRowsView(Vec<Vec<Vec<VId>>>);

impl SuperRowsView {
    pub fn new(mm: &MemoryManager) -> Self {
        if mm.len() == 0 {
            Self(vec![])
        } else {
            Self(
                SuperRows::new(mm)
                    .map(|sr| {
                        sr.images()
                            .iter()
                            .map(|&img| img.iter().map(|&v| v).collect())
                            .collect()
                    })
                    .collect(),
            )
        }
    }
}
