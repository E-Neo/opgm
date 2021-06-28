//! Out-of-core property graph matching.

pub mod compiler;
pub mod data;
pub mod data_graph;
pub mod executor;
pub mod front_end;
pub mod memory_manager;
pub mod old_executor;
pub mod old_planner;
pub mod pattern;
pub mod pattern_graph;
pub mod planner;
pub mod task;
pub mod types;

pub(crate) mod tools;
