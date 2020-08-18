//! The planner.

pub use decompose_stars::decompose_stars;
pub use planner::{CharacteristicPlan, MemoryManagerType, Plan, Planner};

mod decompose_stars;
mod planner;
