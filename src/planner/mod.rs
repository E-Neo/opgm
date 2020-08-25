//! The planner.

pub use decompose_stars::decompose_stars;
pub use planner::{CharacteristicInfo, JoinInfo, MemoryManagerType, Plan, Planner, StarInfo};

mod decompose_stars;
mod planner;
