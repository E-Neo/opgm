//! The planner.

pub use decompose_stars::decompose_stars;
pub use planner::{
    CharacteristicInfo, IndexType, IndexedJoinPlan, IntersectionPlan, JoinPlan, MemoryManagerType,
    Plan, Planner, StarInfo, Task,
};

pub mod join;
pub mod scan;

mod decompose_stars;
mod planner;
