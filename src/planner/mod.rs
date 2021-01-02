//! The planner.

pub use decompose_stars::decompose_stars;
pub use planner::{
    CharacteristicInfo, IndexType, IndexedJoinPlan, IntersectionPlan, JoinInfo, JoinPlan,
    MemoryManagerType, Plan, Planner, StarInfo, Task,
};

mod decompose_stars;
mod planner;
