//! The planner.

pub use decompose_stars::decompose_stars;
pub use planner::{
    IndexedJoinPlan, IntersectionPlan, JoinPlan, MemoryManagerType, Plan, Planner, Task,
};
pub use types::{CharacteristicInfo, IndexType, StarInfo};

pub mod join;
pub mod old_types;
pub mod scan;

mod decompose_stars;
mod planner;
mod types;
