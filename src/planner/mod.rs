//! The planner.

pub use decompose_stars::decompose_stars;
pub use join::{IndexedJoinPlan, IntersectionPlan, JoinPlan};
pub use scan::ScanPlan;
pub use types::{CharacteristicInfo, IndexType, StarInfo};

pub mod join;
pub mod scan;

mod decompose_stars;
mod types;
