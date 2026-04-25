//! JOIN planning: extract left/right tables, equi-join keys, join type.

pub mod array_arm;
pub mod constraint;
pub mod plan;

pub use plan::plan_join_from_select;
