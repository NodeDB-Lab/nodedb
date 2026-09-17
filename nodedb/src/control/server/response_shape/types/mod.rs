// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral plan classification and shaped row-set types.

pub mod plan_kind;
pub mod shaped;

pub use plan_kind::{PlanKind, describe_plan};
pub use shaped::{DdlColType, ShapedRow, ShapedRows};
