// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral plan classification and shaped row-set types.

pub mod dml_outcome;
pub mod plan_kind;
pub mod shaped;

pub use dml_outcome::{DmlFoldError, DmlOutcome, FoldedTag, StatementTag};
pub(crate) use dml_outcome::{
    dml_outcome_by_op, dml_outcome_from_payload, payload_to_dml_outcome, staged_dml_outcome,
};
pub use plan_kind::{PlanKind, describe_plan};
pub use shaped::{DdlColType, ShapedRow, ShapedRows};
