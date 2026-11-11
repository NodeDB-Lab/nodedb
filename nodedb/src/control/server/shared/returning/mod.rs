// SPDX-License-Identifier: BUSL-1.1

//! Resolved DML response clauses and physical projection attachment.

mod clause;
mod contract;
mod fallibility;
mod inject;
mod list_shape;
mod payload_bound;
pub(crate) mod precommit;
pub(crate) mod transcript;

// Re-export bridge types so callers only import from this module.
pub use nodedb_physical::physical_plan::{ReturningColumns, ReturningItem, ReturningSpec};

pub use clause::{
    ReturningClause, plan_returning_spec, resolve_returning_for_plans, stored_columns_spec,
};
pub use contract::{plan_carries_returning_contract, tasks_carry_returning_contract};
pub use fallibility::{returning_can_fail, tasks_returning_can_fail};
pub use inject::{
    attach_returning_spec, in_transaction_returning_unsupported, inject_returning_spec,
    refuse_unprojectable_insert_returning,
};
pub use list_shape::RowFields;
