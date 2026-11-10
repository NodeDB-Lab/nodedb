// SPDX-License-Identifier: BUSL-1.1

//! Resolved DML response clauses and physical projection attachment.

mod clause;
mod inject;
pub(crate) mod precommit;
pub(crate) mod transcript;

// Re-export bridge types so callers only import from this module.
pub use nodedb_physical::physical_plan::{ReturningColumns, ReturningItem, ReturningSpec};

pub use clause::{ReturningClause, resolve_returning_for_plans, stored_columns_spec};
pub use inject::{
    attach_returning_spec, in_transaction_returning_unsupported, inject_returning_spec,
    refuse_unprojectable_insert_returning,
};
