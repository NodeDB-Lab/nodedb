// SPDX-License-Identifier: BUSL-1.1

//! RETURNING clause handling for DML statements: strip it from the text,
//! resolve its item list against the planned target, decide whether the
//! resulting plan can carry it, and attach it.
//!
//! Protocol-neutral: the pgwire planner, the neutral DDL router's `UPSERT`
//! path, and the prepared-statement Describe path all go through this
//! module, so a statement's clause is stripped, resolved, judged, and
//! attached identically on every transport.

mod clause;
mod inject;
mod strip;

// Re-export bridge types so callers only import from this module.
pub use nodedb_physical::physical_plan::{ReturningColumns, ReturningItem, ReturningSpec};

pub use clause::{ReturningClause, resolve_returning_clause, resolve_returning_for_plans};
pub use inject::{
    attach_returning_spec, in_transaction_returning_unsupported, inject_returning_spec,
    refuse_unprojectable_insert_returning,
};
pub use strip::strip_returning;
