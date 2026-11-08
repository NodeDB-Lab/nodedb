// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral runtime enforcement for general CHECK constraints.
//!
//! This is runtime write-path enforcement, not a DDL handler, so it does not
//! live under `ddl/`. See [`enforce::enforce_check_constraints`] for the
//! evaluation strategy and [`statement`] for the per-statement entry points
//! every protocol calls before planning.

mod enforce;
pub mod request;
mod simple;
pub mod statement;
mod subquery;
mod subquery_bind;
mod subquery_match;

pub use enforce::enforce_check_constraints;
pub use request::{RequestChecks, enforce_request_checks};
pub use statement::{enforce_statement_checks, enforce_statement_enum_labels};
pub(crate) use subquery::validate_in_subquery_check;
