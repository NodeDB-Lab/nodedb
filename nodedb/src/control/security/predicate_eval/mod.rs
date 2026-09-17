// SPDX-License-Identifier: BUSL-1.1

//! Plan-time predicate evaluation: substitute `$auth.*` references and
//! combine policies into concrete `ScanFilter` values.
//!
//! Converts compiled [`super::predicate::RlsPredicate`] trees into static
//! `ScanFilter` lists that the Data Plane evaluates without session
//! awareness.
//!
//! - [`substitute`] — `substitute_to_scan_filters` and `combine_policies`.
//! - [`sets`] — `CONTAINS` / `INTERSECTS` lowering.
//! - [`filters`] — the `match_all` and deny filter constructors.

pub mod filters;
pub mod sets;
pub mod substitute;

pub use filters::{deny_filter, match_all_filter};
pub use substitute::{combine_policies, substitute_to_scan_filters};
