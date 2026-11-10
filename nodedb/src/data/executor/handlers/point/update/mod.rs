// SPDX-License-Identifier: BUSL-1.1

//! PointUpdate: read-modify-write field-level changes to a single document.
//!
//! Each assignment is either a pre-encoded literal (fast binary merge when
//! possible) or a `SqlExpr` that must be evaluated against the *current* row —
//! the evaluator is `nodedb_query::expr::SqlExpr::eval`, shared with
//! computed-column, window, and typeguard paths.

pub(in crate::data::executor) mod exec;
pub(in crate::data::executor) mod persist;
pub(in crate::data::executor) mod post_image;
pub(in crate::data::executor) mod prepared;

pub(in crate::data::executor) use exec::PointUpdateParams;
