// SPDX-License-Identifier: Apache-2.0

//! ORDER BY processing and search-trigger detection.
//!
//! Splits responsibilities into focused modules:
//!
//! - `apply` — `apply_order_by` entry point: maps an ORDER BY clause to
//!   either sort keys on a scan or a search-shaped plan when the leading
//!   sort expression matches a `SearchTrigger`.
//! - `projection` — `try_hybrid_from_projection` fallback: when no ORDER BY
//!   drives the conversion, scan the SELECT list for a hybrid-search trigger.
//! - `aliases` — alias resolution between ORDER BY and SELECT projection.
//! - `triggers` — generic `SearchTrigger` → `SqlPlan` detection.
//! - `hybrid` — `rrf_score(...)` → `SqlPlan::HybridSearch` construction.
//! - `text_score` — `bm25_score(...)` columns attached to a text or scan plan.
//! - `vector_join` — `vector_distance ⋈ ARRAY_SLICE` fusion target detection.

mod aliases;
mod apply;
mod hybrid;
mod projection;
mod text_score;
mod triggers;
mod vector_join;

pub(crate) use aliases::select_output_aliases;
pub(super) use apply::apply_order_by;
pub(super) use projection::try_hybrid_from_projection;
