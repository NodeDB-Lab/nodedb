// SPDX-License-Identifier: BUSL-1.1

//! Spatial query handler: R-tree index scan with predicate refinement.
//!
//! Documents with geometry fields are auto-indexed into per-field R-trees
//! on insert (see `handlers/point.rs`). Spatial queries use the R-tree for
//! fast bbox candidate selection, then refine with exact predicates.
//!
//! Internal document representation: `nodedb_types::Value` (no JSON intermediary).

mod full_scan;
mod prefilter;
mod rtree_scan;

pub(in crate::data::executor) use rtree_scan::SpatialScanParams;
