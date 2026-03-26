//! Expression extraction helpers for the plan converter.

mod dml;
mod filter;
mod index_scan;
// Integration pending: wired when per-core R-tree spatial index manager is available.
#[allow(dead_code)]
pub(crate) mod spatial_filter;

pub(super) use dml::{extract_insert_values, extract_point_targets, extract_update_assignments};
pub(super) use filter::{expr_to_scan_filters, extract_where_filters};
pub(super) use index_scan::try_range_scan_from_predicate;

pub(super) use super::expr_convert::expr_to_usize;
