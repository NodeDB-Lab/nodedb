// SPDX-License-Identifier: BUSL-1.1

//! Columnar base scan handler.
//!
//! Reads rows from the `MutationEngine` memtable and flushed segments at a
//! version cut, applies projection, WHERE filter predicates, and limit. Used
//! by plain columnar and spatial collections.

pub mod bitemporal;
pub mod block_predicates;
pub mod convert;
pub mod filter;
pub mod flushed_segment;
pub mod materialize_scan;
pub mod materialize_scan_ts;
pub mod materialize_scan_ts_row;
pub mod scan;
pub mod scan_flushed;
pub mod sort;
pub mod system_fold;

pub(in crate::data::executor) use convert::{emit_column_value, rmpv_time_cell};
pub(in crate::data::executor) use scan::ColumnarScanParams;
