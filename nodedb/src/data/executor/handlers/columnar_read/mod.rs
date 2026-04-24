//! Columnar base scan handler.
//!
//! Reads rows from the `MutationEngine` memtable, applies projection,
//! WHERE filter predicates, and limit. Used by plain columnar and spatial
//! collections.

pub mod bitemporal;
pub mod convert;
pub mod filter;
pub mod scan;
pub mod sort;

pub(in crate::data::executor) use convert::emit_column_value;
pub(in crate::data::executor) use scan::ColumnarScanParams;
