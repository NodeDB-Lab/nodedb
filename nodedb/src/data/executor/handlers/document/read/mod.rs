// SPDX-License-Identifier: BUSL-1.1

//! Document read and scan handlers: Scan, PointGet, RangeScan, IndexLookup.

mod audit_body;
pub mod decode;
pub mod emit;
pub mod fetch;
mod fetch_types;
pub mod materialize_scan;
mod materialize_scan_vectors;
pub mod projection;
pub mod scan;

pub(in crate::data::executor) use fetch_types::{DocFetchParams, DocScanMode};
