// SPDX-License-Identifier: BUSL-1.1

//! Document read and scan handlers: Scan, PointGet, RangeScan, IndexLookup.

mod audit_body;
pub mod decode;
pub mod emit;
pub mod fetch;
pub mod fetch_types;
pub mod materialize_scan;
pub mod projection;
pub mod scan;
