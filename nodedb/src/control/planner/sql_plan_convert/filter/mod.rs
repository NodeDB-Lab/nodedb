// SPDX-License-Identifier: BUSL-1.1

//! Filter serialization: SqlPlan filters → ScanFilter msgpack bytes.
//!
//! The boundary between the Control Plane planner and the Data Plane scan
//! evaluator. `serialize` walks the planner's `Filter` tree and encodes the
//! `ScanFilter` list; `expr_lower` reduces a raw WHERE `SqlExpr` to native
//! `(field, op, value)` triples where it can and ships anything else verbatim
//! as a `FilterOp::Expr`.

mod expr_lower;
mod serialize;

pub(super) use expr_lower::{expr_filter, expr_filter_qualified};
pub(super) use serialize::{
    encode_scan_filters, filter_to_scan_filters, serialize_filters, serialize_join_post_filters,
};
