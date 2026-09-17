// SPDX-License-Identifier: BUSL-1.1

//! The two constant `ScanFilter` shapes policy lowering emits.

use crate::bridge::scan_filter::{FilterOp, ScanFilter};

/// A filter every row passes.
pub fn match_all_filter() -> ScanFilter {
    ScanFilter {
        field: String::new(),
        op: FilterOp::MatchAll,
        value: nodedb_types::Value::Null,
        clauses: Vec::new(),
        expr: None,
    }
}

/// A filter no row passes: a field no document carries must be non-null.
pub fn deny_filter() -> ScanFilter {
    ScanFilter {
        field: "__rls_deny__".into(),
        op: FilterOp::IsNotNull,
        value: nodedb_types::Value::Null,
        clauses: Vec::new(),
        expr: None,
    }
}

/// `field <op> value` as one filter.
pub(super) fn compare_filter(field: &str, op: FilterOp, value: nodedb_types::Value) -> ScanFilter {
    ScanFilter {
        field: field.to_string(),
        op,
        value,
        clauses: Vec::new(),
        expr: None,
    }
}

/// `match_all` when `passes`, the deny filter otherwise.
pub(super) fn verdict_filter(passes: bool) -> ScanFilter {
    if passes {
        match_all_filter()
    } else {
        deny_filter()
    }
}
