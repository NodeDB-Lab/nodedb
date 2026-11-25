// SPDX-License-Identifier: BUSL-1.1

//! Serialization of one join side's own `WHERE` predicates.
//!
//! A side scanned by name is read as a bare collection scan, so its predicates
//! must address that collection's own rows — unlike the post-join filters in
//! `filter`, which address alias-prefixed merged rows.

use nodedb_sql::types::{Filter, FilterExpr};

use super::filter::{
    encode_scan_filters, expr_filter, filter_to_scan_filters, is_null_literal_equality,
    never_match_filter,
};

/// Serialize one join side's own `WHERE` predicates for a scan of that side's
/// collection.
///
/// Column qualifiers are dropped: the predicate belongs to the side being
/// scanned, so every column names a field of that side's own rows. A qualifier
/// kept here becomes a literal `"e.score"` field lookup no stored row has, and
/// the predicate matches nothing.
pub(crate) fn serialize_scan_side_filters(filters: &[Filter]) -> crate::Result<Vec<u8>> {
    if filters.is_empty() {
        return Ok(Vec::new());
    }
    let scan_filters = filters
        .iter()
        .flat_map(|filter| side_filter_to_scan_filters(&filter.expr))
        .collect::<Vec<_>>();
    encode_scan_filters(&scan_filters)
}

fn side_filter_to_scan_filters(expr: &FilterExpr) -> Vec<nodedb_query::scan_filter::ScanFilter> {
    use nodedb_query::scan_filter::{FilterOp, ScanFilter};

    match expr {
        FilterExpr::And(filters) => filters
            .iter()
            .flat_map(|filter| side_filter_to_scan_filters(&filter.expr))
            .collect(),
        FilterExpr::Or(filters) => vec![ScanFilter {
            field: String::new(),
            op: FilterOp::Or,
            value: nodedb_types::Value::Null,
            clauses: filters
                .iter()
                .map(|filter| side_filter_to_scan_filters(&filter.expr))
                .collect(),
            expr: None,
        }],
        FilterExpr::Expr(sql_expr) => {
            if is_null_literal_equality(sql_expr) {
                vec![never_match_filter()]
            } else {
                vec![expr_filter(sql_expr)]
            }
        }
        other => filter_to_scan_filters(other),
    }
}
