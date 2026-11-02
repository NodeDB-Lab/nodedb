// SPDX-License-Identifier: BUSL-1.1

//! Lower a scan's row predicates to block-statistics predicates.
//!
//! A flushed segment keeps min / max statistics per 1024-row block. A block
//! whose statistics rule out one conjunct of the WHERE clause, or of the read
//! policy, holds no row the scan returns, so the scan never decodes it.
//!
//! Only a predicate whose row-level result the statistics decide exactly is
//! lowered: an `=`, `<`, `<=`, `>` or `>=` of an `Int64` column against an
//! integer. Integer cells compare to an integer by value, and a null cell
//! matches none of these operators. Every other predicate stays row-level
//! only. A string compares as a number or an instant before it compares as
//! text, so string statistics cannot decide it. `_ts_system` reads in
//! milliseconds but stores an ordinal, so its statistics cannot decide it
//! either.

use nodedb_columnar::predicate::ScanPredicate;
use nodedb_types::columnar::schema::TS_SYSTEM;
use nodedb_types::columnar::{ColumnType, ColumnarSchema};
use nodedb_types::value::Value;

use crate::bridge::scan_filter::{FilterOp, ScanFilter};

/// The block predicates `filters` and `rls_filters` lower to. Both lists
/// are conjunctions, so a block any lowered predicate rules out holds no
/// matching row.
pub(in crate::data::executor) fn block_predicates(
    schema: &ColumnarSchema,
    filters: &[ScanFilter],
    rls_filters: &[ScanFilter],
) -> Vec<ScanPredicate> {
    filters
        .iter()
        .chain(rls_filters)
        .filter_map(|filter| lower(schema, filter))
        .collect()
}

/// The block predicate one row predicate lowers to, or `None` when block
/// statistics cannot decide it.
fn lower(schema: &ColumnarSchema, filter: &ScanFilter) -> Option<ScanPredicate> {
    if !filter.clauses.is_empty() || filter.expr.is_some() || filter.field == TS_SYSTEM {
        return None;
    }
    let Value::Integer(value) = filter.value else {
        return None;
    };
    let col_idx = schema
        .columns
        .iter()
        .position(|col| col.name == filter.field)?;
    if schema.columns[col_idx].column_type != ColumnType::Int64 {
        return None;
    }
    match filter.op {
        FilterOp::Eq => Some(ScanPredicate::eq_i64(col_idx, value)),
        FilterOp::Gt => Some(ScanPredicate::gt_i64(col_idx, value)),
        FilterOp::Gte => Some(ScanPredicate::gte_i64(col_idx, value)),
        FilterOp::Lt => Some(ScanPredicate::lt_i64(col_idx, value)),
        FilterOp::Lte => Some(ScanPredicate::lte_i64(col_idx, value)),
        FilterOp::Ne
        | FilterOp::Contains
        | FilterOp::Like
        | FilterOp::NotLike
        | FilterOp::Ilike
        | FilterOp::NotIlike
        | FilterOp::In
        | FilterOp::NotIn
        | FilterOp::IsNull
        | FilterOp::IsNotNull
        | FilterOp::ArrayContains
        | FilterOp::ArrayContainsAll
        | FilterOp::ArrayOverlap
        | FilterOp::MatchAll
        | FilterOp::Exists
        | FilterOp::NotExists
        | FilterOp::Or
        | FilterOp::Expr
        | FilterOp::GtColumn
        | FilterOp::GteColumn
        | FilterOp::LtColumn
        | FilterOp::LteColumn
        | FilterOp::EqColumn
        | FilterOp::NeColumn => None,
    }
}

#[cfg(test)]
mod tests {
    use nodedb_types::columnar::ColumnDef;

    use super::*;

    fn schema() -> ColumnarSchema {
        ColumnarSchema::new(vec![
            ColumnDef::required("id", ColumnType::Int64).with_primary_key(),
            ColumnDef::nullable("name", ColumnType::String),
        ])
        .expect("valid")
    }

    fn filter(field: &str, op: FilterOp, value: Value) -> ScanFilter {
        ScanFilter {
            field: field.into(),
            op,
            value,
            clauses: Vec::new(),
            expr: None,
        }
    }

    #[test]
    fn only_integer_comparisons_of_int64_columns_lower() {
        let lowered = block_predicates(
            &schema(),
            &[
                filter("id", FilterOp::Gte, Value::Integer(5)),
                filter("id", FilterOp::Ne, Value::Integer(5)),
                filter("id", FilterOp::Gt, Value::Float(5.5)),
                filter("name", FilterOp::Eq, Value::String("a".into())),
                filter("t.id", FilterOp::Eq, Value::Integer(1)),
            ],
            &[filter("id", FilterOp::Lt, Value::Integer(9))],
        );
        assert_eq!(lowered.len(), 2);
        assert!(lowered.iter().all(|p| p.col_idx == 0));
    }
}
