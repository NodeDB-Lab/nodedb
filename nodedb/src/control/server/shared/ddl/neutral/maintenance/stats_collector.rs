// SPDX-License-Identifier: BUSL-1.1

//! Column statistics collector using SIMD-accelerated kernels.
//!
//! Extracts typed slices from scan results and computes per-column
//! statistics (min, max, count, null_count, distinct_count) using
//! the existing SIMD aggregation kernels from nodedb-query.

use std::collections::HashSet;

use nodedb_types::Value;

use crate::control::security::catalog::column_stats::StoredColumnStats;
use crate::control::server::response_shape::types::ShapedRow;

/// Collect column statistics from typed scan rows.
///
/// Each row is one row's column map, with any scan envelope already
/// unwrapped. Field values keep their decoded type, plain or native. For
/// numeric columns, delegates to SIMD kernels for min/max/sum computation.
pub fn collect_stats_from_rows(
    database_id: u64,
    tenant_id: u64,
    collection: &str,
    columns: &[String],
    rows: &[ShapedRow],
    analyzed_at: u64,
) -> Vec<StoredColumnStats> {
    let row_count = rows.len() as u64;
    let mut results = Vec::with_capacity(columns.len());

    for col_name in columns {
        let mut null_count = 0u64;
        let mut f64_values = Vec::new();
        let mut i64_values = Vec::new();
        let mut string_values = Vec::new();
        let mut distinct_set: HashSet<String> = HashSet::new();
        let mut total_len: u64 = 0;

        for row in rows {
            let value = field_value(row.get(col_name.as_str()));
            match value {
                FieldValue::Null => null_count += 1,
                FieldValue::Int(v) => {
                    i64_values.push(v);
                    distinct_set.insert(v.to_string());
                }
                FieldValue::Float(v) => {
                    f64_values.push(v);
                    distinct_set.insert(format!("{v:.6}"));
                }
                FieldValue::Text(s) => {
                    total_len += s.len() as u64;
                    distinct_set.insert(s.clone());
                    string_values.push(s);
                }
            }
        }

        // Compute min/max using SIMD kernels for numeric columns.
        let (min_value, max_value) = if !f64_values.is_empty() {
            let rt = nodedb_query::simd_agg::ts_runtime();
            let min = (rt.min_f64)(&f64_values);
            let max = (rt.max_f64)(&f64_values);
            (Some(format!("{min}")), Some(format!("{max}")))
        } else if !i64_values.is_empty() {
            let rt = nodedb_query::simd_agg_i64::i64_runtime();
            let min = (rt.min_i64)(&i64_values);
            let max = (rt.max_i64)(&i64_values);
            (Some(format!("{min}")), Some(format!("{max}")))
        } else if !string_values.is_empty() {
            let min = string_values.iter().min().cloned();
            let max = string_values.iter().max().cloned();
            (min, max)
        } else {
            (None, None)
        };

        let non_null = row_count - null_count;
        let avg_len = if non_null > 0 && total_len > 0 {
            Some((total_len / non_null) as u32)
        } else {
            None
        };

        results.push(StoredColumnStats {
            database_id,
            tenant_id,
            collection: collection.to_string(),
            column: col_name.clone(),
            row_count,
            null_count,
            distinct_count: distinct_set.len() as u64,
            min_value,
            max_value,
            avg_value_len: avg_len,
            analyzed_at,
        });
    }

    results
}

enum FieldValue {
    Null,
    Int(i64),
    Float(f64),
    Text(String),
}

/// Classify one typed cell for statistics.
///
/// A number is numeric. Every other value is its text: a string, UUID or
/// ULID itself, a Decimal, duration or instant its text, as a plain row
/// stores it, and an array or object its JSON text. So a plain and a native
/// row of the same values give the same statistics.
fn field_value(value: Option<&Value>) -> FieldValue {
    match value {
        None | Some(Value::Null) => FieldValue::Null,
        Some(Value::Integer(i)) => FieldValue::Int(*i),
        Some(Value::Float(f)) => FieldValue::Float(*f),
        Some(Value::String(s) | Value::Uuid(s) | Value::Ulid(s)) => FieldValue::Text(s.clone()),
        Some(Value::Bool(b)) => FieldValue::Text(b.to_string()),
        Some(Value::Decimal(d)) => FieldValue::Text(d.to_string()),
        Some(Value::Duration(d)) => FieldValue::Text(d.to_string()),
        Some(Value::DateTime(dt) | Value::NaiveDateTime(dt)) => FieldValue::Text(dt.to_string()),
        Some(other) => {
            FieldValue::Text(crate::util::wire_json::value_to_wire_json(other).to_string())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(pairs: Vec<(&str, Value)>) -> ShapedRow {
        pairs.into_iter().map(|(k, v)| (k.to_owned(), v)).collect()
    }

    #[test]
    fn collect_numeric_stats() {
        let rows: Vec<ShapedRow> = (0..100)
            .map(|i| {
                row(vec![
                    ("id", Value::Integer(i)),
                    ("value", Value::Float(i as f64 * 1.5)),
                ])
            })
            .collect();
        let stats = collect_stats_from_rows(9, 1, "test", &["id".into()], &rows, 0);
        assert_eq!(stats.len(), 1);
        assert_eq!(stats[0].row_count, 100);
        assert_eq!(stats[0].null_count, 0);
        assert_eq!(stats[0].min_value, Some("0".to_string()));
        assert_eq!(stats[0].max_value, Some("99".to_string()));
        assert_eq!(stats[0].distinct_count, 100);
    }

    #[test]
    fn collect_with_nulls() {
        let rows = vec![
            row(vec![("name", Value::String("alice".into()))]),
            row(vec![("name", Value::Null)]),
            row(vec![("other", Value::Integer(1))]),
        ];
        let stats = collect_stats_from_rows(9, 1, "t", &["name".into()], &rows, 0);
        assert_eq!(stats[0].null_count, 2); // null + missing field
        assert_eq!(stats[0].distinct_count, 1);
    }

    /// A native row's Decimal, UUID and instant cells give the statistics
    /// their plain-stored text gives.
    #[test]
    fn native_cells_count_as_their_plain_text() {
        let at = nodedb_types::NdbDateTime::from_micros(1_583_402_400_000_000);
        let native = vec![row(vec![
            ("price", Value::Decimal("12.50".parse().unwrap())),
            (
                "uid",
                Value::Uuid("550e8400-e29b-41d4-a716-446655440000".into()),
            ),
            ("seen", Value::DateTime(at)),
        ])];
        let plain = vec![row(vec![
            ("price", Value::String("12.50".into())),
            (
                "uid",
                Value::String("550e8400-e29b-41d4-a716-446655440000".into()),
            ),
            ("seen", Value::String(at.to_string())),
        ])];
        let columns = ["price".into(), "uid".into(), "seen".into()];
        let from_native = collect_stats_from_rows(9, 1, "t", &columns, &native, 0);
        let from_plain = collect_stats_from_rows(9, 1, "t", &columns, &plain, 0);
        for (n, p) in from_native.iter().zip(&from_plain) {
            assert_eq!(n.column, p.column);
            assert_eq!(n.min_value, p.min_value);
            assert_eq!(n.max_value, p.max_value);
            assert_eq!(n.distinct_count, p.distinct_count);
        }
    }
}
