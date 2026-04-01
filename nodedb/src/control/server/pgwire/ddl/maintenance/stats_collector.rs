//! Column statistics collector using SIMD-accelerated kernels.
//!
//! Extracts typed slices from scan results and computes per-column
//! statistics (min, max, count, null_count, distinct_count) using
//! the existing SIMD aggregation kernels from nodedb-query.

use std::collections::HashSet;

use crate::control::security::catalog::column_stats::StoredColumnStats;

/// Collect column statistics from JSON scan results.
///
/// Parses each row as JSON, extracts field values, and computes
/// aggregate statistics. For numeric columns, delegates to SIMD
/// kernels for min/max/sum computation.
pub fn collect_stats_from_json_rows(
    tenant_id: u32,
    collection: &str,
    columns: &[String],
    rows: &[String],
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

        for row_json in rows {
            let value = extract_field_from_json(row_json, col_name);
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

/// Extract a field value from a JSON row string.
fn extract_field_from_json(json_str: &str, field_name: &str) -> FieldValue {
    let parsed: Result<serde_json::Value, _> = serde_json::from_str(json_str);
    let Ok(obj) = parsed else {
        return FieldValue::Null;
    };

    let val = match &obj {
        serde_json::Value::Object(map) => map.get(field_name),
        _ => None,
    };

    match val {
        None | Some(serde_json::Value::Null) => FieldValue::Null,
        Some(serde_json::Value::Number(n)) => {
            if let Some(i) = n.as_i64() {
                FieldValue::Int(i)
            } else if let Some(f) = n.as_f64() {
                FieldValue::Float(f)
            } else {
                FieldValue::Text(n.to_string())
            }
        }
        Some(serde_json::Value::String(s)) => FieldValue::Text(s.clone()),
        Some(serde_json::Value::Bool(b)) => FieldValue::Text(b.to_string()),
        Some(other) => FieldValue::Text(other.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn collect_numeric_stats() {
        let rows: Vec<String> = (0..100)
            .map(|i| format!("{{\"id\": {i}, \"value\": {:.2}}}", i as f64 * 1.5))
            .collect();
        let stats = collect_stats_from_json_rows(1, "test", &["id".into()], &rows, 0);
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
            r#"{"name": "alice"}"#.to_string(),
            r#"{"name": null}"#.to_string(),
            r#"{"other": 1}"#.to_string(),
        ];
        let stats = collect_stats_from_json_rows(1, "t", &["name".into()], &rows, 0);
        assert_eq!(stats[0].null_count, 2); // null + missing field
        assert_eq!(stats[0].distinct_count, 1);
    }
}
