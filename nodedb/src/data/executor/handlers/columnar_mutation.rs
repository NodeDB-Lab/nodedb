//! Columnar UPDATE and DELETE handlers for plain/spatial collections.
//!
//! Uses `nodedb-columnar`'s `MutationEngine` for full mutation support
//! (PK index, delete bitmaps, WAL records).

use tracing::{debug, warn};

use crate::bridge::envelope::{ErrorCode, Response};
use crate::bridge::scan_filter::{FilterOp, ScanFilter};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;

impl CoreLoop {
    /// Handle columnar UPDATE: scan memtable for matching rows, apply field updates.
    ///
    /// Currently operates on in-memory memtable rows only.
    /// Returns `{"affected": N}` as JSON payload.
    pub(in crate::data::executor) fn execute_columnar_update(
        &mut self,
        task: &ExecutionTask,
        collection: &str,
        filter_bytes: &[u8],
        updates: &[(String, Vec<u8>)],
    ) -> Response {
        debug!(core = self.core_id, %collection, "columnar update");

        let key = (task.request.tenant_id, collection.to_string());
        let engine = match self.columnar_engines.get_mut(&key) {
            Some(e) => e,
            None => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: format!("columnar engine not found for collection '{collection}'"),
                    },
                );
            }
        };

        // Columnar UPDATE: scan memtable rows matching filter predicates,
        // then apply updates via PK-based MutationEngine (delete + re-insert).
        let schema = engine.schema().clone();
        let pk_cols: Vec<usize> = schema
            .columns
            .iter()
            .enumerate()
            .filter(|(_, c)| c.primary_key)
            .map(|(i, _)| i)
            .collect();

        if pk_cols.is_empty() {
            return self.response_error(
                task,
                ErrorCode::Internal {
                    detail: "columnar UPDATE requires a PRIMARY KEY column".into(),
                },
            );
        }

        let filter_predicates: Vec<ScanFilter> = if !filter_bytes.is_empty() {
            zerompk::from_msgpack(filter_bytes).unwrap_or_default()
        } else {
            Vec::new()
        };

        // Scan memtable rows to find matches and apply updates.
        // Collect rows to update (can't mutate while iterating).
        let rows: Vec<Vec<nodedb_types::value::Value>> = engine.scan_memtable_rows().collect();

        let mut affected = 0u64;
        for row in &rows {
            // Skip rows that don't match WHERE filters.
            if !filter_predicates.is_empty()
                && !row_matches_filters(row, &schema, &filter_predicates)
            {
                continue;
            }
            // Apply field updates to the row.
            let mut new_row = row.clone();
            for (field_name, value_bytes) in updates {
                if let Some(col_idx) = schema.columns.iter().position(|c| c.name == *field_name) {
                    let val: serde_json::Value = if let Ok(v) =
                        nodedb_types::value_from_msgpack(value_bytes)
                    {
                        v.into()
                    } else if let Ok(v) = sonic_rs::from_slice(value_bytes) {
                        v
                    } else {
                        serde_json::Value::String(String::from_utf8_lossy(value_bytes).into_owned())
                    };
                    new_row[col_idx] = json_to_value(&val, &schema.columns[col_idx].column_type);
                }
            }

            // Extract old PK value.
            let old_pk = &row[pk_cols[0]];

            // Execute update via MutationEngine (delete + insert).
            match engine.update(old_pk, &new_row) {
                Ok(_result) => {
                    affected += 1;
                }
                Err(e) => {
                    warn!(core = self.core_id, %collection, error = %e, "columnar update row failed");
                }
            }
        }

        debug!(core = self.core_id, %collection, affected, "columnar update complete");
        let result = serde_json::json!({ "affected": affected });
        match super::super::response_codec::encode_json(&result) {
            Ok(payload) => self.response_with_payload(task, payload),
            Err(e) => self.response_error(
                task,
                ErrorCode::Internal {
                    detail: e.to_string(),
                },
            ),
        }
    }

    /// Handle columnar DELETE: scan memtable for matching rows, delete them.
    ///
    /// Currently operates on in-memory memtable rows only.
    /// Returns `{"affected": N}` as JSON payload.
    pub(in crate::data::executor) fn execute_columnar_delete(
        &mut self,
        task: &ExecutionTask,
        collection: &str,
        filter_bytes: &[u8],
    ) -> Response {
        debug!(core = self.core_id, %collection, "columnar delete");

        let key = (task.request.tenant_id, collection.to_string());
        let engine = match self.columnar_engines.get_mut(&key) {
            Some(e) => e,
            None => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: format!("columnar engine not found for collection '{collection}'"),
                    },
                );
            }
        };

        let schema = engine.schema().clone();
        let pk_cols: Vec<usize> = schema
            .columns
            .iter()
            .enumerate()
            .filter(|(_, c)| c.primary_key)
            .map(|(i, _)| i)
            .collect();

        if pk_cols.is_empty() {
            return self.response_error(
                task,
                ErrorCode::Internal {
                    detail: "columnar DELETE requires a PRIMARY KEY column".into(),
                },
            );
        }

        let filter_predicates: Vec<ScanFilter> = if !filter_bytes.is_empty() {
            zerompk::from_msgpack(filter_bytes).unwrap_or_default()
        } else {
            Vec::new()
        };

        // Collect only the PK values of rows that match the WHERE filter
        // (can't mutate while iterating).
        let rows: Vec<Vec<nodedb_types::value::Value>> = engine.scan_memtable_rows().collect();
        let pk_values: Vec<nodedb_types::value::Value> = rows
            .iter()
            .filter(|row| {
                filter_predicates.is_empty()
                    || row_matches_filters(row, &schema, &filter_predicates)
            })
            .map(|row| row[pk_cols[0]].clone())
            .collect();

        let mut affected = 0u64;
        for pk in &pk_values {
            match engine.delete(pk) {
                Ok(_) => affected += 1,
                Err(e) => {
                    warn!(core = self.core_id, %collection, error = %e, "columnar delete row failed");
                }
            }
        }

        debug!(core = self.core_id, %collection, affected, "columnar delete complete");
        let result = serde_json::json!({ "affected": affected });
        match super::super::response_codec::encode_json(&result) {
            Ok(payload) => self.response_with_payload(task, payload),
            Err(e) => self.response_error(
                task,
                ErrorCode::Internal {
                    detail: e.to_string(),
                },
            ),
        }
    }
}

/// Check whether a memtable row satisfies all filter predicates (AND semantics).
fn row_matches_filters(
    row: &[nodedb_types::value::Value],
    schema: &nodedb_types::columnar::ColumnarSchema,
    filters: &[ScanFilter],
) -> bool {
    for filter in filters {
        if filter.op == FilterOp::MatchAll {
            continue;
        }
        let col_idx = match schema.columns.iter().position(|c| c.name == filter.field) {
            Some(i) => i,
            None => continue, // unknown field — skip predicate
        };
        if col_idx >= row.len() {
            return false;
        }
        if !eval_filter(&row[col_idx], filter.op, &filter.value) {
            return false;
        }
    }
    true
}

/// Evaluate a single filter predicate against a row value.
fn eval_filter(
    val: &nodedb_types::value::Value,
    op: FilterOp,
    filter_val: &nodedb_types::value::Value,
) -> bool {
    use nodedb_types::value::Value;

    let val_f64 = match val {
        Value::Float(f) => Some(*f),
        Value::Integer(i) => Some(*i as f64),
        _ => None,
    };
    let filter_f64 = match filter_val {
        Value::Float(f) => Some(*f),
        Value::Integer(i) => Some(*i as f64),
        _ => None,
    };

    let val_str = match val {
        Value::String(s) => Some(s.as_str()),
        _ => None,
    };
    let filter_str = match filter_val {
        Value::String(s) => Some(s.as_str()),
        _ => None,
    };

    match op {
        FilterOp::Eq => {
            if let (Some(a), Some(b)) = (val_f64, filter_f64) {
                a == b
            } else if let (Some(a), Some(b)) = (val_str, filter_str) {
                a == b
            } else {
                false
            }
        }
        FilterOp::Ne => {
            if let (Some(a), Some(b)) = (val_f64, filter_f64) {
                a != b
            } else if let (Some(a), Some(b)) = (val_str, filter_str) {
                a != b
            } else {
                true
            }
        }
        FilterOp::Gt => val_f64.zip(filter_f64).is_some_and(|(a, b)| a > b),
        FilterOp::Gte => val_f64.zip(filter_f64).is_some_and(|(a, b)| a >= b),
        FilterOp::Lt => val_f64.zip(filter_f64).is_some_and(|(a, b)| a < b),
        FilterOp::Lte => val_f64.zip(filter_f64).is_some_and(|(a, b)| a <= b),
        FilterOp::IsNull => matches!(val, Value::Null),
        FilterOp::IsNotNull => !matches!(val, Value::Null),
        _ => true, // unknown/unsupported op — pass through
    }
}

/// Convert a `serde_json::Value` to `nodedb_types::value::Value` for a given column type.
fn json_to_value(
    json: &serde_json::Value,
    col_type: &nodedb_types::columnar::ColumnType,
) -> nodedb_types::value::Value {
    use nodedb_types::columnar::ColumnType;
    use nodedb_types::value::Value;

    match (col_type, json) {
        (_, serde_json::Value::Null) => Value::Null,
        (ColumnType::Int64, serde_json::Value::Number(n)) => {
            Value::Integer(n.as_i64().unwrap_or(0))
        }
        (ColumnType::Float64, serde_json::Value::Number(n)) => {
            Value::Float(n.as_f64().unwrap_or(0.0))
        }
        (ColumnType::Bool, serde_json::Value::Bool(b)) => Value::Bool(*b),
        (ColumnType::String, serde_json::Value::String(s)) => Value::String(s.clone()),
        (ColumnType::Timestamp, serde_json::Value::Number(n)) => {
            Value::Integer(n.as_i64().unwrap_or(0))
        }
        (ColumnType::Timestamp, serde_json::Value::String(s)) => {
            nodedb_types::datetime::NdbDateTime::parse(s)
                .map(Value::DateTime)
                .unwrap_or_else(|| Value::String(s.clone()))
        }
        // Fallback: try to coerce.
        (_, serde_json::Value::Number(n)) => {
            if let Some(i) = n.as_i64() {
                Value::Integer(i)
            } else {
                Value::Float(n.as_f64().unwrap_or(0.0))
            }
        }
        (_, serde_json::Value::String(s)) => Value::String(s.clone()),
        (_, serde_json::Value::Bool(b)) => Value::Bool(*b),
        _ => Value::Null,
    }
}
