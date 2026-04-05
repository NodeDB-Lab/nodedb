//! Columnar base insert handler.
//!
//! Writes rows to the columnar memtable. Accepts JSON payload (array of
//! objects). Creates the memtable on first insert with schema inferred
//! from the first row.

use sonic_rs;

use crate::bridge::envelope::{ErrorCode, Payload, Response, Status};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::response_codec;
use crate::data::executor::task::ExecutionTask;
use crate::engine::timeseries::columnar_memtable::{
    ColumnType, ColumnValue, ColumnarMemtable, ColumnarMemtableConfig, ColumnarSchema,
};

impl CoreLoop {
    /// Execute a columnar insert: write rows from JSON payload to memtable.
    pub(in crate::data::executor) fn execute_columnar_insert(
        &mut self,
        task: &ExecutionTask,
        collection: &str,
        payload: &[u8],
        _format: &str,
    ) -> Response {
        // Parse JSON payload: expect array of objects or single object.
        let rows: Vec<serde_json::Value> = match sonic_rs::from_slice(payload) {
            Ok(serde_json::Value::Array(arr)) => arr,
            Ok(obj @ serde_json::Value::Object(_)) => vec![obj],
            Ok(_) => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: "payload must be a JSON array of objects or a single object".into(),
                    },
                );
            }
            Err(e) => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: format!("invalid JSON payload: {e}"),
                    },
                );
            }
        };

        if rows.is_empty() {
            return self.response_error(
                task,
                ErrorCode::Internal {
                    detail: "empty payload".into(),
                },
            );
        }

        // Ensure memtable exists (auto-create on first write).
        if !self.columnar_memtables.contains_key(collection) {
            let schema = infer_schema_from_json(&rows[0]);
            let config = ColumnarMemtableConfig {
                max_memory_bytes: 64 * 1024 * 1024,
                hard_memory_limit: 80 * 1024 * 1024,
                max_tag_cardinality: 100_000,
            };
            let mt = ColumnarMemtable::new(schema, config);
            self.columnar_memtables.insert(collection.to_string(), mt);
        }

        let mt = self.columnar_memtables.get_mut(collection).unwrap();
        let schema = mt.schema().clone();
        let mut accepted = 0u64;

        for row in &rows {
            let obj = match row.as_object() {
                Some(o) => o,
                None => continue,
            };

            // Build column values in schema order, collecting owned strings
            // for Symbol columns so we can borrow them in ColumnValue.
            let mut string_values: Vec<String> = Vec::new();
            let mut raw_values: Vec<(usize, &ColumnType, Option<i64>, Option<f64>)> = Vec::new();

            for (idx, (col_name, col_type)) in schema.columns.iter().enumerate() {
                match obj.get(col_name) {
                    Some(serde_json::Value::Number(n)) => match col_type {
                        ColumnType::Float64 => {
                            raw_values.push((idx, col_type, None, Some(n.as_f64().unwrap_or(0.0))));
                        }
                        ColumnType::Int64 | ColumnType::Timestamp => {
                            raw_values.push((idx, col_type, Some(n.as_i64().unwrap_or(0)), None));
                        }
                        ColumnType::Symbol => {
                            string_values.push(n.to_string());
                            raw_values.push((idx, col_type, None, None));
                        }
                    },
                    Some(serde_json::Value::String(s)) => {
                        string_values.push(s.clone());
                        raw_values.push((idx, col_type, None, None));
                    }
                    Some(serde_json::Value::Bool(b)) => {
                        raw_values.push((idx, col_type, Some(if *b { 1 } else { 0 }), None));
                    }
                    _ => match col_type {
                        ColumnType::Float64 => raw_values.push((idx, col_type, None, Some(0.0))),
                        ColumnType::Int64 | ColumnType::Timestamp => {
                            raw_values.push((idx, col_type, Some(0), None))
                        }
                        ColumnType::Symbol => {
                            string_values.push(String::new());
                            raw_values.push((idx, col_type, None, None));
                        }
                    },
                }
            }

            // Build ColumnValue slice. Symbol values reference the owned strings.
            let mut str_idx = 0;
            let values: Vec<ColumnValue<'_>> = raw_values
                .iter()
                .map(|(_, col_type, int_val, float_val)| match col_type {
                    ColumnType::Timestamp => ColumnValue::Timestamp(int_val.unwrap_or(0)),
                    ColumnType::Float64 => ColumnValue::Float64(float_val.unwrap_or(0.0)),
                    ColumnType::Int64 => ColumnValue::Int64(int_val.unwrap_or(0)),
                    ColumnType::Symbol => {
                        let s = &string_values[str_idx];
                        str_idx += 1;
                        ColumnValue::Symbol(s.as_str())
                    }
                })
                .collect();

            let _ = mt.ingest_row(0, &values);
            accepted += 1;
        }

        self.checkpoint_coordinator
            .mark_dirty("columnar", accepted as usize);

        let result = serde_json::json!({
            "accepted": accepted,
            "collection": collection,
        });
        let json = match response_codec::encode_json(&result) {
            Ok(b) => b,
            Err(e) => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: e.to_string(),
                    },
                );
            }
        };
        Response {
            request_id: task.request.request_id,
            status: Status::Ok,
            attempt: 1,
            partial: false,
            payload: Payload::from_vec(json),
            watermark_lsn: self.watermark,
            error_code: None,
        }
    }
}

impl CoreLoop {
    /// Ingest a single JSON document into the columnar memtable for a collection.
    ///
    /// Creates the memtable on first call. Used by the spatial insert path
    /// to ensure spatial data is available for columnar scans and aggregates.
    pub(in crate::data::executor) fn ingest_doc_to_columnar(
        &mut self,
        collection: &str,
        obj: &serde_json::Map<String, serde_json::Value>,
    ) {
        // Ensure memtable exists.
        if !self.columnar_memtables.contains_key(collection) {
            let schema = infer_schema_from_json(&serde_json::Value::Object(obj.clone()));
            let config = ColumnarMemtableConfig {
                max_memory_bytes: 64 * 1024 * 1024,
                hard_memory_limit: 80 * 1024 * 1024,
                max_tag_cardinality: 100_000,
            };
            let mt = ColumnarMemtable::new(schema, config);
            self.columnar_memtables.insert(collection.to_string(), mt);
        }

        let mt = self.columnar_memtables.get_mut(collection).unwrap();
        let schema = mt.schema().clone();

        // Build owned string values for Symbol columns (need stable references).
        let mut string_values: Vec<String> = Vec::new();
        let mut raw_values: Vec<(&ColumnType, Option<i64>, Option<f64>)> = Vec::new();

        for (col_name, col_type) in &schema.columns {
            match obj.get(col_name) {
                Some(serde_json::Value::Number(n)) => match col_type {
                    ColumnType::Float64 => {
                        raw_values.push((col_type, None, Some(n.as_f64().unwrap_or(0.0))));
                    }
                    ColumnType::Int64 | ColumnType::Timestamp => {
                        raw_values.push((col_type, Some(n.as_i64().unwrap_or(0)), None));
                    }
                    ColumnType::Symbol => {
                        string_values.push(n.to_string());
                        raw_values.push((col_type, None, None));
                    }
                },
                Some(serde_json::Value::String(s)) => {
                    string_values.push(s.clone());
                    raw_values.push((col_type, None, None));
                }
                Some(serde_json::Value::Bool(b)) => {
                    raw_values.push((col_type, Some(if *b { 1 } else { 0 }), None));
                }
                _ => match col_type {
                    ColumnType::Float64 => raw_values.push((col_type, None, Some(0.0))),
                    ColumnType::Int64 | ColumnType::Timestamp => {
                        raw_values.push((col_type, Some(0), None))
                    }
                    ColumnType::Symbol => {
                        string_values.push(String::new());
                        raw_values.push((col_type, None, None));
                    }
                },
            }
        }

        let mut str_idx = 0;
        let values: Vec<ColumnValue<'_>> = raw_values
            .iter()
            .map(|(col_type, int_val, float_val)| match col_type {
                ColumnType::Timestamp => ColumnValue::Timestamp(int_val.unwrap_or(0)),
                ColumnType::Float64 => ColumnValue::Float64(float_val.unwrap_or(0.0)),
                ColumnType::Int64 => ColumnValue::Int64(int_val.unwrap_or(0)),
                ColumnType::Symbol => {
                    let s = &string_values[str_idx];
                    str_idx += 1;
                    ColumnValue::Symbol(s.as_str())
                }
            })
            .collect();

        let _ = mt.ingest_row(0, &values);
    }
}

/// Infer a columnar schema from a JSON object (first row).
pub(super) fn infer_schema_from_json(row: &serde_json::Value) -> ColumnarSchema {
    let obj = match row.as_object() {
        Some(o) => o,
        None => {
            return ColumnarSchema {
                columns: vec![("value".into(), ColumnType::Float64)],
                timestamp_idx: 0,
                codecs: Vec::new(),
            };
        }
    };

    let mut columns = Vec::new();
    let mut timestamp_idx = 0;

    for (i, (key, val)) in obj.iter().enumerate() {
        let col_type = match val {
            serde_json::Value::Number(n) if n.is_f64() => ColumnType::Float64,
            serde_json::Value::Number(_) => ColumnType::Int64,
            serde_json::Value::Bool(_) => ColumnType::Int64,
            serde_json::Value::String(_) => ColumnType::Symbol,
            _ => ColumnType::Symbol,
        };

        // Detect timestamp column by name.
        let lower = key.to_lowercase();
        if lower == "timestamp" || lower == "ts" || lower == "time" {
            columns.push((key.clone(), ColumnType::Timestamp));
            timestamp_idx = i;
        } else {
            columns.push((key.clone(), col_type));
        }
    }

    if columns.is_empty() {
        columns.push(("value".into(), ColumnType::Float64));
    }

    ColumnarSchema {
        columns,
        timestamp_idx,
        codecs: Vec::new(),
    }
}
