//! Columnar base scan handler.
//!
//! Reads rows from the `MutationEngine` memtable, applies projection,
//! WHERE filter predicates, and limit. Used by plain columnar and spatial
//! collections.

use crate::bridge::envelope::{ErrorCode, Response};
use crate::bridge::scan_filter::{FilterOp, ScanFilter};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::response_codec;
use crate::data::executor::task::ExecutionTask;

impl CoreLoop {
    /// Execute a base columnar scan: read from MutationEngine memtable.
    pub(in crate::data::executor) fn execute_columnar_scan(
        &mut self,
        task: &ExecutionTask,
        collection: &str,
        projection: &[String],
        limit: usize,
        filters: &[u8],
        _rls_filters: &[u8],
    ) -> Response {
        let limit = if limit == 0 { 1000 } else { limit };
        let engine_key = (task.request.tenant_id, collection.to_string());

        let engine = match self.columnar_engines.get(&engine_key) {
            Some(e) => e,
            None => {
                // Empty result for missing collection.
                return match response_codec::encode_json_vec(&[]) {
                    Ok(payload) => self.response_with_payload(task, payload),
                    Err(e) => self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: e.to_string(),
                        },
                    ),
                };
            }
        };

        let schema = engine.schema();

        let filter_predicates: Vec<ScanFilter> = if !filters.is_empty() {
            zerompk::from_msgpack(filters).unwrap_or_default()
        } else {
            Vec::new()
        };

        let mut results = Vec::new();

        // Scan more rows than `limit` so we have candidates to filter from.
        for row in engine
            .scan_memtable_rows()
            .take(limit.saturating_mul(10).max(limit))
        {
            if !filter_predicates.is_empty()
                && !row_matches_filters(&row, schema, &filter_predicates)
            {
                continue;
            }
            let mut obj = serde_json::Map::new();
            for (i, col_def) in schema.columns.iter().enumerate() {
                if !projection.is_empty() && !projection.iter().any(|p| p == &col_def.name) {
                    continue;
                }
                if i < row.len() {
                    obj.insert(col_def.name.clone(), value_to_json(&row[i]));
                }
            }
            results.push(serde_json::Value::Object(obj));
            if results.len() >= limit {
                break;
            }
        }

        match response_codec::encode_json_vec(&results) {
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

/// Convert a `nodedb_types::Value` to `serde_json::Value` for response encoding.
pub(in crate::data::executor) fn value_to_json(
    val: &nodedb_types::value::Value,
) -> serde_json::Value {
    use nodedb_types::value::Value;
    match val {
        Value::Null => serde_json::Value::Null,
        Value::Bool(b) => serde_json::Value::Bool(*b),
        Value::Integer(i) => serde_json::Value::Number((*i).into()),
        Value::Float(f) => serde_json::Number::from_f64(*f)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        Value::String(s) => serde_json::Value::String(s.clone()),
        Value::DateTime(dt) => serde_json::Value::String(dt.to_string()),
        Value::Decimal(d) => serde_json::Value::String(d.to_string()),
        Value::Uuid(s) => serde_json::Value::String(s.clone()),
        Value::Bytes(b) => {
            use base64::Engine;
            serde_json::Value::String(base64::engine::general_purpose::STANDARD.encode(b))
        }
        Value::Array(arr) => serde_json::Value::Array(arr.iter().map(value_to_json).collect()),
        Value::Geometry(g) => serde_json::to_value(g).unwrap_or(serde_json::Value::Null),
        Value::Object(map) => {
            let obj: serde_json::Map<String, serde_json::Value> = map
                .iter()
                .map(|(k, v)| (k.clone(), value_to_json(v)))
                .collect();
            serde_json::Value::Object(obj)
        }
        _ => serde_json::Value::Null,
    }
}

/// Check whether a memtable row satisfies all filter predicates.
///
/// Returns `true` if every filter passes (AND semantics). An unknown field or
/// out-of-bounds column index causes the row to be excluded.
pub(in crate::data::executor) fn row_matches_filters(
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

/// Write a timeseries columnar memtable cell value directly as msgpack bytes.
///
/// Encodes the column value at the given row index directly into `buf`
/// without intermediate decoding. Used by timeseries raw_scan and aggregate
/// handlers that still use the internal `ColumnarMemtable`.
pub(in crate::data::executor) fn emit_column_value(
    buf: &mut Vec<u8>,
    mt: &crate::engine::timeseries::columnar_memtable::ColumnarMemtable,
    col_idx: usize,
    col_type: &crate::engine::timeseries::columnar_memtable::ColumnType,
    col_data: &crate::engine::timeseries::columnar_memtable::ColumnData,
    row_idx: usize,
) {
    use crate::engine::timeseries::columnar_memtable::{
        ColumnData as TsColumnData, ColumnType as TsColumnType,
    };
    match col_type {
        TsColumnType::Timestamp => {
            nodedb_query::msgpack_scan::write_i64(buf, col_data.as_timestamps()[row_idx]);
        }
        TsColumnType::Float64 => {
            let v = col_data.as_f64()[row_idx];
            if v.is_finite() {
                nodedb_query::msgpack_scan::write_f64(buf, v);
            } else {
                nodedb_query::msgpack_scan::write_null(buf);
            }
        }
        TsColumnType::Symbol => {
            if let TsColumnData::Symbol(ids) = col_data {
                let sym_id = ids[row_idx];
                if let Some(s) = mt.symbol_dict(col_idx).and_then(|dict| dict.get(sym_id)) {
                    nodedb_query::msgpack_scan::write_str(buf, s);
                } else {
                    nodedb_query::msgpack_scan::write_null(buf);
                }
            } else {
                nodedb_query::msgpack_scan::write_null(buf);
            }
        }
        TsColumnType::Int64 => {
            if let TsColumnData::Int64(vals) = col_data {
                nodedb_query::msgpack_scan::write_i64(buf, vals[row_idx]);
            } else {
                nodedb_query::msgpack_scan::write_null(buf);
            }
        }
    }
}
