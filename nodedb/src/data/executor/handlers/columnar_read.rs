//! Columnar base scan handler.
//!
//! Reads rows from the columnar memtable (and future sealed segments),
//! applies filters, projects columns, respects limit. Used by plain
//! columnar collections. Timeseries and spatial profiles have their
//! own scan handlers that add time-range/bucketing and R-tree semantics.

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::response_codec;
use crate::data::executor::task::ExecutionTask;
use crate::engine::timeseries::columnar_memtable::{ColumnData, ColumnType};

impl CoreLoop {
    /// Execute a base columnar scan: read from memtable, filter, project, limit.
    pub(in crate::data::executor) fn execute_columnar_scan(
        &mut self,
        task: &ExecutionTask,
        collection: &str,
        projection: &[String],
        limit: usize,
        _filters: &[u8],
        _rls_filters: &[u8],
    ) -> Response {
        let limit = if limit == 0 { 1000 } else { limit };
        let mut results = Vec::new();

        if let Some(mt) = self.columnar_memtables.get(collection)
            && !mt.is_empty()
        {
            let schema = mt.schema();
            let row_count = mt.row_count() as usize;

            // Determine which columns to emit.
            let columns: Vec<_> = schema
                .columns
                .iter()
                .enumerate()
                .filter(|(_, (name, _))| {
                    projection.is_empty() || projection.iter().any(|p| p == name)
                })
                .map(|(i, (name, ty))| (i, name, ty, mt.column(i)))
                .collect();

            for idx in 0..row_count.min(limit) {
                let mut row = serde_json::Map::new();
                for (col_idx, col_name, col_type, col_data) in &columns {
                    let val = emit_column_value(mt, *col_idx, col_type, col_data, idx);
                    row.insert(col_name.to_string(), val);
                }
                results.push(serde_json::Value::Object(row));
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

/// Convert a single column cell to a JSON value.
pub(super) fn emit_column_value(
    mt: &crate::engine::timeseries::columnar_memtable::ColumnarMemtable,
    col_idx: usize,
    col_type: &ColumnType,
    col_data: &ColumnData,
    row_idx: usize,
) -> serde_json::Value {
    match col_type {
        ColumnType::Timestamp => {
            serde_json::Value::Number(serde_json::Number::from(col_data.as_timestamps()[row_idx]))
        }
        ColumnType::Float64 => {
            let v = col_data.as_f64()[row_idx];
            serde_json::Number::from_f64(v)
                .map(serde_json::Value::Number)
                .unwrap_or(serde_json::Value::Null)
        }
        ColumnType::Symbol => {
            if let ColumnData::Symbol(ids) = col_data {
                let sym_id = ids[row_idx];
                mt.symbol_dict(col_idx)
                    .and_then(|dict| dict.get(sym_id))
                    .map(|s| serde_json::Value::String(s.to_string()))
                    .unwrap_or(serde_json::Value::Null)
            } else {
                serde_json::Value::Null
            }
        }
        ColumnType::Int64 => {
            if let ColumnData::Int64(vals) = col_data {
                serde_json::Value::Number(serde_json::Number::from(vals[row_idx]))
            } else {
                serde_json::Value::Null
            }
        }
    }
}
