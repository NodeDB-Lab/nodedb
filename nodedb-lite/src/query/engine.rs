//! Lite query engine: full SQL via DataFusion over Loro documents.
//!
//! Manages a DataFusion `SessionContext` with collections registered
//! as table providers backed by the CRDT engine.

use std::sync::{Arc, Mutex};

use datafusion::arrow::array::Array;
use datafusion::arrow::datatypes::DataType;
use datafusion::execution::context::SessionContext;
use datafusion::prelude::*;

use nodedb_types::result::QueryResult;
use nodedb_types::value::Value;

use crate::engine::crdt::CrdtEngine;
use crate::error::LiteError;

use super::table_provider::LiteTableProvider;

/// Lite-side query engine wrapping DataFusion.
///
/// Registered collections appear as tables. SQL queries execute
/// entirely in-process against the Loro CRDT state.
pub struct LiteQueryEngine {
    ctx: SessionContext,
    crdt: Arc<Mutex<CrdtEngine>>,
}

impl LiteQueryEngine {
    /// Create a new query engine.
    pub fn new(crdt: Arc<Mutex<CrdtEngine>>) -> Self {
        let config = SessionConfig::new()
            .with_information_schema(false)
            .with_default_catalog_and_schema("nodedb", "public");

        let ctx = SessionContext::new_with_config(config);
        super::spatial_udf::register_spatial_udfs(&ctx);
        Self { ctx, crdt }
    }

    /// Register a collection as a queryable table.
    ///
    /// Call this before executing SQL that references the collection.
    /// For auto-registration, call `register_all_collections()`.
    pub fn register_collection(&self, name: &str) {
        let provider = LiteTableProvider::new(name.to_string(), Arc::clone(&self.crdt));
        // Register directly via the session context.
        let _ = self.ctx.register_table(name, Arc::new(provider));
    }

    /// Register all existing collections as tables.
    pub fn register_all_collections(&self) {
        let crdt = match self.crdt.lock() {
            Ok(c) => c,
            Err(p) => p.into_inner(),
        };

        // Loro's top-level map keys are collection names.
        // We iterate them and register each as a table.
        // CrdtEngine doesn't expose collection listing directly,
        // but we can get it from the state.
        let collections = crdt.collection_names();
        drop(crdt);

        for name in &collections {
            // Skip internal collections.
            if name.starts_with("__") {
                continue;
            }
            self.register_collection(name);
        }
    }

    /// Execute a SQL query and return results.
    pub async fn execute_sql(&self, sql: &str) -> Result<QueryResult, LiteError> {
        // Auto-register collections mentioned in the query.
        // Simple heuristic: register all collections before each query.
        // More sophisticated: parse FROM clause. For now, register all.
        self.register_all_collections();

        let df = self
            .ctx
            .sql(sql)
            .await
            .map_err(|e| LiteError::Query(format!("SQL parse/plan: {e}")))?;

        let batches = df
            .collect()
            .await
            .map_err(|e| LiteError::Query(format!("SQL execute: {e}")))?;

        // Convert Arrow RecordBatches to QueryResult.
        let mut columns: Vec<String> = Vec::new();
        let mut rows: Vec<Vec<Value>> = Vec::new();

        for batch in &batches {
            if columns.is_empty() {
                columns = batch
                    .schema()
                    .fields()
                    .iter()
                    .map(|f| f.name().clone())
                    .collect();
            }

            let num_rows = batch.num_rows();
            for row_idx in 0..num_rows {
                let mut row = Vec::with_capacity(columns.len());
                for col_idx in 0..batch.num_columns() {
                    let col = batch.column(col_idx);
                    let value = arrow_value_at(col, row_idx)?;
                    row.push(value);
                }
                rows.push(row);
            }
        }

        Ok(QueryResult {
            columns,
            rows,
            rows_affected: 0,
        })
    }
}

/// Extract a single value from an Arrow array at the given row index.
///
/// Returns `Err` if the Arrow array type doesn't match the expected downcast.
fn arrow_value_at(col: &dyn Array, row: usize) -> Result<Value, crate::error::LiteError> {
    use datafusion::arrow::array::*;

    if col.is_null(row) {
        return Ok(Value::Null);
    }

    /// Downcast helper that returns a proper error instead of panicking.
    macro_rules! downcast {
        ($col:expr, $arr_type:ty, $type_name:expr) => {
            $col.as_any().downcast_ref::<$arr_type>().ok_or_else(|| {
                crate::error::LiteError::ArrowTypeConversion {
                    expected: $type_name.into(),
                    got: format!("{:?}", $col.data_type()),
                }
            })?
        };
    }

    match col.data_type() {
        DataType::Utf8 => Ok(Value::String(
            downcast!(col, StringArray, "StringArray")
                .value(row)
                .to_string(),
        )),
        DataType::LargeUtf8 => Ok(Value::String(
            downcast!(col, LargeStringArray, "LargeStringArray")
                .value(row)
                .to_string(),
        )),
        DataType::Int8 => Ok(Value::Integer(
            downcast!(col, Int8Array, "Int8Array").value(row) as i64,
        )),
        DataType::Int16 => Ok(Value::Integer(
            downcast!(col, Int16Array, "Int16Array").value(row) as i64,
        )),
        DataType::Int32 => Ok(Value::Integer(
            downcast!(col, Int32Array, "Int32Array").value(row) as i64,
        )),
        DataType::Int64 => Ok(Value::Integer(
            downcast!(col, Int64Array, "Int64Array").value(row),
        )),
        DataType::UInt8 => Ok(Value::Integer(
            downcast!(col, UInt8Array, "UInt8Array").value(row) as i64,
        )),
        DataType::UInt16 => Ok(Value::Integer(
            downcast!(col, UInt16Array, "UInt16Array").value(row) as i64,
        )),
        DataType::UInt32 => Ok(Value::Integer(
            downcast!(col, UInt32Array, "UInt32Array").value(row) as i64,
        )),
        DataType::UInt64 => Ok(Value::Integer(
            downcast!(col, UInt64Array, "UInt64Array").value(row) as i64,
        )),
        DataType::Float32 => Ok(Value::Float(
            downcast!(col, Float32Array, "Float32Array").value(row) as f64,
        )),
        DataType::Float64 => Ok(Value::Float(
            downcast!(col, Float64Array, "Float64Array").value(row),
        )),
        DataType::Boolean => Ok(Value::Bool(
            downcast!(col, BooleanArray, "BooleanArray").value(row),
        )),
        _ => {
            let formatter = datafusion::arrow::util::display::ArrayFormatter::try_new(
                col,
                &datafusion::arrow::util::display::FormatOptions::default(),
            );
            match formatter {
                Ok(fmt) => Ok(Value::String(fmt.value(row).to_string())),
                Err(_) => Ok(Value::Null),
            }
        }
    }
}
