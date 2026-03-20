//! Arrow conversion utilities for the Control Plane.
//!
//! Converts MessagePack-encoded Data Plane response payloads into Arrow
//! RecordBatches for DataFusion vectorized processing. This enables:
//!
//! - Window function computation on result sets
//! - SIMD-accelerated secondary aggregation
//! - Columnar projection without re-scanning
//!
//! The conversion happens on the Control Plane AFTER results cross the
//! SPSC bridge. The Data Plane continues to produce MessagePack payloads
//! (which are 2-3x faster to serialize than Arrow IPC).

use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, Float64Array, Int64Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;

/// Convert a JSON array of document rows into an Arrow RecordBatch.
///
/// Each row is expected to be `{"id": "...", "data": {"field": value, ...}}`.
/// The schema is inferred from the first row's data fields.
///
/// Returns `None` if the input is empty or not a JSON array.
pub fn json_rows_to_record_batch(json_str: &str) -> Option<RecordBatch> {
    let rows: Vec<serde_json::Value> = serde_json::from_str(json_str).ok()?;
    if rows.is_empty() {
        return None;
    }

    // Infer schema from first row.
    let first_data = rows[0].get("data")?;
    let obj = first_data.as_object()?;

    let mut fields = Vec::new();
    fields.push(Field::new("id", DataType::Utf8, false));
    for (key, value) in obj {
        let dt = infer_arrow_type(value);
        fields.push(Field::new(key, dt, true));
    }
    let schema = Arc::new(Schema::new(fields));

    // Build column arrays.
    let mut id_builder: Vec<String> = Vec::with_capacity(rows.len());
    let mut column_builders: Vec<ColumnBuilder> =
        obj.keys().map(|_| ColumnBuilder::new(rows.len())).collect();

    for row in &rows {
        let id = row
            .get("id")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        id_builder.push(id);

        if let Some(data) = row.get("data").and_then(|d| d.as_object()) {
            for (i, key) in obj.keys().enumerate() {
                if let Some(val) = data.get(key) {
                    column_builders[i].push(val);
                } else {
                    column_builders[i].push_null();
                }
            }
        } else {
            for builder in &mut column_builders {
                builder.push_null();
            }
        }
    }

    // Build arrays.
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(1 + column_builders.len());
    arrays.push(Arc::new(StringArray::from(id_builder)) as ArrayRef);

    for builder in column_builders {
        arrays.push(builder.finish());
    }

    RecordBatch::try_new(schema, arrays).ok()
}

/// Infer Arrow data type from a JSON value.
fn infer_arrow_type(value: &serde_json::Value) -> DataType {
    match value {
        serde_json::Value::Number(n) => {
            if n.is_i64() {
                DataType::Int64
            } else {
                DataType::Float64
            }
        }
        serde_json::Value::String(_) => DataType::Utf8,
        serde_json::Value::Bool(_) => DataType::Boolean,
        _ => DataType::Utf8, // Fall back to string representation.
    }
}

/// Dynamic column builder that accumulates values and produces an Arrow array.
enum ColumnBuilder {
    Strings(Vec<Option<String>>),
    Ints(Vec<Option<i64>>),
    Floats(Vec<Option<f64>>),
}

impl ColumnBuilder {
    fn new(capacity: usize) -> Self {
        // Start as strings; will be determined by first value.
        Self::Strings(Vec::with_capacity(capacity))
    }

    fn push(&mut self, value: &serde_json::Value) {
        match self {
            Self::Strings(v) => {
                if let Some(s) = value.as_str() {
                    v.push(Some(s.to_string()));
                } else if value.is_number() {
                    // Promote to numeric type.
                    let len = v.len();
                    if value.is_i64() {
                        let mut ints: Vec<Option<i64>> = v.iter().map(|_| None).collect();
                        ints.push(value.as_i64());
                        *self = Self::Ints(ints);
                    } else {
                        let mut floats: Vec<Option<f64>> = vec![None; len];
                        floats.push(value.as_f64());
                        *self = Self::Floats(floats);
                    }
                } else {
                    v.push(Some(value.to_string()));
                }
            }
            Self::Ints(v) => v.push(value.as_i64()),
            Self::Floats(v) => v.push(value.as_f64()),
        }
    }

    fn push_null(&mut self) {
        match self {
            Self::Strings(v) => v.push(None),
            Self::Ints(v) => v.push(None),
            Self::Floats(v) => v.push(None),
        }
    }

    fn finish(self) -> ArrayRef {
        match self {
            Self::Strings(v) => Arc::new(StringArray::from(v)) as ArrayRef,
            Self::Ints(v) => Arc::new(Int64Array::from(v)) as ArrayRef,
            Self::Floats(v) => Arc::new(Float64Array::from(v)) as ArrayRef,
        }
    }
}

/// Get the schema of an Arrow RecordBatch as a reference.
pub fn batch_schema(batch: &RecordBatch) -> SchemaRef {
    batch.schema()
}

/// Compute SUM of a numeric column in a RecordBatch.
pub fn arrow_sum(batch: &RecordBatch, column_name: &str) -> Option<f64> {
    let idx = batch.schema().index_of(column_name).ok()?;
    let array = batch.column(idx);

    if let Some(f64_arr) = array.as_any().downcast_ref::<Float64Array>() {
        Some(datafusion::arrow::compute::kernels::aggregate::sum(f64_arr).unwrap_or(0.0))
    } else if let Some(i64_arr) = array.as_any().downcast_ref::<Int64Array>() {
        let sum = datafusion::arrow::compute::kernels::aggregate::sum(i64_arr).unwrap_or(0);
        Some(sum as f64)
    } else {
        None
    }
}

/// Compute MIN of a numeric column using Arrow SIMD kernels.
pub fn arrow_min(batch: &RecordBatch, column_name: &str) -> Option<f64> {
    let idx = batch.schema().index_of(column_name).ok()?;
    let array = batch.column(idx);

    if let Some(f64_arr) = array.as_any().downcast_ref::<Float64Array>() {
        datafusion::arrow::compute::kernels::aggregate::min(f64_arr)
    } else if let Some(i64_arr) = array.as_any().downcast_ref::<Int64Array>() {
        datafusion::arrow::compute::kernels::aggregate::min(i64_arr).map(|v| v as f64)
    } else {
        None
    }
}

/// Compute MAX of a numeric column using Arrow SIMD kernels.
pub fn arrow_max(batch: &RecordBatch, column_name: &str) -> Option<f64> {
    let idx = batch.schema().index_of(column_name).ok()?;
    let array = batch.column(idx);

    if let Some(f64_arr) = array.as_any().downcast_ref::<Float64Array>() {
        datafusion::arrow::compute::kernels::aggregate::max(f64_arr)
    } else if let Some(i64_arr) = array.as_any().downcast_ref::<Int64Array>() {
        datafusion::arrow::compute::kernels::aggregate::max(i64_arr).map(|v| v as f64)
    } else {
        None
    }
}

/// Compute COUNT of non-null values in a column.
pub fn arrow_count(batch: &RecordBatch, column_name: &str) -> Option<usize> {
    let idx = batch.schema().index_of(column_name).ok()?;
    let array = batch.column(idx);
    Some(array.len() - array.null_count())
}

/// Compute AVG of a numeric column (SUM / COUNT).
pub fn arrow_avg(batch: &RecordBatch, column_name: &str) -> Option<f64> {
    let sum = arrow_sum(batch, column_name)?;
    let count = arrow_count(batch, column_name)?;
    if count == 0 {
        None
    } else {
        Some(sum / count as f64)
    }
}

/// Decode Arrow IPC stream bytes back into a RecordBatch.
///
/// Used by the Control Plane to receive Arrow data from the Data Plane
/// across the SPSC bridge.
pub fn decode_arrow_ipc(bytes: &[u8]) -> Option<RecordBatch> {
    use datafusion::arrow::ipc::reader::StreamReader;
    let cursor = std::io::Cursor::new(bytes);
    let mut reader = StreamReader::try_new(cursor, None).ok()?;
    reader.next()?.ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn json_to_arrow_roundtrip() {
        let json = r#"[
            {"id": "d1", "data": {"name": "alice", "age": 30}},
            {"id": "d2", "data": {"name": "bob", "age": 25}}
        ]"#;

        let batch = json_rows_to_record_batch(json).unwrap();
        assert_eq!(batch.num_rows(), 2);
        assert!(batch.schema().field_with_name("id").is_ok());
        assert!(batch.schema().field_with_name("name").is_ok());
        assert!(batch.schema().field_with_name("age").is_ok());
    }

    #[test]
    fn arrow_sum_works() {
        let json = r#"[
            {"id": "d1", "data": {"price": 10.5}},
            {"id": "d2", "data": {"price": 20.0}},
            {"id": "d3", "data": {"price": 30.0}}
        ]"#;

        let batch = json_rows_to_record_batch(json).unwrap();
        let total = arrow_sum(&batch, "price").unwrap();
        assert!((total - 60.5).abs() < 0.01);
    }

    #[test]
    fn empty_input() {
        assert!(json_rows_to_record_batch("[]").is_none());
        assert!(json_rows_to_record_batch("").is_none());
    }
}
