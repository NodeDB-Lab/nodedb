//! Arrow scalar to serde_json::Value conversion.
//!
//! Used by the statement executor to convert DataFusion evaluation results
//! into JSON values for ASSIGN and OUT parameter handling.

use std::sync::Arc;

use datafusion::arrow::array::*;
use datafusion::arrow::datatypes::DataType;

/// Extract a single scalar value from an Arrow array at the given row index
/// and convert to serde_json::Value.
pub fn arrow_scalar_to_json(col: &Arc<dyn Array>, row: usize) -> serde_json::Value {
    if col.is_null(row) {
        return serde_json::Value::Null;
    }

    match col.data_type() {
        DataType::Boolean => {
            let arr = col.as_any().downcast_ref::<BooleanArray>().unwrap();
            serde_json::Value::Bool(arr.value(row))
        }
        DataType::Int8 => {
            serde_json::json!(col.as_any().downcast_ref::<Int8Array>().unwrap().value(row))
        }
        DataType::Int16 => {
            serde_json::json!(
                col.as_any()
                    .downcast_ref::<Int16Array>()
                    .unwrap()
                    .value(row)
            )
        }
        DataType::Int32 => {
            serde_json::json!(
                col.as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .value(row)
            )
        }
        DataType::Int64 => {
            serde_json::json!(
                col.as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .value(row)
            )
        }
        DataType::UInt8 => {
            serde_json::json!(
                col.as_any()
                    .downcast_ref::<UInt8Array>()
                    .unwrap()
                    .value(row)
            )
        }
        DataType::UInt16 => {
            serde_json::json!(
                col.as_any()
                    .downcast_ref::<UInt16Array>()
                    .unwrap()
                    .value(row)
            )
        }
        DataType::UInt32 => {
            serde_json::json!(
                col.as_any()
                    .downcast_ref::<UInt32Array>()
                    .unwrap()
                    .value(row)
            )
        }
        DataType::UInt64 => {
            serde_json::json!(
                col.as_any()
                    .downcast_ref::<UInt64Array>()
                    .unwrap()
                    .value(row)
            )
        }
        DataType::Float32 => {
            serde_json::json!(
                col.as_any()
                    .downcast_ref::<Float32Array>()
                    .unwrap()
                    .value(row)
            )
        }
        DataType::Float64 => {
            serde_json::json!(
                col.as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap()
                    .value(row)
            )
        }
        DataType::Utf8 => {
            let arr = col.as_any().downcast_ref::<StringArray>().unwrap();
            serde_json::Value::String(arr.value(row).to_string())
        }
        DataType::LargeUtf8 => {
            let arr = col.as_any().downcast_ref::<LargeStringArray>().unwrap();
            serde_json::Value::String(arr.value(row).to_string())
        }
        _ => {
            // Fallback: format as string via ScalarValue.
            let scalar = datafusion::common::ScalarValue::try_from_array(col, row);
            match scalar {
                Ok(s) => serde_json::Value::String(s.to_string()),
                Err(_) => serde_json::Value::Null,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn null_value() {
        let arr: Arc<dyn Array> = Arc::new(Int32Array::from(vec![None]));
        assert_eq!(arrow_scalar_to_json(&arr, 0), serde_json::Value::Null);
    }

    #[test]
    fn int32_value() {
        let arr: Arc<dyn Array> = Arc::new(Int32Array::from(vec![42]));
        assert_eq!(arrow_scalar_to_json(&arr, 0), serde_json::json!(42));
    }

    #[test]
    fn float64_value() {
        let arr: Arc<dyn Array> = Arc::new(Float64Array::from(vec![1.5]));
        assert_eq!(arrow_scalar_to_json(&arr, 0), serde_json::json!(1.5));
    }

    #[test]
    fn string_value() {
        let arr: Arc<dyn Array> = Arc::new(StringArray::from(vec!["hello"]));
        assert_eq!(
            arrow_scalar_to_json(&arr, 0),
            serde_json::Value::String("hello".into())
        );
    }

    #[test]
    fn boolean_value() {
        let arr: Arc<dyn Array> = Arc::new(BooleanArray::from(vec![true]));
        assert_eq!(arrow_scalar_to_json(&arr, 0), serde_json::Value::Bool(true));
    }
}
