// SPDX-License-Identifier: BUSL-1.1

//! Value coercion and JSON conversion for strict document columns.

use nodedb_types::columnar::ColumnType;
use nodedb_types::value::Value;

/// Coerce a `nodedb_types::Value` to match a column's declared type.
pub fn coerce_value(val: &Value, col_type: &ColumnType, col_name: &str) -> crate::Result<Value> {
    match col_type {
        ColumnType::Bool => match val {
            Value::Bool(_) => Ok(val.clone()),
            Value::Integer(n) => Ok(Value::Bool(*n != 0)),
            Value::String(s) => match s.to_lowercase().as_str() {
                "true" | "1" | "yes" => Ok(Value::Bool(true)),
                "false" | "0" | "no" => Ok(Value::Bool(false)),
                _ => Err(crate::Error::BadRequest {
                    detail: format!("column '{col_name}': cannot coerce '{s}' to BOOL"),
                }),
            },
            _ => Err(crate::Error::BadRequest {
                detail: format!("column '{col_name}': expected BOOL, got {val:?}"),
            }),
        },
        ColumnType::Int64 => match val {
            Value::Integer(_) => Ok(val.clone()),
            Value::Float(f) => Ok(Value::Integer(*f as i64)),
            Value::String(s) => {
                s.parse::<i64>()
                    .map(Value::Integer)
                    .map_err(|_| crate::Error::BadRequest {
                        detail: format!("column '{col_name}': cannot parse '{s}' as INT"),
                    })
            }
            _ => Err(crate::Error::BadRequest {
                detail: format!("column '{col_name}': expected INT, got {val:?}"),
            }),
        },
        ColumnType::Float64 => match val {
            Value::Float(_) => Ok(val.clone()),
            Value::Integer(n) => Ok(Value::Float(*n as f64)),
            Value::String(s) => {
                s.parse::<f64>()
                    .map(Value::Float)
                    .map_err(|_| crate::Error::BadRequest {
                        detail: format!("column '{col_name}': cannot parse '{s}' as FLOAT"),
                    })
            }
            _ => Err(crate::Error::BadRequest {
                detail: format!("column '{col_name}': expected FLOAT, got {val:?}"),
            }),
        },
        ColumnType::String | ColumnType::Uuid | ColumnType::Ulid | ColumnType::Regex => match val {
            Value::String(_) | Value::Uuid(_) | Value::Ulid(_) | Value::Regex(_) => Ok(val.clone()),
            Value::Integer(n) => Ok(Value::String(n.to_string())),
            Value::Float(f) => Ok(Value::String(f.to_string())),
            Value::Bool(b) => Ok(Value::String(b.to_string())),
            other => Ok(Value::String(format!("{other:?}"))),
        },
        ColumnType::Bytes => match val {
            Value::Bytes(_) => Ok(val.clone()),
            Value::String(s) => {
                let bytes = base64::Engine::decode(&base64::engine::general_purpose::STANDARD, s)
                    .unwrap_or_else(|_| s.as_bytes().to_vec());
                Ok(Value::Bytes(bytes))
            }
            _ => Err(crate::Error::BadRequest {
                detail: format!("column '{col_name}': expected BYTES, got {val:?}"),
            }),
        },
        ColumnType::Timestamp => match val {
            Value::NaiveDateTime(_) => Ok(val.clone()),
            Value::DateTime(dt) => Ok(Value::NaiveDateTime(*dt)),
            Value::Integer(ms) => Ok(Value::NaiveDateTime(
                nodedb_types::NdbDateTime::from_millis(*ms).map_err(|e| {
                    crate::Error::BadRequest {
                        detail: format!("column '{col_name}': {e}"),
                    }
                })?,
            )),
            Value::Float(f) => Ok(Value::NaiveDateTime(
                nodedb_types::NdbDateTime::from_millis(*f as i64).map_err(|e| {
                    crate::Error::BadRequest {
                        detail: format!("column '{col_name}': {e}"),
                    }
                })?,
            )),
            Value::String(s) => {
                if let Ok(ms) = s.parse::<i64>() {
                    Ok(Value::NaiveDateTime(
                        nodedb_types::NdbDateTime::from_millis(ms).map_err(|e| {
                            crate::Error::BadRequest {
                                detail: format!("column '{col_name}': {e}"),
                            }
                        })?,
                    ))
                } else {
                    Ok(Value::String(s.clone()))
                }
            }
            _ => Err(crate::Error::BadRequest {
                detail: format!("column '{col_name}': expected TIMESTAMP, got {val:?}"),
            }),
        },
        ColumnType::Timestamptz => match val {
            Value::DateTime(_) => Ok(val.clone()),
            Value::NaiveDateTime(dt) => Ok(Value::DateTime(*dt)),
            Value::Integer(ms) => Ok(Value::DateTime(
                nodedb_types::NdbDateTime::from_millis(*ms).map_err(|e| {
                    crate::Error::BadRequest {
                        detail: format!("column '{col_name}': {e}"),
                    }
                })?,
            )),
            Value::Float(f) => Ok(Value::DateTime(
                nodedb_types::NdbDateTime::from_millis(*f as i64).map_err(|e| {
                    crate::Error::BadRequest {
                        detail: format!("column '{col_name}': {e}"),
                    }
                })?,
            )),
            Value::String(s) => {
                if let Ok(ms) = s.parse::<i64>() {
                    Ok(Value::DateTime(
                        nodedb_types::NdbDateTime::from_millis(ms).map_err(|e| {
                            crate::Error::BadRequest {
                                detail: format!("column '{col_name}': {e}"),
                            }
                        })?,
                    ))
                } else {
                    Ok(Value::String(s.clone()))
                }
            }
            _ => Err(crate::Error::BadRequest {
                detail: format!("column '{col_name}': expected TIMESTAMPTZ, got {val:?}"),
            }),
        },
        ColumnType::SystemTimestamp => {
            // Engine-assigned; user-supplied values must not reach coercion.
            let _ = val;
            Err(crate::Error::BadRequest {
                detail: format!(
                    "column '{col_name}': SYSTEM_TIMESTAMP is engine-assigned, not user-supplied"
                ),
            })
        }
        ColumnType::Decimal { .. } => match val {
            Value::Decimal(_) => Ok(val.clone()),
            Value::Float(f) => rust_decimal::Decimal::try_from(*f)
                .map(Value::Decimal)
                .map_err(|_| crate::Error::BadRequest {
                    detail: format!("column '{col_name}': cannot convert {f} to DECIMAL"),
                }),
            Value::Integer(n) => Ok(Value::Decimal(rust_decimal::Decimal::from(*n))),
            Value::String(s) => s
                .parse::<rust_decimal::Decimal>()
                .map(Value::Decimal)
                .map_err(|_| crate::Error::BadRequest {
                    detail: format!("column '{col_name}': cannot parse '{s}' as DECIMAL"),
                }),
            _ => Err(crate::Error::BadRequest {
                detail: format!("column '{col_name}': expected DECIMAL, got {val:?}"),
            }),
        },
        ColumnType::Vector(dim) => match val {
            Value::Bytes(b) if b.len() == *dim as usize * 4 => Ok(val.clone()),
            Value::Array(arr) => {
                let floats = extract_vector_floats(col_name, arr)?;
                validate_and_encode_vector(col_name, *dim, &floats)
            }
            Value::String(s) => {
                // UPDATE path may serialize ARRAY literal as string — parse it.
                match crate::data::executor::vector_string::parse_vector_string(s) {
                    Some(floats) => validate_and_encode_vector(col_name, *dim, &floats),
                    None => Err(crate::Error::BadRequest {
                        detail: format!(
                            "column '{col_name}': expected VECTOR array, got String({s:?})"
                        ),
                    }),
                }
            }
            _ => Err(crate::Error::BadRequest {
                detail: format!("column '{col_name}': expected VECTOR array, got {val:?}"),
            }),
        },
        ColumnType::SparseVector => {
            // Variable-length string-backed: the `'{id: weight}'` literal (or a
            // raw byte form) passes through unmodified and is parsed at
            // index-build time. Schema validation catches genuine mismatches.
            Ok(val.clone())
        }
        ColumnType::Geometry => Ok(Value::String(format!("{val:?}"))),
        ColumnType::Duration => match val {
            Value::Duration(_) => Ok(val.clone()),
            Value::Integer(n) => Ok(Value::Integer(*n)),
            Value::String(s) => {
                s.parse::<i64>()
                    .map(Value::Integer)
                    .map_err(|_| crate::Error::BadRequest {
                        detail: format!("column '{col_name}': cannot parse '{s}' as DURATION"),
                    })
            }
            _ => Err(crate::Error::BadRequest {
                detail: format!("column '{col_name}': expected DURATION, got {val:?}"),
            }),
        },
        ColumnType::Json
        | ColumnType::Array
        | ColumnType::Set
        | ColumnType::Range
        | ColumnType::Record => {
            // Variable-length inline MessagePack column: val is raw bytes — deserialize to Value.
            if let Value::Bytes(b) = val {
                Ok(match nodedb_types::value_from_msgpack(b) {
                    Ok(v) => v,
                    Err(e) => {
                        tracing::warn!(len = b.len(), error = %e, "corrupted msgpack in strict column");
                        Value::Null
                    }
                })
            } else {
                Ok(val.clone())
            }
        }
        // ColumnType is #[non_exhaustive]; unknown future types pass the value
        // through unmodified — the schema validator will catch type mismatches.
        _ => Ok(val.clone()),
    }
}

/// Extract `f32` floats from a `Value::Array` for a VECTOR column.
///
/// Accepts the element shapes the planner actually produces for an
/// `ARRAY[...]` literal — `Float`, `Integer`, `Decimal` (a folded numeric
/// literal arrives as `Decimal`), and numeric `String` — matching the
/// schemaless point-put path (`vector_string::floats_from_value`), so one
/// literal behaves the same on every engine.
///
/// A non-numeric element is rejected by index and type. A `filter_map` here
/// used to drop it silently, which reported the literal as having fewer
/// elements than the user wrote and surfaced the mismatch as an internal
/// error.
fn extract_vector_floats(col_name: &str, arr: &[Value]) -> crate::Result<Vec<f32>> {
    let mut floats = Vec::with_capacity(arr.len());
    for (i, v) in arr.iter().enumerate() {
        let f = match v {
            Value::Float(f) => *f as f32,
            Value::Integer(n) => *n as f32,
            Value::Decimal(d) => {
                use rust_decimal::prelude::ToPrimitive;
                d.to_f32().ok_or_else(|| crate::Error::BadRequest {
                    detail: format!(
                        "column '{col_name}': VECTOR element {i}: cannot represent DECIMAL {d} as FLOAT"
                    ),
                })?
            }
            Value::String(s) => s.parse::<f32>().map_err(|_| crate::Error::BadRequest {
                detail: format!(
                    "column '{col_name}': VECTOR element {i}: expected a numeric element, got String({s:?})"
                ),
            })?,
            other => {
                return Err(crate::Error::BadRequest {
                    detail: format!(
                        "column '{col_name}': VECTOR element {i}: expected a numeric element, got {}",
                        other.type_name()
                    ),
                });
            }
        };
        if !f.is_finite() {
            return Err(crate::Error::BadRequest {
                detail: format!(
                    "column '{col_name}': VECTOR element {i}: expected a finite element, got {f}"
                ),
            });
        }
        floats.push(f);
    }
    Ok(floats)
}

/// Validate dimension count and encode as little-endian bytes.
fn validate_and_encode_vector(col_name: &str, dim: u32, floats: &[f32]) -> crate::Result<Value> {
    if floats.len() != dim as usize {
        return Err(crate::Error::BadRequest {
            detail: format!(
                "column '{col_name}': expected VECTOR({dim}), got {} elements",
                floats.len()
            ),
        });
    }
    let bytes: Vec<u8> = floats.iter().flat_map(|f| f.to_le_bytes()).collect();
    Ok(Value::Bytes(bytes))
}

/// Convert a typed `Value` to JSON (for pgwire output only).
pub fn value_to_json(val: &Value) -> serde_json::Value {
    match val {
        Value::Null => serde_json::Value::Null,
        Value::Bool(b) => serde_json::Value::Bool(*b),
        Value::Integer(i) => serde_json::json!(*i),
        Value::Float(f) => serde_json::Number::from_f64(*f)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        Value::String(s) | Value::Uuid(s) | Value::Ulid(s) => serde_json::Value::String(s.clone()),
        Value::Bytes(b) => serde_json::Value::String(base64::Engine::encode(
            &base64::engine::general_purpose::STANDARD,
            b,
        )),
        Value::DateTime(dt) | Value::NaiveDateTime(dt) => serde_json::json!(dt.micros / 1000),
        Value::Duration(d) => serde_json::json!(d.as_millis()),
        Value::Decimal(d) => serde_json::Value::String(d.to_string()),
        Value::Array(arr) => serde_json::Value::Array(arr.iter().map(value_to_json).collect()),
        Value::Object(map) => {
            let mut obj = serde_json::Map::new();
            for (k, v) in map {
                obj.insert(k.clone(), value_to_json(v));
            }
            serde_json::Value::Object(obj)
        }
        Value::Geometry(_)
        | Value::Set(_)
        | Value::Regex(_)
        | Value::Range { .. }
        | Value::Record { .. }
        | Value::ArrayCell(_) => serde_json::Value::Null,
        // Value is #[non_exhaustive]; future variants collapse to JSON null
        // at the pgwire output boundary.
        _ => serde_json::Value::Null,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_types::columnar::{ColumnDef, StrictSchema};

    fn test_schema() -> StrictSchema {
        StrictSchema {
            columns: vec![
                ColumnDef::required("id", ColumnType::String).with_primary_key(),
                ColumnDef::required("name", ColumnType::String),
                ColumnDef::nullable("age", ColumnType::Int64),
            ],
            version: 1,
            dropped_columns: Vec::new(),
            bitemporal: false,
        }
    }

    #[test]
    fn roundtrip_via_value() {
        let schema = test_schema();
        let mut map = std::collections::HashMap::new();
        map.insert("id".into(), Value::String("u1".into()));
        map.insert("name".into(), Value::String("Alice".into()));
        map.insert("age".into(), Value::Integer(30));

        let tuple_bytes =
            super::super::encode::value_to_binary_tuple(&Value::Object(map), &schema, "docs")
                .unwrap();
        let decoded = super::super::decode::binary_tuple_to_json(&tuple_bytes, &schema).unwrap();
        assert_eq!(decoded["id"], "u1");
        assert_eq!(decoded["name"], "Alice");
        assert_eq!(decoded["age"], 30);
    }

    #[test]
    fn nullable_field_omitted() {
        let schema = test_schema();
        let mut map = std::collections::HashMap::new();
        map.insert("id".into(), Value::String("u2".into()));
        map.insert("name".into(), Value::String("Bob".into()));

        let tuple_bytes =
            super::super::encode::value_to_binary_tuple(&Value::Object(map), &schema, "docs")
                .unwrap();
        let decoded = super::super::decode::binary_tuple_to_json(&tuple_bytes, &schema).unwrap();
        assert_eq!(decoded["id"], "u2");
        assert!(decoded["age"].is_null());
    }

    #[test]
    fn non_nullable_missing_errors() {
        let schema = test_schema();
        let mut map = std::collections::HashMap::new();
        map.insert("id".into(), Value::String("u3".into()));

        let result =
            super::super::encode::value_to_binary_tuple(&Value::Object(map), &schema, "docs");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("NOT NULL"));
    }

    #[test]
    fn bitemporal_value_roundtrip() {
        let schema = StrictSchema::new_bitemporal(vec![
            ColumnDef::required("id", ColumnType::String).with_primary_key(),
            ColumnDef::required("name", ColumnType::String),
        ])
        .unwrap();
        let mut map = std::collections::HashMap::new();
        map.insert("id".into(), Value::String("u1".into()));
        map.insert("name".into(), Value::String("Alice".into()));

        let tuple = super::super::encode::value_to_binary_tuple_bitemporal(
            &Value::Object(map),
            &schema,
            1_700_000_000_000,
            0,
            i64::MAX,
            "docs",
        )
        .unwrap();

        let decoder = nodedb_strict::TupleDecoder::new(&schema);
        let (sys, vf, vu) = decoder.extract_bitemporal_timestamps(&tuple).unwrap();
        assert_eq!(sys, 1_700_000_000_000);
        assert_eq!(vf, 0);
        assert_eq!(vu, i64::MAX);
        assert_eq!(
            decoder.extract_by_name(&tuple, "id").unwrap(),
            Value::String("u1".into())
        );
    }

    #[test]
    fn bitemporal_encode_rejects_non_bitemporal_schema() {
        let schema = test_schema();
        let map = std::collections::HashMap::new();
        let result = super::super::encode::value_to_binary_tuple_bitemporal(
            &Value::Object(map),
            &schema,
            0,
            0,
            0,
            "docs",
        );
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("not bitemporal"));
    }

    #[test]
    fn unknown_field_errors() {
        let schema = test_schema();
        let mut map = std::collections::HashMap::new();
        map.insert("id".into(), Value::String("u4".into()));
        map.insert("name".into(), Value::String("Eve".into()));
        map.insert("extra".into(), Value::String("boom".into()));

        let result =
            super::super::encode::value_to_binary_tuple(&Value::Object(map), &schema, "docs");
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("\"extra\"") && msg.contains("does not exist"));
    }

    #[test]
    fn vector_accepts_decimal_and_numeric_string_elements() {
        // `ARRAY[0.1, 0.2, 0.3]` folds its numeric literals to `Decimal`
        // elements; numeric strings are accepted like the schemaless path.
        let arr = vec![
            Value::Decimal(rust_decimal::Decimal::new(1, 1)),
            Value::String("0.2".into()),
            Value::Integer(3),
        ];
        let floats = extract_vector_floats("embedding", &arr).unwrap();
        assert_eq!(floats.len(), 3);
        assert!((floats[0] - 0.1).abs() < 1e-6, "{floats:?}");
        assert!((floats[1] - 0.2).abs() < 1e-6, "{floats:?}");
        assert!((floats[2] - 3.0).abs() < 1e-6, "{floats:?}");
    }

    #[test]
    fn vector_element_error_names_index_and_type() {
        let arr = vec![Value::Float(0.1), Value::String("x".into())];
        let err = extract_vector_floats("embedding", &arr)
            .unwrap_err()
            .to_string();
        assert!(err.contains("VECTOR element 1"), "{err}");
        assert!(err.contains("String"), "{err}");
    }

    #[test]
    fn vector_rejects_non_finite_elements() {
        let arr = vec![Value::Float(f64::NAN)];
        assert!(extract_vector_floats("embedding", &arr).is_err());
    }

    #[test]
    fn vector_encodes_three_decimals_at_full_width() {
        let arr = vec![
            Value::Decimal(rust_decimal::Decimal::new(1, 1)),
            Value::Decimal(rust_decimal::Decimal::new(2, 1)),
            Value::Decimal(rust_decimal::Decimal::new(3, 1)),
        ];
        let floats = extract_vector_floats("embedding", &arr).unwrap();
        let encoded = validate_and_encode_vector("embedding", 3, &floats).unwrap();
        match encoded {
            Value::Bytes(b) => assert_eq!(b.len(), 12),
            other => panic!("expected Bytes, got {other:?}"),
        }
    }
}
