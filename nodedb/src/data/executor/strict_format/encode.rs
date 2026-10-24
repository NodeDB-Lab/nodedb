// SPDX-License-Identifier: BUSL-1.1

//! Strict document encoding: Value/bytes → Binary Tuple.

use nodedb_types::columnar::StrictSchema;
use nodedb_types::value::Value;

use super::coerce::coerce_value;

/// Encode a `nodedb_types::Value::Object` as a Binary Tuple according to the schema.
///
/// Accepts zerompk bytes (from planner) and decodes them internally.
/// Missing nullable columns become NULL; missing non-nullable columns error.
/// `collection` names the target in an unknown-field error and carries no
/// other meaning.
pub fn bytes_to_binary_tuple(
    bytes: &[u8],
    schema: &StrictSchema,
    collection: &str,
) -> crate::Result<Vec<u8>> {
    let value =
        nodedb_types::value_from_msgpack(bytes).map_err(|e| crate::Error::Serialization {
            format: "msgpack".to_string(),
            detail: format!("zerompk decode: {e}"),
        })?;
    value_to_binary_tuple(&value, schema, collection)
}

/// Encode a `nodedb_types::Value` as a Binary Tuple according to the schema.
/// `collection` names the target in an unknown-field error and carries no
/// other meaning.
pub fn value_to_binary_tuple(
    value: &Value,
    schema: &StrictSchema,
    collection: &str,
) -> crate::Result<Vec<u8>> {
    let map = match value {
        Value::Object(m) => m,
        _ => {
            return Err(crate::Error::BadRequest {
                detail: "strict value must be an Object".to_string(),
            });
        }
    };

    let schema_columns: std::collections::HashSet<&str> =
        schema.columns.iter().map(|c| c.name.as_str()).collect();
    if let Some(unknown) = map.keys().find(|k| !schema_columns.contains(k.as_str())) {
        return Err(crate::Error::UnknownStrictField {
            collection: collection.to_string(),
            column: unknown.clone(),
        });
    }

    let encoder = nodedb_strict::TupleEncoder::new(schema);
    let mut values = Vec::with_capacity(schema.columns.len());

    for col in &schema.columns {
        let field_val = map.get(&col.name);
        let typed = match field_val {
            None | Some(Value::Null) => {
                if !col.nullable {
                    return Err(crate::Error::BadRequest {
                        detail: format!("column '{}' is NOT NULL but no value provided", col.name),
                    });
                }
                Value::Null
            }
            Some(v) => coerce_value(v, col)?,
        };
        values.push(typed);
    }

    encoder
        .encode(&values)
        .map_err(|e| tuple_encode_error(e, map))
}

/// The error for a row the tuple encoder refuses.
///
/// A value of a kind the column does not hold is `DatatypeMismatch`
/// (SQLSTATE `42804`). A value of an accepted kind that does not convert,
/// such as text that is no UUID, is `InvalidTextRepresentation` (SQLSTATE
/// `22P02`). Both name the column, the value from `row`, and the type. Any
/// other encoder error is a malformed request.
fn tuple_encode_error(
    error: nodedb_strict::StrictError,
    row: &std::collections::HashMap<String, Value>,
) -> crate::Error {
    match error {
        nodedb_strict::StrictError::TypeMismatch { column, expected } => {
            crate::Error::DatatypeMismatch {
                detail: format!(
                    "column '{column}': expected {expected}, got {:?}",
                    row.get(&column).unwrap_or(&Value::Null)
                ),
            }
        }
        nodedb_strict::StrictError::InvalidValue {
            column,
            expected,
            detail,
        } => crate::Error::InvalidTextRepresentation {
            detail: format!(
                "column '{column}': {:?} is not a valid {expected} value: {detail}",
                row.get(&column).unwrap_or(&Value::Null)
            ),
        },
        other => crate::Error::BadRequest {
            detail: format!("Binary Tuple encode: {other}"),
        },
    }
}

/// Bitemporal variant: decode msgpack to `Value`, then encode as a Binary
/// Tuple with reserved slots 0/1/2 populated from the supplied system-time
/// ordinal and valid-time bounds.
/// `collection` names the target in an unknown-field error and carries no
/// other meaning.
pub fn bytes_to_binary_tuple_bitemporal(
    bytes: &[u8],
    schema: &StrictSchema,
    system_from: i64,
    valid_from_ms: i64,
    valid_until_ms: i64,
    collection: &str,
) -> crate::Result<Vec<u8>> {
    let value =
        nodedb_types::value_from_msgpack(bytes).map_err(|e| crate::Error::Serialization {
            format: "msgpack".to_string(),
            detail: format!("zerompk decode: {e}"),
        })?;
    value_to_binary_tuple_bitemporal(
        &value,
        schema,
        system_from,
        valid_from_ms,
        valid_until_ms,
        collection,
    )
}

/// Bitemporal variant: encode a user-supplied `Value::Object` together
/// with the three reserved bitemporal timestamps. `collection` names the
/// target in an unknown-field error and carries no other meaning.
pub fn value_to_binary_tuple_bitemporal(
    value: &Value,
    schema: &StrictSchema,
    system_from: i64,
    valid_from_ms: i64,
    valid_until_ms: i64,
    collection: &str,
) -> crate::Result<Vec<u8>> {
    if !schema.bitemporal {
        return Err(crate::Error::BadRequest {
            detail: "schema is not bitemporal".to_string(),
        });
    }
    let map = match value {
        Value::Object(m) => m,
        _ => {
            return Err(crate::Error::BadRequest {
                detail: "strict value must be an Object".to_string(),
            });
        }
    };

    // A bitemporal strict schema must carry the three reserved timestamp
    // columns in slots 0/1/2 ahead of the user columns (see
    // `StrictSchema::new_bitemporal`). Guard the slice so a malformed or
    // short schema surfaces a typed error instead of panicking on the
    // out-of-range index.
    let reserved = nodedb_types::columnar::BITEMPORAL_RESERVED_COLUMNS.len();
    if schema.columns.len() < reserved {
        return Err(crate::Error::BadRequest {
            detail: format!(
                "bitemporal strict schema must have the {reserved} reserved columns \
                 in slots 0..{reserved}, found only {}",
                schema.columns.len()
            ),
        });
    }
    let user_columns = &schema.columns[reserved..];
    let user_names: std::collections::HashSet<&str> =
        user_columns.iter().map(|c| c.name.as_str()).collect();
    if let Some(unknown) = map.keys().find(|k| {
        !user_names.contains(k.as_str())
            && !nodedb_types::columnar::BITEMPORAL_RESERVED_COLUMNS.contains(&k.as_str())
    }) {
        return Err(crate::Error::UnknownStrictField {
            collection: collection.to_string(),
            column: unknown.clone(),
        });
    }

    let mut user_values = Vec::with_capacity(user_columns.len());
    for col in user_columns {
        let field_val = map.get(&col.name);
        let typed = match field_val {
            None | Some(Value::Null) => {
                if !col.nullable {
                    return Err(crate::Error::BadRequest {
                        detail: format!("column '{}' is NOT NULL but no value provided", col.name),
                    });
                }
                Value::Null
            }
            Some(v) => coerce_value(v, col)?,
        };
        user_values.push(typed);
    }

    let encoder = nodedb_strict::TupleEncoder::new(schema);
    encoder
        .encode_bitemporal(system_from, valid_from_ms, valid_until_ms, &user_values)
        .map_err(|e| tuple_encode_error(e, map))
}
