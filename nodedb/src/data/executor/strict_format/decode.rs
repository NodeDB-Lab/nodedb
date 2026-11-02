// SPDX-License-Identifier: BUSL-1.1

//! Strict document decoding: Binary Tuple → Value/msgpack/JSON.

use nodedb_types::columnar::schema::is_reserved_bitemporal_column;
use nodedb_types::columnar::{SchemaOps, StrictSchema};
use nodedb_types::value::Value;

use super::coerce::value_to_json;

/// Decode a Binary Tuple to `nodedb_types::Value::Object` using the schema.
///
/// Returns `None` if the bytes are not a valid binary tuple (e.g., if they
/// are already msgpack — detected by checking for msgpack map headers).
pub fn binary_tuple_to_value(tuple_bytes: &[u8], schema: &StrictSchema) -> Option<Value> {
    // Reject bytes that look like msgpack maps (fixmap 0x80-0x8F, map16 0xDE, map32 0xDF).
    // Binary tuples start with a u32 LE schema version — the low byte (first byte)
    // of any realistic version is well below 0x80. This catches the common case
    // where data is already stored as msgpack.
    if looks_like_msgpack_map(tuple_bytes) {
        return None;
    }

    let decoder = nodedb_strict::TupleDecoder::new(schema);

    // Validate schema version matches before decoding.
    let version = decoder.schema_version(tuple_bytes).ok()?;
    if version == 0 || version > schema.version {
        return None;
    }

    // Version-aware decoding: if the tuple was written with an older schema
    // (fewer columns due to ADD COLUMN), build a sub-schema decoder matching
    // the physical layout and fill defaults for new columns.
    let mut map = std::collections::HashMap::with_capacity(schema.columns.len());
    if version < schema.version {
        let old_schema = schema.schema_for_version(version);
        let old_decoder = nodedb_strict::TupleDecoder::new(&old_schema);
        let old_values = old_decoder.extract_all(tuple_bytes).ok()?;

        // Map old columns by name.
        for (i, col) in old_schema.columns.iter().enumerate() {
            map.insert(col.name.clone(), old_values[i].clone());
        }
        // Fill defaults for columns added after this tuple's version.
        for col in &schema.columns {
            if col.added_at_version > version {
                let default_val = col
                    .default
                    .as_deref()
                    .map(StrictSchema::parse_default_literal)
                    .unwrap_or(Value::Null);
                map.insert(col.name.clone(), default_val);
            }
        }
    } else {
        let values = decoder.extract_all(tuple_bytes).ok()?;
        for (i, col) in schema.columns.iter().enumerate() {
            map.insert(col.name.clone(), values[i].clone());
        }
    }

    Some(Value::Object(map))
}

/// Whether `bytes` start like a msgpack map (fixmap 0x80-0x8F, map16 0xDE,
/// map32 0xDF) rather than a Binary Tuple, whose first byte is the low byte of
/// a schema version.
fn looks_like_msgpack_map(bytes: &[u8]) -> bool {
    bytes
        .first()
        .is_some_and(|&first| (0x80..=0x8F).contains(&first) || first == 0xDE || first == 0xDF)
}

/// Decode only `columns` of a Binary Tuple to a msgpack map. A name the schema
/// does not declare is left out. A tuple written under an older schema version
/// takes the full decode, which fills the defaults of later columns.
pub fn binary_tuple_columns_to_msgpack(
    tuple_bytes: &[u8],
    schema: &StrictSchema,
    columns: &[String],
) -> Option<Vec<u8>> {
    let decoder = nodedb_strict::TupleDecoder::new(schema);
    let current = !looks_like_msgpack_map(tuple_bytes)
        && decoder.schema_version(tuple_bytes).ok()? == schema.version;
    let mut map = std::collections::HashMap::with_capacity(columns.len());
    if current {
        for column in columns {
            if schema.column(column).is_some() {
                map.insert(
                    column.clone(),
                    decoder.extract_by_name(tuple_bytes, column).ok()?,
                );
            }
        }
    } else {
        let Value::Object(mut full) = binary_tuple_to_value(tuple_bytes, schema)? else {
            return None;
        };
        for column in columns {
            if let Some(value) = full.remove(column) {
                map.insert(column.clone(), value);
            }
        }
    }
    nodedb_types::value_to_msgpack(&Value::Object(map)).ok()
}

/// Decode a Binary Tuple to standard msgpack bytes.
pub fn binary_tuple_to_msgpack(tuple_bytes: &[u8], schema: &StrictSchema) -> Option<Vec<u8>> {
    let val = binary_tuple_to_value(tuple_bytes, schema)?;
    nodedb_types::value_to_msgpack(&val).ok()
}

/// The error for a stored Binary Tuple that does not decode against the
/// collection's strict schema. Names the row by its client-visible identity,
/// never its storage key, so the error text never leaks the internal hex key.
pub fn undecodable_strict_row(collection: &str, identity: &str) -> crate::Error {
    crate::Error::Serialization {
        format: "binary_tuple".into(),
        detail: format!("document \"{identity}\" of collection \"{collection}\" does not decode"),
    }
}

/// Decode a Binary Tuple to the row a projection reads: every declared
/// column, in schema order, with the reserved bitemporal bookkeeping columns
/// dropped. A column the tuple does not carry is `Value::Null`.
///
/// The walk is shared by [`binary_tuple_to_json`], so the JSON and `Value`
/// forms of a row can never disagree about which columns it has.
pub fn binary_tuple_to_row_value(tuple_bytes: &[u8], schema: &StrictSchema) -> Option<Value> {
    let Value::Object(mut map) = binary_tuple_to_value(tuple_bytes, schema)? else {
        return None;
    };
    let mut row = std::collections::HashMap::with_capacity(map.len());
    for col in projected_columns(schema) {
        let v = map.remove(&col.name).unwrap_or(Value::Null);
        row.insert(col.name.clone(), v);
    }
    Some(Value::Object(row))
}

/// Decode a Binary Tuple to a JSON object using the schema (for pgwire output).
pub fn binary_tuple_to_json(
    tuple_bytes: &[u8],
    schema: &StrictSchema,
) -> Option<serde_json::Value> {
    let Value::Object(map) = binary_tuple_to_row_value(tuple_bytes, schema)? else {
        return None;
    };
    let mut obj = serde_json::Map::with_capacity(map.len());
    for col in projected_columns(schema) {
        let v = map.get(&col.name).unwrap_or(&Value::Null);
        obj.insert(col.name.clone(), value_to_json(v));
    }
    Some(serde_json::Value::Object(obj))
}

/// The schema's columns a projection sees, in declaration order.
fn projected_columns(
    schema: &StrictSchema,
) -> impl Iterator<Item = &nodedb_types::columnar::ColumnDef> {
    schema
        .columns
        .iter()
        .filter(|col| !is_reserved_bitemporal_column(&col.name))
}
