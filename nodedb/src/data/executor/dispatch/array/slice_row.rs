// SPDX-License-Identifier: BUSL-1.1

//! The row a slice or project read emits for one cell.
//!
//! A row is a msgpack map with one field per dim and one per emitted attr,
//! keyed by the schema name and written in schema order:
//! `{x: 0, y: 1, nir: 0.8, red: 0.2}`. A SQL query reads the rows as a
//! relation, so every column resolves by name. An audit-log slice (`AS OF
//! SYSTEM TIME NULL`) also stamps each row with `_ts_system`, the
//! millisecond of the version's system-time ordinal.
//!
//! The fixed key order makes two rows of one cell encode to the same bytes,
//! so a cluster merge that breaks ties by row bytes orders them the same on
//! every run.

use nodedb_array::query::project::Projection;
use nodedb_array::schema::ArraySchema;
use nodedb_array::schema::dim_spec::DimType;
use nodedb_types::{NdbDateTime, NodeDbError, Value};

/// The field an audit-log row carries its system time under.
pub(super) const SYSTEM_TIME_FIELD: &str = "_ts_system";

/// Encode the named row for one cell.
///
/// `coords` holds one value per schema dim, in dim order. `attrs` holds the
/// emitted attrs: the attrs `proj` selects, in its order, or every attr in
/// schema order when `proj` is `None`. A timestamp dim's millisecond value
/// becomes a timestamp. `system_time`, a tile version's system-time
/// ordinal, adds the `_ts_system` field as its millisecond.
pub(super) fn named_cell_row(
    schema: &ArraySchema,
    coords: Vec<Value>,
    attrs: Vec<Value>,
    proj: Option<&Projection>,
    system_time: Option<i64>,
) -> Result<Vec<u8>, NodeDbError> {
    if coords.len() != schema.dims.len() {
        return Err(NodeDbError::codec(format!(
            "array '{}' cell has {} coords for {} dims",
            schema.name,
            coords.len(),
            schema.dims.len()
        )));
    }
    let attr_count = proj.map_or(schema.attrs.len(), |p| p.attr_indices.len());
    if attrs.len() != attr_count {
        return Err(NodeDbError::codec(format!(
            "array '{}' cell has {} attr values for {attr_count} emitted attrs",
            schema.name,
            attrs.len()
        )));
    }

    let mut values: Vec<Value> = Vec::with_capacity(coords.len() + attrs.len() + 1);
    let mut names: Vec<&str> = Vec::with_capacity(values.capacity());
    for (dim, value) in schema.dims.iter().zip(coords) {
        names.push(&dim.name);
        values.push(dim_value(schema, dim.dtype, value)?);
    }
    for (position, value) in attrs.into_iter().enumerate() {
        let index = match proj {
            Some(p) => p.attr_indices.get(position).copied(),
            None => Some(position),
        };
        let attr = index.and_then(|i| schema.attrs.get(i)).ok_or_else(|| {
            NodeDbError::codec(format!(
                "array '{}' emits attr {index:?} of {} attrs",
                schema.name,
                schema.attrs.len()
            ))
        })?;
        names.push(&attr.name);
        values.push(value);
    }
    if let Some(ordinal) = system_time {
        names.push(SYSTEM_TIME_FIELD);
        values.push(Value::Integer(nodedb_types::ordinal_to_ms(ordinal)));
    }

    let fields: Vec<(&str, &Value)> = names.into_iter().zip(values.iter()).collect();
    nodedb_types::ordered_map_to_msgpack(&fields)
        .map_err(|e| NodeDbError::codec(format!("array '{}' row encode: {e}", schema.name)))
}

/// A coord value as its dim's SQL type renders it. A timestamp dim stores
/// milliseconds and reads as a timestamp.
fn dim_value(schema: &ArraySchema, dtype: DimType, value: Value) -> Result<Value, NodeDbError> {
    match (dtype, value) {
        (DimType::TimestampMs, Value::Integer(ms)) => NdbDateTime::from_millis(ms)
            .map(Value::NaiveDateTime)
            .map_err(|e| {
                NodeDbError::codec(format!(
                    "array '{}' timestamp coord {ms} ms: {e}",
                    schema.name
                ))
            }),
        (DimType::Int64 | DimType::Float64 | DimType::String | DimType::TimestampMs, value) => {
            Ok(value)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_array::schema::ArraySchemaBuilder;
    use nodedb_array::schema::attr_spec::{AttrSpec, AttrType};
    use nodedb_array::schema::dim_spec::DimSpec;
    use nodedb_array::types::domain::{Domain, DomainBound};

    fn schema() -> ArraySchema {
        ArraySchemaBuilder::new("img")
            .dim(DimSpec::new(
                "x",
                DimType::Int64,
                Domain::new(DomainBound::Int64(0), DomainBound::Int64(3)),
            ))
            .dim(DimSpec::new(
                "t",
                DimType::TimestampMs,
                Domain::new(
                    DomainBound::TimestampMs(0),
                    DomainBound::TimestampMs(10_000),
                ),
            ))
            .attr(AttrSpec::new("nir", AttrType::Float64, true))
            .attr(AttrSpec::new("red", AttrType::Float64, true))
            .tile_extents(vec![2, 1000])
            .build()
            .expect("schema")
    }

    fn decode(bytes: &[u8]) -> std::collections::HashMap<String, Value> {
        match nodedb_types::value_from_msgpack(bytes).expect("row decodes") {
            Value::Object(map) => map,
            other => panic!("row is not an object: {other:?}"),
        }
    }

    /// The map keys in the order the row bytes hold them.
    fn key_order(bytes: &[u8]) -> Vec<String> {
        let (count, mut pos) = nodedb_query::msgpack_scan::map_header(bytes, 0).expect("map");
        let mut keys = Vec::with_capacity(count);
        for _ in 0..count {
            keys.push(
                nodedb_query::msgpack_scan::read_str(bytes, pos)
                    .expect("key")
                    .to_string(),
            );
            pos = nodedb_query::msgpack_scan::skip_value(bytes, pos).expect("skip key");
            pos = nodedb_query::msgpack_scan::skip_value(bytes, pos).expect("skip value");
        }
        keys
    }

    #[test]
    fn every_dim_and_attr_is_a_named_field_in_schema_order() {
        let bytes = named_cell_row(
            &schema(),
            vec![Value::Integer(2), Value::Integer(1500)],
            vec![Value::Float(0.8), Value::Float(0.2)],
            None,
            None,
        )
        .expect("row");
        assert_eq!(key_order(&bytes), ["x", "t", "nir", "red"]);
        let row = decode(&bytes);
        assert_eq!(row["x"], Value::Integer(2));
        assert_eq!(
            row["t"],
            Value::NaiveDateTime(NdbDateTime::from_millis(1500).expect("ms"))
        );
        assert_eq!(row["nir"], Value::Float(0.8));
        assert_eq!(row["red"], Value::Float(0.2));
    }

    #[test]
    fn a_projection_names_attrs_in_its_order_and_audit_rows_carry_system_time() {
        let proj = Projection::new(vec![1]);
        let bytes = named_cell_row(
            &schema(),
            vec![Value::Integer(0), Value::Integer(0)],
            vec![Value::Float(0.2)],
            Some(&proj),
            Some(300 * 1_000_000 + 7),
        )
        .expect("row");
        assert_eq!(key_order(&bytes), ["x", "t", "red", SYSTEM_TIME_FIELD]);
        let row = decode(&bytes);
        assert_eq!(row["red"], Value::Float(0.2));
        assert_eq!(row[SYSTEM_TIME_FIELD], Value::Integer(300));
    }

    #[test]
    fn a_short_cell_is_an_error() {
        assert!(
            named_cell_row(
                &schema(),
                vec![Value::Integer(0)],
                vec![Value::Float(0.8), Value::Float(0.2)],
                None,
                None,
            )
            .is_err()
        );
    }
}
