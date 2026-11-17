// SPDX-License-Identifier: BUSL-1.1

//! The row image limit for a row measured as its native MessagePack image:
//! a columnar row and a vector-primary row.
//!
//! Encoding a row to measure it costs as much as storing it, so a row is
//! encoded only when a cheap upper bound on its image is over
//! [`MAX_ROW_IMAGE_BYTES`].

use std::collections::HashMap;

use nodedb_types::columnar::ColumnarSchema;
use nodedb_types::{MAX_ROW_IMAGE_BYTES, Value};

use super::row_image_limit::check_row_image;

/// Header bytes one tagged value adds beyond its payload, at most: the tag
/// array header, the tag, and a `str 32`, `bin 32`, `array 32` or `map 32`
/// header.
const VALUE_HEADERS: usize = 8;

/// Bytes a fixed-width tagged value encodes to, at most: a null, a boolean,
/// an integer, a float, an instant, a duration or a decimal.
const FIXED_VALUE_BYTES: usize = 40;

/// Header bytes one object key adds beyond its text, at most: a `str 32`
/// header.
const KEY_HEADER: usize = 5;

/// Bytes a vector-primary row's image adds beyond its sidecar, its vector
/// column's name and its floats, at most: the row's object tag and widened
/// map header, the column's key header and the vector's tagged `bin 32`
/// header.
const VECTOR_ROW_FRAMING: usize = 24;

/// An upper bound on the native image of `value`. A geometry or an array
/// cell has no cheap bound and is encoded.
fn value_bound(value: &Value) -> usize {
    match value {
        Value::Null
        | Value::Bool(_)
        | Value::Integer(_)
        | Value::Float(_)
        | Value::DateTime(_)
        | Value::NaiveDateTime(_)
        | Value::Duration(_)
        | Value::Decimal(_) => FIXED_VALUE_BYTES,
        Value::String(text) | Value::Uuid(text) | Value::Ulid(text) | Value::Regex(text) => {
            text.len().saturating_add(VALUE_HEADERS)
        }
        Value::Bytes(bytes) => bytes.len().saturating_add(VALUE_HEADERS),
        Value::Vector(floats) => floats.len().saturating_mul(4).saturating_add(VALUE_HEADERS),
        Value::Array(items) | Value::Set(items) => items
            .iter()
            .map(value_bound)
            .fold(VALUE_HEADERS, usize::saturating_add),
        Value::Object(fields) => fields
            .iter()
            .map(|(name, value)| field_bound(name, value))
            .fold(VALUE_HEADERS, usize::saturating_add),
        Value::Range { start, end, .. } => [start, end]
            .into_iter()
            .map(|bound| bound.as_deref().map_or(1, value_bound))
            .fold(VALUE_HEADERS.saturating_add(1), usize::saturating_add),
        Value::Record { table, id } => table
            .len()
            .saturating_add(id.len())
            .saturating_add(2 * VALUE_HEADERS),
        // `Value` is non-exhaustive: a kind with no cheap bound is encoded.
        _ => zerompk::to_msgpack_vec(value).map_or(usize::MAX, |image| image.len()),
    }
}

/// An upper bound on one object entry: its key and its value.
fn field_bound(name: &str, value: &Value) -> usize {
    name.len()
        .saturating_add(KEY_HEADER)
        .saturating_add(value_bound(value))
}

/// Refuse a row of `fields` whose native image, one tagged object, is over
/// [`MAX_ROW_IMAGE_BYTES`]. `row_id` names the row in the refusal and runs
/// only when the row refuses.
///
/// A row that does not encode has no image to measure here. Its write
/// refuses it with its own error.
pub(crate) fn check_native_row<'a, I>(
    collection: &str,
    row_id: impl FnOnce() -> String,
    fields: I,
) -> crate::Result<()>
where
    I: Iterator<Item = (&'a str, &'a Value)> + Clone,
{
    let bound = fields
        .clone()
        .map(|(name, value)| field_bound(name, value))
        .fold(VALUE_HEADERS, usize::saturating_add);
    if bound <= MAX_ROW_IMAGE_BYTES {
        return Ok(());
    }
    let row = Value::Object(
        fields
            .map(|(name, value)| (name.to_string(), value.clone()))
            .collect(),
    );
    match zerompk::to_msgpack_vec(&row) {
        Ok(image) if image.len() > MAX_ROW_IMAGE_BYTES => {
            check_row_image(collection, &row_id(), image.len())
        }
        Ok(_) | Err(_) => Ok(()),
    }
}

/// Refuse a columnar row, `values` in `schema` column order, whose native
/// image is over [`MAX_ROW_IMAGE_BYTES`]. The refusal names the row by its
/// primary key.
pub(crate) fn check_columnar_row(
    collection: &str,
    schema: &ColumnarSchema,
    values: &[Value],
) -> crate::Result<()> {
    let names = schema.columns.iter().map(|column| column.name.as_str());
    check_native_row(
        collection,
        || columnar_row_id(schema, values),
        names.zip(values),
    )
}

/// A columnar row's primary key as a refusal names it: `column=value` for
/// each key column.
fn columnar_row_id(schema: &ColumnarSchema, values: &[Value]) -> String {
    schema
        .columns
        .iter()
        .zip(values)
        .filter(|(column, _)| column.primary_key)
        .map(|(column, value)| format!("{}={value}", column.name))
        .collect::<Vec<_>>()
        .join(", ")
}

/// Refuse a vector-primary row whose native image is over
/// [`MAX_ROW_IMAGE_BYTES`]: its payload `fields`, stored as a sidecar of
/// `sidecar_len` bytes, with the vector column `field` holding `dim` floats.
/// `row_id` names the row in the refusal and runs only when the row refuses.
///
/// The image's length does not depend on the floats' values, so the
/// measure takes their count alone.
pub(crate) fn check_vector_row(
    collection: &str,
    row_id: impl FnOnce() -> String,
    field: &str,
    dim: usize,
    fields: &HashMap<String, Value>,
    sidecar_len: usize,
) -> crate::Result<()> {
    let bound = sidecar_len
        .saturating_add(field.len())
        .saturating_add(dim.saturating_mul(4))
        .saturating_add(VECTOR_ROW_FRAMING);
    if bound <= MAX_ROW_IMAGE_BYTES {
        return Ok(());
    }
    let column = Value::Vector(vec![0.0f32; dim].into());
    let row = fields
        .iter()
        .filter(|(name, _)| !name.eq_ignore_ascii_case(field))
        .map(|(name, value)| (name.as_str(), value))
        .chain(std::iter::once((field, &column)));
    check_native_row(collection, row_id, row)
}

#[cfg(test)]
mod tests {
    use nodedb_types::columnar::{ColumnDef, ColumnType};
    use nodedb_types::{NdbDateTime, NdbDuration};

    use super::*;

    fn growing_values() -> Vec<Value> {
        vec![
            Value::Null,
            Value::Bool(true),
            Value::Integer(i64::MIN),
            Value::Float(f64::MIN_POSITIVE),
            Value::DateTime(NdbDateTime::from_micros(i64::MAX)),
            Value::NaiveDateTime(NdbDateTime::from_micros(i64::MIN)),
            Value::Duration(NdbDuration::from_micros(i64::MIN + 1)),
            Value::Decimal(rust_decimal::Decimal::MAX),
            Value::String("s".repeat(300)),
            Value::Uuid("01234567-89ab-cdef-0123-456789abcdef".into()),
            Value::Bytes(vec![1; 70_000]),
            Value::Vector(vec![1.5f32; 9].into()),
            Value::Set(vec![Value::Integer(1); 20]),
            Value::Range {
                start: Some(Box::new(Value::Integer(1))),
                end: None,
                inclusive: true,
            },
            Value::Record {
                table: "t".into(),
                id: "i".into(),
            },
        ]
    }

    /// Every value encodes within its bound, alone and nested.
    #[test]
    fn every_value_encodes_within_its_bound() {
        let mut values = growing_values();
        values.push(Value::Array(growing_values()));
        values.push(Value::Object(
            growing_values()
                .into_iter()
                .enumerate()
                .map(|(index, value)| (format!("f{index}"), value))
                .collect(),
        ));
        for value in &values {
            let image = zerompk::to_msgpack_vec(value).expect("encode");
            assert!(
                image.len() <= value_bound(value),
                "{value:?}: {} bytes over a bound of {}",
                image.len(),
                value_bound(value)
            );
        }
    }

    fn schema() -> ColumnarSchema {
        ColumnarSchema::new(vec![
            ColumnDef::required("id", ColumnType::Int64).with_primary_key(),
            ColumnDef::nullable("s", ColumnType::String),
        ])
        .expect("schema")
    }

    #[test]
    fn a_columnar_row_over_the_limit_refuses_and_names_its_key() {
        let schema = schema();
        let within = [Value::Integer(7), Value::String("s".repeat(1024))];
        check_columnar_row("events", &schema, &within).expect("a small row");
        let over = [
            Value::Integer(7),
            Value::String("s".repeat(MAX_ROW_IMAGE_BYTES)),
        ];
        let error = check_columnar_row("events", &schema, &over).expect_err("refused");
        let detail = error.to_string();
        assert!(detail.contains("id=7"), "{detail}");
    }

    /// A row whose bound is over the limit but whose image is not stores.
    #[test]
    fn a_row_measures_exactly_past_its_bound() {
        let schema = schema();
        let fill = MAX_ROW_IMAGE_BYTES - 64;
        let row = [Value::Integer(7), Value::String("s".repeat(fill))];
        let names = schema.columns.iter().map(|column| column.name.as_str());
        let bound = names
            .zip(row.iter())
            .map(|(name, value)| field_bound(name, value))
            .fold(VALUE_HEADERS, usize::saturating_add);
        assert!(bound > MAX_ROW_IMAGE_BYTES, "{bound}");
        check_columnar_row("events", &schema, &row).expect("the image is within the limit");
    }

    #[test]
    fn a_vector_row_over_the_limit_refuses() {
        let fields = HashMap::from([("note".to_string(), Value::String("n".into()))]);
        check_vector_row("docs", || "1".into(), "embedding", 16, &fields, 16).expect("a small row");
        check_vector_row(
            "docs",
            || "1".into(),
            "embedding",
            MAX_ROW_IMAGE_BYTES / 4,
            &fields,
            16,
        )
        .expect_err("refused");
    }
}
