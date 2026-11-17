// SPDX-License-Identifier: Apache-2.0

//! The stored form of an edge's property object.
//!
//! An edge stores its properties as a native tagged object, `[7, {field:
//! tagged value}]`, the form a document body takes. Every value keeps its
//! exact type: a Decimal stays a Decimal, a UUID stays a UUID. Empty bytes
//! store no properties and read as the empty object.
//!
//! Readers decode through [`nodedb_types::ObjectView`], which reads the
//! native form and a plain MessagePack map alike.

use std::collections::HashMap;

use nodedb_types::{MsgpackResult, ObjectView, Value};

/// The stored image of `fields`. An empty map stores no properties.
pub fn encode_edge_properties(fields: HashMap<String, Value>) -> MsgpackResult<Vec<u8>> {
    if fields.is_empty() {
        return Ok(Vec::new());
    }
    Ok(zerompk::to_msgpack_vec(&Value::Object(fields))?)
}

/// The property object a stored image holds. Empty bytes are `{}`. Bytes
/// that are not one well-formed object are an error.
pub fn decode_edge_properties(bytes: &[u8]) -> MsgpackResult<HashMap<String, Value>> {
    if bytes.is_empty() {
        return Ok(HashMap::new());
    }
    let mut fields = HashMap::new();
    let view = ObjectView::new(bytes)?;
    for entry in view.fields() {
        let (key, value) = entry?;
        fields.insert(key.to_owned(), view.decode_value(value)?);
    }
    Ok(fields)
}

/// The numeric weight a property value stands for: an Integer, a Float, or
/// a Decimal rounded to the nearest `f64`. `None` for any other kind.
pub fn property_weight(value: &Value) -> Option<f64> {
    match value {
        Value::Integer(i) => Some(*i as f64),
        Value::Float(f) => Some(*f),
        Value::Decimal(d) => Some(nodedb_types::decimal_float::decimal_to_f64(*d)),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn typed_fields() -> HashMap<String, Value> {
        HashMap::from([
            (
                "price".to_string(),
                Value::Decimal("12.50".parse().expect("decimal")),
            ),
            (
                "id".to_string(),
                Value::Uuid("550e8400-e29b-41d4-a716-446655440000".into()),
            ),
            (
                "at".to_string(),
                Value::DateTime(nodedb_types::NdbDateTime::from_micros(
                    1_700_000_000_000_000,
                )),
            ),
            ("blob".to_string(), Value::Bytes(vec![0, 1, 255])),
        ])
    }

    #[test]
    fn typed_properties_round_trip_exactly() {
        let fields = typed_fields();
        let bytes = encode_edge_properties(fields.clone()).expect("encode");
        assert_eq!(
            nodedb_types::ObjectView::new(&bytes)
                .expect("object")
                .format(),
            nodedb_types::ObjectFormat::Native
        );
        assert_eq!(decode_edge_properties(&bytes).expect("decode"), fields);
    }

    #[test]
    fn no_properties_store_nothing_and_read_as_empty() {
        assert!(
            encode_edge_properties(HashMap::new())
                .expect("encode")
                .is_empty()
        );
        assert!(decode_edge_properties(&[]).expect("decode").is_empty());
    }

    #[test]
    fn a_plain_map_reads_as_its_fields() {
        let plain =
            nodedb_types::json_msgpack::value_to_msgpack(&Value::Object(HashMap::from([(
                "w".to_string(),
                Value::Integer(3),
            )])))
            .expect("encode plain");
        assert_eq!(
            decode_edge_properties(&plain).expect("decode"),
            HashMap::from([("w".to_string(), Value::Integer(3))])
        );
    }

    #[test]
    fn bytes_that_are_not_an_object_are_an_error() {
        let array = zerompk::to_msgpack_vec(&Value::Array(vec![Value::Integer(1)])).expect("enc");
        assert!(decode_edge_properties(&array).is_err());
        assert!(decode_edge_properties(b"{\"w\":1}").is_err());
    }

    #[test]
    fn weights_read_every_numeric_kind() {
        assert_eq!(property_weight(&Value::Integer(4)), Some(4.0));
        assert_eq!(property_weight(&Value::Float(0.25)), Some(0.25));
        assert_eq!(
            property_weight(&Value::Decimal("12.50".parse().expect("decimal"))),
            Some(12.5)
        );
        assert_eq!(property_weight(&Value::String("5".into())), None);
    }
}
