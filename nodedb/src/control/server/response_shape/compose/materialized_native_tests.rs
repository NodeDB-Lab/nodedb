// SPDX-License-Identifier: BUSL-1.1

//! Shaping of native and flat rows through `shape_payload_no_plan`.

use super::*;

fn shape_rows(payload: &[u8]) -> Result<ShapedRows, NodeDbError> {
    shape_rows_in(payload, RowContext::ScanEnvelope)
}

fn shape_rows_in(payload: &[u8], context: RowContext) -> Result<ShapedRows, NodeDbError> {
    match shape_payload_no_plan(
        payload,
        PlanKind::MultiRow,
        context,
        None,
        None,
        None,
        nodedb_crs::CustomCrsSet::empty(),
    )? {
        ShapeOutcome::Rows(shaped) => Ok(shaped),
        ShapeOutcome::Passthrough => panic!("multi-row plan must yield rows"),
    }
}

fn cell_of(value: &nodedb_types::Value) -> Option<String> {
    crate::control::server::response_shape::cell::cell_text(value)
}

/// A scan envelope over a native body shapes into the row its typed value
/// shapes into: each cell keeps its type for the protocol edge, a nested
/// object stays an object, and a plain array field stays an array. A
/// plain body shapes unchanged, and a malformed native body fails.
#[test]
fn a_native_scan_row_shapes_as_its_typed_value() {
    use nodedb_types::Value;

    let typed = Value::Object(
        [
            ("id".to_owned(), Value::String("r1".into())),
            (
                "price".to_owned(),
                Value::Decimal("123.45".parse().unwrap()),
            ),
            (
                "uid".to_owned(),
                Value::Uuid("550e8400-e29b-41d4-a716-446655440000".into()),
            ),
            (
                "seen".to_owned(),
                Value::DateTime(nodedb_types::NdbDateTime::from_micros(
                    1_700_000_000_000_000,
                )),
            ),
            (
                "meta".to_owned(),
                Value::Object([("k".to_owned(), Value::Integer(1))].into()),
            ),
            (
                "tags".to_owned(),
                Value::Array(vec![Value::Integer(7), Value::String("x".into())]),
            ),
        ]
        .into(),
    );
    let key = nodedb_types::StorageKey::for_surrogate(nodedb_types::Surrogate::new(5));
    let envelope = |body: Vec<u8>| {
        crate::data::executor::response_codec::encode_raw_document_rows(&[(key.to_string(), body)])
            .expect("encode envelope")
    };

    let native = zerompk::to_msgpack_vec(&typed).expect("encode native");
    let shaped = shape_rows(&envelope(native)).expect("shape native row");
    let Value::Object(fields) = &typed else {
        panic!("typed row is an object");
    };
    assert_eq!(shaped.rows.len(), 1);
    for (column, value) in fields {
        assert_eq!(shaped.rows[0].get(column), Some(value), "column {column}");
    }
    // The protocol edges render the typed cells: HTTP JSON through
    // `value_to_wire_json`, pgwire text through `cell_text`.
    let http = crate::control::server::response_shape::cell::row_to_wire_json(&shaped.rows[0]);
    assert_eq!(http["price"], "123.45");
    assert_eq!(http["uid"], "550e8400-e29b-41d4-a716-446655440000");
    assert_eq!(http["seen"], "2023-11-14T22:13:20.000000Z");
    let cell = |column: &str| {
        crate::control::server::response_shape::cell::cell_text(&shaped.rows[0][column])
    };
    assert_eq!(cell("price").as_deref(), Some("123.45"));
    assert_eq!(
        cell("seen"),
        cell_of(&Value::DateTime(nodedb_types::NdbDateTime::from_micros(
            1_700_000_000_000_000
        )))
    );

    let plain = nodedb_types::value_to_msgpack(&typed).expect("encode plain");
    let plain_shaped = shape_rows(&envelope(plain.clone())).expect("shape plain row");
    let Value::Object(plain_fields) =
        nodedb_types::value_from_msgpack(&plain).expect("decode plain")
    else {
        panic!("plain row is an object");
    };
    for (column, value) in &plain_fields {
        assert_eq!(
            plain_shaped.rows[0].get(column),
            Some(value),
            "column {column}"
        );
    }

    let malformed = vec![0x92, 0x07, 0x81, 0xa1, b'a', 0x92, 0x63, 0x01];
    assert!(shape_rows(&envelope(malformed)).is_err());
}

/// A user SELECT whose columns are `id` and `data` shapes unchanged in a
/// flat payload, whether `data` is an array shaped like a native object
/// or an object.
#[test]
fn a_flat_id_data_row_shapes_unchanged() {
    use nodedb_types::Value;

    for data in [
        Value::Array(vec![
            Value::Integer(7),
            Value::Object([("k".to_owned(), Value::Integer(1))].into()),
        ]),
        Value::Object([("k".to_owned(), Value::Integer(1))].into()),
    ] {
        let row: std::collections::HashMap<String, Value> = [
            ("id".to_owned(), Value::String("x".into())),
            ("data".to_owned(), data.clone()),
        ]
        .into();
        let payload = nodedb_types::value_to_msgpack(&Value::Array(vec![Value::Object(row)]))
            .expect("encode");
        let shaped = shape_rows_in(&payload, RowContext::Flat).expect("shape flat row");
        assert_eq!(shaped.rows.len(), 1);
        assert_eq!(shaped.rows[0].get("id"), Some(&Value::String("x".into())));
        assert_eq!(shaped.rows[0].get("data"), Some(&data));
    }
}

/// An aggregate or constant row now travels as a native object. Its pgwire
/// cell text and HTTP JSON are what the plain row it replaces gave: a DECIMAL
/// renders `1.50` (a JSON string), a UUID its text, a count its integer.
#[test]
fn a_native_aggregate_row_renders_as_its_plain_row_did() {
    use nodedb_types::Value;

    let row = Value::Object(
        [
            ("amount".to_owned(), Value::Decimal("1.50".parse().unwrap())),
            (
                "uid".to_owned(),
                Value::Uuid("550e8400-e29b-41d4-a716-446655440000".into()),
            ),
            ("n".to_owned(), Value::Integer(2)),
            ("avg".to_owned(), Value::Float(0.5)),
        ]
        .into(),
    );
    let native =
        crate::data::executor::response_codec::encode_native_rows(std::slice::from_ref(&row))
            .expect("encode native rows");
    let plain = nodedb_types::value_to_msgpack(&Value::Array(vec![row])).expect("encode plain");

    let native = shape_rows_in(&native, RowContext::Flat).expect("shape native rows");
    let plain = shape_rows_in(&plain, RowContext::Flat).expect("shape plain rows");
    let wire = crate::control::server::response_shape::cell::row_to_wire_json;
    let native_http = wire(&native.rows[0]);
    let plain_http = wire(&plain.rows[0]);
    for column in ["amount", "uid", "n", "avg"] {
        assert_eq!(
            cell_of(&native.rows[0][column]),
            cell_of(&plain.rows[0][column]),
            "column {column}"
        );
        assert_eq!(
            native_http[column], plain_http[column],
            "HTTP column {column}"
        );
    }
    assert_eq!(cell_of(&native.rows[0]["amount"]).as_deref(), Some("1.50"));
    assert_eq!(native_http["amount"], "1.50");
}

/// A columnar scan row now travels as a native object. Through the shaping
/// core it gives each column the pgwire cell text and the HTTP JSON its plain
/// row gave. A DECIMAL is the JSON string of its exact digits and scale from
/// either row.
#[test]
fn a_native_columnar_row_renders_as_its_plain_row_did() {
    use crate::control::server::response_shape::cell::row_to_wire_json;
    use nodedb_types::Value;

    let typed = Value::Object(
        [
            ("id".to_owned(), Value::Integer(1)),
            ("amount".to_owned(), Value::Decimal("1.50".parse().unwrap())),
            (
                "uid".to_owned(),
                Value::Uuid("550e8400-e29b-41d4-a716-446655440000".into()),
            ),
            (
                "seen".to_owned(),
                Value::DateTime(nodedb_types::NdbDateTime::from_micros(
                    1_700_000_000_000_000,
                )),
            ),
            ("note".to_owned(), Value::String("n".into())),
        ]
        .into(),
    );
    let native =
        crate::data::executor::response_codec::encode_native_rows(std::slice::from_ref(&typed))
            .expect("encode native rows");
    let plain = nodedb_types::value_to_msgpack(&Value::Array(vec![typed])).expect("encode plain");

    let native = shape_rows_in(&native, RowContext::Flat).expect("shape native rows");
    let plain = shape_rows_in(&plain, RowContext::Flat).expect("shape plain rows");
    let native_http = row_to_wire_json(&native.rows[0]);
    let plain_http = row_to_wire_json(&plain.rows[0]);
    for column in ["id", "amount", "uid", "seen", "note"] {
        assert_eq!(
            cell_of(&native.rows[0][column]),
            cell_of(&plain.rows[0][column]),
            "pgwire column {column}"
        );
        assert_eq!(
            native_http[column], plain_http[column],
            "HTTP column {column}"
        );
    }
    assert_eq!(native_http["amount"], "1.50");
}
