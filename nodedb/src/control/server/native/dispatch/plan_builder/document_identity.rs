// SPDX-License-Identifier: BUSL-1.1

//! The stored body of a schemaless document written over the native protocol.
//!
//! SQL reads a schemaless row's identity from its identity column: the
//! declared primary key, else `id`. A native write keys the row by the
//! document id it carries, so the stored body names that id under the
//! identity column.

use nodedb_types::{CollectionType, DEFAULT_IDENTITY_COLUMN, Value};

/// Whether a collection stores schemaless document bodies. A collection the
/// catalog does not hold defaults to a schemaless document collection.
pub(super) fn stores_schemaless_bodies(collection_type: Option<&CollectionType>) -> bool {
    collection_type.is_none_or(CollectionType::is_schemaless)
}

/// The identity column of a schemaless collection: its declared primary key,
/// else `id`. The planner resolves a schemaless collection's key by the same
/// rule.
pub(super) fn identity_column(declared_primary_key: Option<String>) -> String {
    declared_primary_key.unwrap_or_else(|| DEFAULT_IDENTITY_COLUMN.to_string())
}

/// `body` as a MessagePack body that carries `doc_id` under `column`.
///
/// `body` is a native object `[7, {field: <tagged value>}]`, a plain
/// MessagePack map or JSON object text. A native body keeps every field's
/// exact type and stays native. A body `column` field that is not the string
/// `doc_id` is refused: the row is keyed by `doc_id`, so SQL and key reads
/// would disagree on which document the row is.
pub(super) fn identified_body(body: &[u8], doc_id: &str, column: &str) -> crate::Result<Vec<u8>> {
    if crate::data::executor::response_codec::is_native_row(body) {
        return identified_native_body(body, doc_id, column);
    }
    if nodedb_query::msgpack_scan::map_header(body, 0).is_some() {
        if let Some((start, end)) = nodedb_query::msgpack_scan::extract_field(body, 0, column) {
            let cell = body
                .get(start..end)
                .ok_or_else(|| crate::Error::BadRequest {
                    detail: format!("document '{doc_id}': body field '{column}' is truncated"),
                })?;
            let value =
                nodedb_types::value_from_msgpack(cell).map_err(|e| crate::Error::BadRequest {
                    detail: format!(
                        "document '{doc_id}': body field '{column}' does not decode: {e}"
                    ),
                })?;
            check_body_id(&value, doc_id, column)?;
            return Ok(body.to_vec());
        }
        return Ok(nodedb_query::msgpack_scan::inject_str_field(
            body, column, doc_id,
        ));
    }
    let json: serde_json::Value =
        sonic_rs::from_slice(body).map_err(|e| crate::Error::BadRequest {
            detail: format!(
                "document '{doc_id}': body is neither a MessagePack map nor JSON text: {e}"
            ),
        })?;
    identified_json_body(json, doc_id, column)
}

/// A native object body that carries `doc_id` under `column`, still native.
fn identified_native_body(body: &[u8], doc_id: &str, column: &str) -> crate::Result<Vec<u8>> {
    let Value::Object(mut fields) =
        nodedb_types::object_msgpack::decode_object(body).map_err(|e| {
            crate::Error::BadRequest {
                detail: format!("document '{doc_id}': native body does not decode: {e}"),
            }
        })?
    else {
        return Err(crate::Error::BadRequest {
            detail: format!("document '{doc_id}': native body is not an object"),
        });
    };
    match fields.get(column) {
        Some(value) => check_body_id(value, doc_id, column)?,
        None => {
            fields.insert(column.to_string(), Value::String(doc_id.to_string()));
        }
    }
    zerompk::to_msgpack_vec(&Value::Object(fields)).map_err(|e| crate::Error::Serialization {
        format: "msgpack".into(),
        detail: format!("document '{doc_id}': native body encode failed: {e}"),
    })
}

/// A JSON object as a MessagePack map that carries `doc_id` under `column`.
///
/// A non-object is refused: a document's fields are a map. A body `column`
/// field that is not the string `doc_id` is refused. Numbers keep their exact
/// value: an unsigned integer above `i64::MAX` stays an unsigned MessagePack
/// integer.
pub(super) fn identified_json_body(
    json: serde_json::Value,
    doc_id: &str,
    column: &str,
) -> crate::Result<Vec<u8>> {
    let Some(object) = json.as_object() else {
        return Err(crate::Error::BadRequest {
            detail: format!("document '{doc_id}': body must be an object, got {json}"),
        });
    };
    if let Some(id) = object.get(column) {
        match id {
            serde_json::Value::String(s) if s == doc_id => {}
            other => {
                return Err(id_mismatch(doc_id, column, &other.to_string()));
            }
        }
    }
    let map = nodedb_types::json_to_msgpack(&json).map_err(|e| crate::Error::Serialization {
        format: "msgpack".into(),
        detail: format!("document '{doc_id}': body encode failed: {e}"),
    })?;
    Ok(nodedb_query::msgpack_scan::inject_str_field(
        &map, column, doc_id,
    ))
}

/// Refuse a decoded body identity that is not the string `doc_id`.
pub(super) fn check_body_id(value: &Value, doc_id: &str, column: &str) -> crate::Result<()> {
    match value {
        Value::String(s) if s == doc_id => Ok(()),
        other => Err(id_mismatch(doc_id, column, &format!("{other:?}"))),
    }
}

fn id_mismatch(doc_id: &str, column: &str, body_id: &str) -> crate::Error {
    crate::Error::BadRequest {
        detail: format!(
            "document '{doc_id}': body field '{column}' holds {body_id}, which differs from \
             the document id '{doc_id}'; '{column}' is the collection's identity column; \
             remove the field or set it to the document id"
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn decoded(body: &[u8]) -> Value {
        nodedb_types::value_from_msgpack(body).expect("stored body decodes")
    }

    fn field<'a>(value: &'a Value, name: &str) -> Option<&'a Value> {
        match value {
            Value::Object(map) => map.get(name),
            other => panic!("stored body must be a map, got {other:?}"),
        }
    }

    fn packed(entries: &[(&str, Value)]) -> Vec<u8> {
        let map = entries
            .iter()
            .map(|(k, v)| ((*k).to_string(), v.clone()))
            .collect();
        nodedb_types::value_to_msgpack(&Value::Object(map)).expect("encode")
    }

    fn assert_refused_naming_both(result: crate::Result<Vec<u8>>, body_id: &str) {
        match result {
            Err(crate::Error::BadRequest { detail }) => {
                assert!(detail.contains("'d1'"), "names the document id: {detail}");
                assert!(detail.contains(body_id), "names the body id: {detail}");
            }
            other => panic!("expected BadRequest, got {other:?}"),
        }
    }

    /// A native body keeps every field's exact type: a Decimal keeps its
    /// scale, and a UUID, an instant and bytes stay typed. It gains the
    /// document id and stays native. A native body naming another id is
    /// refused.
    #[test]
    fn a_native_body_keeps_its_types_and_gains_the_document_id() {
        let at = nodedb_types::NdbDateTime::from_micros(1_700_000_000_000_000);
        let fields: std::collections::HashMap<String, Value> = [
            (
                "price".to_string(),
                Value::Decimal("12.50".parse().unwrap()),
            ),
            (
                "uid".to_string(),
                Value::Uuid("550e8400-e29b-41d4-a716-446655440000".into()),
            ),
            ("at".to_string(), Value::DateTime(at)),
            ("blob".to_string(), Value::Bytes(vec![0, 255])),
        ]
        .into();
        let body = zerompk::to_msgpack_vec(&Value::Object(fields.clone())).unwrap();
        let stored = identified_body(&body, "d1", "id").expect("native body");
        assert!(crate::data::executor::response_codec::is_native_row(
            &stored
        ));
        let Value::Object(read) = nodedb_types::object_msgpack::decode_object(&stored).unwrap()
        else {
            panic!("object");
        };
        assert_eq!(read.get("id"), Some(&Value::String("d1".into())));
        for (name, value) in &fields {
            assert_eq!(read.get(name), Some(value), "field {name}");
        }

        let mut other = fields;
        other.insert("id".into(), Value::String("mine".into()));
        let body = zerompk::to_msgpack_vec(&Value::Object(other)).unwrap();
        assert_refused_naming_both(identified_body(&body, "d1", "id"), "mine");
    }

    #[test]
    fn the_identity_column_is_the_declared_key_else_id() {
        assert_eq!(identity_column(None), "id");
        assert_eq!(identity_column(Some("sku".into())), "sku");
    }

    #[test]
    fn json_body_gains_the_document_id() {
        let body = identified_body(br#"{"body":"hello"}"#, "d1", "id").expect("json object");
        let value = decoded(&body);
        assert_eq!(field(&value, "id"), Some(&Value::String("d1".into())));
        assert_eq!(field(&value, "body"), Some(&Value::String("hello".into())));
    }

    #[test]
    fn msgpack_body_gains_the_document_id() {
        let body = packed(&[("n", Value::Integer(7))]);
        let value = decoded(&identified_body(&body, "d2", "id").expect("msgpack map"));
        assert_eq!(field(&value, "id"), Some(&Value::String("d2".into())));
        assert_eq!(field(&value, "n"), Some(&Value::Integer(7)));
    }

    #[test]
    fn a_declared_key_carries_the_document_id_instead_of_id() {
        let body = identified_body(br#"{"name":"pen"}"#, "d1", "sku").expect("json object");
        let value = decoded(&body);
        assert_eq!(field(&value, "sku"), Some(&Value::String("d1".into())));
        assert_eq!(field(&value, "id"), None);

        let body = packed(&[("name", Value::String("pen".into()))]);
        let value = decoded(&identified_body(&body, "d1", "sku").expect("msgpack map"));
        assert_eq!(field(&value, "sku"), Some(&Value::String("d1".into())));
        assert_eq!(field(&value, "id"), None);
    }

    #[test]
    fn a_declared_key_naming_another_id_is_refused() {
        assert_refused_naming_both(identified_body(br#"{"sku":"other"}"#, "d1", "sku"), "other");
        let body = packed(&[("sku", Value::String("other".into()))]);
        assert_refused_naming_both(identified_body(&body, "d1", "sku"), "other");
        // `id` is an ordinary field once another column is the key.
        assert!(identified_body(br#"{"id":"x"}"#, "d1", "sku").is_ok());
    }

    #[test]
    fn a_body_naming_another_id_is_refused() {
        assert_refused_naming_both(identified_body(br#"{"id":"mine"}"#, "d1", "id"), "mine");
        assert_refused_naming_both(identified_body(br#"{"id":5}"#, "d1", "id"), "5");
        let body = packed(&[("id", Value::String("mine".into()))]);
        assert_refused_naming_both(identified_body(&body, "d1", "id"), "mine");
        let body = packed(&[("id", Value::Integer(5))]);
        assert_refused_naming_both(identified_body(&body, "d1", "id"), "5");
    }

    #[test]
    fn a_body_naming_its_own_id_is_accepted() {
        let body = identified_body(br#"{"id":"d1","n":1}"#, "d1", "id").expect("json object");
        assert_eq!(
            field(&decoded(&body), "id"),
            Some(&Value::String("d1".into()))
        );
        let body = packed(&[("id", Value::String("d1".into()))]);
        let stored = identified_body(&body, "d1", "id").expect("msgpack map");
        assert_eq!(
            field(&decoded(&stored), "id"),
            Some(&Value::String("d1".into()))
        );
    }

    #[test]
    fn a_u64_above_i64_max_keeps_its_number() {
        let body =
            identified_body(br#"{"big":18446744073709551615}"#, "d1", "id").expect("json object");
        assert_eq!(
            field(&decoded(&body), "big"),
            Some(&Value::Decimal(rust_decimal::Decimal::from(u64::MAX)))
        );
        let json = nodedb_types::json_from_msgpack(&body).expect("json read");
        assert_eq!(json["big"].as_u64(), Some(u64::MAX));
    }

    #[test]
    fn a_non_object_body_is_refused() {
        assert!(identified_body(b"[1,2]", "d1", "id").is_err());
        assert!(identified_body(b"", "d1", "id").is_err());
    }

    #[test]
    fn only_schemaless_documents_store_identified_bodies() {
        use nodedb_types::DocumentMode;
        assert!(stores_schemaless_bodies(None));
        assert!(stores_schemaless_bodies(Some(&CollectionType::Document(
            DocumentMode::Schemaless
        ))));
    }
}
