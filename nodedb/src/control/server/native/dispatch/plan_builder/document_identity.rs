// SPDX-License-Identifier: BUSL-1.1

//! The stored body of a schemaless document written over the native protocol.
//!
//! SQL reads a schemaless row's identity from the body's `id` field. A body
//! with no `id` reads back with its surrogate as its id. A native write keys
//! the row by the document id it carries, so the stored body names that id.

use nodedb_types::{CollectionType, DEFAULT_IDENTITY_COLUMN, Value};

/// Whether a collection stores schemaless document bodies. A collection the
/// catalog does not hold defaults to a schemaless document collection.
pub(super) fn stores_schemaless_bodies(collection_type: Option<&CollectionType>) -> bool {
    collection_type.is_none_or(CollectionType::is_schemaless)
}

/// `body` as a MessagePack map that carries `doc_id` under `id`.
///
/// `body` is a MessagePack map or JSON object text. A body `id` that is not
/// the string `doc_id` is refused: the row is keyed by `doc_id`, so SQL and
/// key reads would disagree on which document the row is.
pub(super) fn identified_body(body: &[u8], doc_id: &str) -> crate::Result<Vec<u8>> {
    if nodedb_query::msgpack_scan::map_header(body, 0).is_some() {
        if let Some((start, end)) =
            nodedb_query::msgpack_scan::extract_field(body, 0, DEFAULT_IDENTITY_COLUMN)
        {
            let cell = body
                .get(start..end)
                .ok_or_else(|| crate::Error::BadRequest {
                    detail: format!("document '{doc_id}': body field 'id' is truncated"),
                })?;
            let value =
                nodedb_types::value_from_msgpack(cell).map_err(|e| crate::Error::BadRequest {
                    detail: format!("document '{doc_id}': body field 'id' does not decode: {e}"),
                })?;
            check_body_id(&value, doc_id)?;
            return Ok(body.to_vec());
        }
        return Ok(nodedb_query::msgpack_scan::inject_str_field(
            body,
            DEFAULT_IDENTITY_COLUMN,
            doc_id,
        ));
    }
    let json: serde_json::Value =
        sonic_rs::from_slice(body).map_err(|e| crate::Error::BadRequest {
            detail: format!(
                "document '{doc_id}': body is neither a MessagePack map nor JSON text: {e}"
            ),
        })?;
    identified_json_body(json, doc_id)
}

/// A JSON object as a MessagePack map that carries `doc_id` under `id`.
///
/// A non-object is refused: a document's fields are a map. A body `id` that
/// is not the string `doc_id` is refused. Numbers keep their exact value: an
/// unsigned integer above `i64::MAX` stays an unsigned MessagePack integer.
pub(super) fn identified_json_body(
    json: serde_json::Value,
    doc_id: &str,
) -> crate::Result<Vec<u8>> {
    let Some(object) = json.as_object() else {
        return Err(crate::Error::BadRequest {
            detail: format!("document '{doc_id}': body must be an object, got {json}"),
        });
    };
    if let Some(id) = object.get(DEFAULT_IDENTITY_COLUMN) {
        match id {
            serde_json::Value::String(s) if s == doc_id => {}
            other => {
                return Err(id_mismatch(doc_id, &other.to_string()));
            }
        }
    }
    let map = nodedb_types::json_to_msgpack(&json).map_err(|e| crate::Error::Serialization {
        format: "msgpack".into(),
        detail: format!("document '{doc_id}': body encode failed: {e}"),
    })?;
    Ok(nodedb_query::msgpack_scan::inject_str_field(
        &map,
        DEFAULT_IDENTITY_COLUMN,
        doc_id,
    ))
}

/// Refuse a decoded body `id` that is not the string `doc_id`.
fn check_body_id(value: &Value, doc_id: &str) -> crate::Result<()> {
    match value {
        Value::String(s) if s == doc_id => Ok(()),
        other => Err(id_mismatch(doc_id, &format!("{other:?}"))),
    }
}

fn id_mismatch(doc_id: &str, body_id: &str) -> crate::Error {
    crate::Error::BadRequest {
        detail: format!(
            "document '{doc_id}': body field 'id' holds {body_id}, which differs from the \
             document id '{doc_id}'; remove the field or set it to the document id"
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

    #[test]
    fn json_body_gains_the_document_id() {
        let body = identified_body(br#"{"body":"hello"}"#, "d1").expect("json object");
        let value = decoded(&body);
        assert_eq!(field(&value, "id"), Some(&Value::String("d1".into())));
        assert_eq!(field(&value, "body"), Some(&Value::String("hello".into())));
    }

    #[test]
    fn msgpack_body_gains_the_document_id() {
        let body = packed(&[("n", Value::Integer(7))]);
        let value = decoded(&identified_body(&body, "d2").expect("msgpack map"));
        assert_eq!(field(&value, "id"), Some(&Value::String("d2".into())));
        assert_eq!(field(&value, "n"), Some(&Value::Integer(7)));
    }

    #[test]
    fn a_body_naming_another_id_is_refused() {
        assert_refused_naming_both(identified_body(br#"{"id":"mine"}"#, "d1"), "mine");
        assert_refused_naming_both(identified_body(br#"{"id":5}"#, "d1"), "5");
        let body = packed(&[("id", Value::String("mine".into()))]);
        assert_refused_naming_both(identified_body(&body, "d1"), "mine");
        let body = packed(&[("id", Value::Integer(5))]);
        assert_refused_naming_both(identified_body(&body, "d1"), "5");
    }

    #[test]
    fn a_body_naming_its_own_id_is_accepted() {
        let body = identified_body(br#"{"id":"d1","n":1}"#, "d1").expect("json object");
        assert_eq!(
            field(&decoded(&body), "id"),
            Some(&Value::String("d1".into()))
        );
        let body = packed(&[("id", Value::String("d1".into()))]);
        let stored = identified_body(&body, "d1").expect("msgpack map");
        assert_eq!(
            field(&decoded(&stored), "id"),
            Some(&Value::String("d1".into()))
        );
    }

    #[test]
    fn a_u64_above_i64_max_keeps_its_number() {
        let body = identified_body(br#"{"big":18446744073709551615}"#, "d1").expect("json object");
        assert_eq!(
            field(&decoded(&body), "big"),
            Some(&Value::Decimal(rust_decimal::Decimal::from(u64::MAX)))
        );
        let json = nodedb_types::json_from_msgpack(&body).expect("json read");
        assert_eq!(json["big"].as_u64(), Some(u64::MAX));
    }

    #[test]
    fn a_non_object_body_is_refused() {
        assert!(identified_body(b"[1,2]", "d1").is_err());
        assert!(identified_body(b"", "d1").is_err());
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
