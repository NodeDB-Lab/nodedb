// SPDX-License-Identifier: BUSL-1.1

//! Validation and normalization of the clauses an edge write carries.
//!
//! Split from the handlers so the label guard and the `PROPERTIES` encoding
//! live next to each other rather than at either end of the dispatch flow.

use nodedb_sql::ddl_ast::GraphProperties;

use super::super::super::result::DdlError;
use super::support::ddl_err;

/// Maximum byte length for an edge label string. Keeps a single `TYPE`
/// clause from bloating the CSR label table and the msgpack wire payload.
const MAX_EDGE_LABEL_BYTES: usize = 256;

/// Validate a user-supplied edge label. Rejects empty, overlong, and
/// labels containing ASCII control characters (0x00..=0x1F, 0x7F).
///
/// Runs at every DSL ingress so the CSR interner never sees degenerate
/// input — a complement to the `u32` widening of the label id space.
pub(super) fn validate_edge_label(label: &str) -> Result<(), DdlError> {
    if label.is_empty() {
        return Err(ddl_err("42601", "edge TYPE label must not be empty"));
    }
    if label.len() > MAX_EDGE_LABEL_BYTES {
        return Err(ddl_err(
            "42601",
            format!(
                "edge TYPE label is {} bytes; maximum is {MAX_EDGE_LABEL_BYTES}",
                label.len()
            ),
        ));
    }
    if label.chars().any(|c| c.is_control() || c == '\u{007F}') {
        return Err(ddl_err(
            "42601",
            "edge TYPE label must not contain control characters",
        ));
    }
    Ok(())
}

/// Convert a parsed `PROPERTIES` clause to the plain-MessagePack map stored
/// in `GraphOp::EdgePut`. Every edge-property writer and reader uses this
/// encoding. Object-literal forms go through the shared
/// `nodedb_sql::parser::object_literal::parse_object_literal_complete`
/// so the type coercions (numbers, bools, nested objects) match
/// every other object-literal ingress (INSERT { ... }, UPSERT).
pub(super) fn properties_to_msgpack(properties: GraphProperties) -> Result<Vec<u8>, DdlError> {
    match properties {
        GraphProperties::None => Ok(Vec::new()),
        GraphProperties::Quoted(text) => {
            let json: serde_json::Value = sonic_rs::from_str(&text).map_err(|e| {
                ddl_err(
                    "42601",
                    format!("PROPERTIES '{text}' is not valid JSON: {e}"),
                )
            })?;
            if !json.is_object() {
                return Err(ddl_err(
                    "42601",
                    format!("PROPERTIES '{text}' must be a JSON object"),
                ));
            }
            nodedb_types::json_msgpack::json_to_msgpack(&json)
                .map_err(|e| DdlError::internal(format!("PROPERTIES encode error: {e}")))
        }
        GraphProperties::Object(obj_str) => {
            // The graph lexer hands over the balanced `{ … }` and nothing else,
            // so the strict form is the right contract: anything trailing means
            // the statement was misparsed upstream, and saying so beats
            // persisting an edge whose properties silently lost a clause.
            match nodedb_sql::parser::object_literal::parse_object_literal_complete(&obj_str) {
                Some(Ok(fields)) => nodedb_types::json_msgpack::value_to_msgpack(
                    &nodedb_types::Value::Object(fields),
                )
                .map_err(|e| DdlError::internal(format!("PROPERTIES encode error: {e}"))),
                Some(Err(msg)) => Err(ddl_err(
                    "42601",
                    format!("PROPERTIES object literal error: {msg}"),
                )),
                // The graph lexer emits an object token only for text that
                // opens with `{`. Text that does not is a lexer defect, and
                // storing no properties for it would lose the clause.
                None => Err(DdlError::internal(format!(
                    "PROPERTIES object token '{obj_str}' does not open with '{{'"
                ))),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_types::Value;

    fn decode(bytes: &[u8]) -> Value {
        nodedb_types::json_msgpack::value_from_msgpack(bytes).expect("plain msgpack")
    }

    #[test]
    fn quoted_json_object_stores_plain_msgpack() {
        let bytes = properties_to_msgpack(GraphProperties::Quoted(
            r#"{"weight": 2.5, "kind": "road"}"#.into(),
        ))
        .expect("encode");
        let Value::Object(fields) = decode(&bytes) else {
            panic!("properties must decode as a map");
        };
        assert_eq!(fields.get("weight"), Some(&Value::Float(2.5)));
        assert_eq!(fields.get("kind"), Some(&Value::String("road".into())));
        assert_eq!(
            nodedb_graph::csr::weights::extract_weight_from_properties(&bytes),
            2.5
        );
    }

    #[test]
    fn object_literal_stores_plain_msgpack() {
        let bytes =
            properties_to_msgpack(GraphProperties::Object("{weight: 4, owner: 'x'}".into()))
                .expect("encode");
        let Value::Object(fields) = decode(&bytes) else {
            panic!("properties must decode as a map");
        };
        assert_eq!(fields.get("owner"), Some(&Value::String("x".into())));
        assert_eq!(
            nodedb_graph::csr::weights::extract_weight_from_properties(&bytes),
            4.0
        );
    }

    #[test]
    fn absent_properties_store_nothing() {
        assert!(
            properties_to_msgpack(GraphProperties::None)
                .expect("encode")
                .is_empty()
        );
    }

    #[test]
    fn an_object_token_without_an_opening_brace_is_an_internal_error() {
        assert!(properties_to_msgpack(GraphProperties::Object("weight: 4".into())).is_err());
    }

    #[test]
    fn quoted_non_object_is_rejected() {
        assert!(properties_to_msgpack(GraphProperties::Quoted("[1, 2]".into())).is_err());
        assert!(properties_to_msgpack(GraphProperties::Quoted("not json".into())).is_err());
    }
}
