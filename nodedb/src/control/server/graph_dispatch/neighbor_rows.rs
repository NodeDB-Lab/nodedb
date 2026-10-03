// SPDX-License-Identifier: BUSL-1.1

//! Decode of `NeighborsMulti` rows: `{src, label, node[, properties]}`.
//!
//! A local broadcast and a remote `dispatch_route` return the same msgpack
//! array, since the same op produces it on any node. A row carries no
//! orientation: `src` is the frontier node and `node` its neighbour in the
//! direction the hop ran.

use std::collections::HashMap;

use nodedb_types::Value;

/// One crossed edge as a hop row.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct NeighborRow {
    /// The frontier node the hop expanded.
    pub src: String,
    pub label: String,
    /// The neighbour reached.
    pub node: String,
    /// The edge's property object, when the hop returns properties.
    pub properties: Option<HashMap<String, Value>>,
}

impl NeighborRow {
    /// `(frontier node, label, neighbour)`.
    pub(crate) fn into_triple(self) -> (String, String, String) {
        (self.src, self.label, self.node)
    }
}

/// Decode raw Data-Plane response bytes into rows.
///
/// A payload that is not an array, or a row without a non-empty string
/// `src` and `node`, fails with a `Codec` error naming the row. Dropping it
/// returns a partial neighbor set as a complete one. `label` is "" when
/// absent: a label-less edge is a valid graph shape. `properties` is absent
/// or nil when the hop returns none, else a map.
pub(crate) fn decode_neighbor_rows(payload: &[u8]) -> crate::Result<Vec<NeighborRow>> {
    if payload.is_empty() {
        return Ok(Vec::new());
    }
    let Value::Array(items) = crate::data::executor::response_codec::decode_payload_value(payload)?
    else {
        return Err(crate::Error::Codec {
            detail: "graph neighbor rows: payload is not an array".into(),
        });
    };
    items
        .into_iter()
        .enumerate()
        .map(|(index, item)| decode_row(index, item))
        .collect()
}

fn decode_row(index: usize, item: Value) -> crate::Result<NeighborRow> {
    let mut fields = match item {
        Value::Object(fields) => fields,
        other => return Err(row_err(index, format!("is not a map: {other:?}"))),
    };
    let src = non_empty_string(&mut fields, "src")
        .ok_or_else(|| row_err(index, "lacks a non-empty string `src`".into()))?;
    let node = non_empty_string(&mut fields, "node")
        .ok_or_else(|| row_err(index, "lacks a non-empty string `node`".into()))?;
    let label = match fields.remove("label") {
        None | Some(Value::Null) => String::new(),
        Some(Value::String(label)) => label,
        Some(other) => {
            return Err(row_err(
                index,
                format!("has a non-string `label`: {other:?}"),
            ));
        }
    };
    let properties = match fields.remove("properties") {
        None | Some(Value::Null) => None,
        Some(Value::Object(properties)) => Some(properties),
        Some(other) => {
            return Err(row_err(
                index,
                format!("has `properties` that are not a map: {other:?}"),
            ));
        }
    };
    Ok(NeighborRow {
        src,
        label,
        node,
        properties,
    })
}

fn non_empty_string(fields: &mut HashMap<String, Value>, key: &str) -> Option<String> {
    match fields.remove(key) {
        Some(Value::String(s)) if !s.is_empty() => Some(s),
        _ => None,
    }
}

fn row_err(index: usize, what: String) -> crate::Error {
    crate::Error::Codec {
        detail: format!("graph neighbor row {index} {what}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn encode(rows: serde_json::Value) -> Vec<u8> {
        nodedb_types::json_msgpack::json_to_msgpack(&rows).expect("encode rows")
    }

    fn row(src: &str, label: &str, node: &str) -> NeighborRow {
        NeighborRow {
            src: src.into(),
            label: label.into(),
            node: node.into(),
            properties: None,
        }
    }

    #[test]
    fn well_formed_rows_decode_with_an_optional_label() {
        let rows = decode_neighbor_rows(&encode(serde_json::json!([
            {"src": "a", "label": "K", "node": "b"},
            {"src": "b", "node": "c"},
        ])))
        .unwrap();
        assert_eq!(rows, vec![row("a", "K", "b"), row("b", "", "c")]);
    }

    #[test]
    fn properties_decode_as_a_typed_map() {
        let rows = decode_neighbor_rows(&encode(serde_json::json!([
            {"src": "a", "label": "K", "node": "b", "properties": {"score": 9, "kind": "road"}},
            {"src": "a", "label": "K", "node": "c", "properties": null},
        ])))
        .unwrap();
        let properties = rows[0]
            .properties
            .as_ref()
            .expect("first row has properties");
        assert_eq!(properties.get("score"), Some(&Value::Integer(9)));
        assert_eq!(properties.get("kind"), Some(&Value::String("road".into())));
        assert_eq!(rows[1].properties, None);
    }

    #[test]
    fn a_payload_that_is_not_an_array_is_a_codec_error() {
        assert!(matches!(
            decode_neighbor_rows(&encode(serde_json::json!({"src": "a"}))),
            Err(crate::Error::Codec { .. })
        ));
    }

    #[test]
    fn a_malformed_row_is_a_codec_error_naming_the_row() {
        for rows in [
            serde_json::json!([{"src": "a", "node": "b"}, {"src": "a"}]),
            serde_json::json!([{"src": "a", "node": "b"}, {"src": "a", "node": "c", "label": 3}]),
            serde_json::json!([{"src": "a", "node": "b"}, {"src": "a", "node": "c", "properties": [1]}]),
            serde_json::json!([{"src": "a", "node": "b"}, "row"]),
        ] {
            match decode_neighbor_rows(&encode(rows)) {
                Err(crate::Error::Codec { detail }) => {
                    assert!(detail.contains("row 1"), "{detail}");
                }
                other => panic!("expected a codec error, got {other:?}"),
            }
        }
    }

    #[test]
    fn an_empty_node_id_is_a_codec_error() {
        assert!(matches!(
            decode_neighbor_rows(&encode(serde_json::json!([{"src": "", "node": "b"}]))),
            Err(crate::Error::Codec { .. })
        ));
    }

    #[test]
    fn an_empty_payload_is_no_neighbors() {
        assert!(decode_neighbor_rows(&[]).unwrap().is_empty());
    }
}
