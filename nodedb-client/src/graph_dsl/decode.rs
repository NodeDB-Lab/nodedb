// SPDX-License-Identifier: Apache-2.0

//! Strict decoders for the `GRAPH TRAVERSE` and `GRAPH PATH` results. Both
//! clients use them.
//!
//! The server answers either statement with one `result` column holding one
//! JSON text cell:
//!
//! - `GRAPH TRAVERSE`: `{"nodes": [{"id", "depth"}], "edges": [{"from",
//!   "to", "label"[, "properties"]}]}`.
//! - `GRAPH PATH`: `["src", …, "dst"]`, or `[]` when no path exists.
//!
//! Any other shape is an error naming what is wrong. A dropped row would
//! return a partial subgraph as a complete one.

use std::collections::HashMap;

use nodedb_types::error::{NodeDbError, NodeDbResult};
use nodedb_types::id::{EdgeId, NodeId};
use nodedb_types::result::{SubGraph, SubGraphEdge, SubGraphNode};
use nodedb_types::value::Value;
use serde_json::{Map, Value as Json};

/// Decode a `GRAPH TRAVERSE` result.
pub(crate) fn decode_traverse_result(
    columns: &[String],
    rows: &[Vec<Value>],
) -> NodeDbResult<SubGraph> {
    let json = result_json(columns, rows, "GRAPH TRAVERSE")?;
    let Json::Object(mut top) = json else {
        return Err(malformed("GRAPH TRAVERSE result is not a JSON object"));
    };
    let nodes = take_array(&mut top, "nodes")?
        .into_iter()
        .enumerate()
        .map(|(index, node)| decode_node(index, node))
        .collect::<NodeDbResult<Vec<_>>>()?;
    let edges = take_array(&mut top, "edges")?
        .into_iter()
        .enumerate()
        .map(|(index, edge)| decode_edge(index, edge))
        .collect::<NodeDbResult<Vec<_>>>()?;
    Ok(SubGraph { nodes, edges })
}

/// Decode a `GRAPH PATH` result. `None` when no path exists.
pub(crate) fn decode_path_result(
    columns: &[String],
    rows: &[Vec<Value>],
) -> NodeDbResult<Option<Vec<NodeId>>> {
    let json = result_json(columns, rows, "GRAPH PATH")?;
    let Json::Array(items) = json else {
        return Err(malformed("GRAPH PATH result is not a JSON array"));
    };
    if items.is_empty() {
        return Ok(None);
    }
    items
        .into_iter()
        .enumerate()
        .map(|(index, item)| match item {
            Json::String(id) => node_id(id, &format!("GRAPH PATH node {index}")),
            other => Err(malformed(format!(
                "GRAPH PATH node {index} is not a string: {other}"
            ))),
        })
        .collect::<NodeDbResult<Vec<_>>>()
        .map(Some)
}

/// The JSON text of the one `result` cell.
fn result_json(columns: &[String], rows: &[Vec<Value>], statement: &str) -> NodeDbResult<Json> {
    if columns.len() != 1 || columns[0] != "result" {
        return Err(malformed(format!(
            "{statement} answered columns {columns:?}, expected [\"result\"]"
        )));
    }
    let [row] = rows else {
        return Err(malformed(format!(
            "{statement} answered {} rows, expected 1",
            rows.len()
        )));
    };
    let [Value::String(text)] = row.as_slice() else {
        return Err(malformed(format!(
            "{statement} result row is not one text cell: {row:?}"
        )));
    };
    sonic_rs::from_str(text).map_err(|e| malformed(format!("{statement} result is not JSON: {e}")))
}

fn take_array(top: &mut Map<String, Json>, key: &str) -> NodeDbResult<Vec<Json>> {
    match top.remove(key) {
        Some(Json::Array(items)) => Ok(items),
        Some(other) => Err(malformed(format!(
            "GRAPH TRAVERSE `{key}` is not an array: {other}"
        ))),
        None => Err(malformed(format!("GRAPH TRAVERSE result lacks `{key}`"))),
    }
}

fn decode_node(index: usize, node: Json) -> NodeDbResult<SubGraphNode> {
    let what = format!("GRAPH TRAVERSE node {index}");
    let Json::Object(mut fields) = node else {
        return Err(malformed(format!("{what} is not an object")));
    };
    let id = node_id(take_string(&mut fields, "id", &what)?, &what)?;
    let depth = match fields.remove("depth") {
        Some(Json::Number(n)) => n
            .as_u64()
            .and_then(|d| u8::try_from(d).ok())
            .ok_or_else(|| malformed(format!("{what} depth {n} is not an integer in 0..=255")))?,
        Some(other) => {
            return Err(malformed(format!("{what} depth is not a number: {other}")));
        }
        None => return Err(malformed(format!("{what} lacks `depth`"))),
    };
    Ok(SubGraphNode {
        id,
        depth,
        properties: HashMap::new(),
    })
}

fn decode_edge(index: usize, edge: Json) -> NodeDbResult<SubGraphEdge> {
    let what = format!("GRAPH TRAVERSE edge {index}");
    let Json::Object(mut fields) = edge else {
        return Err(malformed(format!("{what} is not an object")));
    };
    let from = node_id(take_string(&mut fields, "from", &what)?, &what)?;
    let to = node_id(take_string(&mut fields, "to", &what)?, &what)?;
    let label = take_string(&mut fields, "label", &what)?;
    let properties = match fields.remove("properties") {
        None => HashMap::new(),
        Some(Json::Object(properties)) => properties
            .into_iter()
            .map(|(name, value)| (name, Value::from(value)))
            .collect(),
        Some(other) => {
            return Err(malformed(format!(
                "{what} properties are not an object: {other}"
            )));
        }
    };
    let id = EdgeId::try_first(from.clone(), to.clone(), label.clone()).map_err(|e| {
        malformed(format!(
            "{what} label '{label}' is not a valid edge label: {e}"
        ))
    })?;
    Ok(SubGraphEdge {
        id,
        from,
        to,
        label,
        properties,
    })
}

fn take_string(fields: &mut Map<String, Json>, key: &str, what: &str) -> NodeDbResult<String> {
    match fields.remove(key) {
        Some(Json::String(s)) => Ok(s),
        Some(other) => Err(malformed(format!(
            "{what} `{key}` is not a string: {other}"
        ))),
        None => Err(malformed(format!("{what} lacks `{key}`"))),
    }
}

fn node_id(id: String, what: &str) -> NodeDbResult<NodeId> {
    NodeId::try_new(id.clone())
        .map_err(|e| malformed(format!("{what} id '{id}' is not a valid node id: {e}")))
}

fn malformed(detail: impl std::fmt::Display) -> NodeDbError {
    NodeDbError::serialization("json", detail)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn result(json: &str) -> (Vec<String>, Vec<Vec<Value>>) {
        (
            vec!["result".to_string()],
            vec![vec![Value::String(json.to_string())]],
        )
    }

    fn traverse(json: &str) -> NodeDbResult<SubGraph> {
        let (columns, rows) = result(json);
        decode_traverse_result(&columns, &rows)
    }

    fn path(json: &str) -> NodeDbResult<Option<Vec<NodeId>>> {
        let (columns, rows) = result(json);
        decode_path_result(&columns, &rows)
    }

    #[test]
    fn a_well_formed_traversal_decodes_with_properties() {
        let sg = traverse(
            r#"{"nodes":[{"id":"a","depth":0},{"id":"b","depth":1}],
                "edges":[{"from":"a","to":"b","label":"KNOWS","properties":{"since":2020,"w":0.5}},
                         {"from":"b","to":"a","label":"KNOWS"}]}"#,
        )
        .expect("decodes");
        assert_eq!(sg.nodes.len(), 2);
        assert_eq!(sg.nodes[1].id.as_str(), "b");
        assert_eq!(sg.nodes[1].depth, 1);
        assert_eq!(sg.edges.len(), 2);
        assert_eq!(sg.edges[0].label, "KNOWS");
        assert_eq!(sg.edges[0].from.as_str(), "a");
        assert_eq!(
            sg.edges[0].properties.get("since"),
            Some(&Value::Integer(2020))
        );
        assert_eq!(sg.edges[0].properties.get("w"), Some(&Value::Float(0.5)));
        assert!(sg.edges[1].properties.is_empty());
    }

    #[test]
    fn an_absent_start_decodes_as_the_empty_subgraph() {
        let sg = traverse(r#"{"nodes":[],"edges":[]}"#).expect("decodes");
        assert!(sg.nodes.is_empty());
        assert!(sg.edges.is_empty());
    }

    #[test]
    fn every_malformed_traversal_shape_is_an_error() {
        for json in [
            r#"[]"#,
            r#"{"edges":[]}"#,
            r#"{"nodes":[]}"#,
            r#"{"nodes":{},"edges":[]}"#,
            r#"{"nodes":["a"],"edges":[]}"#,
            r#"{"nodes":[{"depth":0}],"edges":[]}"#,
            r#"{"nodes":[{"id":7,"depth":0}],"edges":[]}"#,
            r#"{"nodes":[{"id":"","depth":0}],"edges":[]}"#,
            r#"{"nodes":[{"id":"a"}],"edges":[]}"#,
            r#"{"nodes":[{"id":"a","depth":1.5}],"edges":[]}"#,
            r#"{"nodes":[{"id":"a","depth":-1}],"edges":[]}"#,
            r#"{"nodes":[{"id":"a","depth":256}],"edges":[]}"#,
            r#"{"nodes":[],"edges":[{"to":"b","label":"L"}]}"#,
            r#"{"nodes":[],"edges":[{"from":"a","label":"L"}]}"#,
            r#"{"nodes":[],"edges":[{"from":"a","to":"b"}]}"#,
            r#"{"nodes":[],"edges":[{"from":"a","to":"b","label":3}]}"#,
            r#"{"nodes":[],"edges":[{"from":"a","to":"b","label":""}]}"#,
            r#"{"nodes":[],"edges":[{"from":"a","to":"b","label":"L","properties":[1]}]}"#,
            "not json",
        ] {
            assert!(traverse(json).is_err(), "{json} must be refused");
        }
    }

    #[test]
    fn a_malformed_row_names_its_index() {
        let error = traverse(r#"{"nodes":[{"id":"a","depth":0},{"id":"b"}],"edges":[]}"#)
            .expect_err("node 1 lacks depth");
        assert!(error.to_string().contains("node 1"), "{error}");
        let error = traverse(
            r#"{"nodes":[],"edges":[{"from":"a","to":"b","label":"L"},{"from":"a","to":"b"}]}"#,
        )
        .expect_err("edge 1 lacks a label");
        assert!(error.to_string().contains("edge 1"), "{error}");
    }

    #[test]
    fn the_result_column_shape_is_checked() {
        let rows = vec![vec![Value::String("{}".into())]];
        assert!(decode_traverse_result(&["node_id".to_string()], &rows).is_err());
        assert!(decode_traverse_result(&["result".to_string()], &[]).is_err());
        assert!(
            decode_traverse_result(&["result".to_string()], &[vec![Value::Integer(1)]]).is_err()
        );
        assert!(decode_traverse_result(&[], &[]).is_err());
    }

    #[test]
    fn a_path_decodes_and_an_empty_path_is_none() {
        let found = path(r#"["a","b","c"]"#).expect("decodes").expect("a path");
        let ids: Vec<&str> = found.iter().map(NodeId::as_str).collect();
        assert_eq!(ids, vec!["a", "b", "c"]);
        assert_eq!(path("[]").expect("decodes"), None);
    }

    #[test]
    fn a_malformed_path_is_an_error() {
        for json in [r#"{"path":[]}"#, r#"["a",1]"#, r#"["a",""]"#, "nope"] {
            assert!(path(json).is_err(), "{json} must be refused");
        }
    }
}
