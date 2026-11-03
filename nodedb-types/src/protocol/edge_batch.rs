// SPDX-License-Identifier: Apache-2.0

//! The `EdgePutBatch` / `EdgeDeleteBatch` request payloads and the per
//! statement edge caps every edge-batch surface shares.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::id::NodeId;
use crate::value::Value;

/// The most edges one edge write carries: one `GRAPH INSERT EDGE` or
/// `GRAPH DELETE EDGE` statement, or one native edge-batch request. A
/// larger write is refused whole, never cut down.
pub const MAX_EDGES_PER_STATEMENT: usize = 1000;

/// The most bytes of SQL text one `GRAPH INSERT EDGE` or `GRAPH DELETE EDGE`
/// statement carries. A longer statement is refused before it is parsed.
pub const MAX_EDGE_STATEMENT_SQL_BYTES: usize = 1024 * 1024;

/// The refusal text for an edge write of `count` edges over
/// [`MAX_EDGES_PER_STATEMENT`], or `None` when `count` is within the cap.
/// `statement` names the write, such as `GRAPH INSERT EDGE`.
pub fn edge_batch_over_cap(statement: &str, count: usize) -> Option<String> {
    (count > MAX_EDGES_PER_STATEMENT).then(|| {
        format!(
            "{statement} carries {count} edges; the cap is {MAX_EDGES_PER_STATEMENT} edges per \
             statement; split the batch into statements of at most {MAX_EDGES_PER_STATEMENT} edges"
        )
    })
}

/// One edge to insert. An empty `properties` map stores no properties.
/// `valid_from_ms: None` takes the write's wall time. `valid_until_ms: None`
/// leaves the valid time open, and needs `valid_from_ms` when set.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct EdgeWrite {
    pub src: NodeId,
    pub dst: NodeId,
    pub label: String,
    pub properties: HashMap<String, Value>,
    pub valid_from_ms: Option<i64>,
    pub valid_until_ms: Option<i64>,
}

impl EdgeWrite {
    /// An edge `src -> dst` labelled `label`, with no properties and the
    /// write's own valid time.
    pub fn new(src: NodeId, dst: NodeId, label: impl Into<String>) -> Self {
        Self {
            src,
            dst,
            label: label.into(),
            properties: HashMap::new(),
            valid_from_ms: None,
            valid_until_ms: None,
        }
    }
}

/// One edge to delete, named by its endpoints and label.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct EdgeKey {
    pub src: NodeId,
    pub dst: NodeId,
    pub label: String,
}

/// The `payload` of an `EdgePutBatch` request. The request's `collection`
/// names the collection every edge goes into.
#[derive(
    Debug,
    Clone,
    Default,
    PartialEq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct EdgePutBatch {
    pub edges: Vec<EdgeWrite>,
}

/// The `payload` of an `EdgeDeleteBatch` request. The request's
/// `collection` names the collection every edge is deleted from.
#[derive(
    Debug,
    Clone,
    Default,
    PartialEq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct EdgeDeleteBatch {
    pub edges: Vec<EdgeKey>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node(id: &str) -> NodeId {
        NodeId::try_new(id).expect("valid node id")
    }

    #[test]
    fn put_batch_roundtrips() {
        let mut edge = EdgeWrite::new(node("a"), node("b"), "knows");
        edge.properties.insert("w".into(), Value::Integer(3));
        edge.valid_from_ms = Some(5);
        edge.valid_until_ms = Some(9);
        let batch = EdgePutBatch { edges: vec![edge] };
        let bytes = zerompk::to_msgpack_vec(&batch).expect("encode");
        let back: EdgePutBatch = zerompk::from_msgpack(&bytes).expect("decode");
        assert_eq!(back, batch);
    }

    #[test]
    fn delete_batch_roundtrips() {
        let batch = EdgeDeleteBatch {
            edges: vec![EdgeKey {
                src: node("a"),
                dst: node("b"),
                label: "knows".into(),
            }],
        };
        let bytes = zerompk::to_msgpack_vec(&batch).expect("encode");
        let back: EdgeDeleteBatch = zerompk::from_msgpack(&bytes).expect("decode");
        assert_eq!(back, batch);
    }

    #[test]
    fn the_cap_admits_its_own_size_and_refuses_one_more() {
        assert_eq!(
            edge_batch_over_cap("GRAPH INSERT EDGE", MAX_EDGES_PER_STATEMENT),
            None
        );
        let refusal = edge_batch_over_cap("GRAPH DELETE EDGE", MAX_EDGES_PER_STATEMENT + 1)
            .expect("one edge over the cap is refused");
        assert!(refusal.contains("GRAPH DELETE EDGE"), "{refusal}");
        assert!(refusal.contains("1001 edges"), "{refusal}");
        assert!(refusal.contains("cap is 1000"), "{refusal}");
        assert!(refusal.contains("split the batch"), "{refusal}");
    }
}
