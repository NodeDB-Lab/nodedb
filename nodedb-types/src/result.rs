// SPDX-License-Identifier: Apache-2.0

//! Query result types returned by the `NodeDb` trait methods.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::id::{EdgeId, NodeId};
use crate::value::Value;

/// Result of a k-NN vector search.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SearchResult {
    /// Document/vector ID.
    pub id: String,
    /// Corresponding graph node ID (if the vector is linked to a node).
    pub node_id: Option<NodeId>,
    /// Distance from the query vector under the configured metric.
    pub distance: f32,
    /// Optional metadata fields attached to this vector.
    pub metadata: HashMap<String, Value>,
}

/// Result of a graph traversal (BFS/DFS).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SubGraph {
    /// Nodes discovered during traversal, keyed by node ID.
    pub nodes: Vec<SubGraphNode>,
    /// Edges traversed.
    pub edges: Vec<SubGraphEdge>,
}

/// A node in a traversal result.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SubGraphNode {
    /// Node identifier.
    pub id: NodeId,
    /// Depth at which this node was discovered (0 = start node).
    pub depth: u8,
    /// Optional properties attached to this node.
    pub properties: HashMap<String, Value>,
}

/// An edge in a traversal result.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SubGraphEdge {
    /// Edge identifier.
    pub id: EdgeId,
    /// Source node.
    pub from: NodeId,
    /// Target node.
    pub to: NodeId,
    /// Edge label/type (e.g., "KNOWS", "RELATES_TO").
    pub label: String,
    /// Optional properties attached to this edge.
    pub properties: HashMap<String, Value>,
}

/// Result of a SQL query or multi-modal query.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct QueryResult {
    /// Column names.
    pub columns: Vec<String>,
    /// Rows of values. Each row has one value per column.
    pub rows: Vec<Vec<Value>>,
    /// Number of rows affected (for INSERT/UPDATE/DELETE).
    pub rows_affected: u64,
    /// The statement's command verb (`INSERT`, `UPDATE`, `DELETE`, `UPSERT`,
    /// `MERGE`, `TRUNCATE`, ...). `None` when the statement produced no DML
    /// outcome.
    #[serde(default)]
    pub command: Option<String>,
}

impl QueryResult {
    /// Empty result set with no columns.
    pub fn empty() -> Self {
        Self {
            columns: Vec::new(),
            rows: Vec::new(),
            rows_affected: 0,
            command: None,
        }
    }

    /// Number of rows in the result set.
    pub fn row_count(&self) -> usize {
        self.rows.len()
    }

    /// Get a single row as a `HashMap<column_name, value>`.
    pub fn row_as_map(&self, index: usize) -> Option<HashMap<&str, &Value>> {
        let row = self.rows.get(index)?;
        Some(
            self.columns
                .iter()
                .zip(row.iter())
                .map(|(col, val)| (col.as_str(), val))
                .collect(),
        )
    }
}

impl SubGraph {
    /// Empty subgraph.
    pub fn empty() -> Self {
        Self {
            nodes: Vec::new(),
            edges: Vec::new(),
        }
    }

    /// Number of nodes in the subgraph.
    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

    /// Number of edges in the subgraph.
    pub fn edge_count(&self) -> usize {
        self.edges.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn search_result() {
        let r = SearchResult {
            id: "vec-1".into(),
            node_id: Some(NodeId::try_new("node-1").expect("test fixture")),
            distance: 0.123,
            metadata: HashMap::new(),
        };
        assert_eq!(r.id, "vec-1");
        assert!(r.distance < 1.0);
    }

    #[test]
    fn subgraph_construction() {
        let mut sg = SubGraph::empty();
        sg.nodes.push(SubGraphNode {
            id: NodeId::try_new("a").expect("test fixture"),
            depth: 0,
            properties: HashMap::new(),
        });
        sg.nodes.push(SubGraphNode {
            id: NodeId::try_new("b").expect("test fixture"),
            depth: 1,
            properties: HashMap::new(),
        });
        sg.edges.push(SubGraphEdge {
            id: EdgeId::try_first(
                NodeId::try_new("a").expect("test fixture"),
                NodeId::try_new("b").expect("test fixture"),
                "KNOWS",
            )
            .expect("test fixture"),
            from: NodeId::try_new("a").expect("test fixture"),
            to: NodeId::try_new("b").expect("test fixture"),
            label: "KNOWS".into(),
            properties: HashMap::new(),
        });
        assert_eq!(sg.node_count(), 2);
        assert_eq!(sg.edge_count(), 1);
    }

    #[test]
    fn query_result_row_as_map() {
        let qr = QueryResult {
            columns: vec!["name".into(), "age".into()],
            rows: vec![vec![Value::String("Alice".into()), Value::Integer(30)]],
            rows_affected: 0,
            command: None,
        };
        let row = qr.row_as_map(0).unwrap();
        assert_eq!(row["name"].as_str(), Some("Alice"));
        assert_eq!(row["age"].as_i64(), Some(30));
    }

    #[test]
    fn query_result_empty() {
        let qr = QueryResult::empty();
        assert_eq!(qr.row_count(), 0);
        assert!(qr.row_as_map(0).is_none());
    }
}
