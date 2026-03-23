//! High-level native protocol client implementing the `NodeDb` trait.
//!
//! Wraps a connection pool and translates trait calls into native protocol
//! opcodes. Also exposes SQL/DDL methods not covered by the trait.

use std::collections::HashMap;

use async_trait::async_trait;

use nodedb_types::document::Document;
use nodedb_types::error::{NodeDbError, NodeDbResult};
use nodedb_types::filter::{EdgeFilter, MetadataFilter};
use nodedb_types::id::{EdgeId, NodeId};
use nodedb_types::protocol::{OpCode, TextFields};
use nodedb_types::result::{QueryResult, SearchResult, SubGraph};
use nodedb_types::value::Value;

use super::pool::{Pool, PoolConfig};
use crate::traits::NodeDb;

/// Native protocol client for NodeDB.
///
/// Connects via the binary MessagePack protocol. Supports all operations:
/// SQL, DDL, direct Data Plane ops, transactions, session parameters.
pub struct NativeClient {
    pool: Pool,
}

impl NativeClient {
    /// Create a client with the given pool configuration.
    pub fn new(config: PoolConfig) -> Self {
        Self {
            pool: Pool::new(config),
        }
    }

    /// Connect to a NodeDB server with default settings.
    pub fn connect(addr: &str) -> Self {
        Self::new(PoolConfig {
            addr: addr.to_string(),
            ..Default::default()
        })
    }

    // ─── SQL/DDL (beyond the NodeDb trait) ──────────────────────

    /// Execute a SQL query and return structured results.
    ///
    /// Retries once with a fresh connection on I/O failure.
    pub async fn query(&self, sql: &str) -> NodeDbResult<QueryResult> {
        let mut conn = self.pool.acquire().await?;
        match conn.execute_sql(sql).await {
            Ok(r) => Ok(r),
            Err(e) if is_connection_error(&e) => {
                drop(conn);
                let mut conn = self.pool.acquire().await?;
                conn.execute_sql(sql).await
            }
            Err(e) => Err(e),
        }
    }

    /// Execute a DDL command.
    pub async fn ddl(&self, sql: &str) -> NodeDbResult<QueryResult> {
        let mut conn = self.pool.acquire().await?;
        match conn.execute_ddl(sql).await {
            Ok(r) => Ok(r),
            Err(e) if is_connection_error(&e) => {
                drop(conn);
                let mut conn = self.pool.acquire().await?;
                conn.execute_ddl(sql).await
            }
            Err(e) => Err(e),
        }
    }

    /// Begin a transaction.
    pub async fn begin(&self) -> NodeDbResult<()> {
        let mut conn = self.pool.acquire().await?;
        conn.begin().await
    }

    /// Commit the current transaction.
    pub async fn commit(&self) -> NodeDbResult<()> {
        let mut conn = self.pool.acquire().await?;
        conn.commit().await
    }

    /// Rollback the current transaction.
    pub async fn rollback(&self) -> NodeDbResult<()> {
        let mut conn = self.pool.acquire().await?;
        conn.rollback().await
    }

    /// Set a session parameter.
    pub async fn set_parameter(&self, key: &str, value: &str) -> NodeDbResult<()> {
        let mut conn = self.pool.acquire().await?;
        conn.set_parameter(key, value).await
    }

    /// Show a session parameter.
    pub async fn show_parameter(&self, key: &str) -> NodeDbResult<String> {
        let mut conn = self.pool.acquire().await?;
        conn.show_parameter(key).await
    }

    /// Ping the server.
    pub async fn ping(&self) -> NodeDbResult<()> {
        let mut conn = self.pool.acquire().await?;
        conn.ping().await
    }
}

#[async_trait]
impl NodeDb for NativeClient {
    async fn vector_search(
        &self,
        collection: &str,
        query: &[f32],
        k: usize,
        _filter: Option<&MetadataFilter>,
    ) -> NodeDbResult<Vec<SearchResult>> {
        let mut conn = self.pool.acquire().await?;
        let resp = conn
            .send(
                OpCode::VectorSearch,
                TextFields {
                    collection: Some(collection.to_string()),
                    query_vector: Some(query.to_vec()),
                    top_k: Some(k as u64),
                    ..Default::default()
                },
            )
            .await?;

        // Parse result JSON from the "result" column into SearchResult.
        parse_search_results(&resp)
    }

    async fn vector_insert(
        &self,
        collection: &str,
        id: &str,
        embedding: &[f32],
        metadata: Option<Document>,
    ) -> NodeDbResult<()> {
        // Use SQL INSERT for vector insert (goes through DataFusion pipeline).
        let meta_json = metadata
            .map(|d| {
                let obj: HashMap<String, Value> = d.fields;
                serde_json::to_string(&obj).unwrap_or_else(|_| "{}".into())
            })
            .unwrap_or_else(|| "{}".into());
        let arr_str = format_f32_array(embedding);
        let sql = format!(
            "INSERT INTO {collection} (id, embedding, metadata) VALUES ('{id}', {arr_str}, '{meta_json}')"
        );
        let mut conn = self.pool.acquire().await?;
        conn.execute_sql(&sql).await?;
        Ok(())
    }

    async fn vector_delete(&self, collection: &str, id: &str) -> NodeDbResult<()> {
        let sql = format!("DELETE FROM {collection} WHERE id = '{id}'");
        let mut conn = self.pool.acquire().await?;
        conn.execute_sql(&sql).await?;
        Ok(())
    }

    async fn graph_traverse(
        &self,
        start: &NodeId,
        depth: u8,
        edge_filter: Option<&EdgeFilter>,
    ) -> NodeDbResult<SubGraph> {
        let mut conn = self.pool.acquire().await?;
        let resp = conn
            .send(
                OpCode::GraphHop,
                TextFields {
                    start_node: Some(start.as_str().to_string()),
                    depth: Some(depth as u64),
                    edge_label: edge_filter.and_then(|f| f.labels.first().cloned()),
                    ..Default::default()
                },
            )
            .await?;
        parse_subgraph_response(&resp)
    }

    async fn graph_insert_edge(
        &self,
        from: &NodeId,
        to: &NodeId,
        edge_type: &str,
        properties: Option<Document>,
    ) -> NodeDbResult<EdgeId> {
        let props_json = properties.and_then(|d| serde_json::to_value(d.fields).ok());
        let mut conn = self.pool.acquire().await?;
        conn.send(
            OpCode::EdgePut,
            TextFields {
                from_node: Some(from.as_str().to_string()),
                to_node: Some(to.as_str().to_string()),
                edge_type: Some(edge_type.to_string()),
                properties: props_json,
                ..Default::default()
            },
        )
        .await?;
        Ok(EdgeId::from_components(
            from.as_str(),
            to.as_str(),
            edge_type,
        ))
    }

    async fn graph_delete_edge(&self, edge_id: &EdgeId) -> NodeDbResult<()> {
        // Parse edge ID format: "src--label-->dst"
        let parts: Vec<&str> = edge_id.as_str().splitn(3, "--").collect();
        if parts.len() < 3 {
            return Err(NodeDbError::BadRequest {
                detail: format!("invalid edge ID format: {}", edge_id.as_str()),
            });
        }
        let src = parts[0];
        let rest = parts[1]; // "label-->dst"
        let (label, dst) = rest
            .split_once("-->")
            .ok_or_else(|| NodeDbError::BadRequest {
                detail: "invalid edge ID".into(),
            })?;

        let mut conn = self.pool.acquire().await?;
        conn.send(
            OpCode::EdgeDelete,
            TextFields {
                from_node: Some(src.to_string()),
                to_node: Some(dst.to_string()),
                edge_type: Some(label.to_string()),
                ..Default::default()
            },
        )
        .await?;
        Ok(())
    }

    async fn document_get(&self, collection: &str, id: &str) -> NodeDbResult<Option<Document>> {
        let mut conn = self.pool.acquire().await?;
        let resp = conn
            .send(
                OpCode::PointGet,
                TextFields {
                    collection: Some(collection.to_string()),
                    document_id: Some(id.to_string()),
                    ..Default::default()
                },
            )
            .await?;

        // Empty result = not found.
        let rows = resp.rows.unwrap_or_default();
        if rows.is_empty() {
            return Ok(None);
        }

        // The result column contains JSON text.
        let json_text = rows[0].first().and_then(|v| v.as_str()).unwrap_or("{}");
        let mut doc = Document::new(id);
        if let Ok(obj) = serde_json::from_str::<HashMap<String, serde_json::Value>>(json_text) {
            for (k, v) in obj {
                doc.set(&k, json_to_value(v));
            }
        }
        Ok(Some(doc))
    }

    async fn document_put(&self, collection: &str, doc: Document) -> NodeDbResult<()> {
        let data = serde_json::to_vec(&doc.fields).map_err(|e| NodeDbError::Serialization {
            format: "json".into(),
            detail: format!("doc serialize: {e}"),
        })?;
        let mut conn = self.pool.acquire().await?;
        conn.send(
            OpCode::PointPut,
            TextFields {
                collection: Some(collection.to_string()),
                document_id: Some(doc.id.clone()),
                data: Some(data),
                ..Default::default()
            },
        )
        .await?;
        Ok(())
    }

    async fn document_delete(&self, collection: &str, id: &str) -> NodeDbResult<()> {
        let mut conn = self.pool.acquire().await?;
        conn.send(
            OpCode::PointDelete,
            TextFields {
                collection: Some(collection.to_string()),
                document_id: Some(id.to_string()),
                ..Default::default()
            },
        )
        .await?;
        Ok(())
    }

    async fn execute_sql(&self, query: &str, _params: &[Value]) -> NodeDbResult<QueryResult> {
        self.query(query).await
    }
}

// ─── Internal helpers ──────────────────────────────────────────────

fn format_f32_array(arr: &[f32]) -> String {
    let inner: Vec<String> = arr.iter().map(|v| format!("{v}")).collect();
    format!("ARRAY[{}]", inner.join(","))
}

fn parse_search_results(
    resp: &nodedb_types::protocol::NativeResponse,
) -> NodeDbResult<Vec<SearchResult>> {
    let rows = match &resp.rows {
        Some(r) => r,
        None => return Ok(Vec::new()),
    };

    // Results are returned as JSON text in a single "result" column.
    let mut results = Vec::new();
    for row in rows {
        if let Some(text) = row.first().and_then(|v| v.as_str()) {
            // Try to parse as JSON array of search results.
            if let Ok(items) = serde_json::from_str::<Vec<serde_json::Value>>(text) {
                for item in items {
                    if let Some(sr) = parse_single_search_result(&item) {
                        results.push(sr);
                    }
                }
            } else if let Ok(item) = serde_json::from_str::<serde_json::Value>(text)
                && let Some(sr) = parse_single_search_result(&item)
            {
                results.push(sr);
            }
        }
    }
    Ok(results)
}

fn parse_single_search_result(v: &serde_json::Value) -> Option<SearchResult> {
    let id = v.get("id")?.as_str()?.to_string();
    let distance = v.get("distance")?.as_f64()? as f32;
    Some(SearchResult {
        id,
        node_id: None,
        distance,
        metadata: HashMap::new(),
    })
}

fn json_to_value(v: serde_json::Value) -> Value {
    match v {
        serde_json::Value::Null => Value::Null,
        serde_json::Value::Bool(b) => Value::Bool(b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Value::Integer(i)
            } else {
                Value::Float(n.as_f64().unwrap_or(0.0))
            }
        }
        serde_json::Value::String(s) => Value::String(s),
        serde_json::Value::Array(arr) => Value::Array(arr.into_iter().map(json_to_value).collect()),
        serde_json::Value::Object(obj) => Value::Object(
            obj.into_iter()
                .map(|(k, v)| (k, json_to_value(v)))
                .collect(),
        ),
    }
}

/// Check if an error is a connection-level failure (worth retrying).
fn is_connection_error(e: &NodeDbError) -> bool {
    matches!(
        e,
        NodeDbError::SyncConnectionFailed { .. } | NodeDbError::Storage { .. }
    )
}

/// Parse a graph traversal response into a SubGraph.
///
/// The Data Plane returns JSON with nodes and edges from the BFS/DFS.
/// We parse the JSON text from the result column.
fn parse_subgraph_response(
    resp: &nodedb_types::protocol::NativeResponse,
) -> NodeDbResult<SubGraph> {
    use nodedb_types::result::{SubGraphEdge, SubGraphNode};

    let rows = match &resp.rows {
        Some(r) => r,
        None => return Ok(SubGraph::empty()),
    };

    let mut nodes = Vec::new();
    let mut edges = Vec::new();

    for row in rows {
        let text = match row.first().and_then(|v| v.as_str()) {
            Some(t) => t,
            None => continue,
        };

        // Try to parse as a JSON object or array.
        if let Ok(val) = serde_json::from_str::<serde_json::Value>(text) {
            // If it's an object with "nodes" and "edges" keys, extract them.
            if let Some(obj) = val.as_object() {
                if let Some(ns) = obj.get("nodes").and_then(|v| v.as_array()) {
                    for n in ns {
                        if let Some(id) = n.get("id").and_then(|v| v.as_str()) {
                            let depth = n.get("depth").and_then(|v| v.as_u64()).unwrap_or(0) as u8;
                            nodes.push(SubGraphNode {
                                id: NodeId::new(id),
                                depth,
                                properties: HashMap::new(),
                            });
                        }
                    }
                }
                if let Some(es) = obj.get("edges").and_then(|v| v.as_array()) {
                    for e in es {
                        let from = e
                            .get("from")
                            .or_else(|| e.get("src"))
                            .and_then(|v| v.as_str())
                            .unwrap_or("");
                        let to = e
                            .get("to")
                            .or_else(|| e.get("dst"))
                            .and_then(|v| v.as_str())
                            .unwrap_or("");
                        let label = e.get("label").and_then(|v| v.as_str()).unwrap_or("");
                        edges.push(SubGraphEdge {
                            id: EdgeId::from_components(from, to, label),
                            from: NodeId::new(from),
                            to: NodeId::new(to),
                            label: label.to_string(),
                            properties: HashMap::new(),
                        });
                    }
                }
            }
            // If it's an array of edge tuples (flat format), parse those.
            if let Some(arr) = val.as_array() {
                for item in arr {
                    if let Some(id) = item.get("id").and_then(|v| v.as_str()) {
                        let depth = item.get("depth").and_then(|v| v.as_u64()).unwrap_or(0) as u8;
                        nodes.push(SubGraphNode {
                            id: NodeId::new(id),
                            depth,
                            properties: HashMap::new(),
                        });
                    }
                }
            }
        }
    }

    Ok(SubGraph { nodes, edges })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn format_f32_array_works() {
        let arr = [0.1f32, 0.2, 0.3];
        let s = format_f32_array(&arr);
        assert!(s.starts_with("ARRAY["));
        assert!(s.contains("0.1"));
        assert!(s.ends_with(']'));
    }

    #[test]
    fn json_to_value_conversion() {
        assert_eq!(json_to_value(serde_json::Value::Null), Value::Null);
        assert_eq!(
            json_to_value(serde_json::Value::Bool(true)),
            Value::Bool(true)
        );
        assert_eq!(json_to_value(serde_json::json!(42)), Value::Integer(42));
        assert_eq!(
            json_to_value(serde_json::json!("hello")),
            Value::String("hello".into())
        );
    }

    #[test]
    fn parse_search_result_from_json() {
        let v = serde_json::json!({"id": "vec-1", "distance": 0.123});
        let sr = parse_single_search_result(&v).unwrap();
        assert_eq!(sr.id, "vec-1");
        assert!((sr.distance - 0.123).abs() < 0.001);
    }
}
