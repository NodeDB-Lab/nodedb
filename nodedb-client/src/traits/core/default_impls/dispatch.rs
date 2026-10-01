// SPDX-License-Identifier: Apache-2.0

use nodedb_types::document::Document;
use nodedb_types::error::{NodeDbError, NodeDbResult};
use nodedb_types::filter::MetadataFilter;
use nodedb_types::result::SearchResult;
use nodedb_types::text_search::TextSearchParams;
use std::collections::HashSet;

use super::super::trait_def::NodeDb;

pub(in crate::traits::core) async fn document_put_with_vector_default<T: NodeDb + ?Sized>(
    this: &T,
    doc_collection: &str,
    doc: Document,
    vector_collection: &str,
    id: &str,
    embedding: &[f32],
) -> NodeDbResult<()> {
    this.document_put(doc_collection, doc).await?;
    if !embedding.is_empty() {
        this.vector_insert(vector_collection, id, embedding, None)
            .await?;
    }
    Ok(())
}

pub(in crate::traits::core) fn document_get_as_of_default(
    collection: &str,
    id: &str,
    as_of_ms: Option<i64>,
    valid_time_ms: Option<i64>,
) -> NodeDbResult<Option<Document>> {
    let _ = (collection, id, as_of_ms, valid_time_ms);
    Err(NodeDbError::storage(
        "document_get_as_of is not implemented on this client",
    ))
}

pub(in crate::traits::core) fn document_put_with_valid_time_default(
    collection: &str,
    doc: Document,
    valid_from_ms: Option<i64>,
    valid_until_ms: Option<i64>,
) -> NodeDbResult<()> {
    let _ = (collection, doc, valid_from_ms, valid_until_ms);
    Err(NodeDbError::storage(
        "document_put_with_valid_time is not implemented on this client",
    ))
}

pub(in crate::traits::core) fn vector_insert_field_default(
    collection: &str,
    field_name: &str,
    id: &str,
    embedding: &[f32],
    metadata: Option<Document>,
) -> NodeDbResult<()> {
    let _ = (collection, id, embedding, metadata);
    Err(NodeDbError::storage(format!(
        "vector_insert_field is not implemented on this client; \
         field_name={field_name} would have been silently dropped"
    )))
}

pub(in crate::traits::core) fn vector_search_field_default(
    collection: &str,
    field_name: &str,
    query: &[f32],
    k: usize,
    filter: Option<&MetadataFilter>,
) -> NodeDbResult<Vec<SearchResult>> {
    let _ = (collection, query, k, filter);
    Err(NodeDbError::storage(format!(
        "vector_search_field is not implemented on this client; \
         field_name={field_name} would have been silently dropped"
    )))
}

pub(in crate::traits::core) fn text_search_default(
    collection: &str,
    field: &str,
    query: &str,
    top_k: usize,
    params: TextSearchParams,
    allowed_ids: Option<&HashSet<String>>,
) -> NodeDbResult<Vec<SearchResult>> {
    let _ = (collection, field, query, top_k, params, allowed_ids);
    Err(NodeDbError::storage(
        "text_search is not implemented on this client",
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::capabilities::Capabilities;
    use async_trait::async_trait;
    use nodedb_types::document::Document;
    use nodedb_types::error::{NodeDbError, NodeDbResult};
    use nodedb_types::filter::{EdgeFilter, MetadataFilter};
    use nodedb_types::graph::GraphStats;
    use nodedb_types::id::{EdgeId, NodeId};
    use nodedb_types::result::{QueryResult, SearchResult, SubGraph};
    use nodedb_types::value::Value;
    use std::collections::HashMap;

    /// Mock implementation to verify the trait is object-safe and
    /// can be used as `Arc<dyn NodeDb>`.
    struct MockDb;

    #[cfg_attr(not(target_arch = "wasm32"), async_trait)]
    #[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
    impl NodeDb for MockDb {
        async fn vector_search(
            &self,
            _collection: &str,
            _query: &[f32],
            _k: usize,
            _filter: Option<&MetadataFilter>,
            _allowed_ids: Option<&HashSet<String>>,
        ) -> NodeDbResult<Vec<SearchResult>> {
            Ok(vec![SearchResult {
                id: "vec-1".into(),
                node_id: None,
                distance: 0.1,
                metadata: HashMap::new(),
            }])
        }

        async fn vector_insert(
            &self,
            _collection: &str,
            _id: &str,
            _embedding: &[f32],
            _metadata: Option<Document>,
        ) -> NodeDbResult<()> {
            Ok(())
        }

        async fn vector_delete(&self, _collection: &str, _id: &str) -> NodeDbResult<()> {
            Ok(())
        }

        async fn graph_traverse(
            &self,
            _collection: &str,
            _start: &NodeId,
            _depth: u8,
            _direction: nodedb_types::graph::Direction,
            _edge_filter: Option<&EdgeFilter>,
        ) -> NodeDbResult<SubGraph> {
            Ok(SubGraph::empty())
        }

        async fn graph_insert_edge(
            &self,
            _collection: &str,
            from: &NodeId,
            to: &NodeId,
            edge_type: &str,
            _properties: Option<Document>,
        ) -> NodeDbResult<EdgeId> {
            EdgeId::try_first(from.clone(), to.clone(), edge_type)
                .map_err(|e| NodeDbError::storage(format!("invalid edge label: {e}")))
        }

        async fn graph_delete_edge(
            &self,
            _collection: &str,
            _edge_id: &EdgeId,
        ) -> NodeDbResult<()> {
            Ok(())
        }

        async fn graph_stats(
            &self,
            collection: Option<&str>,
            _as_of: Option<i64>,
        ) -> NodeDbResult<Vec<GraphStats>> {
            Ok(vec![GraphStats::zero(collection.unwrap_or("mock"))])
        }

        async fn document_get(
            &self,
            _collection: &str,
            id: &str,
        ) -> NodeDbResult<Option<Document>> {
            let mut doc = Document::new(id);
            doc.set("title", Value::String("test".into()));
            Ok(Some(doc))
        }

        async fn document_put(&self, _collection: &str, _doc: Document) -> NodeDbResult<()> {
            Ok(())
        }

        async fn document_delete(&self, _collection: &str, _id: &str) -> NodeDbResult<()> {
            Ok(())
        }

        async fn list_insert(
            &self,
            _collection: &str,
            _document_id: &str,
            _list_path: &str,
            _index: usize,
            _fields: &Value,
        ) -> NodeDbResult<()> {
            Ok(())
        }

        async fn list_delete(
            &self,
            _collection: &str,
            _document_id: &str,
            _list_path: &str,
            _index: usize,
        ) -> NodeDbResult<()> {
            Ok(())
        }

        async fn list_move(
            &self,
            _collection: &str,
            _document_id: &str,
            _list_path: &str,
            _from_index: usize,
            _to_index: usize,
        ) -> NodeDbResult<()> {
            Ok(())
        }

        async fn execute_sql(&self, _query: &str, _params: &[Value]) -> NodeDbResult<QueryResult> {
            Ok(QueryResult::empty())
        }
    }

    #[test]
    fn trait_is_object_safe() {
        fn _accepts_dyn(_db: &dyn NodeDb) {}
        let db = MockDb;
        _accepts_dyn(&db);
    }

    #[test]
    fn trait_works_with_arc() {
        use std::sync::Arc;
        let db: Arc<dyn NodeDb> = Arc::new(MockDb);
        let _ = db;
    }

    #[tokio::test]
    async fn mock_vector_search() {
        let db = MockDb;
        let results = db
            .vector_search("embeddings", &[0.1, 0.2, 0.3], 5, None, None)
            .await
            .unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].id, "vec-1");
        assert!(results[0].distance < 1.0);
    }

    #[tokio::test]
    async fn mock_vector_insert_and_delete() {
        let db = MockDb;
        db.vector_insert("coll", "v1", &[1.0, 2.0], None)
            .await
            .unwrap();
        db.vector_delete("coll", "v1").await.unwrap();
    }

    #[tokio::test]
    async fn mock_graph_stats_returns_zero() {
        let db = MockDb;
        let result = db.graph_stats(Some("social"), None).await.unwrap();
        assert_eq!(result.len(), 1);
        let stats = &result[0];
        assert_eq!(stats.collection, "social");
        assert_eq!(stats.node_count, 0);
        assert_eq!(stats.edge_count, 0);
        assert_eq!(stats.distinct_label_count, 0);
        assert!(stats.labels.is_empty());
    }

    #[tokio::test]
    async fn mock_graph_stats_tenant_wide_uses_mock_key() {
        let db = MockDb;
        let result = db.graph_stats(None, None).await.unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].collection, "mock");
    }

    #[tokio::test]
    async fn mock_graph_operations() {
        let db = MockDb;
        let start = NodeId::try_new("alice").expect("test fixture");
        let subgraph = db
            .graph_traverse(
                "social",
                &start,
                2,
                nodedb_types::graph::Direction::Out,
                None,
            )
            .await
            .unwrap();
        assert_eq!(subgraph.node_count(), 0);

        let from = NodeId::try_new("alice").expect("test fixture");
        let to = NodeId::try_new("bob").expect("test fixture");
        let edge_id = db
            .graph_insert_edge("social", &from, &to, "KNOWS", None)
            .await
            .unwrap();
        assert_eq!(edge_id.src.as_str(), "alice");
        assert_eq!(edge_id.dst.as_str(), "bob");
        assert_eq!(edge_id.label, "KNOWS");
        assert_eq!(edge_id.seq, 0);

        db.graph_delete_edge("social", &edge_id).await.unwrap();
    }

    #[tokio::test]
    async fn mock_document_operations() {
        let db = MockDb;
        let doc = db.document_get("notes", "n1").await.unwrap().unwrap();
        assert_eq!(doc.id, "n1");
        assert_eq!(doc.get_str("title"), Some("test"));

        let mut new_doc = Document::new("n2");
        new_doc.set("body", Value::String("hello".into()));
        db.document_put("notes", new_doc).await.unwrap();

        db.document_delete("notes", "n1").await.unwrap();
    }

    #[tokio::test]
    async fn mock_execute_sql() {
        let db = MockDb;
        let result = db.execute_sql("SELECT 1", &[]).await.unwrap();
        assert_eq!(result.row_count(), 0);
    }

    /// Verify the full "one API, any runtime" pattern: application
    /// code switches between `NodeDbLite` and `NodeDbRemote` only at
    /// the construction site.
    #[tokio::test]
    async fn unified_api_pattern() {
        use std::sync::Arc;

        let db: Arc<dyn NodeDb> = Arc::new(MockDb);

        let results = db
            .vector_search("knowledge_base", &[0.1, 0.2], 5, None, None)
            .await
            .unwrap();
        assert!(!results.is_empty());

        let start = NodeId::from_validated(results[0].id.clone());
        let _subgraph = db
            .graph_traverse(
                "knowledge_base",
                &start,
                2,
                nodedb_types::graph::Direction::Out,
                None,
            )
            .await
            .unwrap();

        let doc = Document::new("note-1");
        db.document_put("notes", doc).await.unwrap();
    }

    #[test]
    fn default_proto_version_is_zero() {
        let db = MockDb;
        assert_eq!(db.proto_version(), 0);
    }

    #[test]
    fn default_capabilities_is_zero() {
        let db = MockDb;
        assert_eq!(db.capabilities(), 0);
        let caps = Capabilities::from_raw(db.capabilities());
        assert!(!caps.supports_streaming());
        assert!(!caps.supports_graphrag());
    }

    #[test]
    fn default_server_version_is_empty() {
        let db = MockDb;
        assert!(db.server_version().is_empty());
    }

    #[test]
    fn default_limits_all_none() {
        let db = MockDb;
        let limits = db.limits();
        assert!(limits.max_vector_dim.is_none());
        assert!(limits.max_top_k.is_none());
        assert!(limits.max_scan_limit.is_none());
        assert!(limits.max_batch_size.is_none());
        assert!(limits.max_crdt_delta_bytes.is_none());
        assert!(limits.max_query_text_bytes.is_none());
        assert!(limits.max_graph_depth.is_none());
    }

    #[test]
    fn capabilities_newtype_smoke() {
        use nodedb_types::protocol::{CAP_FTS, CAP_STREAMING};
        let caps = Capabilities::from_raw(CAP_STREAMING | CAP_FTS);
        assert!(caps.supports_streaming());
        assert!(caps.supports_fts());
        assert!(!caps.supports_graphrag());
        assert!(!caps.supports_crdt());
    }
}
