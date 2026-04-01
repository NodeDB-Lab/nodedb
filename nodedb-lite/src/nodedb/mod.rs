mod batch;
pub mod collection;
pub(crate) mod convert;
mod core;
pub mod definitions;
mod diagnostic;
mod graph_rag;
mod health;
pub(crate) mod lock_ext;
mod sync_delegate;
mod trait_impl;

pub use collection::{CollectionMeta, TransactionOp};
pub use core::NodeDbLite;
pub use diagnostic::DiagnosticDump;
pub use health::{HealthStatus, OverallStatus};
pub(crate) use lock_ext::LockExt;

#[cfg(test)]
mod tests {
    use nodedb_client::NodeDb;
    use nodedb_types::document::Document;
    use nodedb_types::id::NodeId;
    use nodedb_types::value::Value;

    use crate::RedbStorage;

    use super::*;

    async fn make_db() -> NodeDbLite<RedbStorage> {
        let storage = RedbStorage::open_in_memory().unwrap();
        NodeDbLite::open(storage, 1).await.unwrap()
    }

    #[tokio::test]
    async fn open_empty_db() {
        let db = make_db().await;
        assert_eq!(db.governor().total_used(), 0);
    }

    #[tokio::test]
    async fn vector_insert_and_search() {
        let db = make_db().await;

        db.vector_insert("embeddings", "v1", &[1.0, 0.0, 0.0], None)
            .await
            .unwrap();
        db.vector_insert("embeddings", "v2", &[0.0, 1.0, 0.0], None)
            .await
            .unwrap();
        db.vector_insert("embeddings", "v3", &[0.0, 0.0, 1.0], None)
            .await
            .unwrap();

        let results = db
            .vector_search("embeddings", &[1.0, 0.0, 0.0], 2, None)
            .await
            .unwrap();

        assert_eq!(results.len(), 2);
        assert_eq!(results[0].id, "v1"); // Closest.
    }

    #[tokio::test]
    async fn vector_delete() {
        let db = make_db().await;
        db.vector_insert("coll", "v1", &[1.0, 0.0], None)
            .await
            .unwrap();
        db.vector_delete("coll", "v1").await.unwrap();

        let results = db
            .vector_search("coll", &[1.0, 0.0], 5, None)
            .await
            .unwrap();
        assert!(results.is_empty());
    }

    #[tokio::test]
    async fn graph_insert_and_traverse() {
        let db = make_db().await;

        db.graph_insert_edge(&NodeId::new("alice"), &NodeId::new("bob"), "KNOWS", None)
            .await
            .unwrap();
        db.graph_insert_edge(&NodeId::new("bob"), &NodeId::new("carol"), "KNOWS", None)
            .await
            .unwrap();

        let subgraph = db
            .graph_traverse(&NodeId::new("alice"), 2, None)
            .await
            .unwrap();

        assert!(subgraph.node_count() >= 2);
        assert!(subgraph.edge_count() >= 1);
    }

    #[tokio::test]
    async fn graph_delete_edge() {
        let db = make_db().await;
        let edge_id = db
            .graph_insert_edge(&NodeId::new("a"), &NodeId::new("b"), "L", None)
            .await
            .unwrap();

        db.graph_delete_edge(&edge_id).await.unwrap();

        let subgraph = db.graph_traverse(&NodeId::new("a"), 1, None).await.unwrap();
        assert_eq!(subgraph.edge_count(), 0);
    }

    #[tokio::test]
    async fn document_crud() {
        let db = make_db().await;

        let doc = db.document_get("notes", "n1").await.unwrap();
        assert!(doc.is_none());

        let mut doc = Document::new("n1");
        doc.set("title", Value::String("Hello".into()));
        doc.set("score", Value::Float(9.5));
        db.document_put("notes", doc).await.unwrap();

        let doc = db.document_get("notes", "n1").await.unwrap().unwrap();
        assert_eq!(doc.id, "n1");
        assert_eq!(doc.get_str("title"), Some("Hello"));

        db.document_delete("notes", "n1").await.unwrap();
        let doc = db.document_get("notes", "n1").await.unwrap();
        assert!(doc.is_none());
    }

    #[tokio::test]
    async fn sql_basic_query() {
        let db = make_db().await;
        let result = db.execute_sql("SELECT 1 AS value", &[]).await.unwrap();
        assert_eq!(result.row_count(), 1);
        assert_eq!(result.columns, vec!["value"]);
    }

    #[tokio::test]
    async fn sql_query_documents() {
        let db = make_db().await;
        let mut doc1 = Document::new("u1");
        doc1.set("name", Value::String("Alice".into()));
        doc1.set("age", Value::Integer(30));
        db.document_put("users", doc1).await.unwrap();

        let mut doc2 = Document::new("u2");
        doc2.set("name", Value::String("Bob".into()));
        doc2.set("age", Value::Integer(25));
        db.document_put("users", doc2).await.unwrap();

        let result = db
            .execute_sql("SELECT id, document FROM users", &[])
            .await
            .unwrap();
        assert_eq!(result.row_count(), 2);
    }

    #[tokio::test]
    async fn flush_and_reopen() {
        {
            let s = RedbStorage::open_in_memory().unwrap();
            let db = NodeDbLite::open(s, 1).await.unwrap();

            let mut doc = Document::new("d1");
            doc.set("key", Value::String("val".into()));
            db.document_put("docs", doc).await.unwrap();
            db.graph_insert_edge(&NodeId::new("x"), &NodeId::new("y"), "REL", None)
                .await
                .unwrap();

            db.flush().await.unwrap();

            let doc = db.document_get("docs", "d1").await.unwrap();
            assert!(doc.is_some());
        }
    }

    #[tokio::test]
    async fn crdt_deltas_generated() {
        let db = make_db().await;

        let mut doc = Document::new("d1");
        doc.set("x", Value::Integer(42));
        db.document_put("docs", doc).await.unwrap();

        let deltas = db.pending_crdt_deltas().unwrap();
        assert!(!deltas.is_empty());
    }

    #[tokio::test]
    async fn acknowledge_deltas() {
        let db = make_db().await;

        db.document_put("a", Document::new("1")).await.unwrap();
        db.document_put("a", Document::new("2")).await.unwrap();

        let deltas = db.pending_crdt_deltas().unwrap();
        assert_eq!(deltas.len(), 2);

        let max_id = deltas.iter().map(|d| d.mutation_id).max().unwrap();
        db.acknowledge_deltas(max_id).unwrap();

        let deltas = db.pending_crdt_deltas().unwrap();
        assert!(deltas.is_empty());
    }

    #[tokio::test]
    async fn memory_governor_tracks_usage() {
        let db = make_db().await;

        for i in 0..100 {
            db.vector_insert("vecs", &format!("v{i}"), &[i as f32, 0.0, 0.0], None)
                .await
                .unwrap();
        }

        assert!(db.governor().total_used() > 0);
    }

    #[tokio::test]
    async fn search_nonexistent_collection() {
        let db = make_db().await;
        let results = db
            .vector_search("no_such_collection", &[1.0], 5, None)
            .await
            .unwrap();
        assert!(results.is_empty());
    }
}
