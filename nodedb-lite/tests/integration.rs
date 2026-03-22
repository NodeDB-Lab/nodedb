//! Integration tests for NodeDB-Lite.
//!
//! Tests the full stack: StorageEngine → Engines → NodeDbLite → NodeDb trait.
//! Uses batch methods for bulk setup, measures query latency in benchmarks.

use std::sync::Arc;
use std::time::Instant;

use nodedb_client::NodeDb;
use nodedb_lite::{NodeDbLite, RedbStorage};
use nodedb_types::document::Document;
use nodedb_types::id::NodeId;
use nodedb_types::value::Value;

async fn open_test_db() -> NodeDbLite<RedbStorage> {
    let storage = RedbStorage::open_in_memory().unwrap();
    NodeDbLite::open(storage, 1).await.unwrap()
}

// ─── 1K Vector Insert + Search ───────────────────────────────────────

#[tokio::test]
async fn vector_1k_batch_insert_and_search() {
    let db = open_test_db().await;

    let vectors: Vec<(String, Vec<f32>)> = (0..1000)
        .map(|i| {
            let emb: Vec<f32> = (0..32).map(|d| ((i * 32 + d) as f32) * 0.001).collect();
            (format!("v{i}"), emb)
        })
        .collect();

    let refs: Vec<(&str, &[f32])> = vectors
        .iter()
        .map(|(id, emb)| (id.as_str(), emb.as_slice()))
        .collect();

    db.batch_vector_insert("vecs", &refs).unwrap();

    // Search for top-5 nearest to vector 500.
    let query: Vec<f32> = (0..32).map(|d| ((500 * 32 + d) as f32) * 0.001).collect();
    let results = db.vector_search("vecs", &query, 10, None).await.unwrap();

    assert_eq!(results.len(), 10);
    // Results should be sorted by distance (ascending).
    for w in results.windows(2) {
        assert!(
            w[0].distance <= w[1].distance,
            "results not sorted by distance"
        );
    }
    // The top result should be reasonably close (not a random vector).
    // HNSW is approximate — we don't require exact match at this scale.
    assert!(
        results[0].distance < 0.5,
        "top result distance {} is too large",
        results[0].distance
    );
}

// ─── 10K Graph Edges + Traverse ──────────────────────────────────────

#[tokio::test]
async fn graph_10k_edges_batch_and_traverse() {
    let db = open_test_db().await;

    let mut edges: Vec<(String, String, &str)> = Vec::with_capacity(10_000);
    for i in 0..500 {
        for j in 1..=20 {
            let dst = (i * 20 + j) % 500;
            edges.push((format!("n{i}"), format!("n{dst}"), "LINK"));
        }
    }
    let refs: Vec<(&str, &str, &str)> = edges
        .iter()
        .map(|(s, d, l)| (s.as_str(), d.as_str(), *l))
        .collect();

    db.batch_graph_insert_edges(&refs).unwrap();
    db.compact_graph().unwrap();

    let subgraph = db
        .graph_traverse(&NodeId::new("n0"), 2, None)
        .await
        .unwrap();

    assert!(subgraph.node_count() > 10);
    assert!(subgraph.edge_count() > 0);
}

// ─── Document CRUD ───────────────────────────────────────────────────

#[tokio::test]
async fn document_crud_100() {
    let db = open_test_db().await;

    for i in 0..100 {
        let mut doc = Document::new(format!("doc-{i}"));
        doc.set("title", Value::String(format!("Document {i}")));
        doc.set("score", Value::Float(i as f64 * 0.1));
        db.document_put("notes", doc).await.unwrap();
    }

    let doc = db.document_get("notes", "doc-50").await.unwrap().unwrap();
    assert_eq!(doc.id, "doc-50");
    assert_eq!(doc.get_str("title"), Some("Document 50"));

    // Update.
    let mut updated = Document::new("doc-50");
    updated.set("title", Value::String("Updated 50".into()));
    db.document_put("notes", updated).await.unwrap();
    let doc = db.document_get("notes", "doc-50").await.unwrap().unwrap();
    assert_eq!(doc.get_str("title"), Some("Updated 50"));

    // Delete.
    db.document_delete("notes", "doc-50").await.unwrap();
    assert!(db.document_get("notes", "doc-50").await.unwrap().is_none());
    assert!(db.document_get("notes", "doc-49").await.unwrap().is_some());
}

// ─── Multi-Modal Query ───────────────────────────────────────────────

#[tokio::test]
async fn multi_modal_vector_graph_document() {
    let db = open_test_db().await;

    db.batch_vector_insert(
        "kb",
        &[
            ("concept-ai", &[1.0, 0.0, 0.0][..]),
            ("concept-ml", &[0.9, 0.1, 0.0]),
            ("concept-db", &[0.0, 0.0, 1.0]),
        ],
    )
    .unwrap();

    db.batch_graph_insert_edges(&[
        ("concept-ai", "concept-ml", "RELATES_TO"),
        ("concept-ml", "concept-db", "USES"),
    ])
    .unwrap();

    let mut doc = Document::new("note-1");
    doc.set("body", Value::String("AI and ML are related".into()));
    db.document_put("notes", doc).await.unwrap();

    // Vector search → graph traverse → document read.
    let results = db
        .vector_search("kb", &[1.0, 0.0, 0.0], 2, None)
        .await
        .unwrap();
    assert!(!results.is_empty());

    let start = NodeId::new(results[0].id.clone());
    let subgraph = db.graph_traverse(&start, 2, None).await.unwrap();
    assert!(subgraph.node_count() >= 1);

    let note = db.document_get("notes", "note-1").await.unwrap().unwrap();
    assert!(note.get_str("body").unwrap().contains("AI"));
}

// ─── Persistence: Flush and Reopen ───────────────────────────────────

#[tokio::test]
async fn flush_and_reopen_persists_all() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("persist.db");

    {
        let storage = RedbStorage::open(&path).unwrap();
        let db = NodeDbLite::open(storage, 1).await.unwrap();

        db.batch_vector_insert("vecs", &[("v1", &[1.0, 2.0, 3.0][..])])
            .unwrap();
        db.batch_graph_insert_edges(&[("a", "b", "KNOWS")]).unwrap();
        let mut doc = Document::new("d1");
        doc.set("key", Value::String("persistent".into()));
        db.document_put("docs", doc).await.unwrap();

        db.flush().await.unwrap();
    }

    {
        let storage = RedbStorage::open(&path).unwrap();
        let db = NodeDbLite::open(storage, 1).await.unwrap();

        let doc = db.document_get("docs", "d1").await.unwrap();
        assert!(doc.is_some(), "document should persist across restart");

        let results = db
            .vector_search("vecs", &[1.0, 2.0, 3.0], 1, None)
            .await
            .unwrap();
        assert!(!results.is_empty(), "vector should persist across restart");

        let sg = db.graph_traverse(&NodeId::new("a"), 1, None).await.unwrap();
        assert!(sg.node_count() >= 2, "graph should persist across restart");
    }
}

// ─── CRDT Deltas ─────────────────────────────────────────────────────

#[tokio::test]
async fn all_operations_generate_deltas() {
    let db = open_test_db().await;

    db.vector_insert("v", "v1", &[1.0], None).await.unwrap();
    db.graph_insert_edge(&NodeId::new("a"), &NodeId::new("b"), "L", None)
        .await
        .unwrap();
    db.document_put("d", Document::new("d1")).await.unwrap();
    db.document_delete("d", "d1").await.unwrap();

    let deltas = db.pending_crdt_deltas().unwrap();
    assert!(
        deltas.len() >= 4,
        "expected >= 4 deltas, got {}",
        deltas.len()
    );
}

// ─── Arc<dyn NodeDb> Pattern ─────────────────────────────────────────

#[tokio::test]
async fn arc_dyn_nodedb_pattern() {
    let storage = RedbStorage::open_in_memory().unwrap();
    let db: Arc<dyn NodeDb> = Arc::new(NodeDbLite::open(storage, 1).await.unwrap());

    db.vector_insert("coll", "v1", &[1.0, 0.0], None)
        .await
        .unwrap();
    let results = db
        .vector_search("coll", &[1.0, 0.0], 1, None)
        .await
        .unwrap();
    assert_eq!(results.len(), 1);

    db.document_put("docs", Document::new("d1")).await.unwrap();
    assert!(db.document_get("docs", "d1").await.unwrap().is_some());
}

// ─── Benchmarks ──────────────────────────────────────────────────────

#[tokio::test]
async fn benchmark_vector_search_1k() {
    let db = open_test_db().await;

    let vectors: Vec<(String, Vec<f32>)> = (0..1000)
        .map(|i| {
            let emb: Vec<f32> = (0..32).map(|d| ((i * 32 + d) as f32) * 0.001).collect();
            (format!("v{i}"), emb)
        })
        .collect();
    let refs: Vec<(&str, &[f32])> = vectors
        .iter()
        .map(|(id, emb)| (id.as_str(), emb.as_slice()))
        .collect();
    db.batch_vector_insert("bench", &refs).unwrap();

    let query: Vec<f32> = (0..32).map(|d| (d as f32) * 0.01).collect();

    // Warm up.
    let _ = db.vector_search("bench", &query, 5, None).await;

    // Measure search latency only (not insert).
    let start = Instant::now();
    let iterations = 100;
    for _ in 0..iterations {
        let _ = db.vector_search("bench", &query, 5, None).await.unwrap();
    }
    let elapsed = start.elapsed();
    let per_query_us = elapsed.as_micros() / iterations as u128;

    assert!(
        per_query_us < 1000,
        "vector search took {per_query_us}us, target < 1000us"
    );
}

#[tokio::test]
async fn benchmark_graph_bfs_10k_edges() {
    let db = open_test_db().await;

    // 10K edges: 2000 nodes × 5 edges each (sparse enough that 2-hop
    // BFS doesn't visit the entire graph).
    let mut edges: Vec<(String, String, &str)> = Vec::with_capacity(10_000);
    for i in 0..2000 {
        for j in 1..=5 {
            let dst = (i + j * 7) % 2000; // spread-out targets
            edges.push((format!("n{i}"), format!("n{dst}"), "E"));
        }
    }
    let refs: Vec<(&str, &str, &str)> = edges
        .iter()
        .map(|(s, d, l)| (s.as_str(), d.as_str(), *l))
        .collect();
    db.batch_graph_insert_edges(&refs).unwrap();

    // Compact CSR: merge buffer into dense arrays for fast traversal.
    db.compact_graph().unwrap();

    // Warm up.
    let _ = db.graph_traverse(&NodeId::new("n0"), 2, None).await;

    // Measure traverse latency only (dense CSR, not buffer).
    let start = Instant::now();
    let iterations = 100;
    for _ in 0..iterations {
        let _ = db
            .graph_traverse(&NodeId::new("n0"), 2, None)
            .await
            .unwrap();
    }
    let elapsed = start.elapsed();
    let per_query_us = elapsed.as_micros() / iterations as u128;

    // Debug builds are 10-20x slower than release. The 1ms target is for
    // release mode. In debug, accept 10ms (10000us).
    let limit = if cfg!(debug_assertions) {
        10_000
    } else {
        1_000
    };
    assert!(
        per_query_us < limit,
        "graph BFS took {per_query_us}us, target < {limit}us"
    );
}
