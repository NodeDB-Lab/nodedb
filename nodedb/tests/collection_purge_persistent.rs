//! Aggregation gate: after `purge_collection` / `delete_all_for_collection`
//! on each persistent engine, the engine's own lookup surface returns
//! zero hits for the purged `(tenant, collection)` pair while siblings
//! in the same tenant and same-name collections in other tenants stay
//! intact. Each engine has its own unit-level coverage; this file is
//! the cross-engine regression gate that catches a future refactor
//! where only some engines honor the scoped purge.

use nodedb::engine::kv::KvEngine;
use nodedb::engine::sparse::btree::SparseEngine;
use nodedb::engine::sparse::inverted::InvertedIndex;
use nodedb::types::TenantId;
use nodedb_types::Surrogate;

const TENANT: u32 = 7;

fn open_sparse() -> (tempfile::TempDir, SparseEngine) {
    let tmp = tempfile::tempdir().unwrap();
    let sparse = SparseEngine::open(&tmp.path().join("sparse.redb")).unwrap();
    (tmp, sparse)
}

#[test]
fn sparse_engine_purge_leaves_no_documents_for_collection() {
    let (_tmp, sparse) = open_sparse();
    let doc_bytes = b"{\"k\":1}".to_vec();
    sparse.put(TENANT, "keep", "d1", &doc_bytes).unwrap();
    sparse.put(TENANT, "purge_me", "d1", &doc_bytes).unwrap();
    sparse.put(TENANT, "purge_me", "d2", &doc_bytes).unwrap();

    let (docs_removed, _idx_removed) = sparse
        .delete_all_for_collection(TENANT, "purge_me")
        .unwrap();
    assert_eq!(docs_removed, 2);

    assert!(sparse.get(TENANT, "purge_me", "d1").unwrap().is_none());
    assert!(sparse.get(TENANT, "purge_me", "d2").unwrap().is_none());
    assert!(sparse.get(TENANT, "keep", "d1").unwrap().is_some());
}

#[test]
fn sparse_engine_cross_tenant_isolation() {
    let (_tmp, sparse) = open_sparse();
    let doc_bytes = b"{\"k\":1}".to_vec();
    sparse.put(1, "docs", "d1", &doc_bytes).unwrap();
    sparse.put(2, "docs", "d1", &doc_bytes).unwrap();

    let (removed, _) = sparse.delete_all_for_collection(1, "docs").unwrap();
    assert_eq!(removed, 1);
    assert!(sparse.get(1, "docs", "d1").unwrap().is_none());
    assert!(
        sparse.get(2, "docs", "d1").unwrap().is_some(),
        "tenant 2's same-named collection must survive tenant 1's purge"
    );
}

#[test]
fn kv_engine_purge_leaves_no_keys_for_collection() {
    let now_ms = 0u64;
    let mut kv = KvEngine::new(now_ms, 16, 0.75, 4, 64, 1000, 1024);

    kv.put(
        TENANT,
        "keep",
        b"k1",
        b"v1",
        0,
        now_ms,
        nodedb_types::Surrogate::ZERO,
    );
    kv.put(
        TENANT,
        "purge_me",
        b"k1",
        b"v1",
        0,
        now_ms,
        nodedb_types::Surrogate::ZERO,
    );
    kv.put(
        TENANT,
        "purge_me",
        b"k2",
        b"v2",
        0,
        now_ms,
        nodedb_types::Surrogate::ZERO,
    );

    let removed = kv.purge_collection(TENANT, "purge_me");
    assert!(
        removed >= 1,
        "purge_collection must remove at least the table"
    );

    assert!(kv.get(TENANT, "purge_me", b"k1", now_ms).is_none());
    assert!(kv.get(TENANT, "purge_me", b"k2", now_ms).is_none());
    assert_eq!(
        kv.get(TENANT, "keep", b"k1", now_ms),
        Some(b"v1".to_vec()),
        "sibling collection must survive"
    );
}

#[test]
fn kv_engine_cross_tenant_isolation() {
    let now_ms = 0u64;
    let mut kv = KvEngine::new(now_ms, 16, 0.75, 4, 64, 1000, 1024);

    kv.put(
        1,
        "docs",
        b"k",
        b"a",
        0,
        now_ms,
        nodedb_types::Surrogate::ZERO,
    );
    kv.put(
        2,
        "docs",
        b"k",
        b"b",
        0,
        now_ms,
        nodedb_types::Surrogate::ZERO,
    );

    kv.purge_collection(1, "docs");
    assert!(kv.get(1, "docs", b"k", now_ms).is_none());
    assert_eq!(
        kv.get(2, "docs", b"k", now_ms),
        Some(b"b".to_vec()),
        "tenant 2's same-named collection must survive"
    );
}

#[test]
fn inverted_index_purge_is_scoped_to_collection() {
    let (_tmp, sparse) = open_sparse();
    let inverted = InvertedIndex::open(sparse.db().clone()).unwrap();
    let tid = TenantId::new(TENANT);

    inverted
        .index_document(tid, "keep", Surrogate(1), "hello world")
        .unwrap();
    inverted
        .index_document(tid, "purge_me", Surrogate(2), "hello universe")
        .unwrap();

    inverted.purge_collection(tid, "purge_me").unwrap();

    let purged = inverted
        .search(tid, "purge_me", "hello", 10, false, None)
        .unwrap();
    assert!(purged.is_empty(), "purged collection must return no hits");
    let kept = inverted
        .search(tid, "keep", "hello", 10, false, None)
        .unwrap();
    assert_eq!(kept.len(), 1);
}
