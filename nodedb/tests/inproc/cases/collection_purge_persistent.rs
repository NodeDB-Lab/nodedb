// SPDX-License-Identifier: BUSL-1.1

//! Aggregation gate: after `purge_collection` / `delete_all_for_collection`
//! on each persistent engine, the engine's own lookup surface returns
//! zero hits for the purged `(tenant, collection)` pair while siblings
//! in the same tenant and same-name collections in other tenants stay
//! intact. Each engine has its own unit-level coverage; this file is
//! the cross-engine regression gate that catches a future refactor
//! where only some engines honor the scoped purge.

use nodedb::engine::document::store::StorageKey;
use nodedb::engine::kv::{KvEngine, KvPutParams};
use nodedb::engine::sparse::btree::SparseEngine;
use nodedb::engine::sparse::inverted::InvertedIndex;
use nodedb::types::TenantId;
use nodedb_fts::FtsSearchParams;
use nodedb_fts::posting::QueryMode;
use nodedb_types::Surrogate;

const TENANT: u64 = 7;
const DB: u64 = 0;

fn open_sparse() -> (tempfile::TempDir, SparseEngine) {
    let tmp = tempfile::tempdir().unwrap();
    let sparse = SparseEngine::open(&tmp.path().join("sparse.redb")).unwrap();
    (tmp, sparse)
}

fn key(n: u32) -> StorageKey {
    StorageKey::for_surrogate(Surrogate::new(n))
}

#[test]
fn sparse_engine_purge_leaves_no_documents_for_collection() {
    let (_tmp, sparse) = open_sparse();
    let doc_bytes = b"{\"k\":1}".to_vec();
    sparse.put(DB, TENANT, "keep", &key(1), &doc_bytes).unwrap();
    sparse
        .put(DB, TENANT, "purge_me", &key(1), &doc_bytes)
        .unwrap();
    sparse
        .put(DB, TENANT, "purge_me", &key(2), &doc_bytes)
        .unwrap();

    let (docs_removed, _idx_removed) = sparse
        .delete_all_for_collection(DB, TENANT, "purge_me")
        .unwrap();
    assert_eq!(docs_removed, 2);

    assert!(
        sparse
            .get(DB, TENANT, "purge_me", &key(1))
            .unwrap()
            .is_none()
    );
    assert!(
        sparse
            .get(DB, TENANT, "purge_me", &key(2))
            .unwrap()
            .is_none()
    );
    assert!(sparse.get(DB, TENANT, "keep", &key(1)).unwrap().is_some());
}

#[test]
fn sparse_engine_cross_tenant_isolation() {
    let (_tmp, sparse) = open_sparse();
    let doc_bytes = b"{\"k\":1}".to_vec();
    sparse.put(DB, 1, "docs", &key(1), &doc_bytes).unwrap();
    sparse.put(DB, 2, "docs", &key(1), &doc_bytes).unwrap();

    let (removed, _) = sparse.delete_all_for_collection(DB, 1, "docs").unwrap();
    assert_eq!(removed, 1);
    assert!(sparse.get(DB, 1, "docs", &key(1)).unwrap().is_none());
    assert!(
        sparse.get(DB, 2, "docs", &key(1)).unwrap().is_some(),
        "tenant 2's same-named collection must survive tenant 1's purge"
    );
}

#[test]
fn kv_engine_purge_leaves_no_keys_for_collection() {
    let now_ms = 0u64;
    let mut kv = KvEngine::new(now_ms, 16, 0.75, 4, 64, 1000, 1024);

    kv.put(KvPutParams {
        database_id: 0,
        tenant_id: TENANT,
        collection: "keep",
        key: b"k1",
        value: b"v1",
        ttl_ms: 0,
        now_ms,
        surrogate: nodedb_types::Surrogate::ZERO,
    });
    kv.put(KvPutParams {
        database_id: 0,
        tenant_id: TENANT,
        collection: "purge_me",
        key: b"k1",
        value: b"v1",
        ttl_ms: 0,
        now_ms,
        surrogate: nodedb_types::Surrogate::ZERO,
    });
    kv.put(KvPutParams {
        database_id: 0,
        tenant_id: TENANT,
        collection: "purge_me",
        key: b"k2",
        value: b"v2",
        ttl_ms: 0,
        now_ms,
        surrogate: nodedb_types::Surrogate::ZERO,
    });

    let removed = kv.purge_collection(0, TENANT, "purge_me");
    assert!(
        removed >= 1,
        "purge_collection must remove at least the table"
    );

    assert!(kv.get(0, TENANT, "purge_me", b"k1", now_ms).is_none());
    assert!(kv.get(0, TENANT, "purge_me", b"k2", now_ms).is_none());
    assert_eq!(
        kv.get(0, TENANT, "keep", b"k1", now_ms),
        Some(b"v1".to_vec()),
        "sibling collection must survive"
    );
}

#[test]
fn kv_engine_cross_tenant_isolation() {
    let now_ms = 0u64;
    let mut kv = KvEngine::new(now_ms, 16, 0.75, 4, 64, 1000, 1024);

    kv.put(KvPutParams {
        database_id: 0,
        tenant_id: 1,
        collection: "docs",
        key: b"k",
        value: b"a",
        ttl_ms: 0,
        now_ms,
        surrogate: nodedb_types::Surrogate::ZERO,
    });
    kv.put(KvPutParams {
        database_id: 0,
        tenant_id: 2,
        collection: "docs",
        key: b"k",
        value: b"b",
        ttl_ms: 0,
        now_ms,
        surrogate: nodedb_types::Surrogate::ZERO,
    });

    kv.purge_collection(0, 1, "docs");
    assert!(kv.get(0, 1, "docs", b"k", now_ms).is_none());
    assert_eq!(
        kv.get(0, 2, "docs", b"k", now_ms),
        Some(b"b".to_vec()),
        "tenant 2's same-named collection must survive"
    );
}

#[test]
fn inverted_index_purge_is_scoped_to_collection() {
    let (_tmp, sparse) = open_sparse();
    let inverted = InvertedIndex::open(
        sparse.db().clone(),
        nodedb::data::executor::core_loop::test_governor(),
    )
    .unwrap();
    let tid = TenantId::new(TENANT);

    inverted
        .index_document(DB, tid, "keep", Surrogate(1), "hello world")
        .unwrap();
    inverted
        .index_document(DB, tid, "purge_me", Surrogate(2), "hello universe")
        .unwrap();

    inverted.purge_collection(DB, tid, "purge_me").unwrap();

    let purged = inverted
        .search(
            DB,
            tid,
            "purge_me",
            FtsSearchParams {
                query: "hello",
                top_k: 10,
                fuzzy_enabled: false,
                mode: QueryMode::And,
                prefilter: None,
            },
        )
        .unwrap();
    assert!(purged.is_empty(), "purged collection must return no hits");
    let kept = inverted
        .search(
            DB,
            tid,
            "keep",
            FtsSearchParams {
                query: "hello",
                top_k: 10,
                fuzzy_enabled: false,
                mode: QueryMode::And,
                prefilter: None,
            },
        )
        .unwrap();
    assert_eq!(kept.len(), 1);
}
