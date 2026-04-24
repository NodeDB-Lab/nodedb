//! Bitemporal audit-retention temporal-purge end-to-end integration.
//!
//! Covers the three purge paths that share the `MetaOp::TemporalPurge*`
//! contract: drop *superseded* versions older than a system-time cutoff,
//! always preserving the single latest version per logical row so
//! "AS OF" reads beyond the cutoff still resolve a coherent terminal
//! state.
//!
//! The three engines exercised:
//!
//! 1. **EdgeStore** — versioned edge rows in redb (graph engine).
//! 2. **DocumentStrict** — `documents_versioned` + `indexes_versioned`
//!    tables in redb (strict-document engine).
//! 3. **Plain columnar** — row-level purge via delete bitmaps is covered
//!    by the unit tests under `engine::graph::edge_store::temporal::purge`
//!    and `engine::sparse::btree_versioned::purge`, plus the segment-
//!    scanning logic under `dispatch::meta_retention::columnar_plain`.
//!    This file adds the registry + WAL-payload edge cases that bind
//!    the three engines together.
//!
//! Scheduler wiring (`BitemporalRetentionRegistry` + enforcement loop)
//! is exercised here without spinning up the full Tokio event plane:
//! we use the registry's snapshot API directly and verify each entry
//! maps to the correct `MetaOp::TemporalPurge*` variant by tag.

use nodedb_types::TenantId;
use nodedb_types::config::BitemporalRetention;
use nodedb_types::temporal::ms_to_ordinal_upper;
use nodedb_wal::{TemporalPurgeEngine, TemporalPurgePayload};

use nodedb::engine::bitemporal::{
    BitemporalEngineKind, BitemporalRetentionRegistry, RegisterError,
};
use nodedb::engine::graph::edge_store::EdgeStore;
use nodedb::engine::graph::edge_store::temporal::EdgeRef;
use nodedb::engine::sparse::btree::SparseEngine;
use nodedb::engine::sparse::btree_versioned::VersionedPut;

// ---------- EdgeStore ----------

#[test]
fn edge_store_end_to_end_purge() {
    let dir = tempfile::tempdir().unwrap();
    let store = EdgeStore::open(&dir.path().join("g.redb")).unwrap();
    let tid = TenantId::new(1);
    let edge = EdgeRef::new(tid, "friends", "alice", "KNOWS", "bob");

    // Three system-time versions (ordinals derived from ms so the
    // purge's ms→ordinal conversion lines up with the stored keys).
    for ms in [100i64, 200, 300] {
        let ord = ms_to_ordinal_upper(ms);
        store
            .put_edge_versioned(edge, b"v", ord, 0, i64::MAX)
            .unwrap();
    }

    // Cutoff 150 ms: only v@100 is both < cutoff AND superseded.
    let purged = store
        .purge_superseded_versions(tid, "friends", /* cutoff_system_ms */ 150)
        .unwrap();
    assert_eq!(purged, 1, "only v@100 is both below cutoff and superseded");

    // Idempotent.
    let purged2 = store
        .purge_superseded_versions(tid, "friends", 150)
        .unwrap();
    assert_eq!(purged2, 0);
}

// ---------- DocumentStrict ----------

#[test]
fn document_strict_end_to_end_purge() {
    let dir = tempfile::tempdir().unwrap();
    let eng = SparseEngine::open(&dir.path().join("s.redb")).unwrap();

    for sys in [100i64, 200, 300] {
        eng.versioned_put(VersionedPut {
            tenant: 1,
            coll: "users",
            doc_id: "u1",
            body: b"payload",
            sys_from_ms: sys,
            valid_from_ms: 0,
            valid_until_ms: i64::MAX,
        })
        .unwrap();
    }

    let (docs, idx) = eng
        .purge_superseded_document_versions(1, "users", 150)
        .unwrap();
    assert_eq!(docs, 1, "v@100 is dropped, v@200 + v@300 preserved");
    assert_eq!(idx, 0, "no secondary index versions written in this test");

    // Preserve the latest even when every version is below cutoff.
    eng.versioned_put(VersionedPut {
        tenant: 1,
        coll: "orphan",
        doc_id: "o1",
        body: b"payload",
        sys_from_ms: 400,
        valid_from_ms: 0,
        valid_until_ms: i64::MAX,
    })
    .unwrap();
    let (docs, _) = eng
        .purge_superseded_document_versions(1, "orphan", 10_000_000)
        .unwrap();
    assert_eq!(docs, 0, "single version is the latest — never deleted");
}

// ---------- Registry (compliance floor + engine kind routing) ----------

#[test]
fn registry_rejects_audit_below_floor() {
    let reg = BitemporalRetentionRegistry::new();
    let err = reg
        .register(
            TenantId::new(1),
            "users",
            BitemporalEngineKind::DocumentStrict,
            BitemporalRetention {
                data_retain_ms: 0,
                audit_retain_ms: 60_000,
                minimum_audit_retain_ms: 120_000,
            },
        )
        .expect_err("must reject below floor");
    matches!(err, RegisterError::Invalid(_));
    assert!(reg.is_empty());
}

#[test]
fn registry_snapshot_yields_one_entry_per_engine_kind() {
    let reg = BitemporalRetentionRegistry::new();
    let r = BitemporalRetention {
        data_retain_ms: 0,
        audit_retain_ms: 604_800_000, // 7d
        minimum_audit_retain_ms: 0,
    };
    reg.register(
        TenantId::new(1),
        "friends",
        BitemporalEngineKind::EdgeStore,
        r,
    )
    .unwrap();
    reg.register(
        TenantId::new(1),
        "users",
        BitemporalEngineKind::DocumentStrict,
        r,
    )
    .unwrap();
    reg.register(
        TenantId::new(1),
        "events",
        BitemporalEngineKind::Columnar,
        r,
    )
    .unwrap();

    let snap = reg.snapshot();
    assert_eq!(snap.len(), 3);

    let mut kinds: Vec<_> = snap.iter().map(|e| e.engine).collect();
    kinds.sort_by_key(|k| match k {
        BitemporalEngineKind::EdgeStore => 0,
        BitemporalEngineKind::DocumentStrict => 1,
        BitemporalEngineKind::Columnar => 2,
    });
    assert_eq!(
        kinds,
        vec![
            BitemporalEngineKind::EdgeStore,
            BitemporalEngineKind::DocumentStrict,
            BitemporalEngineKind::Columnar
        ]
    );
    // Wire-tag mapping is stable and distinct per kind.
    assert_eq!(
        BitemporalEngineKind::EdgeStore.wire_tag(),
        TemporalPurgeEngine::EdgeStore
    );
    assert_eq!(
        BitemporalEngineKind::DocumentStrict.wire_tag(),
        TemporalPurgeEngine::DocumentStrict
    );
    assert_eq!(
        BitemporalEngineKind::Columnar.wire_tag(),
        TemporalPurgeEngine::Columnar
    );
}

// ---------- WAL payload (end-to-end roundtrip of the audit record) ----------

#[test]
fn wal_temporal_purge_payload_survives_engine_roundtrip() {
    // Simulate what the scheduler emits after a successful EdgeStore purge.
    let payload = TemporalPurgePayload::new(
        TemporalPurgeEngine::EdgeStore,
        "friends",
        /* cutoff_system_ms */ 1_700_000_000_000,
        /* purged_count */ 42,
    );
    let bytes = payload.to_bytes().unwrap();
    let decoded = TemporalPurgePayload::from_bytes(&bytes).unwrap();
    assert_eq!(decoded.engine, TemporalPurgeEngine::EdgeStore);
    assert_eq!(decoded.collection, "friends");
    assert_eq!(decoded.cutoff_system_ms, 1_700_000_000_000);
    assert_eq!(decoded.purged_count, 42);
}
