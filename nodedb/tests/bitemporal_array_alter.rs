//! Integration tests: `ALTER NDARRAY ... SET (...)`.
//!
//! Verifies the four correctness properties required by Phase B:
//! 1. Raise retention round-trip — catalog + registry both reflect new value.
//! 2. Lower below compliance floor → `Err`; catalog + registry unchanged.
//! 3. `audit_retain_ms = NULL` → registry no longer contains entry.
//! 4. Non-existent array → plan error (not-found equivalent).

mod common;

use common::pgwire_harness::TestServer;
use nodedb::engine::bitemporal::{BitemporalEngineKind, BitemporalRetentionRegistry};
use nodedb_types::TenantId;

// ── test 1 ────────────────────────────────────────────────────────────────────

/// `ALTER NDARRAY` raising `audit_retain_ms` updates both the catalog entry
/// and the bitemporal retention registry.
///
/// Flow:
/// 1. CREATE with `audit_retain_ms = 1000`.
/// 2. ALTER to `audit_retain_ms = 5000`.
/// 3. Catalog entry has `audit_retain_ms = Some(5000)`.
/// 4. Hydrate a fresh registry from the catalog — registry contains 5000.
#[tokio::test]
async fn alter_raises_retention_round_trip() {
    let srv = TestServer::start().await;

    srv.exec(
        "CREATE ARRAY alter_raise \
         DIMS (x INT64 [0..1000]) \
         ATTRS (v INT64) \
         TILE_EXTENTS (100) \
         WITH (audit_retain_ms = 1000, minimum_audit_retain_ms = 0)",
    )
    .await
    .expect("CREATE ARRAY must succeed");

    srv.exec("ALTER NDARRAY alter_raise SET (audit_retain_ms = 5000)")
        .await
        .expect("ALTER NDARRAY must succeed");

    // Catalog must reflect new value.
    let cat = srv.shared.array_catalog.read().unwrap();
    let entry = cat
        .lookup_by_name("alter_raise")
        .expect("entry must exist after ALTER");
    assert_eq!(
        entry.audit_retain_ms,
        Some(5000),
        "catalog must reflect updated audit_retain_ms"
    );
    drop(cat);

    // Registry must reflect new value.
    let snap = srv.shared.bitemporal_retention_registry.snapshot();
    let arr = snap
        .iter()
        .find(|e| e.collection == "alter_raise")
        .expect("registry must contain alter_raise after ALTER");
    assert_eq!(
        arr.retention.audit_retain_ms, 5000,
        "registry must reflect updated audit_retain_ms"
    );
    assert_eq!(arr.engine, BitemporalEngineKind::Array);

    // Simulate restart: hydrate a fresh registry from the catalog and verify.
    let fresh_registry = BitemporalRetentionRegistry::new();
    {
        let guard = srv.shared.array_catalog.read().unwrap();
        for e in guard.all_entries() {
            let Some(ms) = e.audit_retain_ms else {
                continue;
            };
            if ms < 0 {
                continue;
            }
            let retention = nodedb_types::config::BitemporalRetention {
                data_retain_ms: 0,
                audit_retain_ms: ms as u64,
                minimum_audit_retain_ms: e.minimum_audit_retain_ms.unwrap_or(0),
            };
            fresh_registry
                .register(
                    TenantId::new(0),
                    e.name.clone(),
                    BitemporalEngineKind::Array,
                    retention,
                )
                .expect("fresh registry register must succeed");
        }
    }
    let fresh_snap = fresh_registry.snapshot();
    let fresh_arr = fresh_snap
        .iter()
        .find(|e| e.collection == "alter_raise")
        .expect("fresh registry must contain alter_raise after re-hydration");
    assert_eq!(
        fresh_arr.retention.audit_retain_ms, 5000,
        "fresh registry must have updated value after re-hydration"
    );
}

// ── test 2 ────────────────────────────────────────────────────────────────────

/// `ALTER NDARRAY` attempting to lower `audit_retain_ms` below the compliance
/// floor must be rejected. The catalog entry and registry must remain
/// unchanged.
#[tokio::test]
async fn alter_below_floor_rejected() {
    let srv = TestServer::start().await;

    srv.exec(
        "CREATE ARRAY alter_floor \
         DIMS (x INT64 [0..1000]) \
         ATTRS (v INT64) \
         TILE_EXTENTS (100) \
         WITH (audit_retain_ms = 10000, minimum_audit_retain_ms = 5000)",
    )
    .await
    .expect("CREATE ARRAY must succeed");

    // Try to lower below the floor.
    let result = srv
        .exec("ALTER NDARRAY alter_floor SET (audit_retain_ms = 1000)")
        .await;
    assert!(
        result.is_err(),
        "ALTER NDARRAY below compliance floor must be rejected"
    );

    // Catalog must be unchanged.
    let cat = srv.shared.array_catalog.read().unwrap();
    let entry = cat
        .lookup_by_name("alter_floor")
        .expect("entry must still exist after rejected ALTER");
    assert_eq!(
        entry.audit_retain_ms,
        Some(10000),
        "catalog must be unchanged after rejected ALTER"
    );
    assert_eq!(
        entry.minimum_audit_retain_ms,
        Some(5000),
        "floor must be unchanged after rejected ALTER"
    );
    drop(cat);

    // Registry must still contain the original value.
    let snap = srv.shared.bitemporal_retention_registry.snapshot();
    let arr = snap
        .iter()
        .find(|e| e.collection == "alter_floor")
        .expect("registry must still contain alter_floor after rejected ALTER");
    assert_eq!(
        arr.retention.audit_retain_ms, 10000,
        "registry must be unchanged after rejected ALTER"
    );
}

// ── test 3 ────────────────────────────────────────────────────────────────────

/// `ALTER NDARRAY ... SET (audit_retain_ms = NULL)` unregisters the array
/// from the bitemporal retention registry. The catalog entry must reflect
/// `audit_retain_ms = None`.
#[tokio::test]
async fn alter_set_null_unregisters_from_registry() {
    let srv = TestServer::start().await;

    srv.exec(
        "CREATE ARRAY alter_null \
         DIMS (x INT64 [0..1000]) \
         ATTRS (v INT64) \
         TILE_EXTENTS (100) \
         WITH (audit_retain_ms = 86400000)",
    )
    .await
    .expect("CREATE ARRAY must succeed");

    // Confirm it's registered.
    let before = srv.shared.bitemporal_retention_registry.snapshot();
    assert!(
        before.iter().any(|e| e.collection == "alter_null"),
        "registry must contain alter_null before ALTER"
    );

    srv.exec("ALTER NDARRAY alter_null SET (audit_retain_ms = NULL)")
        .await
        .expect("ALTER NDARRAY SET NULL must succeed");

    // Registry must no longer contain this entry.
    let after = srv.shared.bitemporal_retention_registry.snapshot();
    assert!(
        !after.iter().any(|e| e.collection == "alter_null"),
        "registry must not contain alter_null after SET audit_retain_ms = NULL"
    );

    // Catalog must reflect null.
    let cat = srv.shared.array_catalog.read().unwrap();
    let entry = cat
        .lookup_by_name("alter_null")
        .expect("entry must still exist in catalog");
    assert_eq!(
        entry.audit_retain_ms, None,
        "catalog audit_retain_ms must be None after SET NULL"
    );
}

// ── test 4 ────────────────────────────────────────────────────────────────────

/// `ALTER NDARRAY` on a non-existent array must return an error.
#[tokio::test]
async fn alter_nonexistent_array_returns_error() {
    let srv = TestServer::start().await;

    let result = srv
        .exec("ALTER NDARRAY does_not_exist SET (audit_retain_ms = 5000)")
        .await;

    assert!(
        result.is_err(),
        "ALTER NDARRAY on non-existent array must return an error"
    );
}
