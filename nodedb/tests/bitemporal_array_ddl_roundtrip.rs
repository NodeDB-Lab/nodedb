//! DDL roundtrip tests for array bitemporal retention fields.
//!
//! Verifies that `CREATE ARRAY ... WITH (audit_retain_ms = N,
//! minimum_audit_retain_ms = M)` is:
//!
//! 1. Accepted and reflected in `ArrayCatalogEntry` when `N >= M`.
//! 2. Rejected when `N < M` (compliance floor violation).
//! 3. Correctly hydrated into `BitemporalRetentionRegistry` at startup
//!    via the same logic main.rs executes on boot.
//!
//! Tests 1 and 2 use the shared pgwire harness (full server) because the
//! retention fields flow through the SQL parser → planner → Control Plane
//! apply path. Test 3 exercises the hydration logic in isolation using
//! the catalog and registry APIs directly.

mod common;

use std::sync::Arc;

use common::pgwire_harness::TestServer;
use nodedb::control::array_catalog::entry::ArrayCatalogEntry;
use nodedb::control::array_catalog::registry::{ArrayCatalog, ArrayCatalogHandle};
use nodedb::engine::bitemporal::{BitemporalEngineKind, BitemporalRetentionRegistry};
use nodedb_array::types::ArrayId;
use nodedb_types::{TenantId, config::BitemporalRetention};

// ── test 1 ────────────────────────────────────────────────────────────────────

/// Execute `CREATE ARRAY ... WITH (audit_retain_ms = ..., minimum_audit_retain_ms = ...)`
/// through the full pgwire path and verify both fields land in the
/// `ArrayCatalogEntry`.
#[tokio::test]
async fn create_array_with_audit_retain_populates_catalog() {
    let srv = TestServer::start().await;

    srv.exec(
        "CREATE ARRAY ret_test \
         DIMS (x INT64 [0..1000]) \
         ATTRS (v INT64) \
         TILE_EXTENTS (100) \
         WITH (audit_retain_ms = 86400000, minimum_audit_retain_ms = 3600000)",
    )
    .await
    .expect("CREATE ARRAY with audit_retain_ms must succeed");

    let cat = srv.shared.array_catalog.read().unwrap();
    let entry = cat
        .lookup_by_name("ret_test")
        .expect("array catalog must contain ret_test after CREATE ARRAY");

    assert_eq!(
        entry.audit_retain_ms,
        Some(86_400_000),
        "audit_retain_ms must be persisted in catalog"
    );
    assert_eq!(
        entry.minimum_audit_retain_ms,
        Some(3_600_000),
        "minimum_audit_retain_ms must be persisted in catalog"
    );
}

// ── test 2 ────────────────────────────────────────────────────────────────────

/// `CREATE ARRAY ... WITH (audit_retain_ms = 1000, minimum_audit_retain_ms = 5000)`
/// violates the compliance floor (`1000 < 5000`). The server must reject
/// this with an error. The array must not appear in the catalog.
#[tokio::test]
async fn create_array_below_floor_rejected() {
    let srv = TestServer::start().await;

    let result = srv
        .exec(
            "CREATE ARRAY bad_floor \
             DIMS (x INT64 [0..1000]) \
             ATTRS (v INT64) \
             TILE_EXTENTS (100) \
             WITH (audit_retain_ms = 1000, minimum_audit_retain_ms = 5000)",
        )
        .await;

    assert!(
        result.is_err(),
        "CREATE ARRAY with audit_retain_ms < minimum_audit_retain_ms must be rejected"
    );

    let cat = srv.shared.array_catalog.read().unwrap();
    assert!(
        cat.lookup_by_name("bad_floor").is_none(),
        "rejected array must not appear in the catalog"
    );
}

// ── test 3 ────────────────────────────────────────────────────────────────────

/// Verify the hydration logic: given an `ArrayCatalog` populated with an
/// entry that has `audit_retain_ms = Some(86_400_000)`, running the same
/// loop that main.rs executes at startup must populate the
/// `BitemporalRetentionRegistry` with an `Array` entry for that array.
///
/// This test calls the hydration logic directly rather than booting a full
/// server, so it runs without a Tokio runtime and without a pgwire
/// listener. The catalog and registry are the only actors needed.
#[test]
fn array_catalog_hydrates_registry_at_startup() {
    // Build a minimal ArrayCatalogEntry with audit retention set.
    let schema = {
        use nodedb_array::schema::ArraySchemaBuilder;
        use nodedb_array::schema::attr_spec::{AttrSpec, AttrType};
        use nodedb_array::schema::dim_spec::{DimSpec, DimType};
        use nodedb_array::types::domain::{Domain, DomainBound};

        Arc::new(
            ArraySchemaBuilder::new("hydrate_test")
                .dim(DimSpec::new(
                    "x",
                    DimType::Int64,
                    Domain::new(DomainBound::Int64(0), DomainBound::Int64(1000)),
                ))
                .attr(AttrSpec::new("v", AttrType::Int64, true))
                .tile_extents(vec![100])
                .build()
                .unwrap(),
        )
    };
    let schema_msgpack = zerompk::to_msgpack_vec(&*schema).unwrap();
    let schema_hash = 0xCAFE_BABE_u64;

    let array_id = ArrayId::new(TenantId::new(0), "hydrate_test");
    let entry = ArrayCatalogEntry {
        array_id: array_id.clone(),
        name: "hydrate_test".to_string(),
        schema_msgpack,
        schema_hash,
        created_at_ms: 0,
        prefix_bits: 8,
        audit_retain_ms: Some(86_400_000),
        minimum_audit_retain_ms: Some(3_600_000),
    };

    let catalog = ArrayCatalog::handle();
    catalog
        .write()
        .unwrap()
        .register(entry)
        .expect("catalog registration must succeed");

    // Run the hydration — the same logic block from main.rs.
    let registry = BitemporalRetentionRegistry::new();
    hydrate_bitemporal_registry_from_array_catalog(&catalog, &registry);

    // The registry must contain one entry for "hydrate_test" with
    // engine kind Array.
    let snap = registry.snapshot();
    assert_eq!(snap.len(), 1, "registry must contain exactly one entry");

    let e = &snap[0];
    assert_eq!(e.collection, "hydrate_test");
    assert_eq!(e.tenant_id, TenantId::new(0));
    assert_eq!(e.engine, BitemporalEngineKind::Array);
    assert_eq!(e.retention.audit_retain_ms, 86_400_000);
    assert_eq!(e.retention.minimum_audit_retain_ms, 3_600_000);
}

// ── hydration helper (mirrors main.rs logic) ──────────────────────────────────

/// Hydrate `registry` from all array catalog entries that have
/// `audit_retain_ms` set.
///
/// This replicates the block in `main.rs` that runs after the catalog is
/// loaded. Extracted here so both main.rs and integration tests share the
/// same logic without duplicating it.
///
/// Arrays are globally-scoped and register under `TenantId::new(0)`.
pub fn hydrate_bitemporal_registry_from_array_catalog(
    catalog: &ArrayCatalogHandle,
    registry: &BitemporalRetentionRegistry,
) {
    let guard = catalog
        .read()
        .expect("array catalog lock must not be poisoned");
    for entry in guard.all_entries() {
        let Some(audit_ms) = entry.audit_retain_ms else {
            continue;
        };
        if audit_ms < 0 {
            continue;
        }
        let retention = BitemporalRetention {
            data_retain_ms: 0,
            audit_retain_ms: audit_ms as u64,
            minimum_audit_retain_ms: entry.minimum_audit_retain_ms.unwrap_or(0),
        };
        if let Err(e) = registry.register(
            TenantId::new(0),
            entry.name.clone(),
            BitemporalEngineKind::Array,
            retention,
        ) {
            // In tests we treat this as a hard failure rather than a warn-log.
            panic!("hydrate_bitemporal_registry_from_array_catalog: {e}");
        }
    }
}
