//! Regression coverage for the collection hard-delete pipeline at
//! the SystemCatalog layer. Exercises the idempotency contracts the
//! pgwire `drop_collection` handler relies on, plus the per-engine
//! reclaim helpers at their public surface.
//!
//! Full end-to-end `DROP → UNDROP → INSERT` integration needs a live
//! pgwire session + running Data Plane; that surface is exercised in
//! `tests/cluster_post_apply_follower_dispatch.rs` on the raft path
//! and in `nodedb-wal/tests/wal_collection_tombstone.rs` on the
//! replay path.

mod catalog_integrity_helpers;

use nodedb::control::security::catalog::SystemCatalog;
use nodedb::data::executor::handlers::reclaim;

use catalog_integrity_helpers::{TENANT, make_catalog, make_collection};

/// `delete_collection` is idempotent per its doc comment. The
/// `drop_collection` handler short-circuits re-runs by checking
/// `get_collection` — this test locks in both sides of that
/// contract at the redb layer.
#[test]
fn delete_collection_is_idempotent_and_reflected_in_get() {
    let (_tmp, catalog) = make_catalog();
    let mut coll = make_collection("users");
    coll.is_active = true;
    catalog.put_collection(&coll).unwrap();

    assert!(catalog.get_collection(TENANT, "users").unwrap().is_some());

    catalog.delete_collection(TENANT, "users").unwrap();
    assert!(
        catalog.get_collection(TENANT, "users").unwrap().is_none(),
        "post-delete get_collection must return None"
    );

    // Second call: must not error, still returns None.
    catalog.delete_collection(TENANT, "users").unwrap();
    catalog.delete_collection(TENANT, "users").unwrap();
    assert!(catalog.get_collection(TENANT, "users").unwrap().is_none());
}

/// Soft-delete flips `is_active` without removing the row — this is
/// the invariant `UNDROP` relies on and the sweeper reads to decide
/// whether the retention window has elapsed.
#[test]
fn soft_delete_preserves_row_and_clears_active_flag() {
    let (_tmp, catalog) = make_catalog();
    let mut coll = make_collection("logs");
    coll.is_active = true;
    catalog.put_collection(&coll).unwrap();

    // Simulate the applier's `DeactivateCollection` path: flip
    // `is_active` in place and re-put.
    let mut stored = catalog.get_collection(TENANT, "logs").unwrap().unwrap();
    stored.is_active = false;
    catalog.put_collection(&stored).unwrap();

    let after = catalog.get_collection(TENANT, "logs").unwrap().unwrap();
    assert!(
        !after.is_active,
        "is_active must be false after soft-delete"
    );
    assert_eq!(after.name, "logs", "row must still exist for UNDROP");

    // `load_dropped_collections` must surface the soft-deleted row
    // so the GC sweeper + `_system.dropped_collections` view see it.
    let dropped = catalog.load_dropped_collections().unwrap();
    assert!(
        dropped.iter().any(|c| c.name == "logs"),
        "soft-deleted row must appear in load_dropped_collections"
    );
}

/// L2 cleanup queue CRUD preserves the idempotency the purge pipeline
/// depends on: re-enqueue replaces in place, record-attempt updates
/// without creating a duplicate, remove is safe to call on a missing
/// key.
#[test]
fn l2_cleanup_queue_is_idempotent_end_to_end() {
    use nodedb::control::security::catalog::StoredL2CleanupEntry;

    let (_tmp, catalog) = make_catalog();
    let entry = |lsn: u64, bytes: u64, attempts: u32, err: &str| StoredL2CleanupEntry {
        tenant_id: TENANT,
        name: "events".into(),
        purge_lsn: lsn,
        enqueued_at_ns: 100,
        bytes_pending: bytes,
        last_error: err.to_string(),
        attempts,
    };

    catalog
        .enqueue_l2_cleanup(&entry(500, 2_000, 0, ""))
        .unwrap();
    // Re-enqueue with updated fields — replaces, not appends.
    catalog
        .enqueue_l2_cleanup(&entry(700, 9_000, 0, ""))
        .unwrap();

    let rows = catalog.load_l2_cleanup_queue().unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].purge_lsn, 700);
    assert_eq!(rows[0].bytes_pending, 9_000);

    // record_attempt bumps in place.
    catalog
        .record_l2_cleanup_attempt(TENANT, "events", "s3: 503")
        .unwrap();
    catalog
        .record_l2_cleanup_attempt(TENANT, "events", "s3: 503")
        .unwrap();
    let rows = catalog.load_l2_cleanup_queue().unwrap();
    assert_eq!(rows[0].attempts, 2);
    assert_eq!(rows[0].last_error, "s3: 503");

    // Remove is idempotent.
    catalog.remove_l2_cleanup(TENANT, "events").unwrap();
    catalog.remove_l2_cleanup(TENANT, "events").unwrap();
    assert!(catalog.load_l2_cleanup_queue().unwrap().is_empty());

    // record_attempt on a missing key is a no-op, not an error.
    catalog
        .record_l2_cleanup_attempt(TENANT, "events", "doesn't matter")
        .unwrap();
    assert!(catalog.load_l2_cleanup_queue().unwrap().is_empty());
}

/// Per-engine reclaim handlers are idempotent — missing directories
/// / files produce zero stats, not errors. This is the contract the
/// `execute_unregister_collection` retry loop relies on.
#[test]
fn reclaim_handlers_are_idempotent_on_missing_files() {
    let tmp = tempfile::tempdir().unwrap();
    let base = tmp.path();

    // All four reclaim helpers must return default stats on a fresh
    // empty data dir.
    let vector = reclaim::vector::reclaim_vector_checkpoints(base, TENANT, "x");
    let spatial = reclaim::spatial::reclaim_spatial_checkpoints(base, TENANT, "x");
    let sparse = reclaim::sparse_vector::reclaim_sparse_vector_checkpoints(base, TENANT, "x");
    let ts = reclaim::timeseries::reclaim_timeseries_partitions(base, TENANT, "x");

    assert_eq!(vector.files_unlinked, 0);
    assert_eq!(spatial.files_unlinked, 0);
    assert_eq!(sparse.files_unlinked, 0);
    assert_eq!(ts.files_unlinked, 0);

    // Re-running must still succeed (no "already deleted" error).
    let _ = reclaim::vector::reclaim_vector_checkpoints(base, TENANT, "x");
    let _ = reclaim::spatial::reclaim_spatial_checkpoints(base, TENANT, "x");
    let _ = reclaim::sparse_vector::reclaim_sparse_vector_checkpoints(base, TENANT, "x");
    let _ = reclaim::timeseries::reclaim_timeseries_partitions(base, TENANT, "x");
}

/// The reclaim handlers don't touch other tenants' or other
/// collections' files — the isolation guarantee the pgwire handler
/// relies on when scoping the unlink to `(tenant, collection)`.
#[test]
fn reclaim_is_scoped_to_tenant_and_collection() {
    let tmp = tempfile::tempdir().unwrap();
    let base = tmp.path();
    let vec_dir = base.join("vector-ckpt");
    std::fs::create_dir_all(&vec_dir).unwrap();
    std::fs::write(vec_dir.join("1:users.ckpt"), b"a").unwrap();
    std::fs::write(vec_dir.join("1:orders.ckpt"), b"b").unwrap();
    std::fs::write(vec_dir.join("2:users.ckpt"), b"c").unwrap();

    let stats = reclaim::vector::reclaim_vector_checkpoints(base, 1, "users");
    assert_eq!(stats.files_unlinked, 1);
    assert!(!vec_dir.join("1:users.ckpt").exists());
    // Siblings untouched.
    assert!(vec_dir.join("1:orders.ckpt").exists());
    assert!(vec_dir.join("2:users.ckpt").exists());
}

fn _cat_ref_witness(_cat: &SystemCatalog) {}
