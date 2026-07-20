// SPDX-License-Identifier: BUSL-1.1

//! Regression coverage for the DROP-collection engine-purge fail-CLOSED
//! contract (DEC-4).
//!
//! ## The bug this locks out
//!
//! Collection DROP removes the catalog row at raft apply and then runs
//! the redb + versioned engine purge (`clear_collection_all_engines`,
//! dispatched via `MetaOp::UnregisterCollection`) on every node. Before
//! the fix, that purge ran as a detached fire-and-forget whose failure
//! was swallowed by two layers of `warn!` — so a per-node purge failure
//! left engine storage rows behind a gone catalog row: permanent
//! divergence that resurrects the dropped collection's history when the
//! name is re-CREATEd.
//!
//! The fix makes the engine purge result-checked and, on failure,
//! records a durable `_system.pending_reclaim` entry that a worker (and
//! a boot-time drain) retries until the purge succeeds. This test
//! exercises that durable at-least-once contract at the reachable
//! SystemCatalog layer.
//!
//! ## Why this fails on the pre-fix tree
//!
//! The `_system.pending_reclaim` table, `StoredPendingReclaim`, and the
//! `{enqueue,load,record_pending_reclaim_attempt,remove}_pending_reclaim`
//! surface do not exist before the fix — a failed engine purge had
//! nowhere durable to go and was warn-and-forgotten. This test asserts
//! the failure is durably captured, survives a catalog reopen (the
//! boot-drain's input), and is only cleared once the purge succeeds.
//!
//! ## What is NOT reachable here
//!
//! Forcing an actual Data-Plane `Status::Error` from
//! `dispatch_unregister_collection` needs a live `SharedState` + running
//! TPC Data Plane, which this catalog-layer harness cannot cheaply
//! drive; that call site's result check is structural. The end-to-end
//! raft post-apply drive is cluster-test territory
//! (`nodedb-cluster-tests`, stage 2). Here we pin the durable-record +
//! worker-drain + reboot-persistence semantics that the worker and
//! boot-drain both depend on.

use nodedb::control::security::catalog::{StoredPendingReclaim, SystemCatalog};

const TENANT: u64 = 1;

fn entry(name: &str, purge_lsn: u64, err: &str) -> StoredPendingReclaim {
    StoredPendingReclaim {
        database_id: nodedb_types::DatabaseId::DEFAULT.as_u64(),
        tenant_id: TENANT,
        name: name.to_string(),
        purge_lsn,
        enqueued_at_ns: 42,
        last_error: err.to_string(),
        attempts: 0,
    }
}

/// A failed engine purge is durably recorded — NOT silently swallowed —
/// and only removed once the retry succeeds. This is the whole point of
/// the fix: the pre-fix tree had no such durable record, so the failure
/// (and the surviving engine rows) vanished from every observable
/// surface.
#[test]
fn failed_engine_purge_is_durably_recorded_then_reaped_on_success() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("system.redb");
    let catalog = SystemCatalog::open(&path).unwrap();

    // Drop applied its catalog row but the engine purge failed on this
    // node: the fix records a pending-reclaim entry.
    catalog
        .enqueue_pending_reclaim(&entry(
            "events",
            900,
            "dp: engine purge returned Status::Error",
        ))
        .unwrap();

    // The failure is observable, not lost to a warn log.
    let q = catalog.load_pending_reclaim_queue().unwrap();
    assert_eq!(
        q.len(),
        1,
        "failed engine purge must leave a durable record"
    );
    assert_eq!(q[0].name, "events");
    assert_eq!(q[0].purge_lsn, 900);

    // Worker retry fails again: attempts/last_error advance, entry stays.
    catalog
        .record_pending_reclaim_attempt(0, TENANT, "events", "dp: timeout")
        .unwrap();
    let q = catalog.load_pending_reclaim_queue().unwrap();
    assert_eq!(q.len(), 1);
    assert_eq!(q[0].attempts, 1);
    assert_eq!(q[0].last_error, "dp: timeout");

    // Worker retry finally succeeds: entry is reaped so retries stop.
    catalog.remove_pending_reclaim(0, TENANT, "events").unwrap();
    assert!(
        catalog.load_pending_reclaim_queue().unwrap().is_empty(),
        "successful engine purge must clear the pending-reclaim record"
    );
}

/// A node that crashed with an outstanding purge must still find the
/// entry after reboot so the boot-drain can complete it. Proves the
/// record is durable across a catalog reopen (the boot-repair input).
#[test]
fn pending_reclaim_survives_catalog_reopen_for_boot_drain() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("system.redb");

    {
        let catalog = SystemCatalog::open(&path).unwrap();
        catalog
            .enqueue_pending_reclaim(&entry("orders", 1234, "dp: crash before purge completed"))
            .unwrap();
    }

    // Simulate reboot: reopen the same on-disk catalog.
    let catalog = SystemCatalog::open(&path).unwrap();
    let q = catalog.load_pending_reclaim_queue().unwrap();
    assert_eq!(
        q.len(),
        1,
        "outstanding purge must survive reboot so the boot-drain completes it"
    );
    assert_eq!(q[0].name, "orders");
    assert_eq!(q[0].purge_lsn, 1234);
}
