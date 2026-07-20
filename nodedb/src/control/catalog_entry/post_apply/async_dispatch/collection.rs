// SPDX-License-Identifier: BUSL-1.1

//! Collection-specific async post-apply dispatchers.
//!
//! Runs on **every node** (via `spawn_post_apply_async_side_effects`
//! in `apply_replicated`). Each node's local Data Plane observes
//! catalog mutations symmetrically.

use std::sync::Arc;

use tracing::{debug, warn};

use crate::control::catalog_entry::post_apply::collection;
use crate::control::security::catalog::{StoredCollection, StoredL2CleanupEntry};
use crate::control::state::SharedState;
use crate::types::{DatabaseId, TenantId};

pub async fn put_async(stored: StoredCollection, shared: Arc<SharedState>) {
    collection::put_async(stored, shared).await;
}

/// Dispatch `MetaOp::UnregisterCollection` to this node's local Data
/// Plane so every engine reclaims storage for the purged collection.
/// Called on every node (not just the leader), so each node's Data
/// Plane reclaims its own L1 segment files, memtables, compaction
/// debt, and WAL tombstone entries locally.
///
/// Order of operations:
///
/// 1. Persist the tombstone into the local `_system.wal_tombstones`
///    redb table. Makes startup replay O(1) and survives process
///    crashes between steps 2 and 3.
/// 2. Append a `CollectionTombstoned` record to the local WAL. Replay
///    constructs the same in-memory set whether or not step 1 committed
///    (belt + suspenders — if the redb write failed but WAL succeeded,
///    replay still sees the tombstone).
/// 3. Dispatch `MetaOp::UnregisterCollection` into the Data Plane to
///    reclaim engine-local storage.
pub async fn purge_async(
    database_id: u64,
    tenant_id: u64,
    name: String,
    purge_lsn: u64,
    shared: Arc<SharedState>,
) {
    // The Result is already durably captured inside
    // `reclaim_collection_storage` (failure → `_system.pending_reclaim`
    // for at-least-once retry). This raft post-apply path runs on every
    // node and must not itself fail the apply, so it consumes the Result
    // here. There is deliberately NO warn-and-forget of the engine-purge
    // error on this path — the durable record IS the handling.
    let _ = reclaim_collection_storage(&shared, database_id, tenant_id, &name, purge_lsn).await;
}

/// Borrowed core of [`purge_async`]: reclaim every engine's storage for
/// `(tenant_id, name)` on this node — WAL tombstone, redb tombstone,
/// optional L2 cleanup enqueue, quiesce drain, `MetaOp::UnregisterCollection`
/// dispatch to the local Data Plane, and Lite `CollectionPurged` broadcast.
///
/// Split out from `purge_async` so the synchronous re-CREATE hard-purge
/// (`shared::ddl::neutral::collection::purge::hard_purge_collection`) can reuse the exact
/// same reclaim body against a `&SharedState` without an owned `Arc`, and
/// so the raft post-apply path and the re-CREATE path share one
/// implementation rather than a copy.
pub(crate) async fn reclaim_collection_storage(
    shared: &SharedState,
    database_id: u64,
    tenant_id: u64,
    name: &str,
    purge_lsn: u64,
) -> crate::Result<()> {
    // 1. Persist to redb (every node has its own catalog).
    let catalog = shared.credentials.catalog();
    if let Err(e) = catalog.record_wal_tombstone(database_id, tenant_id, name, purge_lsn) {
        warn!(
            collection = %name,
            tenant = tenant_id,
            purge_lsn,
            error = %e,
            "failed to persist WAL tombstone to _system.wal_tombstones — \
             replay will fall back to WAL extraction"
        );
    }

    // 2. Append to local WAL.
    if let Err(e) = shared.wal.append_collection_tombstone(
        TenantId::new(tenant_id),
        DatabaseId::new(database_id),
        name,
        purge_lsn,
    ) {
        warn!(
            collection = %name,
            tenant = tenant_id,
            purge_lsn,
            error = %e,
            "failed to append CollectionTombstoned WAL record"
        );
    }

    // 2b. Enqueue an L2 cleanup entry if cold storage is configured.
    // Recorded even when `bytes_pending` is unknown (0) — the worker
    // discovers actual byte count at delete time. Doing this BEFORE
    // the Data Plane dispatch means we ack even when the worker is
    // backed up or transiently offline, and `_system.l2_cleanup_queue`
    // surfaces the backlog for operators. Idempotent: re-enqueuing
    // the same `(tenant, name)` replaces the prior entry.
    if shared.cold_storage.is_some() {
        let catalog = shared.credentials.catalog();
        let now_ns = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0);
        let entry = StoredL2CleanupEntry {
            database_id,
            tenant_id,
            name: name.to_string(),
            purge_lsn,
            enqueued_at_ns: now_ns,
            bytes_pending: 0,
            last_error: String::new(),
            attempts: 0,
        };
        if let Err(e) = catalog.enqueue_l2_cleanup(&entry) {
            warn!(
                collection = %name,
                tenant = tenant_id,
                purge_lsn,
                error = %e,
                "failed to enqueue _system.l2_cleanup_queue entry — \
                 L2 bytes will not be reaped until next purge attempt"
            );
        }
    }

    // 3. Quiesce drain: stop accepting new scans for this collection
    //    and wait for in-flight scans to release. Unlinking segment
    //    files while a scan is touching an mmap page faults the
    //    whole TPC reactor — drain ordering is a correctness, not
    //    performance, requirement.
    shared.quiesce.begin_drain(database_id, tenant_id, name);
    shared
        .quiesce
        .wait_until_drained(database_id, tenant_id, name)
        .await;

    // 4. Reclaim on local Data Plane. RESULT-CHECKED: the redb +
    //    versioned engine purge is correctness-critical (the catalog
    //    row is already gone, so surviving engine rows are permanent
    //    divergence that resurrects the dropped collection's history on
    //    re-CREATE). The dispatch `.await`s a Data-Plane SPSC round-trip
    //    bounded by the dispatcher's own deadline timeout — no unbounded
    //    block is introduced on this off-critical-path spawn. On any
    //    failure we record a durable `_system.pending_reclaim` entry so
    //    a worker (and a boot-time drain) retries the purge to
    //    completion, then propagate the error so the interactive
    //    re-CREATE caller can fail closed.
    let purge_result =
        crate::control::server::shared::ddl::neutral::collection::purge::dispatch_unregister_collection(
            shared, database_id, tenant_id, name, purge_lsn,
        )
        .await;

    // 4b. Broadcast `CollectionPurged` to every connected Lite
    //     session subscribed to this collection. Fire-and-forget;
    //     each session's control-frame channel is bounded to 32 and
    //     any saturated channel drops the notification (the client
    //     picks it up on reconnect via the offline-replay path).
    shared
        .crdt_sync_delivery
        .broadcast_collection_purged(tenant_id, name, purge_lsn);

    // 5. Drop the quiesce entry. From here on, the catalog has no
    //    record of the collection; queries return `collection_not_found`.
    shared.quiesce.forget(database_id, tenant_id, name);

    if let Err(e) = &purge_result {
        record_pending_reclaim(
            shared,
            database_id,
            tenant_id,
            name,
            purge_lsn,
            &e.to_string(),
        );
    } else {
        // A prior failed attempt may have left a durable entry; a
        // succeeding purge clears it so the worker stops retrying.
        let catalog = shared.credentials.catalog();
        if let Err(rm) = catalog.remove_pending_reclaim(database_id, tenant_id, name) {
            warn!(
                collection = %name,
                tenant = tenant_id,
                error = %rm,
                "failed to reap _system.pending_reclaim entry after successful engine purge"
            );
        }
        debug!(
            collection = %name,
            tenant = tenant_id,
            purge_lsn,
            "catalog_entry: UnregisterCollection reclaimed on local Data Plane"
        );
    }

    purge_result
}

/// Persist a durable `_system.pending_reclaim` entry so the failed
/// engine purge is retried at-least-once by the pending-reclaim worker
/// and the boot-time drain, instead of being lost to a warn log. This
/// is the whole point of the fix: NEVER warn-and-forget a failed
/// engine purge.
fn record_pending_reclaim(
    shared: &SharedState,
    database_id: u64,
    tenant_id: u64,
    name: &str,
    purge_lsn: u64,
    last_error: &str,
) {
    let catalog = shared.credentials.catalog();
    let now_ns = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);
    let entry = crate::control::security::catalog::StoredPendingReclaim {
        database_id,
        tenant_id,
        name: name.to_string(),
        purge_lsn,
        enqueued_at_ns: now_ns,
        last_error: last_error.to_string(),
        attempts: 0,
    };
    if let Err(e) = catalog.enqueue_pending_reclaim(&entry) {
        // The durable record itself failed. This is the one place we
        // cannot make durable — log loudly at error so operators see it.
        tracing::error!(
            collection = %name,
            tenant = tenant_id,
            purge_lsn,
            purge_error = %last_error,
            record_error = %e,
            "engine purge failed AND recording the pending-reclaim entry failed — \
             collection storage may survive behind a removed catalog row until the \
             DROP is re-proposed or the node reboots and re-drains"
        );
    } else {
        warn!(
            collection = %name,
            tenant = tenant_id,
            purge_lsn,
            error = %last_error,
            "engine purge failed — recorded _system.pending_reclaim entry for \
             at-least-once retry by the pending-reclaim worker"
        );
    }
}
