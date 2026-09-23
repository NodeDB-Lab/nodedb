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

/// Failure outcome of [`reclaim_collection_storage`].
///
/// `retry_queued` distinguishes the two failure shapes a lifecycle-guard
/// holder must handle differently:
///
/// - `true` — a durable `_system.pending_reclaim` record was persisted, so a
///   worker (and the boot-time drain) owns the retry and will release the
///   lifecycle drain via `forget` once it completes. The holder must `disarm`
///   its guard so it does NOT also release the drain.
/// - `false` — no durable owner exists for a retry (the WAL/redb tombstone
///   writes failed before any record was queued, or queuing the record itself
///   failed). The holder must let its guard `Drop` release the in-memory drain
///   so a same-name CREATE can re-acquire the lifecycle and self-heal off the
///   durable inactive catalog row. Leaking the drain here would wedge every
///   future same-name CREATE (and the GC sweeper) until the node restarts.
#[derive(Debug)]
pub(crate) struct ReclaimFailure {
    pub(crate) error: crate::Error,
    pub(crate) retry_queued: bool,
}

impl ReclaimFailure {
    pub(crate) fn no_retry(error: impl Into<crate::Error>) -> Self {
        Self {
            error: error.into(),
            retry_queued: false,
        }
    }

    fn retry_queued(error: crate::Error) -> Self {
        Self {
            error,
            retry_queued: true,
        }
    }
}

impl std::fmt::Display for ReclaimFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.error)
    }
}

/// Reclaim every engine's storage for `(tenant_id, name)` on this node — WAL
/// tombstone, redb tombstone, optional L2 cleanup enqueue, quiesce drain,
/// `MetaOp::UnregisterCollection` dispatch to the local Data Plane, and Lite
/// `CollectionPurged` broadcast.
///
/// Shared by the synchronous replicated post-apply barrier, materialized-view
/// target deletion, and the interactive re-CREATE hard-purge. All callers use
/// this one result-checked implementation rather than duplicating lifecycle
/// cleanup.
pub(crate) async fn reclaim_collection_storage(
    shared: &SharedState,
    database_id: u64,
    tenant_id: u64,
    name: &str,
    purge_lsn: u64,
    drain_already_held: bool,
) -> Result<(), ReclaimFailure> {
    // 1. Persist to redb (every node has its own catalog). A failure here
    // leaves no durable retry owner, so it is a `no_retry` failure: the caller
    // releases its lifecycle guard rather than leaking the drain.
    let catalog = shared.credentials.catalog();
    catalog
        .record_wal_tombstone(database_id, tenant_id, name, purge_lsn)
        .map_err(ReclaimFailure::no_retry)?;

    // 1b. Drop the collection's column-redaction policies. Their key carries
    // no collection generation, so a survivor would re-attach to a same-name
    // collection created later and redact columns nobody protected.
    crate::control::catalog_entry::post_apply::redaction::purge_for_collection(
        shared,
        DatabaseId::new(database_id),
        tenant_id,
        name,
    );

    // 2. Append to local WAL. Both durable tombstone surfaces are required
    // before storage reclaim; otherwise truncation or catalog loss can replay
    // predecessor writes after a same-name CREATE.
    shared
        .wal
        .appender(crate::wal::manager::NO_APPLY_KEY)
        .append_collection_tombstone(
            TenantId::new(tenant_id),
            DatabaseId::new(database_id),
            name,
            purge_lsn,
        )
        .map_err(ReclaimFailure::no_retry)?;

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
    if !drain_already_held {
        shared.quiesce.begin_drain(database_id, tenant_id, name);
    }
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
        .await
        .and_then(|()| {
            crate::control::catalog_entry::apply::collection::finalize_purge(
                database_id,
                tenant_id,
                name,
                shared.credentials.catalog(),
            )
        });

    match purge_result {
        Err(e) => {
            // Keep the lifecycle drain marker set ONLY when a durable retry
            // record is persisted: a worker then owns the retry and releases
            // the drain via `forget`. A same-name CREATE waits until that
            // retry succeeds, because engine keys are name-scoped. If recording
            // the durable entry itself fails there is no owner to release the
            // drain, so this is a `no_retry` failure and the caller must let
            // its guard release the in-memory hold.
            match record_pending_reclaim(
                shared,
                database_id,
                tenant_id,
                name,
                purge_lsn,
                &e.to_string(),
            ) {
                Ok(()) => Err(ReclaimFailure::retry_queued(e)),
                Err(record_error) => Err(ReclaimFailure::no_retry(crate::Error::Storage {
                    engine: "pending-reclaim".into(),
                    detail: format!(
                        "collection reclaim failed ({e}); durable retry record also failed: {record_error}"
                    ),
                })),
            }
        }
        Ok(()) => {
            // Broadcast only after every core reclaimed the old incarnation.
            // Saturated per-session channels may drop the notification; offline
            // replay remains the fallback.
            shared.crdt_sync_delivery.broadcast_collection_purged(
                tenant_id,
                DatabaseId::new(database_id),
                name,
                purge_lsn,
            );

            // A prior failed attempt may have left a durable entry; a
            // succeeding purge clears it, then releases CREATE waiters.
            shared
                .credentials
                .catalog()
                .remove_pending_reclaim(database_id, tenant_id, name)
                .map_err(ReclaimFailure::no_retry)?;
            if !drain_already_held {
                shared.quiesce.forget(database_id, tenant_id, name);
            }
            debug!(
                collection = %name,
                tenant = tenant_id,
                purge_lsn,
                "catalog_entry: UnregisterCollection reclaimed on local Data Plane"
            );
            Ok(())
        }
    }
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
) -> crate::Result<()> {
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
    catalog.enqueue_pending_reclaim(&entry)?;
    warn!(
        collection = %name,
        tenant = tenant_id,
        purge_lsn,
        error = %last_error,
        "engine purge failed — recorded _system.pending_reclaim entry for \
         at-least-once retry by the pending-reclaim worker"
    );
    Ok(())
}
