// SPDX-License-Identifier: BUSL-1.1

//! Collection-specific async post-apply dispatchers.
//!
//! Runs on **every node**: the metadata applier awaits
//! `run_post_apply_async_side_effects`. Each node's local Data Plane
//! observes catalog mutations symmetrically.

use tracing::{debug, warn};

use crate::control::catalog_entry::post_apply::collection;
use crate::control::security::catalog::{StoredCollection, StoredL2CleanupEntry};
use crate::control::state::SharedState;
use crate::types::{DatabaseId, TenantId};

/// Register `stored` on every local Data Plane core. `Err` means a core did
/// not acknowledge the Register.
pub async fn put_async(stored: &StoredCollection, shared: &SharedState) -> crate::Result<()> {
    collection::put_async(stored, shared).await
}

/// Register every shadow collection a `CloneDatabase` entry stamped into
/// `target` on every local Data Plane core.
///
/// The clone writes its shadow descriptors straight into the catalog, so no
/// `PutCollection` entry registers them. An unregistered strict shadow stores
/// a copied-up or materialized row as MessagePack, and a scan that decodes it
/// as a Binary Tuple once the collection registers matches nothing.
pub async fn clone_shadows_async(target: DatabaseId, shared: &SharedState) -> crate::Result<()> {
    for stored in collection::clone_shadows(target, shared)? {
        collection::put_async(&stored, shared).await?;
    }
    Ok(())
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
///   durable inactive catalog row. Leaking the drain here wedges every
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
    // No replicated write reaches a core under this key while its storage
    // goes: the write routes under the key's gate held shared.
    let _gate =
        crate::control::write_gate::exclusive(crate::control::write_gate::GateKey::Collection {
            database_id,
            tenant_id,
            name: name.to_string(),
        })
        .await;
    // 1. Persist to redb (every node has its own catalog). A failure here
    // leaves no durable retry owner, so it is a `no_retry` failure: the caller
    // releases its lifecycle guard rather than leaking the drain.
    crate::fail_point_err!(
        crate::fail_point::FailScope::Node(shared.node_id),
        "collection_reclaim::before_tombstone",
        |detail: String| {
            ReclaimFailure::no_retry(crate::Error::Storage {
                engine: "catalog".into(),
                detail,
            })
        }
    );
    let catalog = shared.credentials.catalog();
    catalog
        .record_wal_tombstone(database_id, tenant_id, name, purge_lsn)
        .map_err(ReclaimFailure::no_retry)?;

    // 1b. Drop the collection's column-redaction policies. Their key carries
    // no collection generation, so a survivor re-attaches to a same-name
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
    let hold =
        (!drain_already_held).then(|| shared.quiesce.begin_drain(database_id, tenant_id, name));
    wait_drained_reporting_progress(shared, database_id, tenant_id, name).await;

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
            // Keep the drain ONLY when a durable retry record is persisted:
            // the hold passes to the pending-reclaim path, which releases it
            // when the retry succeeds. A same-name CREATE waits until then,
            // because engine keys are name-scoped. If recording the durable
            // entry itself fails there is no owner, so this is a `no_retry`
            // failure: this hold drops here, and the caller's guard releases
            // its own.
            match record_pending_reclaim(
                shared,
                database_id,
                tenant_id,
                name,
                purge_lsn,
                &e.to_string(),
            ) {
                Ok(()) => {
                    if let Some(hold) = hold {
                        hold.hand_to_reclaim();
                    }
                    Err(ReclaimFailure::retry_queued(e))
                }
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
            // Saturated per-session channels can drop the notification; offline
            // replay remains the fallback.
            shared.crdt_sync_delivery.broadcast_collection_purged(
                tenant_id,
                DatabaseId::new(database_id),
                name,
                purge_lsn,
            );

            // A prior failed attempt can leave a durable entry; a
            // succeeding purge clears it and the hold that entry owned.
            // This call's own hold drops on return.
            shared
                .credentials
                .catalog()
                .remove_pending_reclaim(database_id, tenant_id, name)
                .map_err(ReclaimFailure::no_retry)?;
            shared
                .quiesce
                .release_reclaim_hold(&crate::bridge::quiesce::ReclaimOwner::new(
                    database_id,
                    tenant_id,
                    name,
                ));
            drop(hold);
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

/// How often a quiesce drain checks for closed scans.
const DRAIN_PROGRESS_TICK: std::time::Duration = std::time::Duration::from_millis(100);

/// Wait until every open scan of the collection closes. Each closed scan
/// counts as apply progress, so a proposer waiting on this purge keeps
/// waiting while scans close and times out only when none does.
///
/// A draining collection admits no new scan, so the open count only falls.
async fn wait_drained_reporting_progress(
    shared: &SharedState,
    database_id: u64,
    tenant_id: u64,
    name: &str,
) {
    let drained = shared
        .quiesce
        .wait_until_drained(database_id, tenant_id, name);
    tokio::pin!(drained);
    let mut open = shared.quiesce.open_scans(database_id, tenant_id, name);
    loop {
        tokio::select! {
            () = &mut drained => return,
            () = tokio::time::sleep(DRAIN_PROGRESS_TICK) => {
                let now = shared.quiesce.open_scans(database_id, tenant_id, name);
                if now < open {
                    shared
                        .metadata_apply_progress
                        .fetch_add(1, std::sync::atomic::Ordering::Release);
                }
                open = now;
            }
        }
    }
}

/// Clear this node's storage under `name` before a new incarnation of the
/// collection registers there.
///
/// Data Plane storage is keyed by `(database, tenant, name)`, so rows an
/// earlier incarnation left behind read as the new incarnation's. That
/// happens when a reclaim was dropped or never ran on this node. Both
/// tombstone surfaces are written first, so WAL replay skips the earlier
/// incarnation's records too.
///
/// It runs on every new incarnation. No local state proves the node never
/// held the name: WAL tombstones are collected once the WAL truncates past
/// them, and a cancelled create or a dropped retry leaves none. Clearing an
/// empty prefix costs one round-trip per core.
///
/// A pending reclaim for the name is covered by this clear, so its row goes
/// and the pending-reclaim path's hold on the name is released.
///
/// No write to the new incarnation precedes the clear on any node. Every
/// data-group entry and every Calvin transaction carries the proposer's
/// applied metadata index, and a replica holds it until its own metadata
/// watcher reaches that index. A data-group snapshot carries its builder's
/// applied metadata index, and its install waits the same way. The watcher bumps only after the applier,
/// this clear included, returned.
pub(crate) async fn clear_before_recreate(
    shared: &SharedState,
    database_id: u64,
    tenant_id: u64,
    name: &str,
) -> crate::Result<()> {
    let catalog = shared.credentials.catalog();
    let purge_lsn = shared.wal.next_lsn().as_u64();
    catalog.record_wal_tombstone(database_id, tenant_id, name, purge_lsn)?;
    shared
        .wal
        .appender(crate::wal::manager::NO_APPLY_KEY)
        .append_collection_tombstone(
            TenantId::new(tenant_id),
            DatabaseId::new(database_id),
            name,
            purge_lsn,
        )?;
    // Scans of the earlier incarnation must release before its segments go.
    let hold = shared.quiesce.begin_drain(database_id, tenant_id, name);
    shared
        .quiesce
        .wait_until_drained(database_id, tenant_id, name)
        .await;
    let cleared =
        crate::control::server::shared::ddl::neutral::collection::purge::dispatch_unregister_collection(
            shared, database_id, tenant_id, name, purge_lsn,
        )
        .await;
    hold.release();
    cleared?;

    let owed = catalog
        .load_pending_reclaim_queue()?
        .into_iter()
        .any(|entry| {
            entry.database_id == database_id && entry.tenant_id == tenant_id && entry.name == name
        });
    if owed {
        catalog.remove_pending_reclaim(database_id, tenant_id, name)?;
    }
    shared
        .quiesce
        .release_reclaim_hold(&crate::bridge::quiesce::ReclaimOwner::new(
            database_id,
            tenant_id,
            name,
        ));
    debug!(
        collection = %name,
        tenant = tenant_id,
        purge_lsn,
        owed,
        "catalog_entry: storage under the name cleared before the new incarnation registers"
    );
    Ok(())
}

/// Persist a durable `_system.pending_reclaim` entry so the failed
/// engine purge is retried at-least-once by the pending-reclaim worker
/// and the boot-time drain, instead of being lost to a warn log. A failed
/// engine purge is never warn-and-forget.
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
    // The row `prepare_purge` left inactive names the incarnation the retry
    // owns. A retry never touches a later row under the same name.
    let target_hlc = catalog
        .get_committed_collection(DatabaseId::new(database_id), tenant_id, name)?
        .map(|row| row.modification_hlc);
    let entry = crate::control::security::catalog::StoredPendingReclaim {
        database_id,
        tenant_id,
        name: name.to_string(),
        purge_lsn,
        enqueued_at_ns: now_ns,
        last_error: last_error.to_string(),
        attempts: 0,
        target_hlc,
        cancelled_create: false,
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
