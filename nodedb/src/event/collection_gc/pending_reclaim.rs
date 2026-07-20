// SPDX-License-Identifier: BUSL-1.1

//! Pending engine-reclaim worker — post-drop storage-purge backlog.
//!
//! Drains `_system.pending_reclaim` — one entry per collection whose
//! catalog row was removed at DROP apply but whose redb + versioned
//! engine purge (`clear_collection_all_engines`, via
//! `MetaOp::UnregisterCollection`) did not succeed on this node. Left
//! outstanding, that failure leaves engine storage rows behind a gone
//! catalog row — permanent divergence that resurrects the dropped
//! collection's history when the name is re-CREATEd. Each pass re-runs
//! the engine purge for every queued entry: on success the entry is
//! removed; on failure `record_pending_reclaim_attempt` bumps `attempts`
//! and stores `last_error` so operators can see via
//! `_system.pending_reclaim` why an entry is stuck.
//!
//! Runs on every node (leader and followers) — each node owns and
//! retries its own local reclaim. Structure mirrors `l2_cleanup.rs`.
//!
//! Tick cadence defaults to 30s. The engine purge is idempotent, so a
//! retry that races a concurrent success is harmless.

use std::sync::Arc;
use std::time::Duration;

use tokio::task::JoinHandle;
use tracing::{debug, info, warn};

use crate::control::state::SharedState;

const TICK_INTERVAL: Duration = Duration::from_secs(30);

/// Handle for the spawned worker task.
#[derive(Debug)]
pub struct PendingReclaimWorker {
    pub handle: JoinHandle<()>,
}

/// Spawn the pending-reclaim worker.
pub fn spawn_pending_reclaim(shared: Arc<SharedState>) -> PendingReclaimWorker {
    let handle = tokio::spawn(async move { run_loop(shared).await });
    PendingReclaimWorker { handle }
}

async fn run_loop(shared: Arc<SharedState>) {
    info!(
        tick_secs = TICK_INTERVAL.as_secs(),
        "pending-reclaim worker started"
    );
    loop {
        tokio::time::sleep(TICK_INTERVAL).await;
        drain_once(&shared).await;
    }
}

/// One worker pass: re-run the engine purge for every queued entry.
/// Public for the boot-time drain and for testing.
pub async fn drain_once(shared: &SharedState) {
    let catalog = shared.credentials.catalog();

    let queue = match catalog.load_pending_reclaim_queue() {
        Ok(q) => q,
        Err(e) => {
            warn!(error = %e, "pending-reclaim: failed to load queue");
            return;
        }
    };

    if queue.is_empty() {
        return;
    }

    for entry in queue {
        match crate::control::server::shared::ddl::neutral::collection::purge::dispatch_unregister_collection(
            shared,
            entry.database_id,
            entry.tenant_id,
            &entry.name,
            entry.purge_lsn,
        )
        .await
        {
            Ok(()) => {
                if let Err(e) = catalog.remove_pending_reclaim(entry.database_id, entry.tenant_id, &entry.name) {
                    warn!(
                        tenant = entry.tenant_id,
                        collection = %entry.name,
                        error = %e,
                        "pending-reclaim: purged engine rows but failed to reap queue entry"
                    );
                    continue;
                }
                debug!(
                    tenant = entry.tenant_id,
                    collection = %entry.name,
                    purge_lsn = entry.purge_lsn,
                    "pending-reclaim: drained queue entry — engine storage purged"
                );
            }
            Err(e) => {
                let msg = e.to_string();
                if let Err(update_err) =
                    catalog.record_pending_reclaim_attempt(
                        entry.database_id,
                        entry.tenant_id,
                        &entry.name,
                        &msg,
                    )
                {
                    warn!(
                        tenant = entry.tenant_id,
                        collection = %entry.name,
                        error = %update_err,
                        "pending-reclaim: failed to record attempt"
                    );
                }
                warn!(
                    tenant = entry.tenant_id,
                    collection = %entry.name,
                    attempts = entry.attempts + 1,
                    error = %msg,
                    "pending-reclaim: engine purge failed; will retry next tick"
                );
            }
        }
    }
}
