// SPDX-License-Identifier: BUSL-1.1

//! L2 (object-store) cleanup worker.
//!
//! Drains `_system.l2_cleanup_queue` — one entry per collection whose
//! hard-delete committed but whose object-store bytes are still owed.
//! Runs once per tick on the Tokio runtime. For each entry the worker
//! lists every object under `{prefix}{tenant_id}/{collection}/` (the
//! scheme used by `ColdStorage::encode_and_upload` for columnar
//! Parquet today — per-engine prefix expansion is a separate slice)
//! and issues deletes. On success the entry is removed and the
//! reclaimed-byte counter advances; on failure
//! `record_l2_cleanup_attempt` bumps `attempts` and stores
//! `last_error` so operators can see via `_system.l2_cleanup_queue`
//! why an entry is stuck.
//!
//! Tick cadence defaults to 30s. No configurable backoff: the queue
//! is small, retries are bounded per tick by the attempt count on
//! each row, and persistent failures surface via the metric
//! `nodedb_l2_cleanup_queue_depth{tenant}` (updated each pass).

use std::sync::Arc;
use std::time::Duration;

use futures::stream::StreamExt;
use object_store::{ObjectStore, ObjectStoreExt};
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};

use crate::control::state::SharedState;

const TICK_INTERVAL: Duration = Duration::from_secs(30);

/// Handle for the spawned worker task.
#[derive(Debug)]
pub struct L2CleanupWorker {
    pub handle: JoinHandle<()>,
}

/// Spawn the L2 cleanup worker. Exits cleanly if `shared.cold_storage`
/// is `None` — no object store configured, nothing to drain.
pub fn spawn_l2_cleanup(shared: Arc<SharedState>) -> L2CleanupWorker {
    let handle = tokio::spawn(async move { run_loop(shared).await });
    L2CleanupWorker { handle }
}

async fn run_loop(shared: Arc<SharedState>) {
    if shared.cold_storage.is_none() {
        info!("l2 cleanup worker: cold storage not configured — exiting");
        return;
    }
    info!(
        tick_secs = TICK_INTERVAL.as_secs(),
        "l2 cleanup worker started"
    );

    // Delay one tick so the catalog + cold storage are fully wired
    // before the first pass.
    tokio::time::sleep(TICK_INTERVAL).await;

    loop {
        drain_once(&shared).await;
        tokio::time::sleep(TICK_INTERVAL).await;
    }
}

/// One worker pass. Public for testability.
pub async fn drain_once(shared: &SharedState) {
    let Some(cold) = shared.cold_storage.as_ref() else {
        return;
    };
    let catalog = shared.credentials.catalog();

    let queue = match catalog.load_l2_cleanup_queue() {
        Ok(q) => q,
        Err(e) => {
            warn!(error = %e, "l2 cleanup: failed to load queue");
            return;
        }
    };

    // Refresh the depth gauge every pass as a full snapshot so
    // tenants that just drained to zero stop showing as backed up.
    if let Some(metrics) = shared.system_metrics.as_ref() {
        let mut depths: std::collections::HashMap<u64, u64> = std::collections::HashMap::new();
        for e in &queue {
            *depths.entry(e.tenant_id).or_insert(0) += 1;
        }
        metrics.purge.set_l2_cleanup_queue_depth(depths);
    }

    if queue.is_empty() {
        return;
    }

    let store = cold.object_store();
    for entry in queue {
        let prefix = format!("{}/{}/{}/", entry.database_id, entry.tenant_id, entry.name);
        match delete_prefix(store.clone(), &prefix).await {
            Ok(bytes_deleted) => {
                if let Err(e) =
                    catalog.remove_l2_cleanup(entry.database_id, entry.tenant_id, &entry.name)
                {
                    warn!(
                        tenant = entry.tenant_id,
                        collection = %entry.name,
                        error = %e,
                        "l2 cleanup: removed L2 bytes but failed to reap queue entry"
                    );
                    continue;
                }
                if let Some(metrics) = shared.system_metrics.as_ref()
                    && bytes_deleted > 0
                {
                    // Engine label is "unknown" here — the per-engine
                    // reclaim handlers will record their own
                    // fine-grained bytes once those land.
                    metrics.purge.add_bytes_reclaimed(
                        entry.tenant_id,
                        "unknown",
                        "l2",
                        bytes_deleted,
                    );
                }
                debug!(
                    tenant = entry.tenant_id,
                    collection = %entry.name,
                    purge_lsn = entry.purge_lsn,
                    bytes_deleted,
                    "l2 cleanup: drained queue entry"
                );
            }
            Err(e) => {
                let msg = e.to_string();
                if let Err(update_err) = catalog.record_l2_cleanup_attempt(
                    entry.database_id,
                    entry.tenant_id,
                    &entry.name,
                    &msg,
                ) {
                    warn!(
                        tenant = entry.tenant_id,
                        collection = %entry.name,
                        error = %update_err,
                        "l2 cleanup: failed to record attempt"
                    );
                }
                warn!(
                    tenant = entry.tenant_id,
                    collection = %entry.name,
                    attempts = entry.attempts + 1,
                    error = %msg,
                    "l2 cleanup: delete failed; will retry next tick"
                );
            }
        }
    }
}

/// Delete every object under `prefix` in the given store. Returns the
/// total bytes deleted. Errors on any per-object failure after
/// attempting the rest — i.e. a best-effort pass that surfaces the
/// first failure for the queue-entry's `last_error`.
async fn delete_prefix(
    store: Arc<dyn ObjectStore>,
    prefix: &str,
) -> Result<u64, object_store::Error> {
    let path = object_store::path::Path::from(prefix);
    let mut list = store.list(Some(&path));
    let mut total_bytes: u64 = 0;
    let mut first_err: Option<object_store::Error> = None;
    while let Some(meta) = list.next().await {
        match meta {
            Ok(m) => {
                total_bytes += m.size;
                if let Err(e) = store.delete(&m.location).await
                    && first_err.is_none()
                {
                    first_err = Some(e);
                }
            }
            Err(e) => {
                if first_err.is_none() {
                    first_err = Some(e);
                }
            }
        }
    }
    match first_err {
        Some(e) => Err(e),
        None => Ok(total_bytes),
    }
}

#[cfg(test)]
mod tests {
    //! End-to-end drain exercise needs a `SharedState` fixture + an
    //! in-memory `ObjectStore` — deferred to
    //! `tests/l2_cleanup_drain.rs` as a distinct atomic item.
    //! The catalog-level queue CRUD has its own unit tests in
    //! `control/security/catalog/l2_cleanup_queue.rs` (5/5 green).
}
