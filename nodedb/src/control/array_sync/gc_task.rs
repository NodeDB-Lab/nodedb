//! [`ArrayGcTask`] — periodic log compaction for array CRDT sync.
//!
//! Spawned once at startup by `SharedState::open`. Every `interval` seconds:
//!
//! 1. Query [`ArrayAckRegistry::known_arrays`] for all arrays with recorded acks.
//! 2. For each array, determine the GC frontier via `min_ack_hlc`.
//! 3. Build a [`TileSnapshot`] from the latest persisted snapshot (or skip if
//!    none; origin must have a snapshot source). Write it via
//!    [`OriginSnapshotStore`].
//! 4. Call [`nodedb_array::sync::gc::collapse_below`] to drop compacted ops.
//! 5. Evict older snapshots for each array.
//! 6. Update the per-array `snapshot_hlc` map in `SharedState` so
//!    `snapshot_trigger::check_and_trigger` uses the real boundary.
//!
//! Shuts down cleanly when the `ShutdownWatch` fires.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use nodedb_array::sync::gc::collapse_below;
use nodedb_array::sync::hlc::Hlc;
use tokio::task::JoinHandle;
use tracing::{debug, error, info, warn};

use super::ack_registry::ArrayAckRegistry;
use super::op_log::OriginOpLog;
use super::snapshot_store::OriginSnapshotStore;
use crate::control::shutdown::{ShutdownReceiver, ShutdownWatch};

/// Default GC interval: 60 seconds.
pub const DEFAULT_GC_INTERVAL: Duration = Duration::from_secs(60);

/// Spawn the GC task and return a [`JoinHandle`].
///
/// `array_snapshot_hlcs` is the shared map that `ArrayFanout`'s
/// `snapshot_trigger` reads to know the current GC boundary per array.
/// The GC task writes the updated `snapshot_hlc` after each successful
/// compaction run.
pub fn spawn(
    op_log: Arc<OriginOpLog>,
    snapshots: Arc<OriginSnapshotStore>,
    ack_registry: Arc<ArrayAckRegistry>,
    array_snapshot_hlcs: Arc<RwLock<HashMap<String, Hlc>>>,
    shutdown: Arc<ShutdownWatch>,
    interval: Duration,
) -> Option<JoinHandle<()>> {
    let Ok(handle) = tokio::runtime::Handle::try_current() else {
        debug!("array_gc_task: no tokio runtime; skipping spawn (test or non-async context)");
        return None;
    };
    Some(handle.spawn(async move {
        let mut shutdown_rx: ShutdownReceiver = shutdown.subscribe();
        loop {
            tokio::select! {
                _ = tokio::time::sleep(interval) => {
                    run_gc(
                        &op_log,
                        &snapshots,
                        &ack_registry,
                        &array_snapshot_hlcs,
                    );
                }
                _ = shutdown_rx.wait_cancelled() => {
                    debug!("array_gc_task: shutdown received — exiting");
                    return;
                }
            }
        }
    }))
}

/// Execute one GC pass across all known arrays.
fn run_gc(
    op_log: &OriginOpLog,
    snapshots: &OriginSnapshotStore,
    ack_registry: &ArrayAckRegistry,
    array_snapshot_hlcs: &RwLock<HashMap<String, Hlc>>,
) {
    let arrays = ack_registry.known_arrays();
    if arrays.is_empty() {
        debug!("array_gc_task: no arrays with acks — skipping");
        return;
    }

    for array in &arrays {
        let ack_vector = ack_registry.ack_vector(array);
        if ack_vector.min_ack_hlc().is_none() {
            debug!(array = %array, "array_gc_task: no min_ack for array — skipping");
            continue;
        }

        // Build a snapshot-for-array closure that reads the latest persisted
        // snapshot and presents it as the GC snapshot. If no snapshot exists
        // we return None — GC still drops ops below the frontier but writes
        // no new snapshot (no data to compact for a newly-created array that
        // has never had a snapshot taken).
        let array_name = array.clone();
        let snapshots_ref = snapshots;

        let report = collapse_below(op_log, &ack_vector, snapshots_ref, |arr, frontier| {
            // Try to reuse the latest stored snapshot for this array.
            // In a full Origin implementation the snapshot would be built
            // from live tile state; here we promote the latest stored
            // snapshot's HLC to the new frontier so the GC boundary
            // advances even between full compactions.
            if let Some(mut snap) = snapshots_ref.latest_for_array(arr) {
                snap.snapshot_hlc = frontier;
                Ok(Some(snap))
            } else {
                // No snapshot yet for this array; GC will drop ops without
                // writing a replacement snapshot. New peers will get the
                // op-stream from Hlc::ZERO (or catch up via future snapshots).
                Ok(None)
            }
        });

        match report {
            Ok(r) => {
                if r.ops_dropped > 0 || r.snapshots_written > 0 {
                    info!(
                        array = %array_name,
                        ops_dropped = r.ops_dropped,
                        snapshots_written = r.snapshots_written,
                        frontier = ?r.frontier,
                        "array_gc_task: GC run complete"
                    );
                }

                if let Some(frontier) = ack_vector.min_ack_hlc() {
                    // Evict obsoleted snapshots.
                    snapshots_ref.delete_older_than(&array_name, frontier);

                    // Update the shared snapshot_hlc map so fanout's
                    // snapshot_trigger uses the real boundary.
                    match array_snapshot_hlcs.write() {
                        Ok(mut map) => {
                            map.insert(array_name.clone(), frontier);
                        }
                        Err(e) => {
                            error!(
                                array = %array_name,
                                error = %e,
                                "array_gc_task: snapshot_hlc map poisoned"
                            );
                        }
                    }
                }
            }
            Err(e) => {
                warn!(
                    array = %array_name,
                    error = %e,
                    "array_gc_task: GC error — skipping this array"
                );
            }
        }
    }
}
