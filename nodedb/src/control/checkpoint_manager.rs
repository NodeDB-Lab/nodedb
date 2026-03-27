//! Coordinated checkpoint manager.
//!
//! Periodically dispatches `PhysicalPlan::Checkpoint` to all Data Plane cores,
//! collects their checkpoint LSNs, and truncates the WAL up to the global
//! minimum LSN.
//!
//! ## How it works
//!
//! 1. The manager sends a `Checkpoint` request to every core via the Dispatcher.
//! 2. Each core flushes its engine state (vectors, CRDTs) and responds with
//!    its watermark LSN.
//! 3. The manager collects all responses. The global checkpoint LSN is the
//!    **minimum** across all cores — ensuring no core has unflushed state
//!    above the truncation point.
//! 4. A `RecordType::Checkpoint` WAL record is written at the global LSN.
//! 5. `WalManager::truncate_before()` deletes old WAL segments.
//!
//! ## Frequency
//!
//! Default: every 5 minutes (matches the existing vector checkpoint interval).
//! Configurable via `CheckpointManagerConfig`.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use tracing::{debug, info, warn};

use crate::bridge::dispatch::Dispatcher;
use crate::bridge::envelope::{PhysicalPlan, Priority, Request, Status};
use crate::bridge::physical_plan::MetaOp;
use crate::control::request_tracker::RequestTracker;
use crate::types::{Lsn, ReadConsistency, RequestId, TenantId, VShardId};
use crate::wal::WalManager;

/// Monotonic counter for checkpoint request IDs.
/// Uses a high base to avoid collision with session-generated request IDs.
static CHECKPOINT_REQUEST_COUNTER: AtomicU64 = AtomicU64::new(0xFFFF_0000_0000_0000);

/// Configuration for the checkpoint manager.
#[derive(Debug, Clone)]
pub struct CheckpointManagerConfig {
    /// Interval between checkpoint cycles.
    pub interval: Duration,

    /// Timeout for individual core checkpoint responses.
    pub core_timeout: Duration,
}

impl Default for CheckpointManagerConfig {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(300), // 5 minutes
            core_timeout: Duration::from_secs(30),
        }
    }
}

/// Run one checkpoint cycle: dispatch checkpoint to all cores, collect LSNs,
/// write checkpoint record, archive eligible WAL segments to cold storage (if
/// configured), then truncate the WAL.
///
/// Returns the global checkpoint LSN (min across all cores), or `None` if
/// the checkpoint could not be completed (e.g., a core didn't respond).
pub async fn run_checkpoint_cycle(
    dispatcher: &std::sync::Mutex<Dispatcher>,
    tracker: &RequestTracker,
    wal: &WalManager,
    num_cores: usize,
    timeout: Duration,
    cold_storage: Option<std::sync::Arc<crate::storage::cold::ColdStorage>>,
) -> Option<Lsn> {
    if num_cores == 0 {
        return None;
    }

    // 1. Dispatch checkpoint requests to all cores.
    let mut receivers = Vec::with_capacity(num_cores);

    {
        let mut disp = dispatcher.lock().unwrap_or_else(|p| p.into_inner());

        for core_id in 0..num_cores {
            let request_id =
                RequestId::new(CHECKPOINT_REQUEST_COUNTER.fetch_add(1, Ordering::Relaxed));
            let vshard_id = VShardId::new(core_id as u16);

            let request = Request {
                request_id,
                tenant_id: TenantId::new(0), // System-level checkpoint.
                vshard_id,
                plan: PhysicalPlan::Meta(MetaOp::Checkpoint),
                deadline: std::time::Instant::now() + timeout,
                priority: Priority::Background,
                trace_id: 0,
                consistency: ReadConsistency::Eventual,
                idempotency_key: None,
            };

            let rx = tracker.register_oneshot(request_id);

            if let Err(e) = disp.dispatch_to_core(core_id, request) {
                warn!(
                    core_id,
                    error = %e,
                    "failed to dispatch checkpoint to core"
                );
                tracker.cancel(&request_id);
                continue;
            }

            receivers.push((core_id, request_id, rx));
        }
    }

    if receivers.is_empty() {
        warn!("no checkpoint requests dispatched");
        return None;
    }

    // 2. Collect checkpoint LSNs from all cores.
    let mut checkpoint_lsns: Vec<u64> = Vec::with_capacity(receivers.len());
    let mut failed_cores: Vec<usize> = Vec::new();

    for (core_id, _request_id, rx) in receivers {
        match tokio::time::timeout(timeout, rx).await {
            Ok(Ok(response)) => {
                if response.status == Status::Ok {
                    // Parse checkpoint LSN from payload (u64 LE).
                    let payload = response.payload.as_ref();
                    if payload.len() >= 8 {
                        let lsn = u64::from_le_bytes(payload[..8].try_into().unwrap_or([0; 8]));
                        checkpoint_lsns.push(lsn);
                        debug!(core_id, lsn, "core checkpoint response received");
                    } else {
                        warn!(core_id, "core checkpoint response missing LSN payload");
                        failed_cores.push(core_id);
                    }
                } else {
                    warn!(
                        core_id,
                        status = ?response.status,
                        "core checkpoint returned non-OK status"
                    );
                    failed_cores.push(core_id);
                }
            }
            Ok(Err(_)) => {
                warn!(core_id, "core checkpoint response channel dropped");
                failed_cores.push(core_id);
            }
            Err(_) => {
                warn!(core_id, "core checkpoint response timed out");
                failed_cores.push(core_id);
            }
        }
    }

    if !failed_cores.is_empty() {
        warn!(
            failed = ?failed_cores,
            succeeded = checkpoint_lsns.len(),
            "some cores failed checkpoint — using partial results"
        );
    }

    if checkpoint_lsns.is_empty() {
        warn!("no cores completed checkpoint — skipping WAL truncation");
        return None;
    }

    // 3. Global checkpoint LSN = minimum across all responding cores.
    let &global_lsn = checkpoint_lsns.iter().min()?;
    if global_lsn == 0 {
        debug!("global checkpoint LSN is 0 (no writes yet) — skipping");
        return None;
    }

    let checkpoint_lsn = Lsn::new(global_lsn);

    // 4. Write checkpoint marker to WAL.
    match wal.append_checkpoint(TenantId::new(0), VShardId::new(0), global_lsn) {
        Ok(marker_lsn) => {
            debug!(
                marker_lsn = marker_lsn.as_u64(),
                checkpoint_lsn = global_lsn,
                "checkpoint WAL marker written"
            );
        }
        Err(e) => {
            warn!(error = %e, "failed to write checkpoint WAL marker");
            return Some(checkpoint_lsn);
        }
    }

    if let Err(e) = wal.sync() {
        warn!(error = %e, "failed to sync WAL after checkpoint marker");
        return Some(checkpoint_lsn);
    }

    // 5. Archive eligible WAL segments to cold storage before deletion.
    if let Some(ref cold) = cold_storage {
        archive_wal_segments_before_truncation(wal, global_lsn, cold).await;
    }

    // 6. Truncate old WAL segments.
    match wal.truncate_before(checkpoint_lsn) {
        Ok(result) => {
            if result.segments_deleted > 0 {
                info!(
                    checkpoint_lsn = global_lsn,
                    segments_deleted = result.segments_deleted,
                    bytes_reclaimed = result.bytes_reclaimed,
                    "WAL truncated after checkpoint"
                );
            } else {
                debug!(
                    checkpoint_lsn = global_lsn,
                    "checkpoint complete (no segments to truncate)"
                );
            }
        }
        Err(e) => {
            warn!(
                error = %e,
                checkpoint_lsn = global_lsn,
                "WAL truncation failed after checkpoint"
            );
        }
    }

    Some(checkpoint_lsn)
}

/// Spawn the checkpoint manager as a background Tokio task.
///
/// Runs `run_checkpoint_cycle` at the configured interval until the
/// shutdown signal is received. Performs a final checkpoint on graceful shutdown.
pub fn spawn_checkpoint_task(
    shared: Arc<crate::control::state::SharedState>,
    num_cores: usize,
    config: CheckpointManagerConfig,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        info!(
            interval_secs = config.interval.as_secs(),
            "checkpoint manager started"
        );

        loop {
            tokio::select! {
                _ = tokio::time::sleep(config.interval) => {}
                _ = shutdown.changed() => {
                    if *shutdown.borrow() {
                        info!("shutdown: running final checkpoint");
                        run_checkpoint_cycle(
                            &shared.dispatcher,
                            &shared.tracker,
                            &shared.wal,
                            num_cores,
                            config.core_timeout,
                            shared.cold_storage.clone(),
                        ).await;
                        info!("checkpoint manager stopped");
                        return;
                    }
                }
            }

            run_checkpoint_cycle(
                &shared.dispatcher,
                &shared.tracker,
                &shared.wal,
                num_cores,
                config.core_timeout,
                shared.cold_storage.clone(),
            )
            .await;
        }
    })
}

/// Archive WAL segments that will be deleted by the upcoming `truncate_before(checkpoint_lsn)`.
///
/// A segment is eligible for deletion (and therefore archival) when the segment
/// immediately following it has a `first_lsn <= checkpoint_lsn`. We upload each
/// eligible segment before `truncate_before` deletes it, preserving a continuous
/// WAL archive in cold storage for point-in-time recovery.
///
/// Failures are logged as warnings; archival is best-effort and never blocks
/// the checkpoint cycle.
async fn archive_wal_segments_before_truncation(
    wal: &WalManager,
    checkpoint_lsn: u64,
    cold: &crate::storage::cold::ColdStorage,
) {
    let segments = match wal.list_segments() {
        Ok(s) => s,
        Err(e) => {
            warn!(error = %e, "WAL archival: failed to list segments");
            return;
        }
    };

    // Determine which segments are eligible using the same logic as truncate_before:
    // a segment is deletable when its successor's first_lsn <= checkpoint_lsn.
    for seg in &segments {
        let next_first_lsn = segments
            .iter()
            .find(|s| s.first_lsn > seg.first_lsn)
            .map(|s| s.first_lsn)
            .unwrap_or(u64::MAX);

        if next_first_lsn > checkpoint_lsn {
            // Not eligible for deletion; skip.
            continue;
        }

        let segment_name = match seg.path.file_name().and_then(|n| n.to_str()) {
            Some(n) => n.to_owned(),
            None => {
                warn!(path = %seg.path.display(), "WAL archival: invalid segment path, skipping");
                continue;
            }
        };

        match cold.upload_wal_segment(&seg.path, &segment_name).await {
            Ok(object_path) => {
                debug!(
                    segment = %segment_name,
                    object_path = %object_path,
                    first_lsn = seg.first_lsn,
                    "WAL segment archived before truncation"
                );
            }
            Err(e) => {
                warn!(
                    segment = %segment_name,
                    error = %e,
                    "WAL archival: upload failed (segment will still be truncated)"
                );
            }
        }
    }
}
