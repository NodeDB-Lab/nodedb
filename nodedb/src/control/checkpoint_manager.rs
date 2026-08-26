// SPDX-License-Identifier: BUSL-1.1

//! Coordinated checkpoint manager.
//!
//! Periodically dispatches `PhysicalPlan::Checkpoint` to all Data Plane cores,
//! collects their checkpoint LSNs, and truncates the WAL up to the global
//! minimum LSN — but only when every core has reported.
//!
//! ## How it works
//!
//! 1. The manager sends a `Checkpoint` request to every core via the Dispatcher.
//! 2. Each core flushes its engine state (vectors, CRDTs) and responds with
//!    its watermark LSN.
//! 3. The manager collects all responses. If any core failed to dispatch,
//!    missed its response deadline, or otherwise did not report a fresh
//!    flush LSN, the whole cycle is deferred — no marker, no truncation,
//!    no tombstone GC — and retried next cycle. Only when every core has
//!    reported does the global checkpoint LSN become the **minimum**
//!    across all cores, ensuring no core has unflushed state above the
//!    truncation point.
//! 4. The Event Plane's persisted watermarks are folded into that minimum as
//!    an equal floor. Its consumers recover only from the WAL suffix above
//!    them, so a truncation past a lagging consumer silently drops every CDC
//!    row, trigger fire and streaming-MV update in the gap.
//! 5. A `RecordType::Checkpoint` WAL record is written at the global LSN.
//! 6. `WalManager::truncate_before()` deletes old WAL segments.
//!
//! ## Frequency
//!
//! Default: every 5 minutes (matches the existing vector checkpoint interval).
//! Configurable via `CheckpointManagerConfig`.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use tracing::{debug, error, info, warn};

use crate::bridge::dispatch::Dispatcher;
use crate::bridge::envelope::{PhysicalPlan, Priority, Request, Status};
use crate::control::request_tracker::RequestTracker;
use crate::types::{DatabaseId, Lsn, ReadConsistency, RequestId, TenantId, TraceId, VShardId};
use crate::wal::WalManager;
use nodedb_physical::physical_plan::MetaOp;

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

/// Decide the WAL truncation LSN for a checkpoint cycle.
///
/// Returns `None` (defer truncation) unless EVERY core reported a fresh
/// flush LSN. A core that failed to dispatch or missed its response
/// deadline may still hold acknowledged-but-unflushed records below the
/// reporting cores' minimum LSN; truncating there would delete them and
/// lose the writes on restart. Also returns `None` when the minimum is 0
/// (no writes yet, nothing to truncate).
///
/// ## Why the Event Plane is a floor too
///
/// The engines are not the only consumer of the WAL. Each Event Plane consumer
/// recovers from exactly one place — the WAL suffix above its persisted
/// watermark — and that watermark is flushed lazily, so it always trails the
/// engines. Once a segment below a consumer's watermark is unlinked, that
/// suffix is gone: every CDC row, trigger fire, and streaming-MV update for the
/// acknowledged writes in it is unrecoverable. Replay now refuses a request
/// below the retained floor rather than returning the shorter suffix that
/// survives, so the loss is loud instead of silent — but refusing is only an
/// alarm. This floor is what keeps it from happening.
///
/// `event_watermarks` is therefore folded in exactly as each core's engine LSN
/// is, with the same conservatism: one entry per core, and a core that has
/// persisted nothing contributes 0, which defers the whole cycle. Deferring
/// costs WAL growth until the next cycle; not deferring costs the events.
fn checkpoint_truncation_lsn(
    reported_lsns: &[u64],
    event_watermarks: &[u64],
    num_cores: usize,
) -> Option<u64> {
    if reported_lsns.len() != num_cores || event_watermarks.len() != num_cores {
        return None;
    }
    let engine_min = *reported_lsns.iter().min()?;
    let event_min = *event_watermarks.iter().min()?;
    let min = engine_min.min(event_min);
    if min == 0 { None } else { Some(min) }
}

/// Everything one checkpoint cycle needs, bundled so the call site passes one
/// struct literal instead of eight positional arguments — and so adding a new
/// truncation floor is a named field rather than another unlabelled parameter.
pub struct CheckpointCycleInputs<'a> {
    /// Dispatches the `Checkpoint` request to each Data Plane core.
    pub dispatcher: &'a std::sync::Mutex<Dispatcher>,
    /// Routes each core's response back to this cycle.
    pub tracker: &'a RequestTracker,
    /// The log this cycle marks and truncates.
    pub wal: &'a WalManager,
    /// The Event Plane's persisted per-core progress. A floor on truncation:
    /// a consumer recovers only from the WAL above its watermark.
    pub watermark_store: &'a crate::event::watermark::WatermarkStore,
    /// How many cores must report before truncation is safe at all.
    pub num_cores: usize,
    /// Per-core response deadline.
    pub timeout: Duration,
    /// When configured, segments are archived before they are unlinked.
    pub cold_storage: Option<std::sync::Arc<crate::storage::cold::ColdStorage>>,
    /// When present, the tombstone set is GC'd to the new truncation point.
    pub catalog: Option<&'a crate::control::security::catalog::SystemCatalog>,
}

/// Run one checkpoint cycle: dispatch checkpoint to all cores, collect LSNs,
/// write checkpoint record, archive eligible WAL segments to cold storage (if
/// configured), then truncate the WAL.
///
/// Returns the global checkpoint LSN (min across all cores), or `None` if
/// the checkpoint could not be completed (e.g., a core didn't respond).
pub async fn run_checkpoint_cycle(inputs: CheckpointCycleInputs<'_>) -> Option<Lsn> {
    let CheckpointCycleInputs {
        dispatcher,
        tracker,
        wal,
        watermark_store,
        num_cores,
        timeout,
        cold_storage,
        catalog,
    } = inputs;

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
            let vshard_id = VShardId::new(core_id as u32);

            let request = Request {
                request_id,
                tenant_id: TenantId::new(0), // System-level checkpoint.
                database_id: DatabaseId::DEFAULT,
                vshard_id,
                plan: PhysicalPlan::Meta(MetaOp::Checkpoint),
                deadline: std::time::Instant::now() + timeout,
                priority: Priority::Background,
                trace_id: TraceId::generate(),
                consistency: ReadConsistency::Eventual,
                idempotency_key: None,
                event_source: crate::event::EventSource::User,
                user_roles: Vec::new(),
                user_id: None,
                statement_digest: None,
                txn_id: None,
                wal_lsn: None,
                resolved_now_ms: None,
                admission: crate::bridge::envelope::Admission::Exempt(
                    crate::bridge::envelope::ExemptReason::AlreadyOrdered,
                ),
            };

            let rx = tracker.register(request_id);

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

    for (core_id, _request_id, mut rx) in receivers {
        match tokio::time::timeout(timeout, async { rx.recv().await.ok_or(()) }).await {
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

    // 3. Read the Event Plane's persisted progress. Its consumers recover only
    // from the WAL suffix above these watermarks, so they bound truncation just
    // as the engines do. A read failure defers the cycle rather than dropping
    // the floor: an unknown watermark is not a permissive one.
    let event_watermarks: Vec<u64> = match watermark_store.load_all(num_cores) {
        Ok(w) => w.into_iter().map(|lsn| lsn.as_u64()).collect(),
        Err(e) => {
            warn!(
                error = %e,
                "checkpoint deferred: Event Plane watermarks unreadable — truncating without \
                 them could delete WAL the consumers have not processed"
            );
            return None;
        }
    };

    // Truncation is only safe when every core reported a fresh flush LSN.
    // A core that failed to dispatch or missed its deadline may hold
    // acknowledged-but-unflushed records below the reporting cores'
    // minimum; truncating would delete them and lose the writes on
    // restart. Defer the entire checkpoint (no marker, no truncation) and
    // retry next cycle.
    let global_lsn = match checkpoint_truncation_lsn(&checkpoint_lsns, &event_watermarks, num_cores)
    {
        Some(lsn) => lsn,
        None => {
            if checkpoint_lsns.len() != num_cores {
                warn!(
                    responded = checkpoint_lsns.len(),
                    expected = num_cores,
                    failed = ?failed_cores,
                    "checkpoint deferred: not all cores reported a flush LSN — skipping WAL truncation this cycle"
                );
            } else if event_watermarks.iter().min().is_some_and(|m| *m == 0) {
                debug!(
                    "checkpoint deferred: an Event Plane consumer has persisted no watermark \
                     yet — its only recovery source is the WAL above it"
                );
            } else {
                debug!("global checkpoint LSN is 0 (no writes yet) — skipping");
            }
            return None;
        }
    };

    let checkpoint_lsn = Lsn::new(global_lsn);

    // 5. Write checkpoint marker to WAL.
    match wal.append_checkpoint(
        TenantId::new(0),
        VShardId::new(0),
        DatabaseId::DEFAULT,
        global_lsn,
    ) {
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

    // Crash injection: die after the checkpoint marker is durable but before
    // any segment is deleted. On restart the marker must not be taken as
    // proof that truncation happened — replay has to cover everything from
    // the marker's LSN onward.
    crate::fail_point!("checkpoint::after_marker_before_truncate");

    // 6. Archive eligible WAL segments to cold storage before deletion.
    if let Some(ref cold) = cold_storage {
        archive_wal_segments_before_truncation(wal, global_lsn, cold).await;
    }

    // 7. Truncate old WAL segments.
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

            // 8. GC the redb tombstone set now that no surviving WAL
            // segment can carry a write older than `checkpoint_lsn`.
            // Without this, `_system.wal_tombstones` grows forever and
            // each startup replay pays to load the accumulated rows.
            // Strict `<` threshold in the catalog primitive — entries
            // whose `purge_lsn == checkpoint_lsn` are kept for one more
            // cycle, matching the WAL's own retention semantics.
            if let Some(cat) = catalog {
                match cat.delete_wal_tombstones_before_lsn(global_lsn) {
                    Ok(removed) if removed > 0 => {
                        info!(
                            checkpoint_lsn = global_lsn,
                            removed, "wal_tombstones GC: reaped rows whose segments are truncated"
                        );
                    }
                    Ok(_) => {}
                    Err(e) => {
                        // Non-fatal: a stale tombstone row is replay-safe,
                        // it just wastes redb space until the next pass.
                        warn!(
                            error = %e,
                            checkpoint_lsn = global_lsn,
                            "wal_tombstones GC failed; will retry next checkpoint"
                        );
                    }
                }
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
///
/// B5 (2026-08-26): registered via [`spawn_loop_no_abort`] so the loop registry
/// JOINS this task during graceful shutdown — the final checkpoint must finish
/// before the process exits. Previously a bare `tokio::spawn` with its own
/// watch meant nothing waited on it, so a normal exit could race the final
/// cycle and leave a torn WAL tail (the force-stop corruption shape).
///
/// B4/B5: each cycle outcome is reported to the runtime-health registry; N
/// consecutive failures escalate to a `CRITICAL` log and mark the node
/// degraded on `/healthz` + `STATUS`. While unhealthy, the next cycle is
/// retried sooner (30s doubling, capped at the configured interval) instead of
/// waiting the full 5-minute interval in silence.
pub fn spawn_checkpoint_task(
    shared: Arc<crate::control::state::SharedState>,
    watermark_store: Arc<crate::event::watermark::WatermarkStore>,
    num_cores: usize,
    config: CheckpointManagerConfig,
    registry: &crate::control::shutdown::LoopRegistry,
    shutdown: &crate::control::shutdown::ShutdownWatch,
) {
    use crate::control::shutdown::spawn_loop_no_abort;

    spawn_loop_no_abort(
        registry,
        shutdown,
        "checkpoint_manager",
        move |mut shutdown| async move {
            info!(
                interval_secs = config.interval.as_secs(),
                "checkpoint manager started"
            );

            let mut consecutive_failures: u32 = 0;
            let mut escalated = false;

            loop {
                let cycle = run_checkpoint_cycle(CheckpointCycleInputs {
                    dispatcher: &shared.dispatcher,
                    tracker: &shared.tracker,
                    wal: &shared.wal,
                    watermark_store: &watermark_store,
                    num_cores,
                    timeout: config.core_timeout,
                    cold_storage: shared.cold_storage.clone(),
                    catalog: Some(shared.credentials.catalog()),
                })
                .await;

                let ok = cycle.is_some();
                if ok {
                    consecutive_failures = 0;
                    escalated = false;
                } else {
                    consecutive_failures += 1;
                }

                // B4/B5: feed the runtime-health registry; escalate once at the
                // threshold so the wedge becomes a CRITICAL + degraded health
                // instead of 5-minute silent deferrals.
                if crate::bridge::runtime_health::record_checkpoint_result(ok)
                    && !escalated
                {
                    escalated = true;
                    error!(
                        consecutive_failures,
                        threshold = crate::bridge::runtime_health::CHECKPOINT_STALL_THRESHOLD,
                        "checkpoint CRITICAL: repeated failed cycles — WAL truncation suspended, \
                         node marked degraded"
                    );
                }

                // Healthy → configured interval. Unhealthy → fast retry with
                // backoff (30s, 60s, 120s, capped at the interval) so recovery
                // is detected promptly instead of after a silent 5 minutes.
                let wait = if consecutive_failures == 0 {
                    config.interval
                } else {
                    let shift = consecutive_failures.min(3);
                    Duration::from_secs(30) * (1u32 << shift)
                }
                .min(config.interval);

                tokio::select! {
                    _ = shutdown.wait_cancelled() => {
                        if consecutive_failures == 0 {
                            info!("shutdown: running final checkpoint");
                        } else {
                            error!(
                                consecutive_failures,
                                "shutdown while checkpoint unhealthy — final checkpoint may defer"
                            );
                        }
                        let final_cycle = run_checkpoint_cycle(CheckpointCycleInputs {
                            dispatcher: &shared.dispatcher,
                            tracker: &shared.tracker,
                            wal: &shared.wal,
                            watermark_store: &watermark_store,
                            num_cores,
                            timeout: config.core_timeout,
                            cold_storage: shared.cold_storage.clone(),
                            catalog: Some(shared.credentials.catalog()),
                        })
                        .await;
                        let _ =
                            crate::bridge::runtime_health::record_checkpoint_result(final_cycle.is_some());
                        info!("checkpoint manager stopped");
                        return;
                    }
                    _ = tokio::time::sleep(wait) => {}
                }
            }
        },
    );
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

#[cfg(test)]
mod tests {
    use super::*;

    /// Event Plane watermarks that never bind, so a case can isolate the
    /// engine-side rule it is actually about.
    fn ahead(num_cores: usize) -> Vec<u64> {
        vec![u64::MAX; num_cores]
    }

    #[test]
    fn all_cores_reported_distinct_lsns_returns_min() {
        assert_eq!(
            checkpoint_truncation_lsn(&[10, 5, 8], &ahead(3), 3),
            Some(5)
        );
    }

    #[test]
    fn one_core_missing_defers_even_though_responders_have_a_min() {
        // Truncating at 5 would delete the missing core's unflushed records.
        assert_eq!(checkpoint_truncation_lsn(&[5, 10], &ahead(3), 3), None);
    }

    #[test]
    fn dispatch_gap_only_one_core_responded_defers() {
        assert_eq!(checkpoint_truncation_lsn(&[7], &ahead(2), 2), None);
    }

    #[test]
    fn all_reported_but_min_is_zero_defers() {
        assert_eq!(checkpoint_truncation_lsn(&[0, 5], &ahead(2), 2), None);
    }

    #[test]
    fn single_core_reported_returns_its_lsn() {
        assert_eq!(checkpoint_truncation_lsn(&[42], &ahead(1), 1), Some(42));
    }

    #[test]
    fn degenerate_empty_input_does_not_panic() {
        assert_eq!(checkpoint_truncation_lsn(&[], &[], 0), None);
    }

    /// The Event Plane binds truncation when it trails the engines. Its
    /// consumers recover ONLY from the WAL above their persisted watermark, so
    /// truncating at the engine minimum would drop every CDC row, trigger fire
    /// and MV update in between — unrecoverably, whether or not replay notices.
    #[test]
    fn event_plane_behind_the_engines_clamps_truncation_to_it() {
        assert_eq!(
            checkpoint_truncation_lsn(&[900, 900], &[400, 950], 2),
            Some(400),
            "the lagging consumer's watermark is the floor, not the engines' minimum"
        );
    }

    /// A consumer that has persisted nothing has processed nothing it can prove,
    /// so the whole cycle defers rather than truncating to the engine minimum.
    #[test]
    fn consumer_with_no_persisted_watermark_defers_the_cycle() {
        assert_eq!(checkpoint_truncation_lsn(&[900, 900], &[900, 0], 2), None);
    }

    /// An Event Plane ahead of the engines changes nothing: the engines are
    /// still the binding floor, exactly as before.
    #[test]
    fn event_plane_ahead_of_the_engines_does_not_raise_the_floor() {
        assert_eq!(
            checkpoint_truncation_lsn(&[500, 800], &[900, 900], 2),
            Some(500)
        );
    }

    /// A watermark set shorter than the core count means a core's progress is
    /// unknown — unknown is never permissive.
    #[test]
    fn missing_event_watermark_for_a_core_defers() {
        assert_eq!(checkpoint_truncation_lsn(&[900, 900], &[900], 2), None);
    }
}
