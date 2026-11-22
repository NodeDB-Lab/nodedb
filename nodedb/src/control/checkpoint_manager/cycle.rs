// SPDX-License-Identifier: BUSL-1.1

//! One checkpoint cycle: dispatch, collect, mark, archive, truncate.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use tracing::{debug, info, warn};

use crate::bridge::dispatch::Dispatcher;
use crate::bridge::envelope::{PhysicalPlan, Priority, Request, Status};
use crate::control::checkpoint_archival::archive_then_bound;
use crate::control::request_tracker::RequestTracker;
use crate::types::{DatabaseId, Lsn, ReadConsistency, RequestId, TenantId, TraceId, VShardId};
use crate::wal::WalManager;
use crate::wal::archiver::WalArchiver;
use nodedb_physical::physical_plan::MetaOp;

use super::truncation::checkpoint_truncation_lsn;

/// Monotonic counter for checkpoint request IDs.
/// Uses a high base to avoid collision with session-generated request IDs.
static CHECKPOINT_REQUEST_COUNTER: AtomicU64 = AtomicU64::new(0xFFFF_0000_0000_0000);

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
    /// Present when cold storage is configured. Truncation never passes the
    /// lowest segment it has not archived.
    pub archiver: Option<&'a mut WalArchiver>,
    /// When present, the tombstone set is GC'd to the new truncation point.
    pub catalog: Option<&'a crate::control::security::catalog::SystemCatalog>,
    /// The Calvin applied ledger of every vShard on this node. Saved in
    /// `catalog` before truncation deletes the stamped redo records it came
    /// from.
    pub calvin_ledgers:
        Option<&'a crate::control::cluster::calvin::scheduler::CalvinAppliedLedgers>,
}

/// The applied state of every ledger, as the catalog stores it.
fn calvin_applied_states(
    ledgers: &crate::control::cluster::calvin::scheduler::CalvinAppliedLedgers,
) -> Vec<crate::control::security::catalog::calvin_applied::StoredCalvinApplied> {
    ledgers.snapshot_all()
}

/// Save `states` in `catalog`.
fn save_calvin_applied(
    states: Vec<crate::control::security::catalog::calvin_applied::StoredCalvinApplied>,
    catalog: Option<&crate::control::security::catalog::SystemCatalog>,
) -> crate::Result<()> {
    if states.is_empty() {
        return Ok(());
    }
    let catalog = catalog.ok_or_else(|| crate::Error::Internal {
        detail: "checkpoint has no catalog to save the Calvin applied state in".into(),
    })?;
    catalog.save_calvin_applied(states)
}

/// Run one checkpoint cycle: dispatch checkpoint to all cores, collect LSNs,
/// write checkpoint record, archive sealed WAL segments to cold storage (if
/// configured), then truncate the WAL.
///
/// Returns the global checkpoint LSN (min across all cores), or `None` if
/// the checkpoint did not complete (e.g., a core didn't respond).
pub async fn run_checkpoint_cycle(inputs: CheckpointCycleInputs<'_>) -> Option<Lsn> {
    let CheckpointCycleInputs {
        dispatcher,
        tracker,
        wal,
        watermark_store,
        num_cores,
        timeout,
        archiver,
        catalog,
        calvin_ledgers,
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
                stream: crate::bridge::envelope::RequestStream::Internal,
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
                commit_hlc: None,
                system_ordinal: None,
                config_pins: None,
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
    // from the WAL suffix above these watermarks, so they bound truncation
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
    // A core that failed to dispatch or missed its deadline can hold
    // acknowledged-but-unflushed records below the reporting cores'
    // minimum; truncating deletes them and loses the writes on
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

    // Read the Calvin applied state before the WAL sync below. A scheduler
    // marks a position applied only after the position's records were
    // appended, so the sync makes every record behind this state durable.
    let calvin_states = calvin_ledgers.map(calvin_applied_states);

    // 5. Write checkpoint marker to WAL.
    match wal
        .appender(crate::wal::manager::NO_APPLY_KEY)
        .append_checkpoint(
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

    // Save every Calvin scheduler's applied state before any segment holding
    // a stamped redo record is deleted. The sequencer log delivers those entries
    // again after a restart, and the saved state is what tells the scheduler
    // they already applied. A save that fails holds the truncation back.
    if let Some(states) = calvin_states
        && let Err(error) = save_calvin_applied(states, catalog)
    {
        warn!(
            checkpoint_lsn = global_lsn,
            %error,
            "WAL truncation deferred: the Calvin applied state was not saved, and the \
             segments hold the only record of which Calvin transactions applied"
        );
        return Some(checkpoint_lsn);
    }

    // 6. Archive sealed WAL segments before deletion. An unarchived segment
    // bounds truncation: deleting it leaves a hole no point-in-time
    // recovery can cross.
    let mut truncate_lsn = global_lsn;
    if let Some(archiver) = archiver {
        let bound = archive_then_bound(archiver, wal, global_lsn).await;
        if bound.held {
            warn!(
                checkpoint_lsn = global_lsn,
                truncate_lsn = bound.lsn,
                "WAL truncation held back to the archived bound: segments from this LSN up are \
                 not in cold storage and stay on local disk until archival recovers"
            );
        }
        truncate_lsn = bound.lsn;
    }

    // 7. Truncate old WAL segments.
    match wal.truncate_before(Lsn::new(truncate_lsn)) {
        Ok(result) => {
            if result.segments_deleted > 0 {
                info!(
                    checkpoint_lsn = global_lsn,
                    truncate_lsn,
                    segments_deleted = result.segments_deleted,
                    bytes_reclaimed = result.bytes_reclaimed,
                    "WAL truncated after checkpoint"
                );
            } else {
                debug!(
                    checkpoint_lsn = global_lsn,
                    truncate_lsn, "checkpoint complete (no segments to truncate)"
                );
            }

            // 8. GC the redb tombstone set: no surviving WAL segment can
            // carry a write older than `truncate_lsn`. The threshold is the
            // truncation point, not the checkpoint LSN — a held-back
            // truncation leaves segments whose rows still need their
            // tombstones. Without this GC, `_system.wal_tombstones` grows
            // forever and each startup replay pays to load the accumulated
            // rows. Strict `<` threshold in the catalog primitive — entries
            // whose `purge_lsn == truncate_lsn` are kept for one more cycle,
            // matching the WAL's own retention semantics.
            if let Some(cat) = catalog {
                match cat.delete_wal_tombstones_before_lsn(truncate_lsn) {
                    Ok(removed) if removed > 0 => {
                        info!(
                            truncate_lsn,
                            removed, "wal_tombstones GC: reaped rows whose segments are truncated"
                        );
                    }
                    Ok(_) => {}
                    Err(e) => {
                        // Non-fatal: a stale tombstone row is replay-safe,
                        // it only wastes redb space until the next pass.
                        warn!(
                            error = %e,
                            truncate_lsn,
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
                truncate_lsn,
                "WAL truncation failed after checkpoint"
            );
        }
    }

    Some(checkpoint_lsn)
}
