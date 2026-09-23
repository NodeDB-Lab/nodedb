// SPDX-License-Identifier: BUSL-1.1

//! WAL catch-up task for timeseries ingest.
//!
//! During sustained high-throughput ILP ingest, the SPSC bridge between
//! Control Plane and Data Plane may drop batches under backpressure.
//! Those batches are durable in WAL but invisible to queries because
//! they never reached the Data Plane memtable.
//!
//! This background task periodically scans WAL for TimeseriesBatch
//! records that haven't been delivered and re-dispatches them to the
//! Data Plane. It uses paginated mmap reads (bounded memory) and passes
//! WAL LSNs so the Data Plane can deduplicate already-ingested records.

use std::sync::Arc;
use std::sync::atomic::Ordering;

use tracing::{debug, info};

use crate::bridge::envelope::PhysicalPlan;
use crate::control::state::SharedState;
use crate::types::{DatabaseId, TenantId, TraceId, VShardId};
use nodedb_physical::physical_plan::TimeseriesOp;
use nodedb_types::{Lsn, RlsWriteCheck};

/// Max WAL records to read per catch-up cycle. Bounds memory to
/// O(PAGE_SIZE) instead of O(all WAL data).
const PAGE_SIZE: usize = 512;

/// Spawn the WAL catch-up background task.
///
/// Runs on the Tokio runtime (Control Plane). Periodically reads unflushed
/// WAL TimeseriesBatch records and dispatches them to the Data Plane.
///
/// `initial_lsn` should be `wal.next_lsn()` after startup WAL replay —
/// everything before that has already been replayed.
pub fn spawn_wal_catchup_task(
    shared: Arc<SharedState>,
    initial_lsn: Lsn,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) {
    shared
        .wal_catchup_lsn
        .store(initial_lsn.as_u64(), Ordering::Release);

    tokio::spawn(async move {
        // Adaptive interval: 500ms default, tighten when catching up, relax when idle.
        let mut interval_ms: u64 = 500;

        loop {
            tokio::select! {
                _ = tokio::time::sleep(std::time::Duration::from_millis(interval_ms)) => {
                    let result = run_catchup_cycle(&shared).await;
                    interval_ms = match result {
                        CatchupResult::HasMore => 100,      // rapid drain
                        CatchupResult::Dispatched => 250,   // active, normal pace
                        CatchupResult::Backpressured => 200, // retry soon
                        CatchupResult::Idle => 2000,        // nothing to do
                    };
                }
                _ = shutdown.changed() => {
                    info!("WAL catch-up task shutting down");
                    break;
                }
            }
        }
    });
}

/// Result of a single catch-up cycle.
enum CatchupResult {
    /// More records remain — schedule next cycle quickly.
    HasMore,
    /// Some records dispatched, no more pending.
    Dispatched,
    /// SPSC bridge is busy — retry soon without reading WAL.
    Backpressured,
    /// Nothing to dispatch.
    Idle,
}

/// Run one catch-up cycle: read new WAL records, dispatch timeseries batches.
///
/// Uses paginated mmap replay to bound memory. Passes WAL LSNs to the
/// Data Plane for deduplication (records already ingested are skipped).
async fn run_catchup_cycle(shared: &SharedState) -> CatchupResult {
    // Backpressure gate: don't compete with live ingest for SPSC slots.
    if shared.max_spsc_utilization() > 50 {
        return CatchupResult::Backpressured;
    }

    let catchup_lsn = shared.wal_catchup_lsn.load(Ordering::Acquire);

    // Read at most PAGE_SIZE WAL records via sequential I/O (bounded memory).
    // Uses sequential I/O (not mmap) to safely read the active segment
    // even when written via O_DIRECT which bypasses the page cache.
    let (records, has_more) = match shared
        .wal
        .replay_from_limit(Lsn::new(catchup_lsn + 1), PAGE_SIZE)
    {
        Ok(r) => r,
        // The cursor points below the earliest LSN the WAL still retains, so
        // the batches in between were truncated before this task could
        // re-dispatch them. It cannot get them back, but leaving the cursor
        // where it is would wedge the task on the same failure forever and
        // strand every batch above the floor as well. Re-anchor to the floor
        // and say plainly what was skipped — the WAL layer has already filed a
        // report at the detection site.
        Err(crate::Error::Wal(nodedb_wal::WalError::ReplayBelowRetainedFloor {
            retained_floor_lsn,
            ..
        })) => {
            tracing::error!(
                cursor_lsn = catchup_lsn,
                retained_floor_lsn,
                skipped_lsns = retained_floor_lsn.saturating_sub(catchup_lsn + 1),
                "WAL catch-up cursor is below the retained WAL floor; timeseries batches in the \
                 truncated range can no longer be re-dispatched"
            );
            shared
                .wal_catchup_lsn
                .store(retained_floor_lsn.saturating_sub(1), Ordering::Release);
            return CatchupResult::Idle;
        }
        Err(e) => {
            debug!(error = %e, lsn = catchup_lsn, "WAL catch-up replay failed");
            return CatchupResult::Idle;
        }
    };

    if records.is_empty() {
        return CatchupResult::Idle;
    }

    // A batch the Data Plane refused after its record was appended carries a
    // `WriteAborted` marker naming it; re-dispatching such a record here would
    // ingest a write the client was told was rejected. Unlike restart replay,
    // this reader is paginated, so a marker that lands in a LATER page cannot
    // gate its record in this one — that record is still excluded from restart
    // replay, which is where a refused write becomes durable state.
    let aborted = match nodedb_wal::extract_replay_filters(&records) {
        Ok(filters) => filters.aborted,
        Err(e) => {
            debug!(error = %e, "WAL catch-up abort-marker extraction failed");
            return CatchupResult::Idle;
        }
    };

    let mut dispatched = 0usize;
    let mut max_lsn = catchup_lsn;

    for record in &records {
        if aborted.contains(record.header.lsn) {
            max_lsn = max_lsn.max(record.header.lsn);
            continue;
        }
        // Only process TimeseriesBatch records.
        let record_type = nodedb_wal::record::RecordType::from_raw(record.logical_record_type());
        if record_type != Some(nodedb_wal::record::RecordType::TimeseriesBatch) {
            max_lsn = max_lsn.max(record.header.lsn);
            continue;
        }

        // A columnar record shares this record type and is not a timeseries
        // ingest; catch-up re-dispatches timeseries ingests only.
        let Ok(decoded) = crate::wal::decode_batch_record(&record.payload) else {
            max_lsn = max_lsn.max(record.header.lsn);
            continue;
        };
        if decoded
            .kind
            .as_deref()
            .is_some_and(|kind| kind != "timeseries")
        {
            max_lsn = max_lsn.max(record.header.lsn);
            continue;
        }

        let tenant_id = TenantId::new(record.header.tenant_id);
        let database_id = DatabaseId::new(record.header.database_id);
        let vshard_id = VShardId::new(record.header.vshard_id);
        let format = decoded.format.unwrap_or_else(|| "ilp".to_string());

        let plan = PhysicalPlan::Timeseries(TimeseriesOp::Ingest {
            collection: nodedb_types::QualifiedCollection::from_stored(decoded.collection),
            payload: decoded.payload,
            format,
            wal_lsn: Some(record.header.lsn),
            // Re-derived on the engine side during apply (record carries
            // raw rows — row identities are reconstructed from the wire).
            surrogates: Vec::new(),
            provenance: None,
            // No predicate here: catch-up replays an already-committed WAL
            // record. The writing identity is not available on this node.
            rls_write_check: RlsWriteCheck::already_decided_elsewhere(),
            returning: None,
            rls_filters: Vec::new(),
        });

        // Dispatch to Data Plane — do NOT re-append to WAL (already there).
        // Untimed rows take the instant the record carries.
        match crate::control::server::dispatch_utils::dispatch_trusted_internal_write_to_data_plane(
            shared,
            crate::control::server::dispatch_utils::WriteDispatch {
                tenant_id,
                database_id,
                vshard_id,
                plan,
                trace_id: TraceId::ZERO,
                event_source: crate::event::EventSource::User,
                txn_id: None,
                wal_lsn: None,
                resolved_now_ms: decoded
                    .default_timestamp_ms
                    .and_then(|ms| u64::try_from(ms).ok()),
            },
        )
        .await
        {
            Ok(_) => {
                dispatched += 1;
                max_lsn = max_lsn.max(record.header.lsn);
            }
            Err(e) => {
                // SPSC full or timeout — stop this cycle, retry next interval.
                debug!(error = %e, "WAL catch-up dispatch failed, will retry");
                break;
            }
        }
    }

    // Advance the catchup watermark.
    if max_lsn > catchup_lsn {
        shared.wal_catchup_lsn.fetch_max(max_lsn, Ordering::Release);
    }

    if dispatched > 0 {
        info!(dispatched, max_lsn, "WAL catch-up cycle completed");
    }

    if has_more {
        CatchupResult::HasMore
    } else if dispatched > 0 {
        CatchupResult::Dispatched
    } else {
        CatchupResult::Idle
    }
}
