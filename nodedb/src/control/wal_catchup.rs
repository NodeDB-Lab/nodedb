//! WAL catch-up task for timeseries ingest.
//!
//! During sustained high-throughput ILP ingest, the SPSC bridge between
//! Control Plane and Data Plane may drop batches under backpressure.
//! Those batches are durable in WAL but invisible to queries because
//! they never reached the Data Plane memtable.
//!
//! This background task periodically scans WAL for TimeseriesBatch
//! records that haven't been delivered and re-dispatches them to the
//! Data Plane. It respects SPSC backpressure and uses adaptive intervals.

use std::sync::Arc;
use std::sync::atomic::Ordering;

use tracing::{debug, info};

use crate::bridge::envelope::PhysicalPlan;
use crate::bridge::physical_plan::TimeseriesOp;
use crate::control::state::SharedState;
use crate::types::{TenantId, VShardId};
use nodedb_types::Lsn;

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
                    let dispatched = run_catchup_cycle(&shared).await;
                    interval_ms = if dispatched > 0 { 250 } else { 2000 };
                }
                _ = shutdown.changed() => {
                    info!("WAL catch-up task shutting down");
                    break;
                }
            }
        }
    });
}

/// Run one catch-up cycle: read new WAL records, dispatch timeseries batches.
///
/// Returns the number of records successfully dispatched.
async fn run_catchup_cycle(shared: &SharedState) -> usize {
    // Backpressure gate: don't compete with live ingest for SPSC slots.
    if shared.max_spsc_utilization() > 75 {
        return 0;
    }

    let catchup_lsn = shared.wal_catchup_lsn.load(Ordering::Acquire);

    // Read WAL records from the current catchup point.
    // This briefly holds the WAL mutex — keep it fast.
    let records = match shared.wal.replay_from(Lsn::new(catchup_lsn + 1)) {
        Ok(r) => r,
        Err(e) => {
            // WAL segment may have been truncated — advance past it.
            debug!(error = %e, lsn = catchup_lsn, "WAL catch-up replay_from failed");
            return 0;
        }
    };

    if records.is_empty() {
        return 0;
    }

    let mut dispatched = 0usize;
    let mut max_lsn = catchup_lsn;

    for record in &records {
        // Only process TimeseriesBatch records.
        let record_type = nodedb_wal::record::RecordType::from_raw(record.logical_record_type());
        if record_type != Some(nodedb_wal::record::RecordType::TimeseriesBatch) {
            max_lsn = max_lsn.max(record.header.lsn);
            continue;
        }

        // Deserialize WAL payload: (collection, raw_ilp_bytes).
        let Ok((collection, payload)): Result<(String, Vec<u8>), _> =
            rmp_serde::from_slice(&record.payload)
        else {
            max_lsn = max_lsn.max(record.header.lsn);
            continue;
        };

        let tenant_id = TenantId::new(record.header.tenant_id);
        let vshard_id = VShardId::new(record.header.vshard_id);

        let plan = PhysicalPlan::Timeseries(TimeseriesOp::Ingest {
            collection,
            payload,
            format: "ilp".to_string(),
        });

        // Dispatch to Data Plane — do NOT re-append to WAL (already there).
        match crate::control::server::dispatch_utils::dispatch_to_data_plane(
            shared, tenant_id, vshard_id, plan, 0,
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

    dispatched
}
