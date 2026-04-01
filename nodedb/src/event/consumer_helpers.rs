//! Utility helpers for the Event Plane consumer loop.
//!
//! Extracted from `consumer.rs` to keep the main consumer module focused
//! on the state machine and dispatch orchestration.

use tracing::{trace, warn};

use super::metrics::CoreMetrics;
use super::types::WriteEvent;
use super::watermark::WatermarkStore;
use crate::types::Lsn;

/// How often to persist the watermark to redb (avoid fsync on every event).
pub const WATERMARK_FLUSH_INTERVAL: std::time::Duration = std::time::Duration::from_secs(5);

/// Detect sequence gaps (events dropped by the producer due to buffer overflow).
pub fn detect_sequence_gap(
    core_id: usize,
    event: &WriteEvent,
    last_sequence: u64,
    metrics: &CoreMetrics,
) {
    if last_sequence > 0 && event.sequence > last_sequence + 1 {
        let gap = event.sequence - last_sequence - 1;
        metrics.record_drop(gap);
        warn!(
            core_id,
            gap,
            last_seq = last_sequence,
            new_seq = event.sequence,
            "event sequence gap — {gap} events dropped (WAL replay needed)"
        );
    }
}

/// Process a single event. Dispatch point for trigger matching, CDC, etc.
pub fn record_event(core_id: usize, event: &WriteEvent, metrics: &CoreMetrics) {
    metrics.record_process_for_tenant(event.lsn.as_u64(), event.sequence, event.tenant_id.as_u32());

    trace!(
        core_id,
        seq = event.sequence,
        collection = %event.collection,
        op = %event.op,
        source = %event.source,
        lsn = event.lsn.as_u64(),
        "event consumed"
    );
}

/// Flush watermark to redb if the flush interval has elapsed.
pub fn maybe_flush_watermark(
    store: &WatermarkStore,
    core_id: usize,
    lsn: Lsn,
    dirty: &mut bool,
    last_flush: &mut tokio::time::Instant,
) {
    if *dirty && last_flush.elapsed() >= WATERMARK_FLUSH_INTERVAL {
        flush_watermark(store, core_id, lsn);
        *dirty = false;
        *last_flush = tokio::time::Instant::now();
    }
}

/// Persist watermark to redb (best-effort — log on failure).
pub fn flush_watermark(store: &WatermarkStore, core_id: usize, lsn: Lsn) {
    if lsn == Lsn::ZERO {
        return;
    }
    if let Err(e) = store.save(core_id, lsn) {
        warn!(core_id, lsn = lsn.as_u64(), error = %e, "failed to persist watermark");
    } else {
        trace!(core_id, lsn = lsn.as_u64(), "watermark flushed");
    }
}
