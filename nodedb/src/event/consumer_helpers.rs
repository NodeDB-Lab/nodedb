// SPDX-License-Identifier: BUSL-1.1

//! Utility helpers for the Event Plane consumer loop.
//!
//! Extracted from `consumer.rs` to keep the main consumer module focused
//! on the state machine and dispatch orchestration.

use std::sync::Arc;

use tracing::{trace, warn};

use super::action::ActionRetryQueue;
use super::bus::EventConsumerRx;
use super::metrics::CoreMetrics;
use super::types::WriteEvent;
use super::watermark::WatermarkStore;
use crate::control::state::SharedState;
use crate::types::Lsn;

/// How often to persist the watermark to redb (avoid fsync on every event).
pub const WATERMARK_FLUSH_INTERVAL: std::time::Duration = std::time::Duration::from_secs(5);

/// Maximum events to process per ring buffer drain before yielding.
pub const DRAIN_BATCH_LIMIT: u32 = 1024;

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
    metrics.record_process_for_tenant(event.lsn.as_u64(), event.sequence, event.tenant_id.as_u64());

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

/// Result of draining a Normal-mode ring-buffer batch.
///
/// A sequence gap consumes the first event beyond the gap because an SPSC
/// receiver cannot push it back. That event is deliberately returned rather
/// than included in `events`: the caller must enter WAL catchup and recover it
/// from the durable log before any side effects are allowed for it.
#[derive(Debug)]
pub enum RingDrainOutcome {
    /// Every drained event was contiguous with the prior safe sequence.
    Contiguous { events: Vec<WriteEvent> },
    /// `events` contains only complete WAL records before the gap record.
    /// `first_gap_event` was consumed from the ring but was neither recorded
    /// nor dispatched and must be recovered by WAL replay. The initial safe
    /// pair is retained so callers can distinguish a rollback to the state at
    /// drain entry from a retained completed-record prefix.
    Gap {
        events: Vec<WriteEvent>,
        first_gap_event: WriteEvent,
        initial_safe_lsn: Lsn,
        initial_safe_sequence: u64,
    },
}

/// Drain available contiguous events from the ring buffer (up to
/// `DRAIN_BATCH_LIMIT`). A sequence gap means the immediately preceding
/// observed WAL record may be partial even if the consumed gap event belongs
/// to a newer LSN. The contiguous suffix sharing the last observed event's
/// LSN is therefore withheld with the gap event so normal dispatch and its
/// safe watermark stop before that potentially partial record; WAL catchup
/// then replays the whole record, including every same-LSN sibling.
pub fn drain_ring_buffer(
    rx: &mut EventConsumerRx,
    metrics: &CoreMetrics,
    core_id: usize,
    last_sequence: &mut u64,
    last_lsn: &mut Lsn,
) -> RingDrainOutcome {
    let initial_safe_lsn = *last_lsn;
    let initial_safe_sequence = *last_sequence;
    let mut events: Vec<WriteEvent> = Vec::new();
    while let Some(event) = rx.try_recv() {
        if *last_sequence > 0 && event.sequence > last_sequence.saturating_add(1) {
            detect_sequence_gap(core_id, &event, *last_sequence, metrics);

            // A WAL record can expand to several WriteEvents. The event
            // immediately before a sequence gap can be a partial record even
            // when the first post-gap event has a newer LSN, so do not dispatch
            // the observed suffix of its record. Replay must start before that
            // record and deliver all of its siblings together.
            let last_observed_lsn = events.last().map(|prior| prior.lsn);
            while events
                .last()
                .is_some_and(|prior| Some(prior.lsn) == last_observed_lsn)
            {
                events.pop();
            }
            if let Some(last_complete_event) = events.last() {
                *last_sequence = last_complete_event.sequence;
                *last_lsn = last_complete_event.lsn;
            } else {
                *last_sequence = initial_safe_sequence;
                *last_lsn = initial_safe_lsn;
            }
            for contiguous_event in &events {
                record_event(core_id, contiguous_event, metrics);
            }
            return RingDrainOutcome::Gap {
                events,
                first_gap_event: event,
                initial_safe_lsn,
                initial_safe_sequence,
            };
        }

        *last_sequence = event.sequence;
        if event.lsn.is_ahead_of(*last_lsn) {
            *last_lsn = event.lsn;
        }

        events.push(event);
        if (events.len() as u32).is_multiple_of(DRAIN_BATCH_LIMIT) {
            break;
        }
    }
    for event in &events {
        record_event(core_id, event, metrics);
    }
    RingDrainOutcome::Contiguous { events }
}

/// Drain ring-buffer events already covered by WAL replay (`lsn <= last_lsn`),
/// returning the first event whose LSN is beyond the replay point so the
/// caller can dispatch it.
///
/// Reconciliation is by **LSN** — the durable, monotonic key — not by
/// `sequence`: the per-core `sequence` counter resets to 0 on process
/// restart, so at boot it collides between WAL-replayed events and freshly
/// produced live events and cannot distinguish them. `last_lsn` is always a
/// completed WAL-record boundary: a ring gap rolls it back before the gap
/// record, so replay includes every same-LSN sibling before this comparison
/// drops their stale ring copies. The ring is SPSC (a `try_recv`'d event cannot
/// be pushed back), so the first non-stale event is handed back to the caller
/// rather than dropped; every event after it in the ring is also fresh (the
/// producer emits in LSN order) and is left for the Normal-mode drain.
pub fn drain_and_skip_stale(rx: &mut EventConsumerRx, last_lsn: Lsn) -> Option<WriteEvent> {
    let mut skipped = 0u32;
    let mut fresh = None;
    while let Some(event) = rx.try_recv() {
        if event.lsn.is_ahead_of(last_lsn) {
            fresh = Some(event);
            break;
        }
        skipped += 1;
    }
    if skipped > 0 {
        trace!(
            skipped,
            "drained stale events from ring buffer after WAL catchup"
        );
    }
    fresh
}

/// Dispatch a single WAL-catchup write event and its awaited trigger actions.
///
/// Normal-mode batches call [`dispatch_event_actions`] directly after their
/// audit step; replay calls it through this helper. Both paths therefore share
/// DEFINE EVENT processing without a ChangeStream cursor or epoch.
pub async fn dispatch_event(
    event: &WriteEvent,
    shared_state: &Arc<SharedState>,
    retry_queue: &mut ActionRetryQueue,
    cdc_router: &Arc<super::cdc::CdcRouter>,
) {
    dispatch_event_actions(event, shared_state, retry_queue).await;
    shared_state
        .watermark_tracker
        .advance_lsn_only(event.vshard_id.as_u32(), event.lsn.as_u64());
    cdc_router.route_event(event, &shared_state.watermark_tracker);
}

/// Dispatch the awaited trigger actions shared by normal Event Plane delivery
/// and WAL catchup. Keeping DEFINE EVENT here ensures both paths use the same
/// WAL-recoverable ordering and complete all actions before their consumer
/// watermark may be persisted.
pub async fn dispatch_event_actions(
    event: &WriteEvent,
    shared_state: &Arc<SharedState>,
    retry_queue: &mut ActionRetryQueue,
) {
    if !event_actions_required(event) {
        return;
    }

    super::trigger::dispatcher::dispatch_triggers(event, shared_state, retry_queue).await;
    crate::control::event_trigger::process_write_event(
        Arc::clone(shared_state),
        event,
        retry_queue,
    )
    .await;
}

fn event_actions_required(event: &WriteEvent) -> bool {
    event.op.is_data_event()
}

/// Apply the non-trigger side effects of a data write event
/// (`op.is_data_event() == true`): advances the wall-time watermark, routes
/// CDC, updates the permission cache, feeds streaming MVs and CRDT sync.
///
/// Row/statement trigger dispatch is NOT done here — it is owned exclusively
/// by [`dispatch_triggers`] (called once per event by both the Normal-mode
/// and WAL-catchup paths) so a per-row event fires its AFTER-ROW trigger
/// exactly once regardless of which path consumed it.
pub fn accumulate_data_event(
    event: &WriteEvent,
    shared_state: &Arc<SharedState>,
    cdc_router: &Arc<super::cdc::CdcRouter>,
) {
    let event_time_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;
    shared_state.watermark_tracker.advance(
        event.vshard_id.as_u32(),
        event.lsn.as_u64(),
        event_time_ms,
    );

    cdc_router.route_event(event, &shared_state.watermark_tracker);
    crate::control::security::permission_tree::event_handler::handle_permission_event(
        event,
        &shared_state.permission_cache,
    );
    let matching_streams = shared_state.stream_registry.find_matching(
        event.database_id,
        event.tenant_id.as_u64(),
        &event.collection,
    );
    for stream_def in &matching_streams {
        super::streaming_mv::processor::process_write_event_for_mvs(
            event,
            &shared_state.mv_registry,
            &stream_def.name,
        );
    }
    shared_state
        .delta_packager
        .package_and_enqueue(event, &shared_state.crdt_sync_delivery);
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::event_actions_required;
    use crate::event::types::{EventSource, RowId, WriteEvent, WriteOp};
    use crate::types::{DatabaseId, Lsn, TenantId, VShardId};

    fn event(op: WriteOp) -> WriteEvent {
        WriteEvent {
            sequence: 1,
            collection: Arc::from("events"),
            op,
            row_id: RowId::row(nodedb_types::RowIdentity::from_user_key("row-1")),
            lsn: Lsn::new(1),
            database_id: DatabaseId::DEFAULT,
            tenant_id: TenantId::new(1),
            vshard_id: VShardId::new(0),
            source: EventSource::User,
            new_value: None,
            old_value: None,
            system_time_ms: None,
            valid_time_ms: None,
            user_id: None,
            statement_digest: None,
        }
    }

    #[test]
    fn normal_and_wal_catchup_share_data_event_action_selection() {
        // Normal batches and `dispatch_event` both route through
        // `dispatch_event_actions`, whose selection is independent of any
        // transient ChangeStream sequence or epoch.
        assert!(event_actions_required(&event(WriteOp::Insert)));
        assert!(event_actions_required(&event(WriteOp::BulkDelete {
            count: 2
        })));
        assert!(!event_actions_required(&event(WriteOp::Heartbeat)));
    }
}
