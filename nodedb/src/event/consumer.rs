//! Event Plane consumer: one Tokio task per Data Plane core ring buffer.
//!
//! Each consumer operates in one of two modes:
//!
//! ```text
//! Normal ──[sequence gap detected]──► WalCatchup
//!   ▲                                    │
//!   └──[caught up to WAL head]───────────┘
//! ```
//!
//! - **Normal**: polls ring buffer, processes events, persists watermark.
//! - **WalCatchup**: pauses ring buffer entirely, reads events exclusively
//!   from WAL on disk until caught up, then switches back. Ring buffer and
//!   WAL are NEVER read simultaneously (prevents "thundering WAL" spiral).

use std::sync::Arc;
use std::time::Duration;

use nodedb_bridge::backpressure::PressureState;
use tokio::sync::watch;
use tracing::{debug, info, trace, warn};

use super::bus::EventConsumerRx;
use super::metrics::CoreMetrics;
use super::trigger::dlq::TriggerDlq;
use super::trigger::retry::TriggerRetryQueue;
use super::types::WriteEvent;
use super::watermark::WatermarkStore;
use crate::control::state::SharedState;
use crate::types::Lsn;
use crate::wal::WalManager;

use super::consumer_helpers::{
    detect_sequence_gap, flush_watermark, maybe_flush_watermark, record_event,
};

/// How often to poll the ring buffer when empty (milliseconds).
const EMPTY_POLL_INTERVAL: Duration = Duration::from_millis(1);

/// Maximum events to process per ring buffer drain before yielding.
const DRAIN_BATCH_LIMIT: u32 = 1024;

/// Consumer mode state machine.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ConsumerMode {
    /// Reading from ring buffer.
    Normal,
    /// Ring buffer paused; reading from WAL on disk.
    WalCatchup,
}

/// How often to process the retry queue (check for due retries).
const RETRY_POLL_INTERVAL: Duration = Duration::from_millis(200);

/// Configuration for spawning a consumer.
pub struct ConsumerConfig {
    pub rx: EventConsumerRx,
    pub shutdown: watch::Receiver<bool>,
    pub wal: Arc<WalManager>,
    pub watermark_store: Arc<WatermarkStore>,
    pub shared_state: Arc<SharedState>,
    pub trigger_dlq: Arc<std::sync::Mutex<TriggerDlq>>,
    pub cdc_router: Arc<super::cdc::CdcRouter>,
    pub num_cores: usize,
    /// Per-consumer slab-pin accounting for WAL memory budget enforcement.
    pub slab_account: Arc<super::slab_budget::ConsumerSlabAccount>,
}

/// Handle to a running consumer task.
pub struct ConsumerHandle {
    pub core_id: usize,
    pub metrics: Arc<CoreMetrics>,
    join_handle: tokio::task::JoinHandle<()>,
}

impl ConsumerHandle {
    pub fn abort(&self) {
        self.join_handle.abort();
    }

    pub fn events_processed(&self) -> u64 {
        use std::sync::atomic::Ordering;
        self.metrics.events_processed.load(Ordering::Relaxed)
    }
}

/// Spawn a consumer Tokio task for one Data Plane core's event ring buffer.
pub fn spawn_consumer(config: ConsumerConfig) -> ConsumerHandle {
    let core_id = config.rx.core_id();
    let metrics = Arc::new(CoreMetrics::new());
    let metrics_clone = Arc::clone(&metrics);

    let join_handle = tokio::spawn(async move {
        consumer_loop(config, metrics_clone).await;
    });

    ConsumerHandle {
        core_id,
        metrics,
        join_handle,
    }
}

/// The main consumer loop.
async fn consumer_loop(config: ConsumerConfig, metrics: Arc<CoreMetrics>) {
    let ConsumerConfig {
        mut rx,
        mut shutdown,
        wal,
        watermark_store,
        shared_state,
        trigger_dlq,
        cdc_router,
        num_cores,
        slab_account,
    } = config;

    let core_id = rx.core_id();
    let mut mode = ConsumerMode::Normal;
    let mut last_sequence: u64 = 0;
    let mut last_lsn = Lsn::ZERO;
    let mut dirty_watermark = false;
    let mut last_watermark_flush = tokio::time::Instant::now();
    let mut retry_queue = TriggerRetryQueue::new();
    let mut last_retry_poll = tokio::time::Instant::now();

    // Load persisted watermark.
    match watermark_store.load(core_id) {
        Ok(lsn) => {
            last_lsn = lsn;
            debug!(core_id, lsn = lsn.as_u64(), "loaded watermark");
        }
        Err(e) => {
            warn!(core_id, error = %e, "failed to load watermark, starting from ZERO");
        }
    }

    debug!(core_id, "event plane consumer started");
    let mut wal_retry_count: u32 = 0;

    loop {
        if *shutdown.borrow() {
            // Final watermark flush before exit.
            if dirty_watermark {
                flush_watermark(&watermark_store, core_id, last_lsn);
            }
            debug!(core_id, "event plane consumer shutting down");
            break;
        }

        match mode {
            ConsumerMode::Normal => {
                // Drain events from ring buffer.
                let events = drain_ring_buffer(
                    &mut rx,
                    &metrics,
                    core_id,
                    &mut last_sequence,
                    &mut last_lsn,
                );
                let batch_count = events.len();

                if batch_count > 0 {
                    dirty_watermark = true;

                    let mut trigger_collector =
                        crate::control::trigger::batch::collector::TriggerBatchCollector::new(
                            crate::control::trigger::batch::BatchConfig::default().batch_size,
                        );

                    // Process events: accumulate triggers in batch collector,
                    // dispatch CDC/MV/CRDT per-event (they don't benefit from batching).
                    for event in &events {
                        if event.op.is_data_event() {
                            let event_time_ms = std::time::SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .unwrap_or_default()
                                .as_millis() as u64;
                            shared_state.watermark_tracker.advance(
                                event.vshard_id.as_u16(),
                                event.lsn.as_u64(),
                                event_time_ms,
                            );
                        } else {
                            shared_state
                                .watermark_tracker
                                .advance_lsn_only(event.vshard_id.as_u16(), event.lsn.as_u64());
                            continue;
                        }

                        // Accumulate trigger rows into batches.
                        // Non-triggerable events (Trigger/RaftFollower/CrdtSync) are
                        // skipped inside push_event. Batch dispatch happens when the
                        // batch fills or collection changes.
                        if let Some(batch) =
                            crate::control::trigger::batch::collector::push_write_event(
                                &mut trigger_collector,
                                event,
                            )
                        {
                            super::trigger::dispatcher::dispatch_trigger_batch(
                                &batch,
                                &shared_state,
                                &mut retry_queue,
                            )
                            .await;
                        }

                        // Per-event: CDC routing, streaming MVs, CRDT sync.
                        // Also dispatch statement-level triggers per-event (they don't batch).
                        super::trigger::dispatcher::dispatch_triggers(
                            event,
                            &shared_state,
                            &mut retry_queue,
                        )
                        .await;
                        cdc_router.route_event(event, &shared_state.watermark_tracker);
                        let matching_streams = shared_state
                            .stream_registry
                            .find_matching(event.tenant_id.as_u32(), &event.collection);
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

                    // Flush any remaining batched trigger rows.
                    if let Some(batch) = trigger_collector.flush() {
                        super::trigger::dispatcher::dispatch_trigger_batch(
                            &batch,
                            &shared_state,
                            &mut retry_queue,
                        )
                        .await;
                    }

                    // Track slab-pinned memory for budget enforcement.
                    let batch_payload_bytes: u64 = events
                        .iter()
                        .map(|e| {
                            e.new_value.as_ref().map_or(0, |v| v.len() as u64)
                                + e.old_value.as_ref().map_or(0, |v| v.len() as u64)
                        })
                        .sum();
                    slab_account.add_pinned(batch_payload_bytes);

                    // Release after processing (Arcs dropped when events go out of scope).
                    drop(events);
                    slab_account.release_pinned(batch_payload_bytes);

                    trace!(core_id, batch_count, "event batch processed");

                    // Check if slab budget enforcement shed this consumer.
                    if slab_account.is_shed() {
                        info!(core_id, "slab budget shed — entering WAL catchup mode");
                        slab_account.reset();
                        slab_account.clear_shed();
                        mode = ConsumerMode::WalCatchup;
                        metrics.record_wal_catchup_enter();
                        continue;
                    }

                    // Check for Suspended backpressure → WAL catchup.
                    if rx.pressure_state() == PressureState::Suspended {
                        info!(
                            core_id,
                            "backpressure SUSPENDED — entering WAL catchup mode"
                        );
                        mode = ConsumerMode::WalCatchup;
                        metrics.record_wal_catchup_enter();
                        continue;
                    }

                    tokio::task::yield_now().await;
                    continue;
                }

                // No new events — process retry queue if due.
                if !retry_queue.is_empty() && last_retry_poll.elapsed() >= RETRY_POLL_INTERVAL {
                    // Step 1: drain exhausted entries and DLQ them (sync, no await).
                    let (ready, exhausted) = retry_queue.drain_due();
                    if !exhausted.is_empty() {
                        let mut dlq = trigger_dlq.lock().unwrap_or_else(|p| p.into_inner());
                        for entry in &exhausted {
                            let _ = dlq.enqueue(super::trigger::dlq::DlqEnqueueParams {
                                tenant_id: entry.tenant_id,
                                source_collection: entry.collection.clone(),
                                row_id: entry.row_id.clone(),
                                operation: entry.operation.clone(),
                                trigger_name: entry.trigger_name.clone(),
                                error: entry.last_error.clone(),
                                retry_count: entry.attempts,
                                source_lsn: entry.source_lsn,
                                source_sequence: entry.source_sequence,
                            });
                        }
                        // dlq MutexGuard dropped here before any await.
                    }

                    // Step 2: retry ready entries (async).
                    for entry in ready {
                        super::trigger::dispatcher::retry_single(
                            &entry,
                            &shared_state,
                            &mut retry_queue,
                        )
                        .await;
                    }
                    last_retry_poll = tokio::time::Instant::now();
                }

                // Flush watermark if due, then sleep.
                maybe_flush_watermark(
                    &watermark_store,
                    core_id,
                    last_lsn,
                    &mut dirty_watermark,
                    &mut last_watermark_flush,
                );

                tokio::select! {
                    _ = tokio::time::sleep(EMPTY_POLL_INTERVAL) => {}
                    _ = shutdown.changed() => {
                        if dirty_watermark {
                            flush_watermark(&watermark_store, core_id, last_lsn);
                        }
                        debug!(core_id, "event plane consumer received shutdown");
                        break;
                    }
                }
            }

            ConsumerMode::WalCatchup => {
                const MAX_WAL_RETRIES: u32 = 10;

                info!(
                    core_id,
                    from_lsn = last_lsn.as_u64(),
                    "WAL catchup: replaying from WAL"
                );

                // Use mmap reader for catchup — kernel manages pages without
                // pinning slab allocator memory. Falls back to sequential reader
                // if mmap fails (e.g., WAL dir on tmpfs without mmap support).
                match super::wal_replay::replay_wal_mmap(
                    &wal,
                    last_lsn.next(),
                    core_id,
                    num_cores,
                    last_sequence,
                )
                .or_else(|_| {
                    super::wal_replay::replay_wal_to_events(
                        &wal,
                        last_lsn.next(),
                        core_id,
                        num_cores,
                        last_sequence,
                    )
                }) {
                    Ok(events) => {
                        wal_retry_count = 0;
                        let count = events.len() as u64;
                        for event in &events {
                            record_event(core_id, event, &metrics);
                            // Also dispatch triggers + CDC for WAL-replayed events.
                            super::trigger::dispatcher::dispatch_triggers(
                                event,
                                &shared_state,
                                &mut retry_queue,
                            )
                            .await;
                            cdc_router.route_event(event, &shared_state.watermark_tracker);
                            last_sequence = event.sequence;
                            if event.lsn.is_ahead_of(last_lsn) {
                                last_lsn = event.lsn;
                            }
                        }
                        if count > 0 {
                            metrics.record_wal_replay(count);
                            info!(
                                core_id,
                                events_replayed = count,
                                new_lsn = last_lsn.as_u64(),
                                "WAL catchup complete"
                            );
                        } else {
                            debug!(core_id, "WAL catchup: no new events");
                        }
                    }
                    Err(e) => {
                        wal_retry_count += 1;
                        if wal_retry_count > MAX_WAL_RETRIES {
                            tracing::error!(
                                core_id,
                                error = %e,
                                retries = MAX_WAL_RETRIES,
                                "WAL catchup failed after max retries, returning to Normal mode"
                            );
                            // Fall through to Normal mode — some events may be lost,
                            // but the consumer won't loop forever.
                        } else {
                            warn!(
                                core_id,
                                error = %e,
                                retry = wal_retry_count,
                                max_retries = MAX_WAL_RETRIES,
                                "WAL catchup replay failed, retrying after delay"
                            );
                            tokio::time::sleep(Duration::from_millis(100)).await;
                            continue;
                        }
                    }
                }

                // Drain any events that accumulated in the ring buffer during catchup.
                // These may overlap with WAL-replayed events — deduplicate by sequence.
                drain_and_skip_stale(&mut rx, last_sequence);

                // Flush watermark and return to normal mode.
                flush_watermark(&watermark_store, core_id, last_lsn);
                dirty_watermark = false;
                last_watermark_flush = tokio::time::Instant::now();

                mode = ConsumerMode::Normal;
                info!(core_id, "returned to Normal mode");
            }
        }
    }

    let processed = {
        use std::sync::atomic::Ordering;
        metrics.events_processed.load(Ordering::Relaxed)
    };
    debug!(
        core_id,
        total_processed = processed,
        "event plane consumer stopped"
    );
}

/// Drain all available events from the ring buffer (up to DRAIN_BATCH_LIMIT).
/// Returns the drained events for async processing (trigger dispatch).
fn drain_ring_buffer(
    rx: &mut EventConsumerRx,
    metrics: &CoreMetrics,
    core_id: usize,
    last_sequence: &mut u64,
    last_lsn: &mut Lsn,
) -> Vec<WriteEvent> {
    let mut events = Vec::new();
    while let Some(event) = rx.try_recv() {
        detect_sequence_gap(core_id, &event, *last_sequence, metrics);
        record_event(core_id, &event, metrics);

        *last_sequence = event.sequence;
        if event.lsn.is_ahead_of(*last_lsn) {
            *last_lsn = event.lsn;
        }

        events.push(event);
        if (events.len() as u32).is_multiple_of(DRAIN_BATCH_LIMIT) {
            break;
        }
    }
    events
}

/// Drain the ring buffer, skipping events with sequence <= last_sequence.
/// Used after WAL catchup to discard stale events that overlap with replay.
fn drain_and_skip_stale(rx: &mut EventConsumerRx, last_sequence: u64) {
    let mut skipped = 0u32;
    while let Some(event) = rx.try_recv() {
        if event.sequence <= last_sequence {
            skipped += 1;
        } else {
            // Shouldn't happen — we just caught up. But if it does,
            // the event is newer than what we replayed. Log and drop
            // (next normal poll will process new events).
            break;
        }
    }
    if skipped > 0 {
        trace!(
            skipped,
            "drained stale events from ring buffer after WAL catchup"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::bus::create_event_bus_with_capacity;
    use crate::event::types::{EventSource, RowId, WriteOp};
    use crate::types::{TenantId, VShardId};

    fn make_event(seq: u64) -> WriteEvent {
        WriteEvent {
            sequence: seq,
            collection: Arc::from("test"),
            op: WriteOp::Insert,
            row_id: RowId::new("row-1"),
            lsn: Lsn::new(seq * 10),
            tenant_id: TenantId::new(1),
            vshard_id: VShardId::new(0),
            source: EventSource::User,
            new_value: Some(Arc::from(b"data".as_slice())),
            old_value: None,
        }
    }

    #[test]
    fn gap_detection() {
        let metrics = CoreMetrics::new();
        let e1 = make_event(1);
        let e5 = make_event(5);

        record_event(0, &e1, &metrics);
        detect_sequence_gap(0, &e5, 1, &metrics);
        record_event(0, &e5, &metrics);

        use std::sync::atomic::Ordering;
        assert_eq!(metrics.events_processed.load(Ordering::Relaxed), 2);
        assert_eq!(metrics.events_dropped.load(Ordering::Relaxed), 3);
    }

    #[tokio::test]
    async fn consumer_processes_and_persists_watermark() {
        let (mut producers, consumers) = create_event_bus_with_capacity(1, 64);
        let dir = tempfile::tempdir().unwrap();
        let (wal, watermark_store, shared_state, trigger_dlq, cdc_router) =
            crate::event::test_utils::event_test_deps(&dir);

        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        // Emit events.
        for i in 1..=5 {
            producers[0].emit(make_event(i));
        }

        let handle = spawn_consumer(ConsumerConfig {
            rx: consumers.into_iter().next().unwrap(),
            shutdown: shutdown_rx,
            wal,
            watermark_store: Arc::clone(&watermark_store),
            shared_state,
            trigger_dlq,
            cdc_router,
            num_cores: 1,
            slab_account: Arc::new(crate::event::slab_budget::ConsumerSlabAccount::new(0)),
        });

        // Let consumer process.
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(handle.events_processed(), 5);

        // Shutdown (triggers final watermark flush).
        shutdown_tx.send(true).ok();
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Verify watermark was persisted.
        let wm = watermark_store.load(0).unwrap();
        assert_eq!(wm, Lsn::new(50)); // seq 5 → lsn = 5*10 = 50
    }
}
