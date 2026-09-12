// SPDX-License-Identifier: BUSL-1.1

//! Event Plane consumer: one Tokio task per Data Plane core ring buffer.
//!
//! Each consumer operates in one of two modes:
//!
//! ```text
//! (boot: resume from persisted watermark)
//!         │
//!         ▼
//! WalCatchup ──[caught up to WAL head]──► Normal
//!   ▲                                       │
//!   └────────[sequence gap detected]────────┘
//! ```
//!
//! - **Normal**: polls ring buffer, processes events, persists watermark.
//! - **WalCatchup**: pauses ring buffer entirely, reads events exclusively
//!   from WAL on disk until caught up, then switches back. Ring buffer and
//!   WAL are NEVER read simultaneously (prevents "thundering WAL" spiral).
//!   The consumer also boots directly into this mode (see `consumer_loop`)
//!   to replay any WAL suffix past the persisted watermark before serving
//!   the ring buffer, closing the restart delivery gap. If that suffix is no
//!   longer in the WAL — truncated away, or unreachable because the watermark
//!   itself was lost — the node fail-stops rather than resume from a position
//!   it cannot prove it reached.

use std::sync::Arc;
use std::time::Duration;

use nodedb_bridge::backpressure::PressureState;
use tokio::sync::watch;
use tracing::{debug, info, trace, warn};

use super::action::ActionRetryQueue;
use super::bus::EventConsumerRx;
use super::metrics::CoreMetrics;
use super::trigger::dlq::TriggerDlq;
use super::watermark::WatermarkStore;
use crate::control::state::SharedState;
use crate::types::Lsn;
use crate::wal::WalManager;

use super::consumer_helpers::{
    RingDrainOutcome, accumulate_data_event, dispatch_event, dispatch_event_actions,
    drain_and_skip_stale, drain_ring_buffer, flush_watermark, maybe_flush_watermark, record_event,
};

/// Initial sleep when the ring buffer is empty. Adaptive backoff ramps
/// up to `EMPTY_POLL_MAX` after `EMPTY_POLL_RAMP` consecutive empty polls
/// so an idle Event Plane consumer does not wake every 1ms forever.
const EMPTY_POLL_MIN: Duration = Duration::from_millis(1);
/// Cap on the empty-poll sleep. 50ms keeps trigger / CDC dispatch latency
/// bounded for the first event after an idle period while limiting idle
/// CPU to ~20 wakes/sec per core.
const EMPTY_POLL_MAX: Duration = Duration::from_millis(50);
/// After this many consecutive empty polls (~32ms of idleness at 1ms),
/// switch to the long sleep.
const EMPTY_POLL_RAMP: u32 = 32;

/// How often to process the retry queue (check for due retries).
const RETRY_POLL_INTERVAL: Duration = Duration::from_millis(200);

/// Consumer mode state machine.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ConsumerMode {
    /// Reading from ring buffer.
    Normal,
    /// Ring buffer paused; reading from WAL on disk.
    WalCatchup,
}

/// Select the only safe post-drain mode. A gap event has already been
/// consumed from the SPSC ring, so Normal mode may not observe or dispatch it.
fn normal_drain_next_mode(outcome: &RingDrainOutcome) -> ConsumerMode {
    match outcome {
        RingDrainOutcome::Contiguous { .. } => ConsumerMode::Normal,
        RingDrainOutcome::Gap { .. } => ConsumerMode::WalCatchup,
    }
}

/// Configuration for spawning a consumer.
pub struct ConsumerConfig {
    pub rx: EventConsumerRx,
    pub shutdown: watch::Receiver<bool>,
    /// The node-wide shutdown coordinator. WAL recovery failure is unsafe to
    /// continue through, so the consumer initiates this canonical bus.
    pub shutdown_bus: crate::control::shutdown::ShutdownBus,
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

    /// Await natural task termination without taking ownership of the handle.
    /// This permits the shutdown supervisor to retain abort ownership until
    /// the configured deadline expires.
    pub async fn wait_for_exit(&mut self) {
        let _ = (&mut self.join_handle).await;
    }

    /// Abort the task and await its termination, consuming the handle so the
    /// task future (and every `Arc` it held) is definitely dropped by the
    /// time this returns. Used in shutdown paths that must observe `Drop`
    /// side effects before reopening resources (e.g. redb file locks).
    pub async fn abort_and_join(mut self) {
        self.join_handle.abort();
        let _ = (&mut self.join_handle).await;
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
        shutdown_bus,
        wal,
        watermark_store,
        shared_state,
        trigger_dlq,
        cdc_router,
        num_cores,
        slab_account,
    } = config;

    let core_id = rx.core_id();
    let mut last_sequence: u64 = 0;
    let mut last_lsn = Lsn::ZERO;
    let mut dirty_watermark = false;
    let mut last_watermark_flush = tokio::time::Instant::now();
    let mut retry_queue = ActionRetryQueue::for_core(&shared_state.data_dir, core_id);
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

    // Boot directly into WalCatchup: the ring buffer is always empty on a
    // fresh process, so any events committed to the WAL but not yet
    // dispatched+watermarked before a crash/restart would otherwise be
    // silently dropped. Replaying [watermark+1, WAL head] here closes that
    // gap; the WalCatchup arm below self-transitions to Normal once caught
    // up (or immediately, on a fresh DB with an empty WAL).
    let mut mode = ConsumerMode::WalCatchup;

    debug!(core_id, "event plane consumer started");
    let mut wal_retry_count: u32 = 0;
    let mut empty_polls: u32 = 0;

    loop {
        if *shutdown.borrow() {
            if dirty_watermark {
                flush_watermark(&watermark_store, core_id, last_lsn);
            }
            debug!(core_id, "event plane consumer shutting down");
            break;
        }

        match mode {
            ConsumerMode::Normal => {
                let drain = drain_ring_buffer(
                    &mut rx,
                    &metrics,
                    core_id,
                    &mut last_sequence,
                    &mut last_lsn,
                );
                let next_mode = normal_drain_next_mode(&drain);
                let (events, gap_event) = match drain {
                    RingDrainOutcome::Contiguous { events } => (events, None),
                    RingDrainOutcome::Gap {
                        events,
                        first_gap_event,
                        ..
                    } => (events, Some(first_gap_event)),
                };
                let batch_count = events.len();

                if batch_count > 0 {
                    empty_polls = 0;
                    dirty_watermark = true;

                    process_normal_batch(&events, &shared_state, &mut retry_queue, &cdc_router)
                        .await;

                    let batch_payload_bytes: u64 = events
                        .iter()
                        .map(|e| {
                            e.new_value.as_ref().map_or(0, |v| v.len() as u64)
                                + e.old_value.as_ref().map_or(0, |v| v.len() as u64)
                        })
                        .sum();
                    slab_account.add_pinned(batch_payload_bytes);
                    drop(events);
                    slab_account.release_pinned(batch_payload_bytes);

                    trace!(core_id, batch_count, "event batch processed");
                }

                if let Some(first_gap_event) = gap_event {
                    // This event has been removed from the SPSC ring but is not
                    // safe to record or dispatch. WAL catchup starts from the
                    // prior complete LSN and must reconstruct the withheld
                    // preceding record as well as the gap event.
                    warn!(
                        core_id,
                        gap_sequence = first_gap_event.sequence,
                        gap_lsn = first_gap_event.lsn.as_u64(),
                        safe_sequence = last_sequence,
                        safe_lsn = last_lsn.as_u64(),
                        "ring sequence gap consumed; entering WAL catchup before event side effects"
                    );
                    mode = next_mode;
                    debug_assert_eq!(mode, ConsumerMode::WalCatchup);
                    metrics.record_wal_catchup_enter();
                    continue;
                }

                if batch_count > 0 {
                    if slab_account.is_shed() {
                        info!(core_id, "slab budget shed — entering WAL catchup mode");
                        slab_account.reset();
                        slab_account.clear_shed();
                        mode = ConsumerMode::WalCatchup;
                        metrics.record_wal_catchup_enter();
                        continue;
                    }

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

                // No new events — process retry queue if due. An empty queue
                // still polls when an operator has requeued something from the
                // DLQ, since that action arrives out-of-band and would
                // otherwise wait for an unrelated failure to wake the poll.
                let requeued_waiting = shared_state
                    .action_requeue
                    .get()
                    .is_some_and(|inbox| inbox.has_work_for(core_id));
                if (!retry_queue.is_empty() || requeued_waiting)
                    && last_retry_poll.elapsed() >= RETRY_POLL_INTERVAL
                {
                    process_retry_queue(&mut retry_queue, &trigger_dlq, &shared_state, core_id)
                        .await;
                    last_retry_poll = tokio::time::Instant::now();
                }

                maybe_flush_watermark(
                    &watermark_store,
                    core_id,
                    last_lsn,
                    &mut dirty_watermark,
                    &mut last_watermark_flush,
                );

                empty_polls = empty_polls.saturating_add(1);
                let poll_sleep = if empty_polls < EMPTY_POLL_RAMP {
                    EMPTY_POLL_MIN
                } else {
                    EMPTY_POLL_MAX
                };

                tokio::select! {
                    _ = tokio::time::sleep(poll_sleep) => {}
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

                match super::wal_replay::replay_wal_mmap(
                    &wal,
                    last_lsn.next(),
                    core_id,
                    num_cores,
                    last_sequence,
                )
                .or_else(|e| {
                    // The sequential arm is a fallback for readers that cannot
                    // see the bytes (mmap misses O_DIRECT writes to the active
                    // segment), not for a log that no longer holds the records.
                    // Both arms read the same directory, so retrying a deleted
                    // suffix only re-derives the same answer.
                    if is_retained_floor_violation(&e) {
                        return Err(e);
                    }
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
                            dispatch_event(event, &shared_state, &mut retry_queue, &cdc_router)
                                .await;
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
                    Err(e) if is_retained_floor_violation(&e) => {
                        // The WAL no longer holds the suffix this consumer has
                        // to replay, so the events between the watermark and
                        // the retained floor were never dispatched and can
                        // never be reconstructed: their CDC rows, trigger
                        // fires, and streaming-MV updates are already lost.
                        // Retrying cannot heal that, and returning to Normal
                        // mode would resume from a watermark that claims
                        // delivery which never happened — divergence that
                        // spreads silently into every downstream consumer.
                        // Stop the node instead, while an operator can still
                        // see where the log begins and restore from a snapshot.
                        fail_stop_wal_catchup(
                            core_id,
                            &e,
                            "the WAL no longer retains the suffix this consumer must replay",
                            wal_retry_count,
                            last_lsn,
                            &shared_state,
                            &shutdown_bus,
                        );
                        break;
                    }
                    Err(e) => {
                        wal_retry_count += 1;
                        if wal_retry_count >= MAX_WAL_RETRIES {
                            fail_stop_wal_catchup(
                                core_id,
                                &e,
                                "WAL catchup replay kept failing",
                                wal_retry_count,
                                last_lsn,
                                &shared_state,
                                &shutdown_bus,
                            );
                            // Do not return to Normal or flush an uncertain
                            // watermark: no later event side effects may run.
                            break;
                        }
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

                // Discard ring-buffer events already re-dispatched by replay
                // (`lsn <= last_lsn`); the first event past the replay point is
                // returned and dispatched here rather than dropped (the ring is
                // SPSC and cannot un-receive it). Everything after it is fresh
                // and is served by the Normal-mode drain on the next iteration.
                if let Some(event) = drain_and_skip_stale(&mut rx, last_lsn) {
                    record_event(core_id, &event, &metrics);
                    dispatch_event(&event, &shared_state, &mut retry_queue, &cdc_router).await;
                    last_sequence = event.sequence;
                    if event.lsn.is_ahead_of(last_lsn) {
                        last_lsn = event.lsn;
                    }
                }

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

/// Is this the WAL reporting that the requested replay suffix was already
/// truncated away?
///
/// Every other replay failure is potentially transient (a partially written
/// active segment, a reader that cannot see O_DIRECT bytes); this one is not,
/// so the consumer routes it past the retry loop straight to fail-stop.
fn is_retained_floor_violation(error: &crate::Error) -> bool {
    matches!(
        error,
        crate::Error::Wal(nodedb_wal::WalError::ReplayBelowRetainedFloor { .. })
    )
}

/// Fail-stop on an unrecoverable WAL catchup failure.
///
/// `reason` states which failure class stopped the node; `attempts` is how many
/// replay attempts preceded it (zero for a failure that is unrecoverable on the
/// first observation and never retried).
///
/// `last_safe_lsn` is deliberately only observed for audit/logging; this path
/// never mutates or flushes it. Continuing without a recoverable WAL prefix
/// would permit later event side effects to overtake missing writes.
fn fail_stop_wal_catchup(
    core_id: usize,
    error: &impl std::fmt::Display,
    reason: &'static str,
    attempts: u32,
    last_safe_lsn: Lsn,
    shared_state: &SharedState,
    shutdown_bus: &crate::control::shutdown::ShutdownBus,
) {
    tracing::error!(
        core_id,
        error = %error,
        reason,
        attempts,
        last_safe_lsn = last_safe_lsn.as_u64(),
        "WAL catchup cannot complete; initiating fail-stop shutdown"
    );
    shared_state.audit_record(
        crate::control::security::audit::AuditEvent::AdminAction,
        None,
        "event_plane",
        &format!(
            "event consumer core {core_id} WAL catchup stopped after {attempts} attempts at safe LSN {} ({reason}): {error}",
            last_safe_lsn.as_u64()
        ),
    );
    drop(shutdown_bus.initiate());
}

/// Process a batch of Normal-mode events: CDC, permission cache, streaming MVs,
/// CRDT sync, and awaited AFTER/DEFINE EVENT trigger actions.
///
/// Row/statement and DEFINE EVENT processing is per-event via
/// [`dispatch_event_actions`]. The normal batch and WAL-catchup
/// (`dispatch_event` → `dispatch_event_actions`) paths therefore execute the
/// same action processor exactly once for every data `WriteEvent`.
async fn process_normal_batch(
    events: &[super::types::WriteEvent],
    shared_state: &Arc<SharedState>,
    retry_queue: &mut ActionRetryQueue,
    cdc_router: &Arc<super::cdc::CdcRouter>,
) {
    for event in events {
        if !event.op.is_data_event() {
            shared_state
                .watermark_tracker
                .advance_lsn_only(event.vshard_id.as_u32(), event.lsn.as_u64());
            continue;
        }

        // DML audit: record to audit log before dispatching triggers.
        super::audit_dml::audit_dml_event(event, shared_state);

        // Await every AFTER/DEFINE EVENT action before advancing the Event
        // Plane's data watermark or publishing non-trigger side effects.
        dispatch_event_actions(event, shared_state, retry_queue).await;

        // Non-trigger side effects (watermark, CDC, permission cache, MVs, CRDT).
        accumulate_data_event(event, shared_state, cdc_router);
    }
}

/// Process the retry queue: DLQ exhausted entries and retry ready ones.
async fn process_retry_queue(
    retry_queue: &mut ActionRetryQueue,
    trigger_dlq: &Arc<std::sync::Mutex<TriggerDlq>>,
    shared_state: &Arc<SharedState>,
    core_id: usize,
) {
    // Collect anything an operator sent back from the DLQ before draining, so
    // a requeued action joins this round instead of waiting for the next poll.
    if let Some(inbox) = shared_state.action_requeue.get() {
        for action in inbox.take_for_core(core_id) {
            retry_queue.enqueue(action);
        }
    }

    let (ready, exhausted) = retry_queue.drain_due();
    if !exhausted.is_empty() {
        let mut dlq = trigger_dlq.lock().unwrap_or_else(|p| p.into_inner());
        for action in exhausted {
            let _ = dlq.enqueue(action);
        }
        // dlq MutexGuard dropped before any await.
    }

    for action in ready {
        super::trigger::dispatcher::retry_action(&action, shared_state, retry_queue).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::bus::create_event_bus_with_capacity;
    use crate::event::consumer_helpers::{
        RingDrainOutcome, detect_sequence_gap, drain_ring_buffer,
    };
    use crate::event::types::{EventSource, RowId, WriteOp};
    use crate::types::{DatabaseId, TenantId, VShardId};

    fn make_event(seq: u64) -> super::super::types::WriteEvent {
        super::super::types::WriteEvent {
            sequence: seq,
            collection: Arc::from("test"),
            op: WriteOp::Insert,
            row_id: RowId::row(nodedb_types::RowIdentity::from_user_key("row-1")),
            lsn: Lsn::new(seq * 10),
            database_id: DatabaseId::new(7),
            tenant_id: TenantId::new(1),
            vshard_id: VShardId::new(0),
            source: EventSource::User,
            new_value: Some(Arc::from(b"data".as_slice())),
            old_value: None,
            system_time_ms: None,
            valid_time_ms: None,
            user_id: None,
            statement_digest: None,
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

    #[test]
    fn gap_event_is_withheld_for_wal_catchup_and_normal_transitions() {
        let (mut producers, mut consumers) = create_event_bus_with_capacity(1, 16);
        producers[0].emit(make_event(1));
        producers[0].emit(make_event(2));
        producers[0].emit(make_event(4));

        let metrics = CoreMetrics::new();
        let mut rx = consumers.remove(0);
        let mut last_sequence = 0;
        let mut last_lsn = Lsn::ZERO;
        let outcome = drain_ring_buffer(&mut rx, &metrics, 0, &mut last_sequence, &mut last_lsn);

        assert_eq!(normal_drain_next_mode(&outcome), ConsumerMode::WalCatchup);
        let RingDrainOutcome::Gap {
            events,
            first_gap_event,
            initial_safe_lsn,
            initial_safe_sequence,
        } = outcome
        else {
            panic!("a sequence gap must force WAL catchup");
        };
        assert_eq!(
            events
                .iter()
                .map(|event| event.sequence)
                .collect::<Vec<_>>(),
            [1]
        );
        // The last observed record (sequence 2) is withheld with the gap;
        // the earlier completed record remains safe for Normal-mode dispatch.
        assert_eq!(first_gap_event.sequence, 4);
        assert_eq!(initial_safe_sequence, 0);
        assert_eq!(initial_safe_lsn, Lsn::ZERO);
        assert_eq!(last_sequence, 1);
        assert_eq!(last_lsn, Lsn::new(10));

        use std::sync::atomic::Ordering;
        assert_eq!(metrics.events_processed.load(Ordering::Relaxed), 1);
        assert_eq!(metrics.last_processed_lsn.load(Ordering::Relaxed), 10);
        assert_eq!(metrics.events_dropped.load(Ordering::Relaxed), 1);
        assert_eq!(rx.try_recv().map(|event| event.sequence), None);
    }

    #[test]
    fn gap_within_wal_record_withholds_its_contiguous_sibling() {
        let (mut producers, mut consumers) = create_event_bus_with_capacity(1, 16);
        let mut first_sibling = make_event(2);
        first_sibling.lsn = Lsn::new(100);
        let mut gap_sibling = make_event(4);
        gap_sibling.lsn = Lsn::new(100);
        producers[0].emit(first_sibling);
        producers[0].emit(gap_sibling);

        let metrics = CoreMetrics::new();
        let mut rx = consumers.remove(0);
        let mut last_sequence = 1;
        let mut last_lsn = Lsn::new(90);
        let outcome = drain_ring_buffer(&mut rx, &metrics, 0, &mut last_sequence, &mut last_lsn);

        let RingDrainOutcome::Gap {
            events,
            first_gap_event,
            initial_safe_lsn,
            initial_safe_sequence,
        } = outcome
        else {
            panic!("a sequence gap must force WAL catchup");
        };
        assert_eq!(initial_safe_lsn, Lsn::new(90));
        assert_eq!(initial_safe_sequence, 1);
        assert!(events.is_empty());
        assert_eq!(first_gap_event.sequence, 4);
        assert_eq!(first_gap_event.lsn, Lsn::new(100));
        // The first sibling at LSN 100 is withheld with the gap sibling. The
        // replay start must therefore not skip the whole record.
        assert_eq!(last_sequence, 1);
        assert_eq!(last_lsn, Lsn::new(90));
        let replay_start = last_lsn.next();
        assert_eq!(replay_start, Lsn::new(91));
        assert!(
            replay_start <= first_gap_event.lsn,
            "replay must include the withheld record"
        );

        use std::sync::atomic::Ordering;
        assert_eq!(metrics.events_processed.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn gap_at_newer_lsn_withholds_preceding_observed_record() {
        let (mut producers, mut consumers) = create_event_bus_with_capacity(1, 16);
        let mut preceding_record_sibling = make_event(2);
        preceding_record_sibling.lsn = Lsn::new(100);
        let mut first_post_gap_event = make_event(4);
        first_post_gap_event.lsn = Lsn::new(101);
        producers[0].emit(preceding_record_sibling);
        producers[0].emit(first_post_gap_event);

        let metrics = CoreMetrics::new();
        let mut rx = consumers.remove(0);
        let mut last_sequence = 1;
        let mut last_lsn = Lsn::new(90);
        let outcome = drain_ring_buffer(&mut rx, &metrics, 0, &mut last_sequence, &mut last_lsn);

        let RingDrainOutcome::Gap {
            events,
            first_gap_event,
            initial_safe_lsn,
            initial_safe_sequence,
        } = outcome
        else {
            panic!("a sequence gap must force WAL catchup");
        };
        assert_eq!(initial_safe_lsn, Lsn::new(90));
        assert_eq!(initial_safe_sequence, 1);
        // Sequence 3 at LSN 100 is missing, so the observed sequence-2
        // sibling cannot be dispatched as a complete durable record.
        assert!(events.is_empty());
        assert_eq!(first_gap_event.sequence, 4);
        assert_eq!(first_gap_event.lsn, Lsn::new(101));
        // Roll back to the preceding safe durable LSN: replay begins before
        // LSN 100 and recovers its missing sibling as well as the gap event.
        assert_eq!(last_sequence, 1);
        assert_eq!(last_lsn, Lsn::new(90));
        assert!(last_lsn.next() <= Lsn::new(100));

        use std::sync::atomic::Ordering;
        assert_eq!(metrics.events_processed.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn max_wal_replay_failure_initiates_shutdown_without_advancing_watermark() {
        let dir = tempfile::tempdir().unwrap();
        let (_wal, watermark_store, shared_state, _trigger_dlq, _cdc_router) =
            crate::event::test_utils::event_test_deps(&dir);
        watermark_store.save(0, Lsn::new(10)).unwrap();
        let (shutdown_bus, _) =
            crate::control::shutdown::ShutdownBus::new(Arc::clone(&shared_state.shutdown));

        fail_stop_wal_catchup(
            0,
            &"replay unavailable",
            "WAL catchup replay kept failing",
            10,
            Lsn::new(10),
            &shared_state,
            &shutdown_bus,
        );

        assert!(shared_state.shutdown.is_shutdown());
        assert_eq!(watermark_store.load(0).unwrap(), Lsn::new(10));
    }

    /// A truncated-away suffix is routed past the retry loop: it is not
    /// transient, and continuing would advance the watermark past events that
    /// were never dispatched.
    #[test]
    fn truncated_suffix_is_recognised_as_unrecoverable() {
        assert!(is_retained_floor_violation(&crate::Error::Wal(
            nodedb_wal::WalError::ReplayBelowRetainedFloor {
                from_lsn: 10,
                retained_floor_lsn: 4096,
                earliest_segment: "wal-00000000000000004096.seg".to_string(),
            }
        )));
        assert!(!is_retained_floor_violation(&crate::Error::Wal(
            nodedb_wal::WalError::Sealed
        )));
    }

    #[tokio::test]
    async fn consumer_processes_and_persists_watermark() {
        let (mut producers, consumers) = create_event_bus_with_capacity(1, 64);
        let dir = tempfile::tempdir().unwrap();
        let (wal, watermark_store, shared_state, trigger_dlq, cdc_router) =
            crate::event::test_utils::event_test_deps(&dir);

        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let shutdown_watch = Arc::new(crate::control::shutdown::ShutdownWatch::new());
        let (shutdown_bus, _) = crate::control::shutdown::ShutdownBus::new(shutdown_watch);

        // Emit events.
        for i in 1..=5 {
            producers[0].emit(make_event(i));
        }

        let handle = spawn_consumer(ConsumerConfig {
            rx: consumers.into_iter().next().unwrap(),
            shutdown: shutdown_rx,
            shutdown_bus,
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
