// SPDX-License-Identifier: BUSL-1.1

//! The consumer loop of one core's ring.
//!
//! Each pass:
//!
//! 1. Replays what a recovery still owes, up to the records whose outcome is
//!    final (see [`super::recovery`]). At boot the recovery covers the WAL
//!    above the persisted watermark.
//! 2. Takes events off the ring (see [`super::drain`]), runs the permission
//!    step on every one of them, and delivers the ones the guard admits (see
//!    [`super::delivery`]). A drop starts or widens a recovery.
//! 3. Advances the safe prefix, which is the persisted watermark: restart
//!    replays the WAL above it.
//!
//! Ring and WAL are read in turn by this one task, never at once.

use std::sync::Arc;
use std::time::Duration;

use tracing::{debug, info, trace, warn};

use crate::control::state::SharedState;
use crate::event::action::ActionRetryQueue;
use crate::event::consumer_helpers::{flush_watermark, maybe_flush_watermark, record_event};
use crate::event::metrics::CoreMetrics;
use crate::event::trigger::dlq::TriggerDlq;
use crate::types::Lsn;
use crate::wal::WalManager;

use super::delivery::DeliveryGuard;
use super::drain::RingCursor;
use super::fail_stop::{fail_stop_wal_catchup, is_retained_floor_violation};
use super::handle::ConsumerConfig;
use super::pipeline::deliver_events;
use super::recovery::Recovery;

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

/// Replay attempts before a failing recovery stops the node.
const MAX_WAL_RETRIES: u32 = 10;

/// The highest LSN in the WAL now.
fn last_wal_lsn(wal: &WalManager) -> Lsn {
    Lsn::new(wal.next_lsn().as_u64().saturating_sub(1))
}

/// Every record at or below this LSN has its final outcome. Records the cores
/// replayed at boot are final, whatever the live outcome floor says.
fn final_outcome_bound(shared_state: &SharedState, boot_end: Lsn) -> Lsn {
    shared_state.outcome_floor.floor().max(boot_end)
}

/// The main consumer loop.
pub(super) async fn consumer_loop(config: ConsumerConfig, metrics: Arc<CoreMetrics>) {
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
    let emit_progress = rx.progress();
    let mut retry_queue = ActionRetryQueue::for_core(&shared_state.data_dir, core_id);
    let mut last_retry_poll = tokio::time::Instant::now();

    let persisted = match watermark_store.load(core_id) {
        Ok(lsn) => {
            debug!(core_id, lsn = lsn.as_u64(), "loaded watermark");
            lsn
        }
        Err(e) => {
            warn!(core_id, error = %e, "failed to load watermark, starting from ZERO");
            Lsn::ZERO
        }
    };
    let mut guard = DeliveryGuard::new(persisted);
    // Events committed to the WAL but not delivered before a restart are
    // recovered first. If that suffix is no longer in the WAL, the node
    // fail-stops rather than resume from a position it cannot prove it reached.
    let boot_end = last_wal_lsn(&wal);
    let mut recovery = Some(Recovery::new(persisted, boot_end));
    let mut cursor = RingCursor::new();

    let mut dirty_watermark = false;
    let mut last_watermark_flush = tokio::time::Instant::now();
    let mut wal_retry_count: u32 = 0;
    let mut empty_polls: u32 = 0;

    debug!(core_id, "event plane consumer started");

    loop {
        if *shutdown.borrow() {
            if dirty_watermark {
                flush_watermark(&shared_state, &watermark_store, core_id, guard.safe());
            }
            debug!(core_id, "event plane consumer shutting down");
            break;
        }

        let mut replayed = 0u64;
        if let Some(active) = recovery.as_mut() {
            let final_bound = final_outcome_bound(&shared_state, boot_end);
            if let Some((from, upto)) = active.next_range(final_bound) {
                match super::replay::replay_range(&wal, from, upto, core_id, num_cores) {
                    Ok(events) => {
                        wal_retry_count = 0;
                        for event in &events {
                            record_event(core_id, event, &metrics);
                        }
                        replayed = deliver_events(
                            core_id,
                            &events,
                            &mut guard,
                            &shared_state,
                            &mut retry_queue,
                            &cdc_router,
                        )
                        .await;
                        active.note_replayed(upto);
                        metrics.record_wal_replay(events.len() as u64);
                        info!(
                            core_id,
                            from = from.as_u64(),
                            upto = upto.as_u64(),
                            delivered = replayed,
                            "WAL catchup pass complete"
                        );
                    }
                    Err(e) if is_retained_floor_violation(&e) => {
                        // The events between the watermark and the retained
                        // floor were never delivered and cannot be rebuilt.
                        // Stop while an operator can still see where the log
                        // begins and restore from a snapshot.
                        fail_stop_wal_catchup(
                            core_id,
                            &e,
                            "the WAL no longer retains the suffix this consumer must replay",
                            wal_retry_count,
                            guard.safe(),
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
                                guard.safe(),
                                &shared_state,
                                &shutdown_bus,
                            );
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
            }
            if active.is_done() {
                recovery = None;
                debug!(core_id, "WAL catchup complete");
            }
        }

        // Bound first, then the counter: every event of a record at or below
        // the bound is counted.
        let final_bound = final_outcome_bound(&shared_state, boot_end);
        cursor.take_snapshot(final_bound, emit_progress.emitted());
        let emitted_before = emit_progress.emitted();
        let drained = cursor.drain(&mut rx, &metrics, core_id, emitted_before);
        let batch_count = drained.events.len();

        crate::control::security::permission_tree::event_handler::apply_ring_events(
            core_id,
            &drained.events,
            &shared_state.permission_cache,
            shared_state.authorization_fence.permission_applied(),
        )
        .await;

        if batch_count > 0 {
            empty_polls = 0;
            let batch_payload_bytes: u64 = drained
                .events
                .iter()
                .map(|e| {
                    e.new_value.as_ref().map_or(0, |v| v.len() as u64)
                        + e.old_value.as_ref().map_or(0, |v| v.len() as u64)
                })
                .sum();
            slab_account.add_pinned(batch_payload_bytes);
            deliver_events(
                core_id,
                &drained.events,
                &mut guard,
                &shared_state,
                &mut retry_queue,
                &cdc_router,
            )
            .await;
            slab_account.release_pinned(batch_payload_bytes);
            trace!(core_id, batch_count, "event batch processed");
        }

        if drained.dropped {
            let target = last_wal_lsn(&wal);
            match recovery.as_mut() {
                Some(active) => active.extend(target),
                None => {
                    recovery = Some(Recovery::new(guard.safe(), target));
                    metrics.record_wal_catchup_enter();
                    warn!(
                        core_id,
                        target = target.as_u64(),
                        "event ring dropped events; recovering them from the WAL"
                    );
                }
            }
        }

        if let Some(bound) = cursor.settled_bound() {
            // A recovery still owes records above what it replayed.
            let candidate = recovery
                .as_ref()
                .map_or(bound, |active| bound.min(active.replayed_through()));
            if guard.advance_safe(candidate) {
                dirty_watermark = true;
            }
        }

        if batch_count > 0 || replayed > 0 {
            if slab_account.is_shed() {
                info!(core_id, "slab budget shed; pinned event payloads released");
                slab_account.reset();
                slab_account.clear_shed();
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
            process_retry_queue(&mut retry_queue, &trigger_dlq, &shared_state, core_id).await;
            last_retry_poll = tokio::time::Instant::now();
        }

        maybe_flush_watermark(
            &shared_state,
            &watermark_store,
            core_id,
            guard.safe(),
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
                    flush_watermark(&shared_state, &watermark_store, core_id, guard.safe());
                }
                debug!(core_id, "event plane consumer received shutdown");
                break;
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
        crate::event::trigger::dispatcher::retry_action(&action, shared_state, retry_queue).await;
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::Ordering;
    use std::time::Duration;

    use nodedb_types::AuditDmlMode;
    use nodedb_types::sync::wire::SyncProvenance;

    use super::super::handle::{ConsumerConfig, spawn_consumer};
    use crate::control::security::audit::AuditEvent;
    use crate::control::state::SharedState;
    use crate::event::bus::create_event_bus_with_capacity;
    use crate::event::cdc::ChangeStreamDef;
    use crate::event::cdc::stream_def::{
        CompactionConfig, LateDataPolicy, OpFilter, RetentionConfig, StreamFormat,
    };
    use crate::event::streaming_mv::StreamingMvDef;
    use crate::event::streaming_mv::types::{AggDef, AggFunction};
    use crate::event::types::{EventSource, RecordPosition, RowId, WriteEvent, WriteOp};
    use crate::types::{DatabaseId, Lsn, TenantId, VShardId};
    use crate::wal::manager::NO_APPLY_KEY;

    const WRITES: u64 = 64;
    const TENANT: u64 = 1;
    const COLLECTION: &str = "orders";

    /// What every side effect saw of one run.
    #[derive(Debug, PartialEq)]
    struct Totals {
        audit_rows: usize,
        mv_count: f64,
        mv_sum: f64,
        crdt_events: u64,
    }

    fn register_sinks(shared: &SharedState) {
        shared
            .audit_dml_cache
            .set(DatabaseId::DEFAULT, AuditDmlMode::Writes);
        shared.stream_registry.register(ChangeStreamDef {
            database_id: DatabaseId::DEFAULT,
            tenant_id: TENANT,
            name: "orders_stream".into(),
            collection: COLLECTION.into(),
            op_filter: OpFilter::all(),
            format: StreamFormat::Json,
            retention: RetentionConfig {
                max_events: 10_000,
                max_age_secs: 3600,
            },
            compaction: CompactionConfig::default(),
            webhook: crate::event::webhook::WebhookConfig::default(),
            late_data: LateDataPolicy::default(),
            kafka: crate::event::kafka::KafkaDeliveryConfig::default(),
            owner: "admin".into(),
            created_at: 0,
            subscriber_roles: Vec::new(),
        });
        shared.mv_registry.register(StreamingMvDef {
            database_id: DatabaseId::DEFAULT,
            tenant_id: TENANT,
            name: "orders_totals".into(),
            source_stream: "orders_stream".into(),
            group_by_columns: Vec::new(),
            aggregates: vec![
                AggDef {
                    output_name: "cnt".into(),
                    function: AggFunction::Count,
                    input_expr: String::new(),
                },
                AggDef {
                    output_name: "total_sum".into(),
                    function: AggFunction::Sum,
                    input_expr: "total".into(),
                },
            ],
            filter_expr: None,
            owner: "admin".into(),
            created_at: 0,
        });
    }

    fn totals(shared: &SharedState) -> Totals {
        let audit_rows = shared
            .audit
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .query_by_event(&AuditEvent::DmlAudit)
            .len();
        let (mv_count, mv_sum) = shared
            .mv_registry
            .get_state(DatabaseId::DEFAULT, TENANT, "orders_totals")
            .and_then(|state| state.read_results().into_iter().next())
            .map_or((0.0, 0.0), |(_, row)| (row[0].1, row[1].1));
        let crdt_events = shared.delta_packager.deltas_skipped.load(Ordering::Relaxed)
            + shared
                .delta_packager
                .deltas_packaged
                .load(Ordering::Relaxed);
        Totals {
            audit_rows,
            mv_count,
            mv_sum,
            crdt_events,
        }
    }

    /// The document value of write `i`.
    fn value(i: u64) -> Vec<u8> {
        nodedb_types::json_to_msgpack(&serde_json::json!({ "total": i })).expect("value")
    }

    /// The ring event the Data Plane emits for write `i`, logged at `lsn`.
    fn ring_event(sequence: u64, i: u64, lsn: Lsn) -> WriteEvent {
        WriteEvent {
            sequence,
            collection: Arc::from(COLLECTION),
            op: WriteOp::Insert,
            row_id: RowId::row(nodedb_types::RowIdentity::from_user_key(format!("o-{i}"))),
            lsn,
            record: Some(RecordPosition::first(lsn)),
            database_id: DatabaseId::DEFAULT,
            tenant_id: TenantId::new(TENANT),
            vshard_id: VShardId::new(0),
            source: EventSource::User,
            new_value: Some(Arc::from(value(i).as_slice())),
            old_value: None,
            system_time_ms: None,
            valid_time_ms: None,
            user_id: None,
            statement_digest: None,
        }
    }

    /// Run `WRITES` logged writes through a consumer whose ring holds
    /// `ring_capacity` events, and return what the side effects saw, the
    /// number of ring drops, and the persisted watermark after shutdown.
    async fn run(ring_capacity: usize) -> (Totals, u64, Lsn, Lsn) {
        let dir = tempfile::tempdir().expect("tempdir");
        let (wal, watermark_store, shared, trigger_dlq, cdc_router) =
            crate::event::test_utils::event_test_deps(&dir);
        register_sinks(&shared);
        crate::event::sink_ledger::load_sink_state(&shared, &wal, &watermark_store, 1)
            .expect("load sink state");

        let (mut producers, mut consumers) = create_event_bus_with_capacity(1, ring_capacity);
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let (shutdown_bus, _) =
            crate::control::shutdown::ShutdownBus::new(Arc::clone(&shared.shutdown));
        let mut handle = spawn_consumer(ConsumerConfig {
            rx: consumers.remove(0),
            shutdown: shutdown_rx,
            shutdown_bus,
            wal: Arc::clone(&wal),
            watermark_store: Arc::clone(&watermark_store),
            shared_state: Arc::clone(&shared),
            trigger_dlq,
            cdc_router,
            num_cores: 1,
            slab_account: Arc::new(crate::event::slab_budget::ConsumerSlabAccount::new(0)),
        });

        // Every write is logged, durable and final before its event leaves,
        // as on the write path. The burst outruns a small ring, which drops events the
        // consumer then rebuilds from the WAL while the ring still carries
        // the rest: some events reach the consumer on both paths.
        // Let the idle consumer ramp to its long poll sleep, so the burst
        // below lands while it sleeps.
        tokio::time::sleep(Duration::from_millis(200)).await;
        let mut dropped = 0u64;
        let mut last_lsn = Lsn::ZERO;
        for i in 1..=WRITES {
            let window = shared.outcome_floor.open_write();
            let provenance: Option<SyncProvenance> = None;
            let payload = zerompk::to_msgpack_vec(&(
                COLLECTION,
                format!("o-{i}"),
                value(i),
                provenance,
                0u32,
            ))
            .expect("put payload");
            let lsn = wal
                .appender(NO_APPLY_KEY)
                .with_event_source(crate::event::EventSource::User)
                .append_put(
                    TenantId::new(TENANT),
                    VShardId::new(0),
                    DatabaseId::DEFAULT,
                    &payload,
                )
                .expect("append put");
            // Durable before final, as group commit orders it: a record the
            // floor calls final is readable by catch-up.
            wal.sync().expect("wal sync");
            window.note_minted(lsn);
            window.settle();
            last_lsn = lsn;
            if !producers[0].emit(ring_event(i, i, lsn)) {
                dropped += 1;
            }
        }

        let expected_crdt = WRITES;
        tokio::time::timeout(Duration::from_secs(10), async {
            while totals(&shared).crdt_events < expected_crdt {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("every write reaches the side effects");
        // A second delivery of any event would land within this window.
        tokio::time::sleep(Duration::from_millis(300)).await;
        let seen = totals(&shared);

        shutdown_tx.send(true).expect("signal consumer shutdown");
        handle.wait_for_exit().await;
        let watermark = watermark_store.load(0).expect("load watermark");
        (seen, dropped, watermark, last_lsn)
    }

    /// An event carried by both the ring and WAL catch-up reaches every side
    /// effect once, and a dropped event reaches each of them all the same.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn catch_up_mid_run_delivers_every_event_exactly_once() {
        let expected = Totals {
            audit_rows: WRITES as usize,
            mv_count: WRITES as f64,
            mv_sum: (1..=WRITES).sum::<u64>() as f64,
            crdt_events: WRITES,
        };

        let (clean, clean_drops, clean_watermark, clean_last) = run(1024).await;
        assert_eq!(clean_drops, 0, "the reference run must not drop");
        assert_eq!(clean, expected);
        assert_eq!(clean_watermark, clean_last);

        let (caught_up, drops, watermark, last) = run(4).await;
        assert!(
            drops > 0,
            "the burst must overflow the ring to force catch-up"
        );
        assert_eq!(
            caught_up, clean,
            "catch-up changed what the side effects saw"
        );
        assert_eq!(
            watermark, last,
            "the persisted watermark must cover every delivered write"
        );
    }

    /// One process of the restart test: its state, its consumer and the
    /// deltas its Lite session received.
    struct Node {
        wal: Arc<crate::wal::WalManager>,
        watermark_store: Arc<crate::event::watermark::WatermarkStore>,
        shared: Arc<SharedState>,
        producers: Vec<crate::event::bus::EventProducer>,
        handle: super::super::handle::ConsumerHandle,
        shutdown_tx: tokio::sync::watch::Sender<bool>,
        deltas: tokio::sync::mpsc::Receiver<crate::event::crdt_sync::types::OutboundDelta>,
        _control: tokio::sync::mpsc::Receiver<nodedb_types::sync::wire::SyncFrame>,
    }

    /// Open the node's state in `dir`, load the sink state, subscribe a Lite
    /// session to the collection, and start the consumer.
    fn open_node(dir: &tempfile::TempDir) -> Node {
        let (wal, watermark_store, shared, trigger_dlq, cdc_router) =
            crate::event::test_utils::event_test_deps(dir);
        register_sinks(&shared);
        crate::event::sink_ledger::load_sink_state(&shared, &wal, &watermark_store, 1)
            .expect("load sink state");
        let (deltas, control) = shared.crdt_sync_delivery.register(
            "lite-1".into(),
            1,
            TENANT,
            DatabaseId::DEFAULT,
            vec![COLLECTION.into()],
            &crate::event::crdt_sync::types::DeliveryConfig::default(),
        );
        let (producers, mut consumers) = create_event_bus_with_capacity(1, 1024);
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let (shutdown_bus, _) =
            crate::control::shutdown::ShutdownBus::new(Arc::clone(&shared.shutdown));
        let handle = spawn_consumer(ConsumerConfig {
            rx: consumers.remove(0),
            shutdown: shutdown_rx,
            shutdown_bus,
            wal: Arc::clone(&wal),
            watermark_store: Arc::clone(&watermark_store),
            shared_state: Arc::clone(&shared),
            trigger_dlq,
            cdc_router,
            num_cores: 1,
            slab_account: Arc::new(crate::event::slab_budget::ConsumerSlabAccount::new(0)),
        });
        Node {
            wal,
            watermark_store,
            shared,
            producers,
            handle,
            shutdown_tx,
            deltas,
            _control: control,
        }
    }

    /// Log and emit writes `range`, as the write path does.
    fn write(node: &mut Node, range: std::ops::RangeInclusive<u64>) {
        for i in range {
            let window = node.shared.outcome_floor.open_write();
            let provenance: Option<SyncProvenance> = None;
            let payload = zerompk::to_msgpack_vec(&(
                COLLECTION,
                format!("o-{i}"),
                value(i),
                provenance,
                0u32,
            ))
            .expect("put payload");
            let lsn = node
                .wal
                .appender(NO_APPLY_KEY)
                .with_event_source(crate::event::EventSource::User)
                .append_put(
                    TenantId::new(TENANT),
                    VShardId::new(0),
                    DatabaseId::DEFAULT,
                    &payload,
                )
                .expect("append put");
            node.wal.sync().expect("wal sync");
            window.note_minted(lsn);
            window.settle();
            node.producers[0].emit(ring_event(i, i, lsn));
        }
    }

    async fn wait_for_mv_count(shared: &SharedState, count: u64) {
        tokio::time::timeout(Duration::from_secs(10), async {
            while totals(shared).mv_count < count as f64 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("the views reach the expected count");
    }

    /// DML audit rows in the durable audit log.
    fn durable_audit_rows(wal: &crate::wal::WalManager) -> usize {
        wal.recover_audit_entries()
            .expect("recover audit entries")
            .iter()
            .filter(|(_, bytes)| {
                zerompk::from_msgpack::<crate::control::security::audit::AuditEntry>(bytes)
                    .is_ok_and(|entry| entry.event == AuditEvent::DmlAudit)
            })
            .count()
    }

    /// The row of every event the change stream retains, in stream order.
    fn stream_rows(shared: &SharedState) -> Vec<String> {
        let buffer = shared
            .cdc_router
            .get_buffer(DatabaseId::DEFAULT, TENANT, "orders_stream")
            .expect("the change stream buffer");
        let mut rows: Vec<(u64, String)> = buffer
            .snapshot()
            .iter()
            .map(|event| {
                let index = event
                    .row_id
                    .trim_start_matches("o-")
                    .parse::<u64>()
                    .expect("row index");
                (index, event.row_id.clone())
            })
            .collect();
        rows.sort();
        rows.into_iter().map(|(_, row)| row).collect()
    }

    fn sequences(
        deltas: &mut tokio::sync::mpsc::Receiver<crate::event::crdt_sync::types::OutboundDelta>,
    ) -> Vec<u64> {
        let mut out = Vec::new();
        while let Ok(delta) = deltas.try_recv() {
            out.push(delta.sequence);
        }
        out
    }

    /// A process that delivered every event but died before its watermark
    /// persisted replays them all after a restart. Each sink applies each
    /// event once: the views and the change streams restore the state and
    /// keys they persisted, the audit log and the CRDT ledger remember what
    /// they recorded.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_restart_before_the_watermark_persists_applies_every_event_once() {
        const HALF: u64 = WRITES / 2;
        let dir = tempfile::tempdir().expect("tempdir");

        // First process: deliver everything, persist the views halfway, then
        // die without persisting the watermark.
        let mut node = open_node(&dir);
        write(&mut node, 1..=HALF);
        wait_for_mv_count(&node.shared, HALF).await;
        node.shared
            .mv_persistence
            .flush_all(&node.shared.mv_registry)
            .expect("persist the views");
        node.shared
            .sink_ledgers
            .get()
            .expect("sink ledgers")
            .cdc
            .flush(&node.shared.cdc_router)
            .expect("persist the change streams");
        write(&mut node, HALF + 1..=WRITES);
        wait_for_mv_count(&node.shared, WRITES).await;
        tokio::time::sleep(Duration::from_millis(200)).await;
        node.handle.abort_and_join().await;
        assert_eq!(
            node.watermark_store.load(0).expect("load watermark"),
            Lsn::ZERO,
            "the first process must die before its watermark persists"
        );
        // The process dies: its background tasks stop and release the state
        // they hold open, as they do when a real process exits. The array GC
        // task holds the array-sync op log until shutdown is signalled.
        node.shared.shutdown.signal();
        tokio::time::timeout(Duration::from_secs(10), async {
            while Arc::strong_count(&node.shared.array_sync_op_log) > 1 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("the first process's background tasks exit on shutdown");
        let mut delivered = sequences(&mut node.deltas);
        let shared = node.shared;
        let wal = node.wal;
        let watermark_store = node.watermark_store;
        assert_eq!(
            Arc::strong_count(&shared),
            1,
            "the first process still runs"
        );
        drop((wal, watermark_store, shared));

        // Second process: the consumer replays the whole WAL above the
        // watermark it never persisted.
        let mut node = open_node(&dir);
        wait_for_mv_count(&node.shared, WRITES).await;
        tokio::time::sleep(Duration::from_millis(300)).await;
        let seen = totals(&node.shared);
        assert_eq!(
            seen.mv_count, WRITES as f64,
            "a view counted an event twice"
        );
        assert_eq!(seen.mv_sum, (1..=WRITES).sum::<u64>() as f64);
        assert_eq!(
            durable_audit_rows(&node.wal),
            WRITES as usize,
            "an event was audited twice or not at all"
        );
        assert_eq!(
            stream_rows(&node.shared),
            (1..=WRITES).map(|i| format!("o-{i}")).collect::<Vec<_>>(),
            "the change stream must hold every event once across the restart"
        );
        delivered.extend(sequences(&mut node.deltas));
        assert_eq!(
            delivered,
            (1..=WRITES).collect::<Vec<u64>>(),
            "every event must be packaged once, with sequences continuing across the restart"
        );

        node.shutdown_tx
            .send(true)
            .expect("signal consumer shutdown");
        node.handle.wait_for_exit().await;
    }
}
