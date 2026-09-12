// SPDX-License-Identifier: BUSL-1.1

//! Event bus: per-core ring buffers from Data Plane to Event Plane.
//!
//! Same design pattern as the SPSC bridge (Control → Data), but in the
//! opposite direction for events. One ring buffer per Data Plane core —
//! no cross-core contention.
//!
//! ```text
//! Data Plane Core 0 ──► [Bounded Ring Buffer 0] ──► Event Plane Consumer 0
//! Data Plane Core 1 ──► [Bounded Ring Buffer 1] ──► Event Plane Consumer 1
//! ...
//! Data Plane Core N ──► [Bounded Ring Buffer N] ──► Event Plane Consumer N
//! ```

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use nodedb_bridge::backpressure::{BackpressureConfig, BackpressureController, PressureState};
use nodedb_bridge::buffer::{Consumer, Producer, RingBuffer};
use nodedb_bridge::error::BridgeError;

use super::types::WriteEvent;

/// Default ring buffer capacity per core (must be power of two).
const DEFAULT_EVENT_BUS_CAPACITY: usize = 65_536;

/// The producer half given to a Data Plane core.
///
/// `Send` at the trait level, but **logically owned by exactly one
/// thread once delivered to its core**. The `Send` bound is required
/// during setup: `create_event_bus` returns a `Vec<EventProducer>` that
/// the runtime distributes to per-core worker threads via `tokio::spawn`
/// / `LocalSet::spawn_local`, and the distribute step is the only
/// cross-thread move ever performed. After that move the producer never
/// leaves its core.
///
/// The SPSC invariant (single writer at any time) is enforced statically
/// by `&mut self` on [`Self::emit`]: there can be at most one mutable
/// borrow, and that borrow lives on the owning thread. A future refactor
/// that shares the producer across threads would have to either clone it
/// (no `Clone` impl exists) or wrap it in a lock (deliberately not
/// provided) — both surface in code review.
pub struct EventProducer {
    inner: Producer<WriteEvent>,
    core_id: usize,
    backpressure: Arc<BackpressureController>,
    /// Latched once the consumer half is dropped, so we log the
    /// disconnect exactly once per producer instead of spamming
    /// a warning for every dropped event.
    disconnect_logged: AtomicBool,
}

impl EventProducer {
    /// Try to emit a write event. Returns `true` if enqueued, `false` if dropped.
    ///
    /// The Data Plane NEVER blocks waiting for the Event Plane to process —
    /// fire-and-forget into the ring buffer. Dropped events are WAL-backed:
    /// the Event Plane detects gaps via sequence numbers and replays from WAL.
    ///
    /// Updates backpressure state after each emit. When Suspended (>95%),
    /// events are dropped more aggressively (the Event Plane will enter
    /// WAL Catchup Mode to recover).
    pub fn emit(&mut self, event: WriteEvent) -> bool {
        let util = self.inner.utilization();

        // Update backpressure state.
        if let Some(new_state) = self.backpressure.update(util) {
            match new_state {
                PressureState::Throttled => {
                    tracing::info!(
                        core = self.core_id,
                        utilization = util,
                        "event bus backpressure: THROTTLED (>85%)"
                    );
                }
                PressureState::Suspended => {
                    tracing::warn!(
                        core = self.core_id,
                        utilization = util,
                        "event bus backpressure: SUSPENDED (>95%) — events will be dropped, WAL catchup needed"
                    );
                }
                PressureState::Normal => {
                    tracing::info!(
                        core = self.core_id,
                        utilization = util,
                        "event bus backpressure: NORMAL"
                    );
                }
            }
        }

        match self.inner.try_push(event) {
            Ok(()) => true,
            Err(BridgeError::Full { .. }) => {
                tracing::warn!(
                    core = self.core_id,
                    utilization = util,
                    "event bus full — event dropped (WAL-backed, will replay on gap)"
                );
                false
            }
            Err(BridgeError::Disconnected { .. }) => {
                // Consumer dropped — Event Plane is gone (shutdown or lifecycle bug).
                // Log once per producer; further events would just be noise.
                if !self.disconnect_logged.swap(true, Ordering::Relaxed) {
                    tracing::warn!(
                        core = self.core_id,
                        "event bus consumer disconnected — Event Plane is not running; \
                         events will be silently dropped on this core until restart"
                    );
                }
                false
            }
            Err(e) => {
                tracing::warn!(
                    core = self.core_id,
                    error = %e,
                    "event bus push failed — event dropped"
                );
                false
            }
        }
    }

    pub fn core_id(&self) -> usize {
        self.core_id
    }

    /// Current ring buffer utilization as a percentage (0–100).
    pub fn utilization(&self) -> u8 {
        self.inner.utilization()
    }

    /// Current backpressure state.
    pub fn pressure_state(&self) -> PressureState {
        self.backpressure.state()
    }

    /// Whether the producer has observed the consumer half being dropped.
    ///
    /// Latched on the first disconnect detection — once `true`, stays `true`
    /// for the producer's lifetime. A `true` reading means the Event Plane
    /// is gone (lifecycle bug or shutdown) and every subsequent `emit` will
    /// be silently dropped at the bus layer; the WAL replay path is the
    /// only recovery.
    pub fn is_consumer_disconnected(&self) -> bool {
        self.disconnect_logged.load(Ordering::Relaxed)
    }
}

/// The consumer half given to an Event Plane consumer task.
pub struct EventConsumerRx {
    inner: Consumer<WriteEvent>,
    core_id: usize,
    backpressure: Arc<BackpressureController>,
}

impl EventConsumerRx {
    /// Try to dequeue the next event. Returns `None` if the buffer is empty.
    pub fn try_recv(&mut self) -> Option<WriteEvent> {
        self.inner.try_pop().ok()
    }

    pub fn core_id(&self) -> usize {
        self.core_id
    }

    /// Current backpressure state (read from the shared controller).
    pub fn pressure_state(&self) -> PressureState {
        self.backpressure.state()
    }
}

/// Creates the event bus: one ring buffer pair per Data Plane core.
///
/// Returns `(producers, consumers)` — producers go to Data Plane cores,
/// consumers go to Event Plane Tokio tasks.
pub fn create_event_bus(num_cores: usize) -> (Vec<EventProducer>, Vec<EventConsumerRx>) {
    create_event_bus_with_capacity(num_cores, DEFAULT_EVENT_BUS_CAPACITY)
}

/// Creates the event bus with a custom ring buffer capacity.
pub fn create_event_bus_with_capacity(
    num_cores: usize,
    capacity: usize,
) -> (Vec<EventProducer>, Vec<EventConsumerRx>) {
    let mut producers = Vec::with_capacity(num_cores);
    let mut consumers = Vec::with_capacity(num_cores);

    for core_id in 0..num_cores {
        let (producer, consumer) = RingBuffer::channel::<WriteEvent>(capacity);
        let backpressure = Arc::new(BackpressureController::new(BackpressureConfig::default()));

        producers.push(EventProducer {
            inner: producer,
            core_id,
            backpressure: Arc::clone(&backpressure),
            disconnect_logged: AtomicBool::new(false),
        });

        consumers.push(EventConsumerRx {
            inner: consumer,
            core_id,
            backpressure,
        });
    }

    (producers, consumers)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::types::{EventSource, RowId, WriteOp};
    use crate::types::{DatabaseId, Lsn, TenantId, VShardId};
    use std::sync::Arc;

    fn make_event(seq: u64) -> WriteEvent {
        WriteEvent {
            sequence: seq,
            collection: Arc::from("test"),
            op: WriteOp::Insert,
            row_id: RowId::row(nodedb_types::RowIdentity::from_user_key("row-1")),
            lsn: Lsn::new(seq),
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
    fn single_core_roundtrip() {
        let (mut producers, mut consumers) = create_event_bus_with_capacity(1, 16);
        let producer = &mut producers[0];
        let consumer = &mut consumers[0];

        assert!(producer.emit(make_event(1)));
        assert!(producer.emit(make_event(2)));

        let e1 = consumer.try_recv().expect("should have event");
        assert_eq!(e1.sequence, 1);

        let e2 = consumer.try_recv().expect("should have event");
        assert_eq!(e2.sequence, 2);

        assert!(consumer.try_recv().is_none());
    }

    #[test]
    fn multi_core_isolation() {
        let (mut producers, mut consumers) = create_event_bus_with_capacity(4, 16);

        // Each core emits to its own buffer.
        for (i, p) in producers.iter_mut().enumerate() {
            assert!(p.emit(make_event(i as u64)));
        }

        // Each consumer sees only its core's event.
        for (i, c) in consumers.iter_mut().enumerate() {
            let event = c.try_recv().expect("should have event");
            assert_eq!(event.sequence, i as u64);
            assert!(c.try_recv().is_none());
        }
    }

    #[test]
    fn full_buffer_drops_event() {
        // Capacity 4 → rounded to 4 (already power of two).
        let (mut producers, _consumers) = create_event_bus_with_capacity(1, 4);
        let producer = &mut producers[0];

        // Fill the buffer.
        for i in 0..4 {
            assert!(producer.emit(make_event(i)));
        }

        // Next emit should fail (buffer full).
        assert!(!producer.emit(make_event(99)));
    }

    #[test]
    fn core_id_propagated() {
        let (producers, consumers) = create_event_bus(2);
        assert_eq!(producers[0].core_id(), 0);
        assert_eq!(producers[1].core_id(), 1);
        assert_eq!(consumers[0].core_id(), 0);
        assert_eq!(consumers[1].core_id(), 1);
    }

    // ─────────────────────────────────────────────────────────────────────
    // Consumer-disconnect classification.
    //
    // When the consumer half is dropped (e.g. the EventPlane handle that
    // owns the consumer tasks is itself dropped), the producer must
    // distinguish the resulting failure from a "buffer full" failure.
    // Conflating the two — by way of an `Err(_)` catch-all on `try_push` —
    // surfaces as a misleading "event bus full, utilization=0" warning at
    // boot and silently loses every subsequent emit on that core. The
    // post-fix code latches a single clear "consumer disconnected" warning
    // and exposes the latched state for runtime inspection.
    // ─────────────────────────────────────────────────────────────────────

    #[test]
    fn emit_after_consumer_drop_is_classified_as_disconnect_not_full() {
        let (mut producers, consumers) = create_event_bus_with_capacity(1, 16);
        let producer = &mut producers[0];

        // Sanity check: a fresh bus is not in disconnected state.
        assert!(
            !producer.is_consumer_disconnected(),
            "fresh producer must not report disconnect before consumer drop"
        );

        // Simulate the lifecycle bug: the EventPlane handle (which owns
        // the consumer side) is dropped while producers are still live.
        drop(consumers);

        // The first emit detects the disconnect.
        assert!(
            !producer.emit(make_event(1)),
            "emit must report failure when consumer is gone"
        );
        assert!(
            producer.is_consumer_disconnected(),
            "producer must classify a dropped-consumer failure as disconnect, \
             not as a generic push failure or a full buffer"
        );

        // Regression guard against the specific failure mode: utilization
        // must read 0 (the buffer is empty — the consumer was never there
        // to drain it). A non-zero utilization paired with a failing emit
        // would mean the producer is misclassifying disconnect as Full.
        assert_eq!(
            producer.utilization(),
            0,
            "utilization must be 0 after consumer drop; a non-zero reading \
             means the producer is mistaking disconnect for buffer-full"
        );
    }

    #[test]
    fn emit_after_consumer_drop_continues_to_fail_silently() {
        // Once the consumer is gone, every subsequent emit on that core
        // must return false. The producer must not silently start
        // succeeding (e.g. by re-creating an internal queue) — recovery
        // requires a full restart and WAL replay on the Event Plane side.
        let (mut producers, consumers) = create_event_bus_with_capacity(1, 16);
        let producer = &mut producers[0];
        drop(consumers);

        for seq in 1..=10 {
            assert!(
                !producer.emit(make_event(seq)),
                "emit #{seq} after consumer drop must fail"
            );
        }
        assert!(producer.is_consumer_disconnected());
    }

    #[test]
    fn full_buffer_does_not_set_disconnected_flag() {
        // Regression guard against the inverse misclassification: a
        // legitimately-full buffer (consumer is alive but slow) must NOT
        // be reported as a consumer disconnect. Conflating the two would
        // make every backpressure event look like a fatal lifecycle bug
        // and trigger the latched "disconnected" log + metric.
        let (mut producers, _consumers) = create_event_bus_with_capacity(1, 4);
        let producer = &mut producers[0];

        // Fill the buffer (consumer is held by `_consumers` and never drains).
        for i in 0..4 {
            assert!(producer.emit(make_event(i)));
        }
        // Next push fails because buffer is full, not because consumer is gone.
        assert!(!producer.emit(make_event(99)));
        assert!(
            !producer.is_consumer_disconnected(),
            "a full buffer must not be classified as a consumer disconnect; \
             the consumer half is alive and will eventually drain"
        );
    }

    #[test]
    fn consumer_drop_is_isolated_to_its_own_core() {
        // The bus is per-core. Dropping one core's consumer must not
        // affect any other core's producer. This protects against a
        // refactor that shares disconnect state across cores (e.g. via a
        // single global `AtomicBool`), which would surface as one slow
        // consumer poisoning every core's bus.
        let (mut producers, mut consumers) = create_event_bus_with_capacity(3, 16);

        // Drop only core 0's consumer. Cores 1 and 2 stay alive.
        let core0_consumer = consumers.remove(0);
        drop(core0_consumer);

        // Core 0 producer detects disconnect.
        assert!(!producers[0].emit(make_event(1)));
        assert!(producers[0].is_consumer_disconnected());

        // Core 1 and core 2 producers continue to operate normally.
        assert!(producers[1].emit(make_event(1)));
        assert!(!producers[1].is_consumer_disconnected());
        assert!(producers[2].emit(make_event(1)));
        assert!(!producers[2].is_consumer_disconnected());

        // Verify the events landed in the correct (still-alive) consumers.
        // (consumers[0] in the original index 1 is now consumers[0] after remove.)
        assert_eq!(
            consumers[0]
                .try_recv()
                .expect("core 1 should have event")
                .sequence,
            1
        );
        assert_eq!(
            consumers[1]
                .try_recv()
                .expect("core 2 should have event")
                .sequence,
            1
        );
    }
}
