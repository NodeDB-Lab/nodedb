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

use nodedb_bridge::backpressure::{BackpressureConfig, BackpressureController, PressureState};
use nodedb_bridge::buffer::{Consumer, Producer, RingBuffer};

use super::types::WriteEvent;

/// Default ring buffer capacity per core (must be power of two).
const DEFAULT_EVENT_BUS_CAPACITY: usize = 65_536;

/// The producer half given to a Data Plane core.
/// `Send` at the trait level, but logically pinned to a single core —
/// the thread-per-core architecture ensures only one thread writes to it.
pub struct EventProducer {
    inner: Producer<WriteEvent>,
    core_id: usize,
    backpressure: Arc<BackpressureController>,
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
            Err(_) => {
                tracing::warn!(
                    core = self.core_id,
                    utilization = util,
                    "event bus full — event dropped (WAL-backed, will replay on gap)"
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
    use crate::types::{Lsn, TenantId, VShardId};
    use std::sync::Arc;

    fn make_event(seq: u64) -> WriteEvent {
        WriteEvent {
            sequence: seq,
            collection: Arc::from("test"),
            op: WriteOp::Insert,
            row_id: RowId::new("row-1"),
            lsn: Lsn::new(seq),
            tenant_id: TenantId::new(1),
            vshard_id: VShardId::new(0),
            source: EventSource::User,
            new_value: Some(Arc::from(b"data".as_slice())),
            old_value: None,
            system_time_ms: None,
            valid_time_ms: None,
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
}
