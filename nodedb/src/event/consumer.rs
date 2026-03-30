//! Event Plane consumer: one Tokio task per Data Plane core ring buffer.
//!
//! Each consumer polls its ring buffer for new [`WriteEvent`]s and dispatches
//! them to registered handlers (triggers, CDC, CRDT sync, etc.). In this
//! foundational batch, the consumer logs and counts events — handler dispatch
//! points are added in subsequent batches.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use tokio::sync::watch;
use tracing::{debug, trace};

use super::bus::EventConsumerRx;
use super::types::WriteEvent;

/// Metrics for a single consumer (one per core).
#[derive(Debug)]
pub struct ConsumerMetrics {
    /// Total events processed by this consumer.
    pub events_processed: AtomicU64,
    /// Total events dropped (buffer overflow detected via sequence gaps).
    pub events_dropped: AtomicU64,
    /// Last processed sequence number per this consumer.
    pub last_sequence: AtomicU64,
}

impl ConsumerMetrics {
    fn new() -> Self {
        Self {
            events_processed: AtomicU64::new(0),
            events_dropped: AtomicU64::new(0),
            last_sequence: AtomicU64::new(0),
        }
    }
}

/// Handle to a running consumer task. Holds metrics and the join handle.
pub struct ConsumerHandle {
    pub core_id: usize,
    pub metrics: Arc<ConsumerMetrics>,
    join_handle: tokio::task::JoinHandle<()>,
}

impl ConsumerHandle {
    /// Abort the consumer task (used during shutdown).
    pub fn abort(&self) {
        self.join_handle.abort();
    }

    pub fn events_processed(&self) -> u64 {
        self.metrics.events_processed.load(Ordering::Relaxed)
    }
}

/// Spawn a consumer Tokio task for one Data Plane core's event ring buffer.
///
/// The task polls the ring buffer in a loop, yielding to the Tokio scheduler
/// when no events are available. Uses a short sleep on empty poll to avoid
/// busy-spinning — this is acceptable because event processing latency
/// requirements are milliseconds (not microseconds like the SPSC bridge).
pub fn spawn_consumer(rx: EventConsumerRx, shutdown: watch::Receiver<bool>) -> ConsumerHandle {
    let core_id = rx.core_id();
    let metrics = Arc::new(ConsumerMetrics::new());
    let metrics_clone = Arc::clone(&metrics);
    let running = Arc::new(AtomicBool::new(true));
    let running_clone = Arc::clone(&running);

    let join_handle = tokio::spawn(async move {
        consumer_loop(rx, metrics_clone, shutdown, running_clone).await;
    });

    ConsumerHandle {
        core_id,
        metrics,
        join_handle,
    }
}

/// The main consumer loop. Runs until shutdown is signaled.
async fn consumer_loop(
    mut rx: EventConsumerRx,
    metrics: Arc<ConsumerMetrics>,
    mut shutdown: watch::Receiver<bool>,
    running: Arc<AtomicBool>,
) {
    let core_id = rx.core_id();
    debug!(core_id, "event plane consumer started");

    loop {
        // Check shutdown signal.
        if *shutdown.borrow() || !running.load(Ordering::Relaxed) {
            debug!(core_id, "event plane consumer shutting down");
            break;
        }

        // Drain all available events in a tight loop.
        let mut batch_count = 0u32;
        while let Some(event) = rx.try_recv() {
            process_event(core_id, &event, &metrics);
            batch_count += 1;

            // Yield periodically to avoid starving other Tokio tasks
            // under sustained high event rate.
            if batch_count.is_multiple_of(1024) {
                tokio::task::yield_now().await;
            }
        }

        if batch_count > 0 {
            trace!(core_id, batch_count, "event batch processed");
            // Immediately try again — more events may have arrived.
            tokio::task::yield_now().await;
            continue;
        }

        // No events available — sleep briefly before re-polling.
        // 1ms is acceptable: event processing latency target is ~1-5ms.
        tokio::select! {
            _ = tokio::time::sleep(std::time::Duration::from_millis(1)) => {}
            _ = shutdown.changed() => {
                debug!(core_id, "event plane consumer received shutdown");
                break;
            }
        }
    }

    let total = metrics.events_processed.load(Ordering::Relaxed);
    debug!(
        core_id,
        total_processed = total,
        "event plane consumer stopped"
    );
}

/// Process a single event. This is the dispatch point where future batches
/// will add trigger matching, CDC routing, CRDT delta packaging, etc.
fn process_event(core_id: usize, event: &WriteEvent, metrics: &ConsumerMetrics) {
    // Sequence gap detection: if the new sequence is more than 1 ahead of
    // the last processed sequence, events were dropped (buffer overflow).
    let last = metrics.last_sequence.load(Ordering::Relaxed);
    if last > 0 && event.sequence > last + 1 {
        let gap = event.sequence - last - 1;
        metrics.events_dropped.fetch_add(gap, Ordering::Relaxed);
        tracing::warn!(
            core_id,
            gap,
            last_seq = last,
            new_seq = event.sequence,
            "event sequence gap detected — {} events dropped (WAL replay needed)",
            gap
        );
    }
    metrics
        .last_sequence
        .store(event.sequence, Ordering::Relaxed);
    metrics.events_processed.fetch_add(1, Ordering::Relaxed);

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::bus::create_event_bus_with_capacity;
    use crate::event::types::{EventSource, RowId, WriteOp};
    use crate::types::{Lsn, TenantId, VShardId};

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
        }
    }

    #[test]
    fn consumer_metrics_gap_detection() {
        let metrics = ConsumerMetrics::new();
        let event1 = make_event(1);
        let event5 = make_event(5);

        process_event(0, &event1, &metrics);
        assert_eq!(metrics.events_processed.load(Ordering::Relaxed), 1);
        assert_eq!(metrics.last_sequence.load(Ordering::Relaxed), 1);

        // Gap: 2, 3, 4 missing.
        process_event(0, &event5, &metrics);
        assert_eq!(metrics.events_processed.load(Ordering::Relaxed), 2);
        assert_eq!(metrics.events_dropped.load(Ordering::Relaxed), 3);
        assert_eq!(metrics.last_sequence.load(Ordering::Relaxed), 5);
    }

    #[tokio::test]
    async fn consumer_processes_events() {
        let (mut producers, consumers) = create_event_bus_with_capacity(1, 64);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        // Emit some events.
        for i in 1..=10 {
            producers[0].emit(make_event(i));
        }

        // Spawn consumer.
        let handle = spawn_consumer(consumers.into_iter().next().unwrap(), shutdown_rx);

        // Give consumer time to process.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        assert_eq!(handle.events_processed(), 10);

        // Shutdown.
        shutdown_tx.send(true).ok();
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
}
