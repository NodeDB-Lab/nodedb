//! Event Plane: top-level lifecycle struct.
//!
//! The Event Plane is the third architectural layer — purpose-built for
//! event-driven, asynchronous, reliable delivery of internal database events.
//! It is `Send + Sync`, runs on Tokio, and NEVER does storage I/O directly.
//!
//! This struct owns all per-core consumer tasks and provides spawn/shutdown
//! lifecycle management.

use std::sync::Arc;
use std::sync::atomic::Ordering;

use tokio::sync::watch;
use tracing::{debug, info};

use super::bus::EventConsumerRx;
use super::consumer::{ConsumerHandle, ConsumerMetrics, spawn_consumer};

/// Top-level Event Plane handle.
///
/// Created during server startup. Owns per-core consumer tasks and
/// provides aggregate metrics for observability.
pub struct EventPlane {
    /// One consumer handle per Data Plane core.
    consumers: Vec<ConsumerHandle>,
    /// Shutdown signal sender.
    shutdown_tx: watch::Sender<bool>,
}

impl EventPlane {
    /// Spawn the Event Plane: one consumer Tokio task per Data Plane core.
    ///
    /// `consumers_rx` must have exactly one entry per core, in core-ID order.
    pub fn spawn(consumers_rx: Vec<EventConsumerRx>) -> Self {
        let num_cores = consumers_rx.len();
        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        let consumers: Vec<ConsumerHandle> = consumers_rx
            .into_iter()
            .map(|rx| spawn_consumer(rx, shutdown_rx.clone()))
            .collect();

        info!(num_cores, "event plane started");

        Self {
            consumers,
            shutdown_tx,
        }
    }

    /// Signal all consumer tasks to shut down gracefully.
    pub fn shutdown(&self) {
        debug!("event plane shutdown requested");
        let _ = self.shutdown_tx.send(true);
    }

    /// Number of consumer tasks (one per core).
    pub fn num_consumers(&self) -> usize {
        self.consumers.len()
    }

    /// Total events processed across all consumers.
    pub fn total_events_processed(&self) -> u64 {
        self.consumers.iter().map(|c| c.events_processed()).sum()
    }

    /// Per-core metrics snapshot.
    pub fn consumer_metrics(&self) -> Vec<(usize, &Arc<ConsumerMetrics>)> {
        self.consumers
            .iter()
            .map(|c| (c.core_id, &c.metrics))
            .collect()
    }

    /// Aggregate events dropped across all consumers.
    pub fn total_events_dropped(&self) -> u64 {
        self.consumers
            .iter()
            .map(|c| c.metrics.events_dropped.load(Ordering::Relaxed))
            .sum()
    }
}

impl Drop for EventPlane {
    fn drop(&mut self) {
        // Signal shutdown and abort all consumer tasks.
        let _ = self.shutdown_tx.send(true);
        for consumer in &self.consumers {
            consumer.abort();
        }
        debug!("event plane dropped, all consumers aborted");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::bus::create_event_bus_with_capacity;
    use crate::event::types::{EventSource, RowId, WriteEvent, WriteOp};
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
            new_value: Some(Arc::from(b"payload".as_slice())),
            old_value: None,
        }
    }

    #[tokio::test]
    async fn event_plane_lifecycle() {
        let (mut producers, consumers) = create_event_bus_with_capacity(2, 64);
        let plane = EventPlane::spawn(consumers);
        assert_eq!(plane.num_consumers(), 2);

        // Emit events on both cores.
        for i in 1..=5 {
            producers[0].emit(make_event(i));
            producers[1].emit(make_event(i));
        }

        // Let consumers process.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        assert_eq!(plane.total_events_processed(), 10);
        assert_eq!(plane.total_events_dropped(), 0);

        // Shutdown.
        plane.shutdown();
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }

    #[tokio::test]
    async fn drop_triggers_shutdown() {
        let (_producers, consumers) = create_event_bus_with_capacity(1, 16);
        let plane = EventPlane::spawn(consumers);
        drop(plane); // Should not panic.
    }
}
