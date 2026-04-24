//! Event Plane: top-level lifecycle struct.
//!
//! The Event Plane is the third architectural layer — purpose-built for
//! event-driven, asynchronous, reliable delivery of internal database events.
//! It is `Send + Sync`, runs on Tokio, and NEVER does storage I/O directly.
//!
//! On startup, each consumer loads its persisted watermark and replays WAL
//! entries from that LSN forward to reconstruct any missed events.

use std::sync::Arc;
use std::sync::atomic::Ordering;

use tracing::{debug, info};

use super::bus::EventConsumerRx;
use super::cdc::CdcRouter;
use super::consumer::{ConsumerConfig, ConsumerHandle, spawn_consumer};
use super::metrics::{AggregateMetrics, CoreMetrics};
use super::trigger::dlq::TriggerDlq;
use super::watermark::WatermarkStore;
use crate::control::shutdown::ShutdownWatch;
use crate::control::state::SharedState;
use crate::wal::WalManager;

/// Top-level Event Plane handle.
///
/// Created during server startup. Owns per-core consumer tasks,
/// the watermark store, and provides aggregate metrics.
///
/// The Event Plane subscribes to the node-wide [`ShutdownWatch`] held on
/// `SharedState` instead of creating its own private `watch::channel`.
/// This ensures all subsystems drain through the unified shutdown bus.
pub struct EventPlane {
    consumers: Vec<ConsumerHandle>,
    watermark_store: Arc<WatermarkStore>,
}

impl EventPlane {
    /// Spawn the Event Plane: one consumer Tokio task per Data Plane core.
    ///
    /// On startup, each consumer loads its persisted watermark and replays
    /// WAL entries from that point forward. `consumers_rx` must have exactly
    /// one entry per core, in core-ID order.
    ///
    /// `shutdown` is the node-wide [`ShutdownWatch`] from `SharedState`.
    /// All Event Plane subsystems subscribe to this watch instead of a
    /// private channel, so the unified shutdown bus controls all drain
    /// signalling.
    pub fn spawn(
        consumers_rx: Vec<EventConsumerRx>,
        wal: Arc<WalManager>,
        watermark_store: Arc<WatermarkStore>,
        shared_state: Arc<SharedState>,
        trigger_dlq: Arc<std::sync::Mutex<TriggerDlq>>,
        cdc_router: Arc<CdcRouter>,
        shutdown: Arc<ShutdownWatch>,
    ) -> Self {
        let num_cores = consumers_rx.len();

        let slab_budget = Arc::new(super::slab_budget::SlabBudget::for_cores(num_cores));
        let mut slab_accounts: Vec<Arc<super::slab_budget::ConsumerSlabAccount>> = Vec::new();

        let consumers: Vec<ConsumerHandle> = consumers_rx
            .into_iter()
            .enumerate()
            .map(|(i, rx)| {
                let account = Arc::new(super::slab_budget::ConsumerSlabAccount::new(i));
                slab_accounts.push(Arc::clone(&account));
                spawn_consumer(ConsumerConfig {
                    rx,
                    shutdown: shutdown.raw_receiver(),
                    wal: Arc::clone(&wal),
                    watermark_store: Arc::clone(&watermark_store),
                    shared_state: Arc::clone(&shared_state),
                    trigger_dlq: Arc::clone(&trigger_dlq),
                    cdc_router: Arc::clone(&cdc_router),
                    num_cores,
                    slab_account: account,
                })
            })
            .collect();

        // Spawn periodic slab budget enforcement (every 5s).
        {
            let budget = Arc::clone(&slab_budget);
            let accounts = slab_accounts.clone();
            let mut shutdown_rx = shutdown.raw_receiver();
            tokio::spawn(async move {
                loop {
                    tokio::select! {
                        _ = tokio::time::sleep(std::time::Duration::from_secs(5)) => {
                            let refs: Vec<&super::slab_budget::ConsumerSlabAccount> =
                                accounts.iter().map(|a| a.as_ref()).collect();
                            budget.check_and_shed(&refs);
                        }
                        _ = shutdown_rx.changed() => {
                            if *shutdown_rx.borrow() { return; }
                        }
                    }
                }
            });
        }

        // Spawn the cron scheduler loop on the Event Plane.
        let _scheduler_handle = super::scheduler::executor::spawn_scheduler(
            Arc::clone(&shared_state),
            Arc::clone(&shared_state.schedule_registry),
            Arc::clone(&shared_state.job_history),
            shutdown.raw_receiver(),
        );

        // Spawn the retention policy enforcement loop.
        let _retention_handle =
            crate::engine::timeseries::retention_policy::enforcement::spawn_enforcement_loop(
                Arc::clone(&shared_state),
                Arc::clone(&shared_state.retention_policy_registry),
                shutdown.raw_receiver(),
            );

        // Spawn the bitemporal audit-retention enforcement loop.
        // Tick interval comes from server tuning config so operators
        // control cadence declaratively; no code change needed to adjust.
        let _bitemporal_retention_handle =
            crate::engine::bitemporal::spawn_bitemporal_retention_loop(
                Arc::clone(&shared_state),
                Arc::clone(&shared_state.bitemporal_retention_registry),
                shutdown.raw_receiver(),
                shared_state.tuning.bitemporal_retention_tick(),
            );

        // Spawn the alert evaluation loop.
        let _alert_handle = super::alert::executor::spawn_alert_eval_loop(
            Arc::clone(&shared_state),
            Arc::clone(&shared_state.alert_registry),
            shutdown.raw_receiver(),
        );

        // Spawn the CDC log compaction background task.
        let _compaction_handle = super::cdc::compaction::spawn_compaction_task(
            Arc::clone(&shared_state.stream_registry),
            Arc::clone(&cdc_router),
            shutdown.raw_receiver(),
        );

        // Restore streaming MV state from redb (from last shutdown).
        shared_state
            .mv_persistence
            .restore_all(&shared_state.mv_registry);

        // Spawn MV state persistence task (flush to redb every 30s).
        let _mv_persist_handle = super::streaming_mv::persist::spawn_persist_task(
            Arc::clone(&shared_state.mv_persistence),
            Arc::clone(&shared_state.mv_registry),
            Arc::clone(&shared_state.watermark_tracker),
            shutdown.raw_receiver(),
        );

        // Spawn cross-shard dispatcher task (cluster mode only).
        if let (Some(dispatcher), Some(transport), Some(metrics), Some(dlq)) = (
            shared_state.cross_shard_dispatcher.as_ref(),
            shared_state.cluster_transport.as_ref(),
            shared_state.cross_shard_metrics.as_ref(),
            shared_state.cross_shard_dlq.as_ref(),
        ) {
            let _cross_shard_handle = super::cross_shard::dispatcher::spawn_dispatcher_task(
                Arc::clone(dispatcher),
                Arc::clone(transport),
                Arc::clone(metrics),
                Arc::clone(dlq),
                Arc::clone(&shared_state.event_plane_budget),
                shutdown.raw_receiver(),
            );
            info!("cross-shard dispatcher task started");
        }

        // Spawn CRDT sync delivery maintenance task.
        let _crdt_sync_handle = super::crdt_sync::delivery::spawn_delivery_task(
            Arc::clone(&shared_state.crdt_sync_delivery),
            shutdown.raw_receiver(),
        );

        // Set the origin peer ID for CRDT delta packaging.
        super::crdt_sync::packager::set_origin_peer_id(shared_state.node_id);

        let plane = Self {
            consumers,
            watermark_store,
        };

        info!(num_cores, "event plane started");
        plane
    }

    /// Number of consumer tasks (one per core).
    pub fn num_consumers(&self) -> usize {
        self.consumers.len()
    }

    /// Total events processed across all consumers.
    pub fn total_events_processed(&self) -> u64 {
        self.consumers.iter().map(|c| c.events_processed()).sum()
    }

    /// Per-core metrics references.
    pub fn core_metrics(&self) -> Vec<(usize, &Arc<CoreMetrics>)> {
        self.consumers
            .iter()
            .map(|c| (c.core_id, &c.metrics))
            .collect()
    }

    /// Compute aggregate metrics across all consumers.
    pub fn aggregate_metrics(&self) -> AggregateMetrics {
        let cores: Vec<Arc<CoreMetrics>> = self
            .consumers
            .iter()
            .map(|c| Arc::clone(&c.metrics))
            .collect();
        AggregateMetrics::from_cores(&cores)
    }

    /// Total events dropped across all consumers.
    pub fn total_events_dropped(&self) -> u64 {
        self.consumers
            .iter()
            .map(|c| c.metrics.events_dropped.load(Ordering::Relaxed))
            .sum()
    }

    /// Reference to the watermark store.
    pub fn watermark_store(&self) -> &Arc<WatermarkStore> {
        &self.watermark_store
    }

    /// Abort every consumer task and await its termination, consuming the
    /// plane so all `Arc<WatermarkStore>` / `Arc<WalManager>` clones held
    /// by the consumer futures are dropped by the time this returns.
    ///
    /// Use this instead of `drop(plane)` when the caller needs to reopen a
    /// resource the consumers held (e.g. the watermark redb file) without
    /// racing against Tokio's abort propagation.
    pub async fn shutdown_and_join(mut self) {
        let consumers = std::mem::take(&mut self.consumers);
        for consumer in consumers {
            consumer.abort_and_join().await;
        }
        debug!("event plane shutdown_and_join complete");
    }
}

impl Drop for EventPlane {
    fn drop(&mut self) {
        // The unified ShutdownWatch (SharedState.shutdown) signals all
        // consumers. Abort is a safety fallback for abnormal teardown.
        for consumer in &self.consumers {
            consumer.abort();
        }
        debug!("event plane dropped, all consumers shut down");
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
            lsn: Lsn::new(seq * 10),
            tenant_id: TenantId::new(1),
            vshard_id: VShardId::new(0),
            source: EventSource::User,
            new_value: Some(Arc::from(b"payload".as_slice())),
            old_value: None,
            system_time_ms: None,
            valid_time_ms: None,
        }
    }

    #[tokio::test]
    async fn event_plane_lifecycle() {
        let (mut producers, consumers) = create_event_bus_with_capacity(2, 64);
        let dir = tempfile::tempdir().unwrap();
        let (wal, watermark_store, shared_state, trigger_dlq, cdc_router) =
            crate::event::test_utils::event_test_deps(&dir);
        let shutdown = Arc::new(crate::control::shutdown::ShutdownWatch::new());

        let plane = EventPlane::spawn(
            consumers,
            wal,
            watermark_store,
            shared_state,
            trigger_dlq,
            cdc_router,
            shutdown,
        );
        assert_eq!(plane.num_consumers(), 2);

        // Emit events on both cores.
        for i in 1..=5 {
            producers[0].emit(make_event(i));
            producers[1].emit(make_event(i));
        }

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert_eq!(plane.total_events_processed(), 10);
        assert_eq!(plane.total_events_dropped(), 0);

        let agg = plane.aggregate_metrics();
        assert_eq!(agg.total_processed, 10);
    }

    #[tokio::test]
    async fn drop_aborts_consumers() {
        let (_producers, consumers) = create_event_bus_with_capacity(1, 16);
        let dir = tempfile::tempdir().unwrap();
        let (wal, watermark_store, shared_state, trigger_dlq, cdc_router) =
            crate::event::test_utils::event_test_deps(&dir);
        let shutdown = Arc::new(crate::control::shutdown::ShutdownWatch::new());

        let plane = EventPlane::spawn(
            consumers,
            wal,
            watermark_store,
            shared_state,
            trigger_dlq,
            cdc_router,
            shutdown,
        );
        drop(plane); // Should not panic.
    }
}
