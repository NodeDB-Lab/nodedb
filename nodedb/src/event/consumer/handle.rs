// SPDX-License-Identifier: BUSL-1.1

//! Spawn a consumer task for one core's ring, and the handle to it.

use std::sync::Arc;

use tokio::sync::watch;

use crate::control::state::SharedState;
use crate::event::bus::EventConsumerRx;
use crate::event::metrics::CoreMetrics;
use crate::event::trigger::dlq::TriggerDlq;
use crate::event::watermark::WatermarkStore;
use crate::wal::WalManager;

use super::run::consumer_loop;

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
    pub cdc_router: Arc<crate::event::cdc::CdcRouter>,
    pub num_cores: usize,
    /// Per-consumer slab-pin accounting for WAL memory budget enforcement.
    pub slab_account: Arc<crate::event::slab_budget::ConsumerSlabAccount>,
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
