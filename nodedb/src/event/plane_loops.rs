// SPDX-License-Identifier: BUSL-1.1

//! Background loops the Event Plane starts next to its consumers.
//!
//! Each `spawn_<subsystem>` function spawns one loop and registers its
//! handle with the node's loop registry. The registry joins the handle in
//! the shutdown phase named at the call site.

use std::sync::Arc;

use tokio::task::JoinHandle;
use tracing::{info, warn};

use super::bus::EventConsumerRx;
use super::cdc::CdcRouter;
use super::consumer::{ConsumerConfig, ConsumerHandle, spawn_consumer};
use super::slab_budget::{ConsumerSlabAccount, SlabBudget};
use super::trigger::dlq::TriggerDlq;
use super::watermark::WatermarkStore;
use crate::control::shutdown::{LoopHandle, ShutdownBus, ShutdownPhase, ShutdownWatch};
use crate::control::state::SharedState;
use crate::wal::WalManager;

/// Interval between slab budget checks.
const SLAB_BUDGET_INTERVAL: std::time::Duration = std::time::Duration::from_secs(5);

/// Register an Event Plane loop so the shutdown drain joins it.
///
/// A closed registry means shutdown has begun and no drain will join the
/// task. The task is aborted, so it never runs past the drain.
pub(super) fn register_event_loop(
    shared_state: &SharedState,
    name: &'static str,
    phase: ShutdownPhase,
    handle: JoinHandle<()>,
) {
    let abort = handle.abort_handle();
    if let Err(e) = shared_state
        .loop_registry
        .register(name, phase, LoopHandle::Async(handle))
    {
        abort.abort();
        warn!(error = %e, "shutdown began before the loop registered; the loop is aborted");
    }
}

/// Everything one consumer task needs, shared by every core.
pub(super) struct ConsumerDeps<'a> {
    pub wal: &'a Arc<WalManager>,
    pub watermark_store: &'a Arc<WatermarkStore>,
    pub shared_state: &'a Arc<SharedState>,
    pub trigger_dlq: &'a Arc<std::sync::Mutex<TriggerDlq>>,
    pub cdc_router: &'a Arc<CdcRouter>,
    pub shutdown: &'a ShutdownWatch,
    pub shutdown_bus: &'a ShutdownBus,
}

/// Spawn one consumer per core, in core-ID order. Returns the handles and
/// the slab account of each consumer.
pub(super) fn spawn_consumers(
    consumers_rx: Vec<EventConsumerRx>,
    deps: &ConsumerDeps<'_>,
) -> (Vec<ConsumerHandle>, Vec<Arc<ConsumerSlabAccount>>) {
    let num_cores = consumers_rx.len();
    let mut accounts = Vec::with_capacity(num_cores);
    let consumers: Vec<ConsumerHandle> = consumers_rx
        .into_iter()
        .enumerate()
        .map(|(core_id, rx)| {
            let account = Arc::new(ConsumerSlabAccount::new(core_id));
            accounts.push(Arc::clone(&account));
            spawn_consumer(ConsumerConfig {
                rx,
                shutdown: deps.shutdown.raw_receiver(),
                shutdown_bus: deps.shutdown_bus.clone(),
                wal: Arc::clone(deps.wal),
                watermark_store: Arc::clone(deps.watermark_store),
                shared_state: Arc::clone(deps.shared_state),
                trigger_dlq: Arc::clone(deps.trigger_dlq),
                cdc_router: Arc::clone(deps.cdc_router),
                num_cores,
                slab_account: account,
                watermark_flush_interval: crate::event::consumer_helpers::WATERMARK_FLUSH_INTERVAL,
            })
        })
        .collect();
    (consumers, accounts)
}

/// Periodic slab budget enforcement across the consumers' accounts.
pub(super) fn spawn_slab_budget(
    shared_state: &SharedState,
    shutdown: &ShutdownWatch,
    num_cores: usize,
    accounts: Vec<Arc<ConsumerSlabAccount>>,
) {
    let budget = SlabBudget::for_cores(num_cores);
    let mut shutdown_rx = shutdown.raw_receiver();
    let handle = tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = tokio::time::sleep(SLAB_BUDGET_INTERVAL) => {
                    let refs: Vec<&ConsumerSlabAccount> =
                        accounts.iter().map(|a| a.as_ref()).collect();
                    budget.check_and_shed(&refs);
                }
                _ = shutdown_rx.changed() => {
                    if *shutdown_rx.borrow() { return; }
                }
            }
        }
    });
    register_event_loop(
        shared_state,
        "event_plane::slab_budget",
        ShutdownPhase::DrainingEventPlane,
        handle,
    );
}

/// The cron scheduler. Joined at the Control Plane drain: a due schedule
/// dispatches SQL through Control -> Data and needs the enqueue gate open.
pub(super) fn spawn_scheduler(shared_state: &Arc<SharedState>, shutdown: &ShutdownWatch) {
    let handle = super::scheduler::executor::spawn_scheduler(
        Arc::clone(shared_state),
        Arc::clone(&shared_state.schedule_registry),
        Arc::clone(&shared_state.job_history),
        shutdown.raw_receiver(),
    );
    register_event_loop(
        shared_state,
        "event_plane::scheduler",
        ShutdownPhase::DrainingControlPlane,
        handle,
    );
}

/// Committed-message delivery. Joined at the Control Plane drain: a
/// delivery proposes to data groups and commits the cursor through the
/// metadata group.
pub(super) fn spawn_publish_delivery(shared_state: &Arc<SharedState>, shutdown: &ShutdownWatch) {
    let handle = super::topic::committed::spawn_publish_delivery(
        Arc::clone(shared_state),
        shutdown.raw_receiver(),
    );
    register_event_loop(
        shared_state,
        "event_plane::publish_delivery",
        ShutdownPhase::DrainingControlPlane,
        handle,
    );
}

/// AFTER trigger and DEFINE EVENT firing. Joined at the Control Plane drain:
/// a trigger body dispatches through Control -> Data, and the firing cursor
/// commits through the metadata group.
pub(super) fn spawn_action_firing(shared_state: &Arc<SharedState>, shutdown: &ShutdownWatch) {
    let handle = super::trigger::lane::spawn_action_firing(
        Arc::clone(shared_state),
        shutdown.raw_receiver(),
    );
    register_event_loop(
        shared_state,
        "event_plane::action_firing",
        ShutdownPhase::DrainingControlPlane,
        handle,
    );
}

/// Retention policy enforcement. Joined at the Control Plane drain:
/// enforcement dispatches MetaOp plans to the Data Plane.
pub(super) fn spawn_retention_policy(shared_state: &Arc<SharedState>, shutdown: &ShutdownWatch) {
    let handle = crate::engine::timeseries::retention_policy::enforcement::spawn_enforcement_loop(
        Arc::clone(shared_state),
        Arc::clone(&shared_state.retention_policy_registry),
        shutdown.raw_receiver(),
    );
    register_event_loop(
        shared_state,
        "event_plane::retention_policy",
        ShutdownPhase::DrainingControlPlane,
        handle,
    );
}

/// Bitemporal audit-retention enforcement, on the tick from server tuning.
/// Joined at the Control Plane drain: a purge pass dispatches
/// `TemporalPurge` plans to the Data Plane.
pub(super) fn spawn_bitemporal_retention(
    shared_state: &Arc<SharedState>,
    shutdown: &ShutdownWatch,
) {
    let handle = crate::engine::bitemporal::spawn_bitemporal_retention_loop(
        Arc::clone(shared_state),
        Arc::clone(&shared_state.bitemporal_retention_registry),
        shutdown.raw_receiver(),
        shared_state.tuning.bitemporal_retention_tick(),
    );
    register_event_loop(
        shared_state,
        "event_plane::bitemporal_retention",
        ShutdownPhase::DrainingControlPlane,
        handle,
    );
}

/// Alert evaluation. Joined at the Control Plane drain: each evaluation
/// dispatches a scan to the Data Plane.
pub(super) fn spawn_alert_eval(shared_state: &Arc<SharedState>, shutdown: &ShutdownWatch) {
    let handle = super::alert::executor::spawn_alert_eval_loop(
        Arc::clone(shared_state),
        Arc::clone(&shared_state.alert_registry),
        shutdown.raw_receiver(),
    );
    register_event_loop(
        shared_state,
        "event_plane::alert_eval",
        ShutdownPhase::DrainingControlPlane,
        handle,
    );
}

/// CDC log compaction.
pub(super) fn spawn_cdc_compaction(
    shared_state: &SharedState,
    cdc_router: &Arc<CdcRouter>,
    shutdown: &ShutdownWatch,
) {
    let handle = super::cdc::compaction::spawn_compaction_task(
        Arc::clone(&shared_state.stream_registry),
        Arc::clone(cdc_router),
        shutdown.raw_receiver(),
    );
    register_event_loop(
        shared_state,
        "event_plane::cdc_compaction",
        ShutdownPhase::DrainingEventPlane,
        handle,
    );
}

/// Streaming MV state persistence to redb.
pub(super) fn spawn_mv_persist(shared_state: &SharedState, shutdown: &ShutdownWatch) {
    let handle = super::streaming_mv::persist::spawn_persist_task(
        Arc::clone(&shared_state.mv_persistence),
        Arc::clone(&shared_state.mv_registry),
        Arc::clone(&shared_state.watermark_tracker),
        shutdown.raw_receiver(),
    );
    register_event_loop(
        shared_state,
        "event_plane::mv_persist",
        ShutdownPhase::DrainingEventPlane,
        handle,
    );
}

/// The cross-shard dispatcher. Runs in cluster mode only, when the
/// dispatcher, transport, metrics, and DLQ are all wired.
pub(super) fn spawn_cross_shard_dispatcher(shared_state: &SharedState, shutdown: &ShutdownWatch) {
    let (Some(dispatcher), Some(transport), Some(metrics), Some(dlq)) = (
        shared_state.cross_shard.dispatcher.as_ref(),
        shared_state.cluster_transport.as_ref(),
        shared_state.cross_shard.metrics.as_ref(),
        shared_state.cross_shard.dlq.as_ref(),
    ) else {
        return;
    };
    let handle = super::cross_shard::dispatcher::spawn_dispatcher_task(
        Arc::clone(dispatcher),
        Arc::clone(transport),
        Arc::clone(metrics),
        Arc::clone(dlq),
        Arc::clone(&shared_state.event_plane_budget),
        shutdown.raw_receiver(),
    );
    register_event_loop(
        shared_state,
        "event_plane::cross_shard_dispatcher",
        ShutdownPhase::DrainingEventPlane,
        handle,
    );
    info!("cross-shard dispatcher task started");
}

/// CRDT sync delivery maintenance.
pub(super) fn spawn_crdt_sync_delivery(shared_state: &SharedState, shutdown: &ShutdownWatch) {
    let handle = super::crdt_sync::delivery::spawn_delivery_task(
        Arc::clone(&shared_state.crdt_sync_delivery),
        shutdown.raw_receiver(),
    );
    register_event_loop(
        shared_state,
        "event_plane::crdt_sync_delivery",
        ShutdownPhase::DrainingEventPlane,
        handle,
    );
}
