// SPDX-License-Identifier: BUSL-1.1

//! [`spawn_background_loops`]: start every persistent background subsystem.

use std::sync::Arc;

use tracing::info;

use super::{maintenance, mirror, timers};
use crate::ServerConfig;
use crate::control::shutdown::ShutdownBus;
use crate::control::state::SharedState;
use crate::event::bus::EventConsumerRx;
use crate::event::trigger::TriggerDlq;
use crate::event::watermark::WatermarkStore;
use crate::wal::WalManager;

/// Interval of the usage metering flush, in seconds.
const METERING_FLUSH_SECS: u64 = 60;

/// Event Plane components passed to [`spawn_background_loops`].
pub struct EventPlaneComponents {
    pub wal: Arc<WalManager>,
    pub event_consumers: Vec<EventConsumerRx>,
    pub watermark_store: Arc<WatermarkStore>,
    pub trigger_dlq: Arc<std::sync::Mutex<TriggerDlq>>,
}

/// Spawn all persistent background subsystems.
///
/// Includes: Event Plane consumers, webhook manager wiring,
/// collection GC, L2 cleanup, tenant rate/audit/memory timers, checkpoint manager,
/// usage metering flush, and cold tier task.
///
/// Returns the [`crate::event::EventPlane`] handle. The caller MUST hold this
/// for the server's lifetime — dropping it aborts every consumer task and
/// turns the per-core event ring buffers into one-way drains, which silently
/// loses every WriteEvent emitted by the Data Plane until process exit.
#[must_use = "EventPlane must be held for the server's lifetime; dropping it stops all event consumers"]
pub fn spawn_background_loops(
    shared: &Arc<SharedState>,
    shutdown_bus: ShutdownBus,
    components: EventPlaneComponents,
    config: &ServerConfig,
    num_cores: usize,
    shutdown_rx: tokio::sync::watch::Receiver<bool>,
    checkpoint: crate::control::checkpoint_manager::CheckpointManagerConfig,
) -> crate::event::EventPlane {
    // Mirror restart: enumerate databases that need observer links
    // re-established and log the decisions. The cluster layer processes
    // these asynchronously via the mirror_link_registry once QUIC transport
    // is available.
    mirror::log_mirror_restart_decisions(shared);
    mirror::spawn_mirror_lag_monitor(shared);

    let event_plane = spawn_event_plane(shared, &shutdown_bus, components, num_cores);

    maintenance::spawn_collection_gc(shared, config);
    maintenance::spawn_pending_history_compaction(shared);
    maintenance::spawn_pending_leave_cleanup(shared);
    maintenance::spawn_orphaned_drain_sweep(shared);
    timers::spawn_tenant_timers(shared);
    crate::bootstrap::database_metrics_sampler::spawn_database_metrics_sampler(shared);
    spawn_security_loops(shared);

    // Checkpoint manager. It is handed the Event Plane's watermark store
    // because WAL truncation is bounded by the consumers' persisted progress as
    // much as by the engines': a consumer recovers only from the WAL above its
    // watermark, and nothing detects a gap if that suffix is deleted.
    let _checkpoint_task = crate::control::checkpoint_task::spawn_checkpoint_task(
        Arc::clone(shared),
        Arc::clone(event_plane.watermark_store()),
        num_cores,
        checkpoint,
        &shutdown_bus,
    );

    // Usage metering flush.
    let _metering_flush = crate::control::security::metering::counter::spawn_flush_task(
        Arc::clone(&shared.usage_counter),
        Arc::clone(&shared.usage_store),
        METERING_FLUSH_SECS,
    );

    // Quota period rollover is lazy — see `QuotaManager::rollover_if_due`.
    // Every reader/writer of quota usage rolls the scope's period over on
    // access, computed exactly from `period_start` and `period_secs`, so
    // there is no background sweep to spawn here and no interval to couple
    // a quota's `period_secs` to.

    maintenance::spawn_clone_materializer_sweep(shared, config);

    // CRDT constraint reconcile (one node cluster-wide). That node
    // re-derives each collection's constraint set from the catalog and
    // replicates it to every data-group replica's CRDT validator, so a
    // collection created/altered under any leader converges everywhere.
    crate::bootstrap::constraint_reconcile::spawn_constraint_reconcile(
        Arc::clone(shared),
        config.tuning.maintenance.constraint_reconcile_interval_ms,
    );
    info!("constraint reconcile loop running");

    // Scope grant expiry sweep. Executes each expired grant's ON EXPIRE
    // action (hard revoke or downgrade to a lesser scope) through the
    // replicated propose path, so the change is durable and cluster-wide.
    crate::control::security::scope::expiry::spawn_expiry_task(
        Arc::clone(shared),
        config.tuning.maintenance.scope_expiry_interval_secs,
    );

    // Periodic continuous-aggregate refresh cuts. The metadata leader
    // proposes one replicated cut per aggregate per period slot.
    crate::control::server::shared::ddl::neutral::continuous_agg::spawn_periodic_refresh_loop(
        Arc::clone(shared),
    );

    spawn_storage_tiers(shared, config, shutdown_rx);

    event_plane
}

/// Wire the stream delivery managers, then start the Event Plane: one
/// consumer Tokio task per Data Plane core.
///
/// The managers are wired first so Event Plane creation can admit CREATE
/// CHANGE STREAM delivery tasks. The returned plane must outlive the
/// server. Its Drop impl aborts every consumer, and the Data Plane
/// producers then drop every WriteEvent they emit.
fn spawn_event_plane(
    shared: &Arc<SharedState>,
    shutdown_bus: &ShutdownBus,
    components: EventPlaneComponents,
    num_cores: usize,
) -> crate::event::EventPlane {
    let EventPlaneComponents {
        wal,
        event_consumers,
        watermark_store,
        trigger_dlq,
    } = components;
    shared.webhook_manager.set_state(shared);
    shared.kafka_manager.set_state(shared);

    let event_plane = crate::event::EventPlane::spawn(crate::event::EventPlaneConfig {
        consumers_rx: event_consumers,
        wal,
        watermark_store,
        shared_state: Arc::clone(shared),
        trigger_dlq,
        cdc_router: Arc::clone(&shared.cdc_router),
        shutdown: Arc::clone(&shared.shutdown),
        shutdown_bus: shutdown_bus.clone(),
    });
    info!(num_cores, "event plane running");
    event_plane
}

/// Data Plane core stall monitor, idle session sweep, and SIEM export.
fn spawn_security_loops(shared: &Arc<SharedState>) {
    // Samples each core's event-loop liveness counter and publishes the set
    // of cores that stopped completing iterations. Nothing else observes a
    // core that wedges without panicking: the per-core panic watchdog is
    // loop-local and counts panics only.
    crate::bootstrap::core_stall_monitor::spawn_core_stall_monitor(shared);
    info!("data plane core stall monitor running");

    // Closes sessions whose idle timeout or OIDC token expiry has elapsed.
    crate::control::security::sessions::spawn_idle_sweep_loop(shared);
    info!("idle session sweep loop running");

    // Ships the audit/auth events buffered by `audit_record_with_db_strict`
    // to the configured webhook. No task when no SIEM destination is
    // configured.
    crate::control::security::siem::spawn_export_loop(shared);
}

/// Cold tier task (when configured), PITR base snapshots, and periodic
/// cluster restore points.
fn spawn_storage_tiers(
    shared: &Arc<SharedState>,
    config: &ServerConfig,
    shutdown_rx: tokio::sync::watch::Receiver<bool>,
) {
    if let Some(cold_settings) = &config.cold_storage {
        crate::control::cold_tier::spawn_cold_tier_task(
            Arc::clone(shared),
            cold_settings.clone(),
            config.server.data_dir.clone(),
            shutdown_rx,
        );
        info!("cold tier task spawned");
    }

    // PITR base snapshots, retention, and archived WAL collection.
    crate::control::pitr::spawn_base_snapshot_task(shared, &config.pitr);
    if config.pitr.enabled && config.cluster.is_some() {
        crate::control::pitr::restore_point::spawn_restore_point_task(
            shared,
            config.pitr.restore_point_interval(),
        );
    }
}
