// SPDX-License-Identifier: BUSL-1.1

//! Background loop and subsystem spawning after SharedState is ready.

use std::sync::Arc;
use std::time::Duration;

use tracing::info;

use crate::ServerConfig;
use crate::control::state::SharedState;
use crate::event::bus::EventConsumerRx;
use crate::event::trigger::TriggerDlq;
use crate::event::watermark::WatermarkStore;
use crate::wal::WalManager;

/// Event Plane components passed to [`spawn_background_loops`].
pub struct EventPlaneComponents {
    pub wal: Arc<WalManager>,
    pub event_consumers: Vec<EventConsumerRx>,
    pub watermark_store: Arc<WatermarkStore>,
    pub trigger_dlq: Arc<std::sync::Mutex<TriggerDlq>>,
}

/// Enumerate mirror databases that need their observer link re-established
/// after a server restart, and log the restart decisions.
///
/// This is called once during startup, after [`SharedState`] and the catalog
/// are fully open. Databases with `MirrorStatus::Promoted` are excluded:
/// they are normal writable databases and must NOT attempt to reconnect.
/// The actual link objects are created by the cluster layer when it
/// processes each restart decision; this function only reads the catalog
/// and logs.
pub fn log_mirror_restart_decisions(shared: &Arc<crate::control::state::SharedState>) {
    let catalog = shared.credentials.catalog();
    match crate::control::mirror::enumerate_resumable_mirrors(catalog) {
        Ok(decisions) => {
            for d in &decisions {
                tracing::info!(
                    database = %d.database_name,
                    resume_lsn = d.resume_from_lsn,
                    needs_bootstrap = d.needs_bootstrap,
                    "mirror restart: observer link will resume"
                );
            }
        }
        Err(e) => {
            tracing::warn!(error = %e, "mirror restart: failed to enumerate mirrors; skipping");
        }
    }
}

/// Spawn all persistent background subsystems.
///
/// Includes: Event Plane consumers, webhook manager wiring,
/// collection GC, L2 cleanup, tenant rate/audit/memory timers, checkpoint manager,
/// usage metering flush, and cold tier task.
///
/// Returns the [`EventPlane`] handle. The caller MUST hold this for the
/// server's lifetime — dropping it aborts every consumer task and turns
/// the per-core event ring buffers into one-way drains, which silently
/// loses every WriteEvent emitted by the Data Plane until process exit.
#[must_use = "EventPlane must be held for the server's lifetime; dropping it stops all event consumers"]
pub fn spawn_background_loops(
    shared: &Arc<SharedState>,
    shutdown_bus: crate::control::shutdown::ShutdownBus,
    components: EventPlaneComponents,
    config: &ServerConfig,
    num_cores: usize,
    shutdown_rx: tokio::sync::watch::Receiver<bool>,
) -> crate::event::EventPlane {
    let EventPlaneComponents {
        wal,
        event_consumers,
        watermark_store,
        trigger_dlq,
    } = components;
    // Mirror restart: enumerate databases that need observer links re-established
    // and log the decisions. The cluster layer processes these asynchronously
    // via the mirror_link_registry once QUIC transport is available.
    log_mirror_restart_decisions(shared);

    // Mirror lag monitor (5-second interval).
    // Reads `_system.mirror_lag` for every active mirror and updates
    // the `nodedb_database_mirror_lag_ms` metric. Also drives status
    // transitions (Following → Degraded → Disconnected) and clears the
    // metric when a mirror is promoted.
    {
        let shared_mirror = Arc::clone(shared);
        crate::control::shutdown::spawn_loop(
            &shared.loop_registry,
            &shared.shutdown,
            "mirror_lag_monitor",
            crate::control::shutdown::ShutdownPhase::DrainingControlPlane,
            move |mut shutdown| async move {
                let mut tick = tokio::time::interval(Duration::from_secs(5));
                loop {
                    tokio::select! {
                        _ = shutdown.wait_cancelled() => break,
                        _ = tick.tick() => {}
                    }
                    if shutdown.is_cancelled() {
                        break;
                    }
                    let catalog = shared_mirror.credentials.catalog();
                    let databases = match catalog.list_databases() {
                        Ok(d) => d,
                        Err(e) => {
                            tracing::warn!(error = %e, "mirror_lag_monitor: catalog list error");
                            continue;
                        }
                    };
                    for db in databases {
                        let origin = match db.mirror_origin.as_ref() {
                            Some(o) => o,
                            None => continue,
                        };
                        // Promoted mirrors are normal writable databases — skip.
                        if matches!(origin.status, nodedb_types::MirrorStatus::Promoted) {
                            continue;
                        }
                        // Read the real receive timestamp from the link registry.
                        // `None` means no link is registered for this database (the
                        // cluster layer has not yet (re)established it after restart);
                        // `update_lag_status` falls back to the catalog's apply time
                        // in that case so the disconnect timer still advances.
                        let last_received =
                            shared_mirror.mirror_link_registry.last_received_ms(db.id);
                        crate::control::mirror::update_lag_status(
                            catalog,
                            db.id,
                            &db.name,
                            &origin.status,
                            last_received,
                            false,
                            &shared_mirror.database_metrics,
                        );
                    }
                }
            },
        );
        info!("mirror lag monitor running");
    }

    // Wire stream delivery managers before Event Plane creation can admit
    // CREATE CHANGE STREAM delivery tasks.
    shared.webhook_manager.set_state(Arc::clone(shared));
    shared.kafka_manager.set_state(Arc::clone(shared));

    // Event Plane: one consumer Tokio task per Data Plane core.
    // Returned to the caller — must outlive the server, otherwise its
    // Drop impl aborts every consumer and the Data Plane producers
    // start dropping every WriteEvent they emit.
    let event_plane = crate::event::EventPlane::spawn(crate::event::EventPlaneConfig {
        consumers_rx: event_consumers,
        wal: Arc::clone(&wal),
        watermark_store,
        shared_state: Arc::clone(shared),
        trigger_dlq,
        cdc_router: Arc::clone(&shared.cdc_router),
        shutdown: Arc::clone(&shared.shutdown),
        shutdown_bus: shutdown_bus.clone(),
    });
    info!(num_cores, "event plane running");

    // Collection hard-delete retention GC.
    if let Ok(mut w) = shared.retention_settings.write() {
        *w = config.retention.clone();
    }
    let _collection_gc = crate::event::collection_gc::spawn_collection_gc(Arc::clone(shared));
    info!(
        retention_days = config.retention.deactivated_collection_retention_days,
        sweep_interval_secs = config.retention.gc_sweep_interval_secs,
        "collection-gc sweeper running"
    );

    // L2 cleanup worker.
    let _l2_cleanup = crate::event::collection_gc::spawn_l2_cleanup(Arc::clone(shared));

    // Pending engine-reclaim worker: retries the redb + versioned engine
    // purge for any dropped collection whose result-checked purge failed
    // on this node, so a per-node hiccup can never leave engine storage
    // rows behind a removed catalog row.
    let _pending_reclaim = crate::event::collection_gc::spawn_pending_reclaim(Arc::clone(shared));

    // Tenant rate counter reset (1-second timer).
    let shared_rate = Arc::clone(shared);
    crate::control::shutdown::spawn_loop(
        &shared.loop_registry,
        &shared.shutdown,
        "tenant_rate_reset",
        crate::control::shutdown::ShutdownPhase::DrainingControlPlane,
        move |mut shutdown| async move {
            let mut tick = tokio::time::interval(Duration::from_secs(1));
            loop {
                tokio::select! {
                    _ = shutdown.wait_cancelled() => break,
                    _ = tick.tick() => shared_rate.reset_tenant_rate_counters(),
                }
            }
        },
    );

    // Data Plane core stall monitor (5-second sampling).
    // Samples each core's event-loop liveness counter and publishes the set of
    // cores that stopped completing iterations. Nothing else observes a core
    // that wedges without panicking: the per-core panic watchdog is loop-local
    // and counts panics only.
    crate::bootstrap::core_stall_monitor::spawn_core_stall_monitor(shared);
    info!("data plane core stall monitor running");

    // Idle session sweep (5-second timer).
    // Closes sessions whose idle timeout or OIDC token expiry has elapsed.
    crate::control::security::sessions::spawn_idle_sweep_loop(shared);
    info!("idle session sweep loop running");

    // Audit log flush (10-second timer).
    let shared_audit = Arc::clone(shared);
    crate::control::shutdown::spawn_loop(
        &shared.loop_registry,
        &shared.shutdown,
        "audit_log_flush",
        crate::control::shutdown::ShutdownPhase::DrainingControlPlane,
        move |mut shutdown| async move {
            let mut tick = tokio::time::interval(Duration::from_secs(10));
            loop {
                tokio::select! {
                    _ = shutdown.wait_cancelled() => break,
                    _ = tick.tick() => shared_audit.flush_audit_log(),
                }
            }
        },
    );

    // SIEM export delivery. Ships the audit/auth events buffered by
    // `audit_record_with_db_strict` to the configured webhook. No-op (and no
    // task) when no SIEM destination is configured.
    crate::control::security::siem::spawn_export_loop(shared);

    // Tenant memory estimation (30-second timer).
    let shared_mem = Arc::clone(shared);
    crate::control::shutdown::spawn_loop(
        &shared.loop_registry,
        &shared.shutdown,
        "tenant_memory_estimate",
        crate::control::shutdown::ShutdownPhase::DrainingControlPlane,
        move |mut shutdown| async move {
            let mut tick = tokio::time::interval(Duration::from_secs(30));
            loop {
                tokio::select! {
                    _ = shutdown.wait_cancelled() => break,
                    _ = tick.tick() => shared_mem.update_tenant_memory_estimates(),
                }
            }
        },
    );

    // Checkpoint manager. It is handed the Event Plane's watermark store
    // because WAL truncation is bounded by the consumers' persisted progress as
    // much as by the engines': a consumer recovers only from the WAL above its
    // watermark, and nothing detects a gap if that suffix is deleted.
    let shared_ckpt = Arc::clone(shared);
    let _checkpoint_task = crate::control::checkpoint_task::spawn_checkpoint_task(
        shared_ckpt,
        Arc::clone(event_plane.watermark_store()),
        num_cores,
        config.checkpoint.to_manager_config(),
        &shutdown_bus,
    );

    // Usage metering flush.
    let _metering_flush = crate::control::security::metering::counter::spawn_flush_task(
        Arc::clone(&shared.usage_counter),
        Arc::clone(&shared.usage_store),
        60,
    );

    // Quota period rollover is lazy — see `QuotaManager::rollover_if_due`.
    // Every reader/writer of quota usage rolls the scope's period over on
    // access, computed exactly from `period_start` and `period_secs`, so
    // there is no background sweep to spawn here and no interval to couple
    // a quota's `period_secs` to.

    // Background clone materializer sweep.
    // Automatically progresses cloned collections from Shadowed → Materialized
    // without requiring explicit DDL.  The foreground ALTER DATABASE MATERIALIZE
    // and DROP DATABASE FORCE paths bypass this loop and call the blocking
    // materializer directly.
    {
        let shared_sweep = Arc::clone(shared);
        let sweep_ms = config.tuning.maintenance.clone_sweep_interval_ms;
        let sweep_interval = Duration::from_millis(sweep_ms);
        crate::control::shutdown::spawn_loop(
            &shared.loop_registry,
            &shared.shutdown,
            "clone_materializer_sweep",
            crate::control::shutdown::ShutdownPhase::DrainingControlPlane,
            move |mut shutdown| async move {
                let mut tick = tokio::time::interval(sweep_interval);
                loop {
                    tokio::select! {
                        _ = shutdown.wait_cancelled() => break,
                        _ = tick.tick() => {}
                    }
                    if shutdown.is_cancelled() {
                        break;
                    }
                    let state_for_sweep = Arc::clone(&shared_sweep);
                    let result = tokio::task::spawn_blocking(move || {
                        let catalog = state_for_sweep.credentials.catalog();
                        let cancel = std::sync::atomic::AtomicBool::new(false);
                        if let Err(e) =
                            crate::control::maintenance::clone_materializer::run_scheduled_sweep(
                                &state_for_sweep,
                                catalog,
                                &cancel,
                            )
                        {
                            tracing::warn!(error = %e, "clone materializer sweep error");
                        }
                    })
                    .await;
                    if let Err(e) = result {
                        tracing::warn!(error = %e, "clone materializer sweep task panicked");
                    }
                }
            },
        );
        info!(
            interval_ms = sweep_ms,
            "clone materializer background sweep running"
        );
    }

    // CRDT constraint reconcile (1-second timer, one node cluster-wide).
    // That node re-derives each collection's constraint set from the
    // catalog and replicates it to every data-group replica's CRDT validator,
    // so a collection created/altered under any leader converges everywhere.
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

    // Cold tier task (if configured).
    if let Some(ref cold_settings) = config.cold_storage {
        let shared_cold = Arc::clone(shared);
        let cold_settings_clone = cold_settings.clone();
        let data_dir_clone = config.server.data_dir.clone();
        let shutdown_rx_cold = shutdown_rx.clone();
        crate::control::cold_tier::spawn_cold_tier_task(
            shared_cold,
            cold_settings_clone,
            data_dir_clone,
            shutdown_rx_cold,
        );
        info!("cold tier task spawned");
    }

    event_plane
}

/// Spawn the response poller loop (routes Data Plane responses to waiting sessions).
///
/// The poller is a `DrainingDataPlane` participant, and it exits on the Data
/// Plane drain latch rather than on the flat shutdown signal. Two things depend
/// on that: the final checkpoint dispatched during `DrainingControlPlane` waits
/// for one response per core, and the Data Plane drain itself is finished only
/// once every in-flight response has reached its session. A poller that stopped
/// at the flat signal would strand both.
///
/// The registry must never abort it, so it registers as no-abort. The drain
/// latch is its one safe exit boundary, and a cancellation short of that
/// strands the responses the poller exists to route.
pub fn spawn_response_poller(
    shared: &Arc<SharedState>,
    bus: &crate::control::shutdown::ShutdownBus,
) {
    let shared_poller = Arc::clone(shared);
    let drain_guard = bus.register_task(
        crate::control::shutdown::ShutdownPhase::DrainingDataPlane,
        "response_poller",
        None,
    );
    crate::control::shutdown::spawn_loop_no_abort(
        &shared.loop_registry,
        &shared.shutdown,
        "response_poller",
        crate::control::shutdown::ShutdownPhase::DrainingDataPlane,
        move |_shutdown| async move {
            let mut idle_iters: u32 = 0;
            loop {
                if shared_poller.data_plane_drain.is_complete() {
                    // One last pass so a response the drain's own final poll
                    // raced is still routed before the loop ends.
                    shared_poller.poll_and_route_responses();
                    break;
                }
                let routed = shared_poller.poll_and_route_responses();
                if routed > 0 {
                    idle_iters = 0;
                    tokio::task::yield_now().await;
                    continue;
                }
                idle_iters = idle_iters.saturating_add(1);
                if idle_iters <= 256 {
                    tokio::task::yield_now().await;
                } else if idle_iters <= 1024 {
                    tokio::time::sleep(std::time::Duration::from_millis(1)).await;
                } else {
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                }
            }
            drain_guard.report_drained();
        },
    );
}
