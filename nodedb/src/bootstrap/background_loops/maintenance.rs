// SPDX-License-Identifier: BUSL-1.1

//! Maintenance loops: collection GC, owed-work retries, orphaned drain
//! sweep, the clone materializer sweep, and the version cleanup pass.

use std::sync::Arc;
use std::time::Duration;

use tracing::info;

use crate::ServerConfig;
use crate::control::shutdown::{ShutdownPhase, spawn_loop};
use crate::control::state::SharedState;

/// Period of each owed-work retry and of the orphaned drain sweep.
const OWED_WORK_PERIOD: Duration = Duration::from_secs(30);

/// Collection hard-delete retention GC, the L2 cleanup worker, and the
/// pending engine-reclaim worker.
///
/// The reclaim worker retries the redb and versioned engine purge for any
/// dropped collection whose result-checked purge failed on this node, so a
/// per-node error never leaves engine storage rows behind a removed
/// catalog row.
pub fn spawn_collection_gc(shared: &Arc<SharedState>, config: &ServerConfig) {
    if let Ok(mut w) = shared.retention_settings.write() {
        *w = config.retention.clone();
    }
    let _collection_gc = crate::event::collection_gc::spawn_collection_gc(Arc::clone(shared));
    info!(
        retention_days = config.retention.deactivated_collection_retention_days,
        sweep_interval_secs = config.retention.gc_sweep_interval_secs,
        "collection-gc sweeper running"
    );
    let _l2_cleanup = crate::event::collection_gc::spawn_l2_cleanup(Arc::clone(shared));
    let _pending_reclaim = crate::event::collection_gc::spawn_pending_reclaim(Arc::clone(shared));
}

/// Owed history compaction retry: re-drives every COMPACT HISTORY whose
/// per-node fan-out failed.
pub fn spawn_pending_history_compaction(shared: &Arc<SharedState>) {
    let shared_compaction = Arc::clone(shared);
    spawn_loop(
        &shared.loop_registry,
        &shared.shutdown,
        "pending_history_compaction",
        ShutdownPhase::DrainingControlPlane,
        move |mut shutdown| async move {
            let mut tick = tokio::time::interval(OWED_WORK_PERIOD);
            loop {
                tokio::select! {
                    _ = shutdown.wait_cancelled() => break,
                    _ = tick.tick() => {
                        if let Err(error) =
                            crate::control::catalog_entry::post_apply::drain_pending_compactions(
                                &shared_compaction,
                            )
                            .await
                        {
                            tracing::warn!(error = %error, "owed history compactions unreadable");
                        }
                    }
                }
            }
        },
    );
}

/// Owed leave cleanup: re-drives the lease release and drain end of every
/// node that left, until its row is gone.
pub fn spawn_pending_leave_cleanup(shared: &Arc<SharedState>) {
    let state = Arc::clone(shared);
    spawn_loop(
        &shared.loop_registry,
        &shared.shutdown,
        "pending_leave_cleanup",
        ShutdownPhase::DrainingControlPlane,
        move |mut shutdown| async move {
            let mut tick = tokio::time::interval(OWED_WORK_PERIOD);
            loop {
                tokio::select! {
                    _ = shutdown.wait_cancelled() => break,
                    _ = tick.tick() => {
                        if let Err(error) =
                            crate::control::lease::leave_cleanup::drain_pending_leave_cleanups(
                                &state,
                            )
                            .await
                        {
                            tracing::warn!(error = %error, "owed leave cleanups unreadable");
                        }
                    }
                }
            }
        },
    );
}

/// Orphaned descriptor drain sweep, on the singleton worker only. Ends a
/// drain whose proposer left the topology when the Leave hook's own end
/// did not apply.
pub fn spawn_orphaned_drain_sweep(shared: &Arc<SharedState>) {
    let state = Arc::clone(shared);
    spawn_loop(
        &shared.loop_registry,
        &shared.shutdown,
        "orphaned_drain_sweep",
        ShutdownPhase::DrainingControlPlane,
        move |mut shutdown| async move {
            let mut tick = tokio::time::interval(OWED_WORK_PERIOD);
            loop {
                tokio::select! {
                    _ = shutdown.wait_cancelled() => break,
                    _ = tick.tick() => {
                        if state.is_singleton_worker() {
                            crate::control::lease::gc::end_orphaned_drains(&state).await;
                        }
                    }
                }
            }
        },
    );
}

/// Background clone materializer sweep.
///
/// Moves cloned collections from Shadowed to Materialized without explicit
/// DDL. The foreground ALTER DATABASE MATERIALIZE and DROP DATABASE FORCE
/// paths bypass this loop and call the blocking materializer directly.
pub fn spawn_clone_materializer_sweep(shared: &Arc<SharedState>, config: &ServerConfig) {
    let shared_sweep = Arc::clone(shared);
    let sweep_ms = config.tuning.maintenance.clone_sweep_interval_ms;
    let sweep_interval = Duration::from_millis(sweep_ms);
    spawn_loop(
        &shared.loop_registry,
        &shared.shutdown,
        "clone_materializer_sweep",
        ShutdownPhase::DrainingControlPlane,
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
                let cancel = std::sync::atomic::AtomicBool::new(false);
                if let Err(e) =
                    crate::control::maintenance::clone_materializer::run_scheduled_sweep(
                        &shared_sweep,
                        shared_sweep.credentials.catalog(),
                        &cancel,
                    )
                    .await
                {
                    tracing::warn!(error = %e, "clone materializer sweep error");
                }
            }
        },
    );
    info!(
        interval_ms = sweep_ms,
        "clone materializer background sweep running"
    );
}

/// Background version cleanup.
///
/// Every interval it computes the cleanup horizon and purges every core's
/// history below it (`control::version_cleanup`). An interval of `0` turns
/// it off. A failed pass files its own report and the next one retries.
pub fn spawn_version_cleanup(shared: &Arc<SharedState>, config: &ServerConfig) {
    let tuning = config.tuning.version_cleanup.clone();
    let Some(interval) = tuning.interval() else {
        info!("version cleanup off: history grows without bound");
        return;
    };
    let batch = tuning.batch();
    let state = Arc::clone(shared);
    spawn_loop(
        &shared.loop_registry,
        &shared.shutdown,
        "version_cleanup",
        ShutdownPhase::DrainingControlPlane,
        move |mut shutdown| async move {
            let mut tick = tokio::time::interval(interval);
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tokio::select! {
                    _ = shutdown.wait_cancelled() => break,
                    _ = tick.tick() => {}
                }
                if shutdown.is_cancelled() {
                    break;
                }
                match crate::control::version_cleanup::run_cleanup_pass(&state, batch).await {
                    Ok(Some(outcome)) => tracing::debug!(
                        horizon = outcome.horizon,
                        rounds = outcome.rounds,
                        "version cleanup pass"
                    ),
                    Ok(None) => {}
                    Err(e) => tracing::warn!(error = %e, "version cleanup pass failed"),
                }
            }
        },
    );
    info!(
        interval_ms = tuning.interval_ms,
        batch_units = batch,
        "version cleanup running"
    );
}
