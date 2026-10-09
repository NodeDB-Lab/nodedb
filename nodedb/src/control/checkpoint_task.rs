// SPDX-License-Identifier: BUSL-1.1

//! The checkpoint manager's background task: a periodic cycle, a shorter WAL
//! archive tick when cold storage is configured, and one final cycle at
//! shutdown.

use std::sync::Arc;

use tracing::{info, warn};

use super::checkpoint_manager::{
    CheckpointCycleInputs, CheckpointManagerConfig, run_checkpoint_cycle,
};
use super::startup::StartupPhase;
use crate::wal::archiver::WalArchiver;

/// Spawn the checkpoint manager as a background Tokio task.
///
/// Runs `run_checkpoint_cycle` at the configured interval, and one final cycle
/// when the shutdown bus enters `DrainingControlPlane`. The task owns the WAL
/// archiver: it archives every sealed segment each archive tick, and each
/// checkpoint cycle archives again before it truncates.
///
/// The phase is load-bearing. The final cycle dispatches a checkpoint request
/// to every Data Plane core and waits for the answers, so it must complete
/// before `DrainingDataPlane` closes the enqueue gate. Registering it as a
/// critical task at the preceding phase is what orders the two: the bus cannot
/// enter the Data Plane drain until this task reports drained.
pub fn spawn_checkpoint_task(
    shared: Arc<crate::control::state::SharedState>,
    watermark_store: Arc<crate::event::watermark::WatermarkStore>,
    num_cores: usize,
    config: CheckpointManagerConfig,
    shutdown_bus: &crate::control::shutdown::ShutdownBus,
) -> tokio::task::JoinHandle<()> {
    let mut guard = shutdown_bus.register_critical_task(
        crate::control::shutdown::ShutdownPhase::DrainingControlPlane,
        "checkpoint_manager::final",
    );
    tokio::spawn(async move {
        // Boot recovery reads the WAL on disk until the gateway opens: data
        // groups replay their Raft logs against it, and Calvin boot recovery
        // scans it for stamped redo records. A replayed core reports its floor at
        // once, so a cycle in that window can delete segments those readers
        // still need. The first cycle waits for the gateway.
        let started = tokio::select! {
            ready = shared.startup.await_phase(StartupPhase::GatewayEnable) => ready.map_err(|error| {
                warn!(%error, "checkpoint manager not started: startup did not complete");
            }),
            // The WAL on disk is intact, so restart replay covers what a final
            // cycle makes redundant.
            _ = guard.await_signal() => {
                info!("shutdown before startup completed: no final checkpoint");
                Err(())
            }
        };
        if started.is_err() {
            guard.report_drained();
            return;
        }
        info!(
            interval_secs = config.interval().as_secs(),
            archive_interval_secs = config.archive_interval().as_secs(),
            "checkpoint manager started"
        );

        let mut archiver = shared.cold_storage.clone().map(|cold| {
            WalArchiver::new(
                shared.node_id,
                shared.data_dir.clone(),
                cold,
                shared.system_metrics.clone(),
            )
        });
        let mut checkpoint_tick = delayed_interval(config.interval());
        let mut archive_tick = delayed_interval(config.archive_interval());

        loop {
            let mut draining = false;
            tokio::select! {
                _ = checkpoint_tick.tick() => {}
                _ = archive_tick.tick(), if archiver.is_some() => {
                    if let Some(archiver) = archiver.as_mut() {
                        archiver.tick(&shared.wal).await;
                        archiver.sweep_stale_markers().await;
                    }
                    continue;
                }
                _ = guard.await_signal() => { draining = true; }
            }

            if draining {
                info!("shutdown: running final checkpoint");
                // Bounded by the shutdown deadline, not by the steady-state
                // per-core timeout. This cycle is a phase barrier, so a core
                // that never answers costs the shutdown its deadline and no more.
                let budget = shared.tuning.shutdown.deadline();
                let cycle = run_checkpoint_cycle(CheckpointCycleInputs {
                    dispatcher: &shared.dispatcher,
                    tracker: &shared.tracker,
                    wal: &shared.wal,
                    watermark_store: &watermark_store,
                    num_cores,
                    timeout: budget,
                    archiver: archiver.as_mut(),
                    catalog: Some(shared.credentials.catalog()),
                    calvin_ledgers: Some(&shared.calvin.applied),
                });
                if tokio::time::timeout(budget, cycle).await.is_err() {
                    warn!(
                        budget_ms = budget.as_millis() as u64,
                        "final checkpoint exceeded the shutdown deadline — the WAL suffix \
                         it would have made redundant is replayed on restart"
                    );
                }
                guard.report_drained();
                info!("checkpoint manager stopped");
                return;
            }

            run_checkpoint_cycle(CheckpointCycleInputs {
                dispatcher: &shared.dispatcher,
                tracker: &shared.tracker,
                wal: &shared.wal,
                watermark_store: &watermark_store,
                num_cores,
                timeout: config.core_timeout(),
                archiver: archiver.as_mut(),
                catalog: Some(shared.credentials.catalog()),
                calvin_ledgers: Some(&shared.calvin.applied),
            })
            .await;
        }
    })
}

/// An interval whose first tick is one `period` away, and which delays rather
/// than bursts after a slow cycle.
fn delayed_interval(period: std::time::Duration) -> tokio::time::Interval {
    let mut interval = tokio::time::interval_at(tokio::time::Instant::now() + period, period);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    interval
}
