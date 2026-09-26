// SPDX-License-Identifier: BUSL-1.1

//! Unified shutdown-bus wiring, plus the test-only slow-drain-task
//! injection used by integration tests to exercise the offender-abort
//! path.

use std::sync::Arc;

use nodedb::control::shutdown::{LoopRegistry, ShutdownBus, ShutdownPhase, ShutdownWatch};
use nodedb::control::state::SharedState;

/// Build the phased shutdown bus, wire it to `SharedState`'s
/// `ShutdownWatch` and system metrics, and — only when
/// `NODEDB_TEST_SLOW_DRAIN_TASK=1` — register a task that deliberately
/// never reports drained, to verify the offender-abort path in
/// integration tests. Kept out of `main()` for readability.
///
/// Returns `(shutdown_rx, shutdown_bus, loop_registry_supervisors)`. All
/// shutdown signals flow through the canonical `ShutdownWatch` held on
/// `SharedState`; the returned raw receiver is a view of that same watch,
/// preserved so the existing listener APIs (`PgListener::run`,
/// `HttpServer::run`, `IlpListener::run`, `RespListener::run`,
/// `spawn_cold_storage_loop`, `spawn_checkpoint_loop`, and the lease renewal
/// loop) keep their `watch::Receiver<bool>` parameter unchanged. New code
/// SHOULD use `shared.shutdown.subscribe()`.
pub(crate) fn wire_shutdown_bus(
    shared: &Arc<SharedState>,
    system_metrics: &Arc<nodedb::control::metrics::SystemMetrics>,
) -> (
    tokio::sync::watch::Receiver<bool>,
    ShutdownBus,
    Vec<tokio::task::JoinHandle<()>>,
) {
    let shutdown_rx = shared.shutdown.raw_receiver();

    // Unified shutdown bus: phased drain with per-phase 500 ms budgets.
    // `ShutdownBus::initiate()` signals the flat `ShutdownWatch` so all
    // existing `watch::Receiver<bool>` subscribers wake up as well.
    let (shutdown_bus, _shutdown_bus_handle) = ShutdownBus::new(Arc::clone(&shared.shutdown));
    // Wire system metrics so the bus records `nodedb_shutdown_phase_duration_seconds{phase}`
    // for each phase transition during graceful shutdown.
    shutdown_bus.set_metrics(Arc::clone(system_metrics));

    // This is deliberately startup-owned rather than signal-handler-owned:
    // every initiator of this exact bus must wait for the registry loops of a
    // phase before that phase can be reported drained.
    let loop_registry_supervisors = spawn_loop_registry_shutdown_supervisors(
        Arc::clone(&shared.loop_registry),
        Arc::clone(&shared.shutdown),
        shared.tuning.shutdown.deadline(),
        &shutdown_bus,
    );

    // Data Plane drain barrier. Registered here, at startup, for the same
    // reason: the bus must not be able to pass through `DrainingDataPlane`
    // without a participant, whichever code path initiates shutdown.
    let _data_plane_drain_supervisor = nodedb::control::shutdown::spawn_data_plane_drain_supervisor(
        Arc::clone(shared),
        &shutdown_bus,
        shared.tuning.shutdown.deadline(),
    );

    // Final WAL fsync. Every append before this phase is buffered until an
    // fsync covers it, and a write whose caller never awaited durability
    // (a Calvin applied marker, a background maintenance record) has only
    // this fsync between it and a graceful exit. Registered at startup so the
    // bus cannot pass `WalFsync` without it.
    spawn_wal_fsync_barrier(Arc::clone(shared), &shutdown_bus);

    // Close the cluster's QUIC endpoint once the Raft loops stopped, so peers
    // see the close at once and the node's UDP port is free for a restart.
    spawn_transport_close(Arc::clone(shared), &shutdown_bus);

    // Test-only injection: if NODEDB_TEST_SLOW_DRAIN_TASK=1, register a drain
    // task that sleeps for 2s without calling report_drained, to verify the
    // offender-abort path in integration tests. This code path is guarded
    // by an env var so it is never activated in production.
    if std::env::var("NODEDB_TEST_SLOW_DRAIN_TASK").as_deref() == Ok("1") {
        let mut guard = shutdown_bus.register_task(
            nodedb::control::shutdown::ShutdownPhase::DrainingListeners,
            "test_slow_task",
            None,
        );
        tokio::spawn(async move {
            guard.await_signal().await;
            // Intentionally do NOT call report_drained — tests the offender path.
            tokio::time::sleep(std::time::Duration::from_secs(2)).await;
            drop(guard); // This will log the "dropped without report_drained" warning.
        });
    }

    (shutdown_rx, shutdown_bus, loop_registry_supervisors)
}

/// Register the `WalFsync` participant: once the phase starts, fsync the
/// WAL on a blocking thread, then report drained.
fn spawn_wal_fsync_barrier(shared: Arc<SharedState>, shutdown_bus: &ShutdownBus) {
    let mut guard =
        shutdown_bus.register_critical_task(ShutdownPhase::WalFsync, "wal::final_fsync");
    tokio::spawn(async move {
        guard.await_signal().await;
        let wal = Arc::clone(&shared.wal);
        match tokio::task::spawn_blocking(move || wal.sync()).await {
            Ok(Ok(())) => tracing::info!("shutdown: WAL fsynced"),
            Ok(Err(error)) => {
                tracing::error!(%error, "shutdown: final WAL fsync failed; restart replays only what reached disk")
            }
            Err(error) => {
                tracing::error!(%error, "shutdown: final WAL fsync task did not complete")
            }
        }
        guard.report_drained();
    });
}

/// Register the cluster transport's close at `WalFsync`, after every Raft
/// loop stopped in an earlier phase. A node with no cluster transport
/// registers nothing.
fn spawn_transport_close(shared: Arc<SharedState>, shutdown_bus: &ShutdownBus) {
    let Some(transport) = shared.cluster_transport.clone() else {
        return;
    };
    let mut guard =
        shutdown_bus.register_task(ShutdownPhase::WalFsync, "cluster::transport_close", None);
    tokio::spawn(async move {
        guard.await_signal().await;
        if transport
            .close(nodedb::control::shutdown::PHASE_BUDGET)
            .await
        {
            tracing::info!("shutdown: cluster transport closed");
        } else {
            tracing::warn!(
                "shutdown: cluster transport closed, but peers did not acknowledge the close \
                 within the phase budget"
            );
        }
        guard.report_drained();
    });
}

/// Drain phases that own registry loops, in shutdown order, each with the
/// bus task name of its barrier.
///
/// Control Plane first: a loop that dispatches to the Data Plane must be
/// joined before `DrainingDataPlane` closes the enqueue gate. Data Plane
/// next: the response poller services that drain and outlives the gate.
/// Event Plane last: those loops touch Event Plane state alone.
const LOOP_REGISTRY_DRAIN_PHASES: [(ShutdownPhase, &str); 3] = [
    (
        ShutdownPhase::DrainingControlPlane,
        "shutdown::loop_registry::control_plane",
    ),
    (
        ShutdownPhase::DrainingDataPlane,
        "shutdown::loop_registry::data_plane",
    ),
    (
        ShutdownPhase::DrainingEventPlane,
        "shutdown::loop_registry::event_plane",
    ),
];

/// Register one canonical LoopRegistry drain barrier per phase and own the
/// tasks outside the registry itself, so strict shutdown never attempts to
/// join its own supervisor.
///
/// Each barrier joins only the loops registered at its own phase and leaves
/// the rest registered for a later barrier. The first barrier to run closes
/// the registry to new registrations.
fn spawn_loop_registry_shutdown_supervisors(
    loop_registry: Arc<LoopRegistry>,
    shutdown: Arc<ShutdownWatch>,
    deadline: std::time::Duration,
    shutdown_bus: &ShutdownBus,
) -> Vec<tokio::task::JoinHandle<()>> {
    LOOP_REGISTRY_DRAIN_PHASES
        .iter()
        .map(|&(phase, task_name)| {
            spawn_phase_barrier(PhaseBarrier {
                loop_registry: Arc::clone(&loop_registry),
                shutdown: Arc::clone(&shutdown),
                phase,
                task_name,
                deadline,
                shutdown_bus,
            })
        })
        .collect()
}

/// Inputs for one phase's drain barrier.
struct PhaseBarrier<'a> {
    loop_registry: Arc<LoopRegistry>,
    shutdown: Arc<ShutdownWatch>,
    phase: ShutdownPhase,
    task_name: &'static str,
    deadline: std::time::Duration,
    shutdown_bus: &'a ShutdownBus,
}

/// Spawn the critical drain barrier for one phase.
fn spawn_phase_barrier(params: PhaseBarrier<'_>) -> tokio::task::JoinHandle<()> {
    let PhaseBarrier {
        loop_registry,
        shutdown,
        phase,
        task_name,
        deadline,
        shutdown_bus,
    } = params;
    let mut guard = shutdown_bus.register_critical_task(phase, task_name);

    tokio::spawn(async move {
        guard.await_signal().await;
        let report = loop_registry
            .shutdown_phase_strict(&shutdown, phase, deadline)
            .await;
        if report.is_clean() {
            tracing::info!(
                phase = %phase,
                clean = report.exited_clean.len(),
                total = ?report.total,
                "background loops of this phase exited cleanly before the shutdown deadline"
            );
        } else {
            tracing::error!(
                phase = %phase,
                clean = report.exited_clean.len(),
                laggards = ?report.laggards,
                total = ?report.total,
                "background loops exceeded the shutdown deadline but all handles terminated before this phase completed"
            );
        }
        guard.report_drained();
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use nodedb::control::shutdown::{LoopHandle, ShutdownPhase};

    use super::*;

    #[tokio::test]
    async fn direct_bus_initiation_waits_for_strict_no_abort_loop() {
        let shutdown = Arc::new(ShutdownWatch::new());
        let loop_registry = Arc::new(LoopRegistry::new());
        let (shutdown_bus, mut shutdown_handle) = ShutdownBus::new(Arc::clone(&shutdown));
        let supervisors = spawn_loop_registry_shutdown_supervisors(
            Arc::clone(&loop_registry),
            Arc::clone(&shutdown),
            Duration::from_millis(20),
            &shutdown_bus,
        );

        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        let held_loop = tokio::spawn(async move {
            let _ = release_rx.await;
        });
        loop_registry
            .register(
                "test::held_no_abort",
                ShutdownPhase::DrainingEventPlane,
                LoopHandle::AsyncNoAbort(held_loop),
            )
            .expect("late registration before shutdown must be included");
        assert_eq!(
            loop_registry.live_count(),
            1,
            "supervisor must not self-register"
        );

        let sequencer = shutdown_bus.initiate();
        shutdown_handle
            .await_phase(ShutdownPhase::DrainingEventPlane)
            .await;
        tokio::time::sleep(Duration::from_millis(600)).await;
        assert_eq!(
            shutdown_bus.current_phase(),
            ShutdownPhase::DrainingEventPlane,
            "strict no-abort loop must block later shutdown phases"
        );

        release_tx.send(()).expect("release held loop");
        shutdown_handle.await_phase(ShutdownPhase::Closed).await;
        sequencer.await.expect("shutdown sequencer");
        for supervisor in supervisors {
            supervisor.await.expect("loop registry supervisor");
        }
    }
}
