// SPDX-License-Identifier: BUSL-1.1

//! Cluster readiness gate: raft election wait, catalog sanity check, peer warm-up.

use std::sync::Arc;
use std::time::{Duration, Instant};

use tracing::info;

use nodedb_types::config::tuning::{DataGroupRecoveryTimeout, RaftReadyTimeout};

use crate::bootstrap::schema_rehydrate::rehydrate_schema_registry;
use crate::control::startup::ReadyGate;
use crate::control::state::SharedState;

/// All readiness gates passed to [`await_cluster_ready`].
pub struct ClusterReadyGates {
    pub raft_gate: ReadyGate,
    pub schema_gate: ReadyGate,
    pub sanity_gate: ReadyGate,
    pub data_groups_gate: ReadyGate,
    pub transport_gate: ReadyGate,
    pub warm_peers_gate: ReadyGate,
    pub health_loop_gate: ReadyGate,
    pub gateway_enable_gate: ReadyGate,
}

/// Wait for the metadata raft group to be ready, run catalog sanity checks,
/// warm the QUIC peer cache, and fire the remaining startup gates.
pub async fn await_cluster_ready(
    shared: &Arc<SharedState>,
    mut raft_ready_rx: tokio::sync::watch::Receiver<bool>,
    data_plane_replay_done: Vec<tokio::sync::oneshot::Receiver<crate::Result<()>>>,
    gates: ClusterReadyGates,
    raft_ready_timeout: RaftReadyTimeout,
    data_group_recovery_timeout: DataGroupRecoveryTimeout,
) -> anyhow::Result<()> {
    let ClusterReadyGates {
        raft_gate,
        schema_gate,
        sanity_gate,
        data_groups_gate,
        transport_gate,
        warm_peers_gate,
        health_loop_gate,
        gateway_enable_gate,
    } = gates;
    // Boot-time readiness gate: wait until the metadata raft group has
    // applied its first entry on this node before opening any
    // client-facing listener. This eliminates the restart-window race
    // where the first DDL observes `metadata propose: not leader`
    // because the election has not yet completed.
    wait_for_raft_ready(
        shared,
        &mut raft_ready_rx,
        &raft_gate,
        raft_ready_timeout,
        RAFT_READY_POLL_INTERVAL,
    )
    .await?;
    raft_gate.fire();

    // Authoritatively rehydrate the Data Plane per-core schema registry
    // from the durable catalog.
    // This is NOT a raft-replay side effect: it enumerates every active
    // stored collection and re-registers it directly, awaited and
    // fail-closed, so no client listener can open against a collection
    // whose schema (including strict-mode `StrictSchema`) hasn't been
    // re-registered to every Data Plane core after a restart.
    // Reconstruct index identity records for catalogs written before the
    // registry existed, so every index those catalogs hold is listable and
    // droppable. Idempotent, and a no-op on a catalog that already has them.
    crate::bootstrap::index_registry_seed::seed_index_registry(shared);

    if let Err(e) = rehydrate_schema_registry(shared).await {
        schema_gate.fail(format!("schema registry rehydration failed: {e}"));
        return Err(anyhow::anyhow!("schema registry rehydration failed: {e}"));
    }
    // The metadata applier skips entries it already applied, so the
    // post-apply register never re-runs at boot.
    if let Err(e) =
        crate::control::server::shared::ddl::neutral::continuous_agg::register_persisted_continuous_aggregates(
            shared,
        )
        .await
    {
        schema_gate.fail(format!("continuous aggregate re-registration failed: {e}"));
        return Err(anyhow::anyhow!(
            "continuous aggregate re-registration failed: {e}"
        ));
    }
    schema_gate.fire();

    // Catalog sanity check: applied-index gate, redb
    // cross-table integrity, and in-memory registry ⇔ redb
    // verification. Any unrepairable divergence or any redb
    // integrity violation aborts startup.
    let verify_report = match crate::control::cluster::verify_and_repair(shared).await {
        Ok(report) => report,
        Err(e) => {
            sanity_gate.fail(format!("catalog sanity check could not run: {e}"));
            return Err(anyhow::anyhow!("catalog sanity check could not run: {e}"));
        }
    };
    if verify_report.is_acceptable() {
        info!(report = %verify_report, "catalog sanity check passed");
    } else {
        sanity_gate.fail(format!("catalog sanity check failed: {verify_report}"));
        return Err(anyhow::anyhow!(
            "catalog sanity check failed: {verify_report}"
        ));
    }
    sanity_gate.fire();

    // Wait for every Data Plane core to finish `replay_all_wal` before opening
    // the client gateway. Each core rebuilds its in-memory indexes (HNSW, etc.)
    // from the WAL on its own thread; `/healthz` must not report ready until
    // that is done, or a just-restarted node would serve queries against
    // half-rebuilt indexes (e.g. an empty vector search). A dropped sender
    // means a core panicked during open/replay, and a signalled error means
    // its replay stopped short of the WAL — fail closed on both, exactly as
    // the raft-readiness gate does, rather than open the gateway on a broken
    // core.
    const REPLAY_READY_TIMEOUT: Duration = Duration::from_secs(300);
    let replay_wait = async {
        for rx in data_plane_replay_done {
            rx.await
                .map_err(|_| {
                    anyhow::anyhow!(
                        "data plane core exited before signalling WAL replay completion"
                    )
                })?
                .map_err(|e| anyhow::anyhow!("data plane core WAL replay failed: {e}"))?;
        }
        Ok::<_, anyhow::Error>(())
    };
    match tokio::time::timeout(REPLAY_READY_TIMEOUT, replay_wait).await {
        Ok(Ok(())) => {
            info!("all data plane cores completed WAL replay");
        }
        Ok(Err(e)) => {
            data_groups_gate.fail(format!("data plane WAL replay failed: {e}"));
            return Err(e);
        }
        Err(_) => {
            data_groups_gate.fail(format!(
                "data plane WAL replay did not complete within {REPLAY_READY_TIMEOUT:?}"
            ));
            return Err(anyhow::anyhow!(
                "data plane WAL replay timeout after {REPLAY_READY_TIMEOUT:?}"
            ));
        }
    }

    // WAL replay only rebuilds what the NodeDB WAL holds. Writes to
    // replicable collections are durable as entries in their data group's
    // Raft log instead, and their engine state comes back only when each
    // group — after winning its own post-restart election — re-delivers that
    // log to the applier. Metadata readiness says nothing about those
    // elections, so without this wait the gateway can open while a data
    // group's engines are still empty and an acknowledged write reads back as
    // if it never happened. Fail closed, like the replay wait above.
    if let Err(e) = crate::bootstrap::data_group_recovery::await_data_group_recovery(
        shared,
        data_group_recovery_timeout,
    )
    .await
    {
        data_groups_gate.fail(format!("data raft group recovery failed: {e}"));
        return Err(e);
    }

    // Retry every owed reclaim before the gateway opens. A row that does not
    // reclaim keeps the drain hold its retry took, so a same-name CREATE waits
    // for the worker's retry and never opens over storage the retry erases.
    // Only an unreadable queue fails the boot.
    if let Err(error) = crate::event::collection_gc::pending_reclaim::drain_at_boot(shared).await {
        data_groups_gate.fail(format!(
            "pending collection reclaim queue unreadable: {error}"
        ));
        return Err(anyhow::anyhow!(
            "pending collection reclaim queue unreadable during startup: {error}"
        ));
    }

    // A compaction applied before a crash, or whose fan-out failed, is still
    // owed. A row that fails again stays queued for the retry worker: stale
    // history answers old-version reads its peers refuse, and never corrupts
    // current state.
    match crate::control::catalog_entry::post_apply::drain_pending_compactions(shared).await {
        Ok(0) => {}
        Ok(still_owed) => tracing::warn!(
            still_owed,
            "history compactions still owed after the boot drain; the retry worker re-drives them"
        ),
        Err(error) => {
            data_groups_gate.fail(format!("owed history compactions unreadable: {error}"));
            return Err(anyhow::anyhow!(
                "owed history compactions unreadable during startup: {error}"
            ));
        }
    }

    // Cleanup owed for nodes that left: their leases and drains block DDL
    // until released. A row still owed after this pass is re-driven by the
    // retry worker.
    match crate::control::lease::leave_cleanup::drain_pending_leave_cleanups(shared).await {
        Ok(0) => {}
        Ok(still_owed) => tracing::warn!(
            still_owed,
            "leave cleanups still owed after the boot drain; the retry worker re-drives them"
        ),
        Err(error) => {
            data_groups_gate.fail(format!("owed leave cleanups unreadable: {error}"));
            return Err(anyhow::anyhow!(
                "owed leave cleanups unreadable during startup: {error}"
            ));
        }
    }

    // Grants and hierarchy edges live in collections the data groups just
    // finished replaying. Load them before the gateway opens, so no statement
    // plans against an empty permission cache.
    if let Err(error) = crate::bootstrap::permission_tree_load::load_permission_trees(shared).await
    {
        data_groups_gate.fail(format!("permission tree load failed: {error}"));
        return Err(anyhow::anyhow!(
            "permission tree load failed during startup: {error}"
        ));
    }

    // Resume or compensate an interrupted MOVE TENANT before any client
    // connects. Its drain ends and its re-capture both propose through the
    // metadata group and read the data groups, so both must be ready first.
    crate::control::server::shared::ddl::neutral::tenant::move_tenant::recovery::recover_all(
        shared,
    )
    .await;

    data_groups_gate.fire();
    transport_gate.fire();

    // Warm the QUIC peer cache so the first replicated request
    // after boot doesn't pay a cold dial.
    if let (Some(transport), Some(topology)) = (
        shared.cluster_transport.as_ref(),
        shared.cluster_topology.as_ref(),
    ) {
        // Clone the topology snapshot so the read guard is dropped
        // before awaiting — clippy::await_holding_lock.
        let topo_snapshot = {
            let guard = topology.read().unwrap_or_else(|p| p.into_inner());
            guard.clone()
        };
        let warm_report = crate::control::cluster::warm_known_peers(
            transport,
            &topo_snapshot,
            shared.node_id,
            Duration::from_secs(2),
        )
        .await;
        if warm_report.attempted > 0 {
            info!(report = %warm_report, "peer cache warm-up complete");
            if !warm_report.is_complete() {
                for (id, err) in &warm_report.failed {
                    tracing::warn!(node_id = id, error = %err, "peer warm failed");
                }
            }
        }
    }
    warm_peers_gate.fire();
    health_loop_gate.fire();

    // A node plans permission-checked statements only under an
    // authorization lease. The renewal loop runs from Raft start, and every
    // input of a grant is live by now: the Raft groups, the replayed data
    // groups, the permission cache and the Event Plane. The gateway opens
    // once the first lease is granted, so the first statements are not
    // refused.
    let admitted = match crate::control::security::auth_lease::barrier::lease_timing(shared) {
        Ok(_) => {
            crate::control::security::auth_lease::await_planning_admitted(
                shared,
                PLANNING_ADMISSION_TIMEOUT,
            )
            .await
        }
        Err(error) => Err(error),
    };
    if let Err(error) = admitted {
        gateway_enable_gate.fail(format!("authorization lease not granted: {error}"));
        return Err(anyhow::anyhow!(
            "authorization lease not granted during startup: {error}"
        ));
    }
    gateway_enable_gate.fire();

    Ok(())
}

/// How often the stall check samples the applied index while waiting.
const RAFT_READY_POLL_INTERVAL: Duration = Duration::from_secs(1);

/// How long boot waits for the first authorization lease before refusing to
/// open the gateway.
///
/// Not the metadata group's stall bound, and not configurable with it: this is
/// a hard deadline on one grant, and it does not reset. The lease loop runs from
/// Raft start, so by the time boot reaches it every input of a grant is live;
/// 30 seconds is generous for a grant that is going to arrive at all.
const PLANNING_ADMISSION_TIMEOUT: Duration = Duration::from_secs(30);

/// Wait until the metadata raft group applies its first entry, and fail
/// `raft_gate` when it does not. See [`raft_ready_progress`].
async fn wait_for_raft_ready(
    shared: &Arc<SharedState>,
    ready_rx: &mut tokio::sync::watch::Receiver<bool>,
    raft_gate: &ReadyGate,
    stall_timeout: RaftReadyTimeout,
    poll_interval: Duration,
) -> anyhow::Result<()> {
    match raft_ready_progress(shared, ready_rx, stall_timeout.0, poll_interval).await {
        Ok(()) => {
            info!("metadata raft group ready — opening client listeners");
            Ok(())
        }
        Err(error) => {
            raft_gate.fail(error.to_string());
            Err(error.into())
        }
    }
}

/// Wait until the metadata raft group that `start_raft` started applies its
/// first entry. `ready_rx` is the receiver `start_raft` returned.
///
/// The same wait boot runs before it opens a listener. An in-process host
/// that registers no startup gate calls this before it serves requests.
///
/// `stall_timeout` is the caller's bound. Boot passes the configured
/// `[tuning.startup] raft_ready_timeout_ms` into [`await_cluster_ready`]; a
/// host with no config decides for itself rather than inheriting a value it
/// never read.
pub async fn await_raft_ready(
    shared: &Arc<SharedState>,
    mut ready_rx: tokio::sync::watch::Receiver<bool>,
    stall_timeout: RaftReadyTimeout,
) -> crate::Result<()> {
    raft_ready_progress(
        shared,
        &mut ready_rx,
        stall_timeout.0,
        RAFT_READY_POLL_INTERVAL,
    )
    .await
}

/// Wait until the metadata raft group applies its first entry.
///
/// Bounds the wait on lack of PROGRESS, not on total elapsed time. A node
/// replaying a large log keeps advancing `applied_index` and must be allowed
/// to finish; a group that is genuinely stuck advances nothing and fails
/// after `stall_timeout`.
async fn raft_ready_progress(
    shared: &Arc<SharedState>,
    ready_rx: &mut tokio::sync::watch::Receiver<bool>,
    stall_timeout: Duration,
    poll_interval: Duration,
) -> crate::Result<()> {
    let applied_index = || {
        shared
            .metadata_cache
            .read()
            .map(|c| c.applied_index)
            .unwrap_or_else(|p| p.into_inner().applied_index)
    };
    let mut last_progress = Instant::now();
    let mut last_index = applied_index();

    loop {
        match tokio::time::timeout(poll_interval, ready_rx.wait_for(|v| *v)).await {
            Ok(Ok(_)) => return Ok(()),
            Ok(Err(_)) => {
                return Err(crate::Error::Internal {
                    detail: "raft readiness watch dropped before signalling ready".into(),
                });
            }
            // Not ready yet. Replay that is still advancing is healthy.
            Err(_) => {
                let current = applied_index();
                if current != last_index {
                    last_index = current;
                    last_progress = Instant::now();
                    continue;
                }
                if last_progress.elapsed() >= stall_timeout {
                    return Err(crate::Error::Internal {
                        detail: format!(
                            "metadata group applied no entry for {stall_timeout:?} \
                             (applied_index stuck at {current})"
                        ),
                    });
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::startup::{StartupPhase, StartupSequencer};
    use std::time::Duration;

    /// A `SharedState` with no live Data Plane — this test only reads the
    /// metadata cache's applied index.
    fn test_shared_state() -> Arc<SharedState> {
        let dir = tempfile::tempdir().expect("tmpdir");
        let wal = Arc::new(
            crate::wal::WalManager::open_for_testing(&dir.path().join("raft-ready.wal"))
                .expect("open wal"),
        );
        let (dispatcher, _data_sides) = crate::bridge::Dispatcher::new(1, 64);
        SharedState::new(dispatcher, wal).expect("build shared state")
    }

    /// A replay that keeps applying entries must not be failed, however long
    /// it runs. The stall bound measures lack of progress, not elapsed time.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_replay_that_keeps_making_progress_is_never_failed() {
        let shared = test_shared_state();
        let (sequencer, _gate) = StartupSequencer::new();
        let raft_gate = sequencer.register_gate(StartupPhase::RaftMetadataReplay, "raft");
        let (ready_tx, mut ready_rx) = tokio::sync::watch::channel(false);

        // Advance the applied index for well past the stall bound, then
        // signal ready. A wall-clock bound would have failed this boot.
        let cache = Arc::clone(&shared.metadata_cache);
        let ticker = tokio::spawn(async move {
            for index in 1..=20u64 {
                tokio::time::sleep(Duration::from_millis(20)).await;
                cache
                    .write()
                    .unwrap_or_else(|p| p.into_inner())
                    .advance_applied_index(index);
            }
            let _ = ready_tx.send(true);
        });

        let result = wait_for_raft_ready(
            &shared,
            &mut ready_rx,
            &raft_gate,
            RaftReadyTimeout(Duration::from_millis(100)),
            Duration::from_millis(10),
        )
        .await;
        ticker.await.expect("progress ticker");
        assert!(
            result.is_ok(),
            "a progressing replay must be allowed to finish: {result:?}"
        );
    }

    /// A group applying nothing fails once the stall bound elapses.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_group_that_applies_nothing_fails_after_the_stall_bound() {
        let shared = test_shared_state();
        let (sequencer, _gate) = StartupSequencer::new();
        let raft_gate = sequencer.register_gate(StartupPhase::RaftMetadataReplay, "raft");
        let (_ready_tx, mut ready_rx) = tokio::sync::watch::channel(false);

        let err = wait_for_raft_ready(
            &shared,
            &mut ready_rx,
            &raft_gate,
            RaftReadyTimeout(Duration::from_millis(100)),
            Duration::from_millis(10),
        )
        .await
        .expect_err("a stuck group must fail the boot");
        assert!(
            err.to_string().contains("applied no entry"),
            "the failure must name the stall, not a generic timeout: {err}"
        );
    }

    /// `await_raft_ready` reports the bound its caller passes, not a default.
    ///
    /// This pins the argument into the metadata gate only. It does not cover
    /// the `await_cluster_ready` call in `main.rs`, where the two bounds arrive
    /// as adjacent arguments of different types.
    #[tokio::test(flavor = "multi_thread")]
    async fn the_bound_a_caller_passes_is_the_bound_the_failure_reports() {
        let shared = test_shared_state();
        let (_ready_tx, ready_rx) = tokio::sync::watch::channel(false);

        let err = await_raft_ready(
            &shared,
            ready_rx,
            RaftReadyTimeout(Duration::from_millis(50)),
        )
        .await
        .expect_err("a group that applies nothing must fail");
        assert!(
            err.to_string().contains("50ms"),
            "the failure must name the caller's bound, not the shipped default: {err}"
        );
    }
}
