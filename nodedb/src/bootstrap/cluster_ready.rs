// SPDX-License-Identifier: BUSL-1.1

//! Cluster readiness gate: raft election wait, catalog sanity check, peer warm-up.

use std::sync::Arc;
use std::time::{Duration, Instant};

use tracing::info;

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
///
/// In single-node mode `raft_ready_rx` is `None` and the raft-ready wait is
/// skipped. Gate fires are always performed regardless of cluster mode.
pub async fn await_cluster_ready(
    shared: &Arc<SharedState>,
    raft_ready_rx: Option<tokio::sync::watch::Receiver<bool>>,
    data_plane_replay_done: Vec<tokio::sync::oneshot::Receiver<()>>,
    gates: ClusterReadyGates,
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
    // Boot-time readiness gate: in cluster mode, wait until the
    // metadata raft group has applied its first entry on this node
    // before opening any client-facing listener. This eliminates the
    // restart-window race where the first DDL would observe
    // `metadata propose: not leader` because election had not yet
    // completed.
    if let Some(mut ready_rx) = raft_ready_rx {
        wait_for_raft_ready(
            shared,
            &mut ready_rx,
            &raft_gate,
            RAFT_READY_STALL_TIMEOUT,
            RAFT_READY_POLL_INTERVAL,
        )
        .await?;
    }
    // Metadata raft group has applied its first entry (or we're
    // in single-node mode with no raft).
    raft_gate.fire();

    // Authoritatively rehydrate the Data Plane per-core schema registry
    // from the durable catalog, in both single-node and cluster mode.
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
    // means a core panicked during open/replay — fail closed, exactly as the
    // raft-readiness gate does, rather than open the gateway on a broken core.
    const REPLAY_READY_TIMEOUT: Duration = Duration::from_secs(300);
    let replay_wait = async {
        for rx in data_plane_replay_done {
            rx.await.map_err(|_| {
                anyhow::anyhow!("data plane core exited before signalling WAL replay completion")
            })?;
        }
        Ok::<(), anyhow::Error>(())
    };
    match tokio::time::timeout(REPLAY_READY_TIMEOUT, replay_wait).await {
        Ok(Ok(())) => info!("all data plane cores completed WAL replay"),
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
    if let Err(e) = crate::bootstrap::data_group_recovery::await_data_group_recovery(shared).await {
        data_groups_gate.fail(format!("data raft group recovery failed: {e}"));
        return Err(e);
    }

    // A pending name-scoped reclaim must complete before the gateway opens.
    // Otherwise a same-name CREATE can install a replacement that the delayed
    // retry subsequently erases. Fail readiness and let the operator restart
    // after the underlying storage fault is resolved.
    if let Err(error) = crate::event::collection_gc::pending_reclaim::drain_once(shared).await {
        data_groups_gate.fail(format!("pending collection reclaim failed: {error}"));
        return Err(anyhow::anyhow!(
            "pending collection reclaim failed during startup: {error}"
        ));
    }

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
    gateway_enable_gate.fire();

    Ok(())
}

/// How long the metadata group may make NO replay progress before the boot
/// fails. Reset on every applied-index advance, so a large replay never trips
/// it — only a genuinely stuck group does.
/// How long the metadata group may go without *any* applied entry before the
/// readiness gate fails startup.
///
/// Raised from 30 s after the 2026-09-20 incident: with a large apply backlog
/// (tens of thousands of entries from a burst of cross-shard writes) the group
/// needs minutes, and aborting the start turned a slow boot into a restart
/// loop. The gate still fails a group that never applies anything.
/// Follow-up: make this configurable.
const RAFT_READY_STALL_TIMEOUT: Duration = Duration::from_secs(300);

/// How often the stall check samples the applied index while waiting.
const RAFT_READY_POLL_INTERVAL: Duration = Duration::from_secs(1);

/// Wait until the metadata raft group applies its first entry.
///
/// Bounds the wait on lack of PROGRESS, not on total elapsed time. A node
/// replaying a large log keeps advancing `applied_index` and must be allowed
/// to finish; a group that is genuinely stuck advances nothing and fails
/// after [`RAFT_READY_STALL_TIMEOUT`].
async fn wait_for_raft_ready(
    shared: &Arc<SharedState>,
    ready_rx: &mut tokio::sync::watch::Receiver<bool>,
    raft_gate: &ReadyGate,
    stall_timeout: Duration,
    poll_interval: Duration,
) -> anyhow::Result<()> {
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
            Ok(Ok(_)) => {
                info!("metadata raft group ready — opening client listeners");
                return Ok(());
            }
            Ok(Err(_)) => {
                raft_gate.fail("raft readiness watch dropped before signalling ready");
                return Err(anyhow::anyhow!(
                    "raft readiness watch dropped before signalling ready"
                ));
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
                    let detail = format!(
                        "metadata group applied no entry for {stall_timeout:?} \
                         (applied_index stuck at {current}) — it failed to apply its first entry"
                    );
                    raft_gate.fail(detail.clone());
                    return Err(anyhow::anyhow!(detail));
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
            Duration::from_millis(100),
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
            Duration::from_millis(100),
            Duration::from_millis(10),
        )
        .await
        .expect_err("a stuck group must fail the boot");
        assert!(
            err.to_string().contains("applied no entry"),
            "the failure must name the stall, not a generic timeout: {err}"
        );
    }
}
