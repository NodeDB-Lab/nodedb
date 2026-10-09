// SPDX-License-Identifier: BUSL-1.1

//! `start_raft` orchestration: bootstrap the sequencer Raft group and
//! phase-1 dependencies ([`super::group_setup`]), build the cross-plane
//! hooks ([`super::hooks`]), construct the `RaftLoop` and start cluster
//! subsystems ([`super::loop_build`]), wire the sync/async Raft proposer and
//! spawn the apply loop ([`super::proposer_wiring`]), and finally publish
//! observability handles and spawn the tick loop / sequencer service / RPC
//! server / health monitor ([`super::observability`]).

use std::sync::Arc;

use nodedb_types::config::tuning::ClusterTransportTuning;

use crate::control::cluster::handle::ClusterHandle;
use crate::control::state::SharedState;

use super::group_setup::build_group_setup;
use super::hooks::build_hooks;
use super::loop_build::build_raft_loop;
use super::observability::{ObservabilityInputs, finish_observability};
use super::proposer_wiring::wire_proposers;

fn bootstrap_listener_addr(
    mut transport_addr: std::net::SocketAddr,
) -> crate::Result<std::net::SocketAddr> {
    let port = transport_addr
        .port()
        .checked_add(1)
        .ok_or_else(|| crate::Error::Config {
            detail: "cluster transport port 65535 leaves no bootstrap-listener port".into(),
        })?;
    transport_addr.set_port(port);
    Ok(transport_addr)
}

/// Start the Raft event loop and RPC server.
///
/// Must be called after `SharedState` is constructed (needs the WAL and
/// dispatcher for the `SpscCommitApplier`). Moves the `MultiRaft` out of
/// `handle.multi_raft` into the `RaftLoop`; must be called **exactly
/// once** per handle.
pub async fn start_raft(
    handle: &ClusterHandle,
    shared: Arc<SharedState>,
    data_dir: &std::path::Path,
    transport_tuning: &ClusterTransportTuning,
) -> crate::Result<tokio::sync::watch::Receiver<bool>> {
    let (mut multi_raft, setup) = build_group_setup(handle, &shared, data_dir, transport_tuning)?;
    let hooks = build_hooks(
        handle,
        &shared,
        data_dir,
        &mut multi_raft,
        &setup.token_state,
        &setup.sequencer_state_machine,
    )
    .await?;
    let loop_build = build_raft_loop(handle, &shared, data_dir, multi_raft, setup, hooks).await?;

    let bootstrap_raft_loop = Arc::clone(&loop_build.raft_loop);
    let bootstrap_token_state = Arc::clone(&loop_build.token_state);

    wire_proposers(
        &shared,
        &loop_build.raft_loop,
        loop_build.tracker,
        loop_build.apply_rx,
        loop_build.calvin_read_result_senders,
        loop_build.sequencer_state_machine,
    )?;
    // The async proposer is installed: a failed redo stream abandon can be
    // retried from here on.
    crate::control::wal_replication::transaction_redo::spawn_redo_abandoner(&shared);

    let ready_rx = finish_observability(
        handle,
        &shared,
        transport_tuning,
        loop_build.raft_loop,
        ObservabilityInputs {
            sequencer_inbox: loop_build.sequencer_inbox,
            reservation_inbox: loop_build.reservation_inbox,
            sequencer_metrics: loop_build.sequencer_metrics,
            calvin_completion_registry: loop_build.calvin_completion_registry,
            ollp_orchestrator: loop_build.ollp_orchestrator,
            sequencer_service: loop_build.sequencer_service,
        },
    );
    // `finish_observability` installed the metadata raft handle: releases
    // handed off by synchronous code can be proposed from here on.
    crate::control::lease::releaser::spawn_lease_releaser(&shared);
    // The metadata leader reclaims a DDL preparation lease whose owner died,
    // left, or got stuck. It proposes through the same handle.
    crate::control::metadata_proposer::ddl_reclaim::spawn_ddl_lease_reclaimer(&shared);

    if let Some(material) = crate::control::cluster::tls::load_bootstrap_issuer_material(data_dir)?
    {
        let proposer: Arc<dyn nodedb_cluster::decommission::MetadataProposer> = Arc::new(
            crate::control::cluster::bootstrap_listener::BootstrapMetadataProposer::new(
                &bootstrap_raft_loop,
                Arc::clone(&handle.group_watchers),
            ),
        );
        let token_store = Arc::new(nodedb_cluster::RaftBackedTokenStore::new(
            Arc::clone(&proposer),
            bootstrap_token_state,
        ));
        let (listen, _task) = crate::control::cluster::bootstrap_listener::spawn(
            bootstrap_listener_addr(handle.transport.local_addr())?,
            &material,
            crate::control::cluster::bootstrap_listener::BootstrapEnrollment {
                token_store,
                transport: Arc::clone(&handle.transport),
                metadata_proposer: proposer,
            },
            shared.shutdown.raw_receiver(),
        )?;
        tracing::info!(%listen, "durable cluster bootstrap listener started");
    }

    Ok(ready_rx)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bootstrap_listener_uses_a_distinct_deterministic_port() {
        let transport = "127.0.0.1:9400".parse().unwrap();
        assert_eq!(
            bootstrap_listener_addr(transport).unwrap(),
            "127.0.0.1:9401".parse::<std::net::SocketAddr>().unwrap()
        );
        assert!(
            bootstrap_listener_addr("127.0.0.1:65535".parse::<std::net::SocketAddr>().unwrap())
                .is_err()
        );
    }
}
