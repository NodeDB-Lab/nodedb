// SPDX-License-Identifier: BUSL-1.1
//! A credential revoke reaches sessions on every node of a cluster.
//!
//! The revoke commits through the metadata group, so each node applies it and
//! ends its own sessions. The sync case runs against a real listener.

use std::sync::Arc;
use std::time::Duration;

use nodedb::control::security::identity::AuthMethod;
use nodedb::control::security::session_identity::stored_user_identity;
use nodedb::control::security::sessions::{
    KillReason, SessionBindRequest, SessionBinding, bind_session,
};
use nodedb::control::server::sync::listener::{SyncListenerConfig, start_sync_listener};
use nodedb::control::shutdown::{ShutdownBus, ShutdownWatch};
use nodedb_test_support::sync_client::SyncTestClient;

use crate::common::cluster_harness::TestCluster;

const CONVERGE: Duration = Duration::from_secs(10);

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_role_change_and_a_drop_reach_every_nodes_sessions() {
    let cluster = TestCluster::spawn_three().await.expect("cluster");
    cluster
        .exec_ddl_on_any_leader("CREATE USER cross_node PASSWORD 'cross-node-pw'")
        .await
        .expect("create user");
    cluster.wait_for_full_apply_convergence(CONVERGE).await;

    let mut sessions: Vec<(SessionBinding, _)> = cluster
        .nodes
        .iter()
        .map(|node| {
            let identity = stored_user_identity(&node.shared, "cross_node", AuthMethod::Trust)
                .expect("the user reached this node");
            let binding = bind_session(
                &node.shared,
                SessionBindRequest::new(&identity, "test", "127.0.0.1:1"),
            )
            .expect("bind");
            (binding, identity)
        })
        .collect();
    for ((binding, identity), node) in sessions.iter_mut().zip(cluster.nodes.iter()) {
        binding
            .refresh(&node.shared, identity)
            .expect("first request");
    }

    cluster
        .exec_ddl_on_any_leader("GRANT ROLE readwrite TO cross_node")
        .await
        .expect("grant role");
    cluster.wait_for_full_apply_convergence(CONVERGE).await;
    for ((binding, identity), node) in sessions.iter_mut().zip(cluster.nodes.iter()) {
        binding
            .refresh(&node.shared, identity)
            .expect("next request");
        assert!(
            identity
                .roles
                .contains(&nodedb::control::security::identity::Role::ReadWrite),
            "the role change reaches the next request on every node"
        );
    }

    let kills: Vec<_> = sessions
        .iter()
        .map(|(binding, _)| binding.kill_signal())
        .collect();
    cluster
        .exec_ddl_on_any_leader("DROP USER cross_node")
        .await
        .expect("drop user");
    cluster.wait_for_full_apply_convergence(CONVERGE).await;
    for (node_idx, kill) in kills.iter().enumerate() {
        assert_eq!(*kill.borrow(), KillReason::UserDropped, "node {node_idx}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_kill_ends_a_sync_session() {
    let cluster = TestCluster::spawn_three().await.expect("cluster");
    let shared = &cluster.nodes[0].shared;
    let config = SyncListenerConfig {
        listen_addr: std::net::SocketAddr::from(([127, 0, 0, 1], 0)),
        ..Default::default()
    };
    let (shutdown_bus, _shutdown_handle) = ShutdownBus::new(Arc::new(ShutdownWatch::new()));
    let listener = start_sync_listener(config, Some(Arc::clone(shared)), shutdown_bus)
        .await
        .expect("start sync listener");
    let mut client = SyncTestClient::connect(listener.config.listen_addr)
        .await
        .expect("sync handshake");

    let mut session_id = None;
    let deadline = tokio::time::Instant::now() + CONVERGE;
    while session_id.is_none() {
        session_id = shared
            .session_registry
            .list_all()
            .into_iter()
            .find(|session| session.protocol == "sync")
            .map(|session| session.session_id);
        assert!(
            tokio::time::Instant::now() < deadline,
            "the handshake binds a session"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let session_id = session_id.expect("the loop exits with an id");

    shared
        .session_registry
        .kill_session_by_id(&session_id, KillReason::AdminKill);

    let closed = tokio::time::timeout(CONVERGE, async {
        while client.recv_next_frame().await.is_ok() {}
    })
    .await;
    assert!(closed.is_ok(), "the kill closes the sync connection");
}
