// SPDX-License-Identifier: BUSL-1.1

//! Authentication object replication tests: users, roles, API keys.

use std::time::Duration;

use crate::common::cluster_harness::{TestCluster, wait_for};

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn user_create_visible_on_every_node() {
    let cluster = TestCluster::spawn_three().await.expect("3-node cluster");

    cluster
        .exec_ddl_on_any_leader("CREATE USER alice WITH PASSWORD 'sekret123' ROLE readwrite")
        .await
        .expect("create user");

    wait_for(
        "all 3 nodes see the replicated user in credentials",
        Duration::from_secs(10),
        Duration::from_millis(50),
        || cluster.nodes.iter().all(|n| n.has_active_user("alice")),
    )
    .await;

    cluster
        .exec_ddl_on_any_leader("DROP USER alice")
        .await
        .expect("drop user");

    wait_for(
        "all 3 nodes see alice as deactivated",
        Duration::from_secs(10),
        Duration::from_millis(50),
        || cluster.nodes.iter().all(|n| !n.has_active_user("alice")),
    )
    .await;

    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn role_create_visible_on_every_node() {
    let cluster = TestCluster::spawn_three().await.expect("3-node cluster");

    cluster
        .exec_ddl_on_any_leader("CREATE ROLE data_analyst")
        .await
        .expect("create role");

    wait_for(
        "all 3 nodes see the replicated role",
        Duration::from_secs(10),
        Duration::from_millis(50),
        || cluster.nodes.iter().all(|n| n.has_role("data_analyst")),
    )
    .await;

    cluster
        .exec_ddl_on_any_leader("DROP ROLE data_analyst")
        .await
        .expect("drop role");

    wait_for(
        "all 3 nodes no longer see the role",
        Duration::from_secs(10),
        Duration::from_millis(50),
        || cluster.nodes.iter().all(|n| !n.has_role("data_analyst")),
    )
    .await;

    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn alter_user_role_replicates() {
    let cluster = TestCluster::spawn_three().await.expect("3-node cluster");

    for sql in [
        "CREATE ROLE auditor",
        "CREATE USER bob WITH PASSWORD 'initial-pass' ROLE readonly",
    ] {
        cluster
            .exec_ddl_on_any_leader(sql)
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
    }

    wait_for(
        "all 3 nodes see bob with the readonly role",
        Duration::from_secs(10),
        Duration::from_millis(50),
        || {
            cluster
                .nodes
                .iter()
                .all(|n| n.user_has_role("bob", "readonly"))
        },
    )
    .await;

    cluster
        .exec_ddl_on_any_leader("ALTER USER bob SET ROLE auditor")
        .await
        .expect("alter user set role");

    wait_for(
        "all 3 nodes see bob with the custom auditor role",
        Duration::from_secs(10),
        Duration::from_millis(50),
        || {
            cluster
                .nodes
                .iter()
                .all(|n| n.user_has_role("bob", "auditor"))
        },
    )
    .await;

    cluster.shutdown().await;
}

/// A role name that is neither built in nor defined is refused on every
/// entry point, on every node, with SQLSTATE 42704. No node ends up with a
/// user holding it.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn an_undefined_role_is_refused_on_every_node() {
    let cluster = TestCluster::spawn_three().await.expect("3-node cluster");

    cluster
        .exec_ddl_on_any_leader("CREATE USER carol WITH PASSWORD 'carol-pass-1' ROLE readonly")
        .await
        .expect("create carol");

    for node in &cluster.nodes {
        for sql in [
            "CREATE USER dave WITH PASSWORD 'dave-pass-1' ROLE read_write",
            "ALTER USER carol SET ROLE read_write",
            "GRANT ROLE read_write TO carol",
        ] {
            let error = node
                .exec(sql)
                .await
                .expect_err("an undefined role must be refused");
            assert!(
                error.contains("42704") && error.contains("read_write"),
                "node {}: {sql}: expected 42704 naming the role, got {error}",
                node.node_id
            );
        }
    }
    for node in &cluster.nodes {
        assert!(
            !node.has_active_user("dave"),
            "node {} created dave",
            node.node_id
        );
        assert!(
            node.user_has_role("carol", "readonly") && !node.user_has_role("carol", "read_write"),
            "node {} changed carol's roles",
            node.node_id
        );
    }

    cluster.shutdown().await;
}

/// A role a user holds is not dropped, as PostgreSQL refuses: the drop
/// fails with SQLSTATE 2BP01 naming the user. Once no user holds it, the
/// drop succeeds on every node.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn a_held_role_is_not_dropped() {
    let cluster = TestCluster::spawn_three().await.expect("3-node cluster");

    for sql in [
        "CREATE ROLE reviewer",
        "CREATE USER erin WITH PASSWORD 'erin-pass-1' ROLE reviewer",
    ] {
        cluster
            .exec_ddl_on_any_leader(sql)
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
    }

    let error = cluster.nodes[0]
        .exec("DROP ROLE reviewer")
        .await
        .expect_err("a held role must not be dropped");
    assert!(
        error.contains("2BP01") && error.contains("erin"),
        "expected 2BP01 naming the holder, got {error}"
    );
    assert!(
        cluster.nodes.iter().all(|n| n.has_role("reviewer")),
        "the refused drop removed the role on some node"
    );

    for sql in ["ALTER USER erin SET ROLE readonly", "DROP ROLE reviewer"] {
        cluster
            .exec_ddl_on_any_leader(sql)
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
    }
    wait_for(
        "all 3 nodes no longer see the dropped role",
        Duration::from_secs(10),
        Duration::from_millis(50),
        || cluster.nodes.iter().all(|n| !n.has_role("reviewer")),
    )
    .await;

    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn api_key_create_and_revoke_replicates() {
    let cluster = TestCluster::spawn_three().await.expect("3-node cluster");

    cluster
        .exec_ddl_on_any_leader("CREATE USER charlie WITH PASSWORD 'pw-charlie-1'")
        .await
        .expect("create user");

    wait_for(
        "all 3 nodes see charlie",
        Duration::from_secs(10),
        Duration::from_millis(50),
        || cluster.nodes.iter().all(|n| n.has_active_user("charlie")),
    )
    .await;

    let all_nodes_have_key = |cluster: &TestCluster| -> bool {
        cluster
            .nodes
            .iter()
            .all(|n| !n.shared.api_keys.list_keys_for_user("charlie").is_empty())
    };

    assert!(!all_nodes_have_key(&cluster));

    cluster
        .exec_ddl_on_any_leader("CREATE API KEY FOR charlie")
        .await
        .expect("create api key");

    wait_for(
        "all 3 nodes see a replicated API key for charlie",
        Duration::from_secs(10),
        Duration::from_millis(50),
        || all_nodes_have_key(&cluster),
    )
    .await;

    // Pick the key_id from any node's cache and revoke it.
    let key_id = cluster.nodes[0]
        .shared
        .api_keys
        .list_keys_for_user("charlie")
        .first()
        .map(|k| k.key_id.clone())
        .expect("key replicated");

    cluster
        .exec_ddl_on_any_leader(&format!("REVOKE API KEY {key_id}"))
        .await
        .expect("revoke api key");

    wait_for(
        "all 3 nodes see the key as revoked",
        Duration::from_secs(10),
        Duration::from_millis(50),
        || cluster.nodes.iter().all(|n| !n.has_active_api_key(&key_id)),
    )
    .await;

    cluster.shutdown().await;
}
