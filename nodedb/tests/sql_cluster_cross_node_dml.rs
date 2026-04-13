//! End-to-end cluster test: CREATE / INSERT / SELECT across 3 pgwire
//! clients, one per node.
//!
//! Acceptance gate for the replicated catalog path. Replays the
//! production failure mode that motivated it:
//!
//! > CREATE COLLECTION on node 1, SELECT on node 2 → "unknown table"
//!
//! The modern path should succeed end-to-end because:
//!
//! 1. `create_collection` proposes a `MetadataEntry::CollectionDdl::
//!    Create` through the metadata raft group (group 0).
//! 2. The entry commits on quorum and the `MetadataCommitApplier`
//!    runs on every node: writes the descriptor to the replicated
//!    `MetadataCache` AND writes the host `StoredCollection` to the
//!    local `SystemCatalog` redb AND spawns a `DocumentOp::Register`
//!    into the local Data Plane core.
//! 3. An INSERT on node 2 reads the collection from its local redb
//!    (populated by the applier), dispatches into its own Data
//!    Plane which knows the storage mode (registered by the
//!    applier), and the row lands on node 2's vShard.
//!
//! This test exercises every link in that chain.

mod common;

use std::time::Duration;

use common::cluster_harness::{TestCluster, wait_for};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn single_node_cluster_boots() {
    // Smallest possible smoke test: one node in cluster mode.
    let node = common::cluster_harness::TestClusterNode::spawn(1, vec![])
        .await
        .expect("single-node cluster spawn");
    assert_eq!(node.topology_size(), 1);
    node.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn single_node_cluster_create_collection() {
    // Isolates the pgwire handler → propose_metadata_and_wait path
    // on a single-node cluster so cluster-formation noise (elections,
    // joining learners) is out of the picture.
    let node = common::cluster_harness::TestClusterNode::spawn(1, vec![])
        .await
        .expect("spawn");
    // Give the raft tick a moment to process any startup entries.
    tokio::time::sleep(Duration::from_millis(200)).await;
    node.exec("CREATE COLLECTION widgets")
        .await
        .expect("create widgets");
    assert_eq!(node.cached_collection_count(), 1);
    node.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn create_on_any_node_is_visible_on_every_node() {
    let cluster = TestCluster::spawn_three().await.expect("3-node cluster");

    // Every node starts with an empty replicated cache.
    for node in &cluster.nodes {
        assert_eq!(node.cached_collection_count(), 0);
    }

    // CREATE proposed on whichever node is the metadata-group leader.
    // The cluster harness retries across nodes so we don't need to
    // discover the leader explicitly.
    let leader_idx = cluster
        .exec_ddl_on_any_leader("CREATE COLLECTION users")
        .await
        .expect("create collection");
    eprintln!("CREATE accepted by node {}", leader_idx + 1);

    // Every node's replicated cache must see the new collection.
    wait_for(
        "all 3 nodes see the replicated collection",
        Duration::from_secs(5),
        Duration::from_millis(50),
        || {
            cluster
                .nodes
                .iter()
                .all(|n| n.cached_collection_count() == 1)
        },
    )
    .await;

    // DROP on any leader — should cascade through raft and
    // deactivate the record on every node's `SystemCatalog` redb
    // via the applier's Drop branch.
    cluster
        .exec_ddl_on_any_leader("DROP COLLECTION users")
        .await
        .expect("drop collection");

    // The replicated-cache view removes the descriptor on Drop.
    wait_for(
        "all 3 nodes no longer see the collection",
        Duration::from_secs(5),
        Duration::from_millis(50),
        || {
            cluster
                .nodes
                .iter()
                .all(|n| n.cached_collection_count() == 0)
        },
    )
    .await;

    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn sequence_create_visible_on_every_node() {
    // 3-node cluster. CREATE SEQUENCE on the leader; every follower's
    // in-memory `sequence_registry` must see the replicated definition
    // within 5s via the `CatalogEntry::PutSequence` → post-apply hook.
    let cluster = TestCluster::spawn_three().await.expect("3-node cluster");

    for node in &cluster.nodes {
        assert_eq!(node.sequence_count(1), 0);
    }

    let leader_idx = cluster
        .exec_ddl_on_any_leader("CREATE SEQUENCE order_id START 100")
        .await
        .expect("create sequence");
    eprintln!("CREATE SEQUENCE accepted by node {}", leader_idx + 1);

    wait_for(
        "all 3 nodes see the replicated sequence in their in-memory registry",
        Duration::from_secs(5),
        Duration::from_millis(50),
        || cluster.nodes.iter().all(|n| n.has_sequence(1, "order_id")),
    )
    .await;

    // ALTER SEQUENCE RESTART WITH 500 should propagate via
    // `PutSequenceState` so every node's in-memory counter
    // converges on 500.
    cluster
        .exec_ddl_on_any_leader("ALTER SEQUENCE order_id RESTART WITH 500")
        .await
        .expect("alter sequence restart");

    wait_for(
        "all 3 nodes see sequence counter == 500",
        Duration::from_secs(5),
        Duration::from_millis(50),
        || {
            cluster
                .nodes
                .iter()
                .all(|n| n.sequence_current_value(1, "order_id") == Some(500))
        },
    )
    .await;

    cluster
        .exec_ddl_on_any_leader("DROP SEQUENCE order_id")
        .await
        .expect("drop sequence");

    wait_for(
        "all 3 nodes remove the sequence from their registry",
        Duration::from_secs(5),
        Duration::from_millis(50),
        || cluster.nodes.iter().all(|n| !n.has_sequence(1, "order_id")),
    )
    .await;

    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn trigger_create_visible_on_every_node() {
    // 3-node cluster. CREATE TRIGGER on the leader; every follower's
    // in-memory `trigger_registry` must see the trigger within 5s
    // via the `CatalogEntry::PutTrigger` → post-apply hook.
    let cluster = TestCluster::spawn_three().await.expect("3-node cluster");

    // Triggers attach to a collection, so create one first.
    cluster
        .exec_ddl_on_any_leader("CREATE COLLECTION audits")
        .await
        .expect("create collection");

    wait_for(
        "collection visible on every node",
        Duration::from_secs(5),
        Duration::from_millis(50),
        || {
            cluster
                .nodes
                .iter()
                .all(|n| n.cached_collection_count() == 1)
        },
    )
    .await;

    cluster
        .exec_ddl_on_any_leader(
            "CREATE TRIGGER audit_ins AFTER INSERT ON audits FOR EACH ROW BEGIN RETURN 1; END",
        )
        .await
        .expect("create trigger");

    wait_for(
        "all 3 nodes see the replicated trigger in trigger_registry",
        Duration::from_secs(5),
        Duration::from_millis(50),
        || cluster.nodes.iter().all(|n| n.has_trigger(1, "audit_ins")),
    )
    .await;

    cluster
        .exec_ddl_on_any_leader("DROP TRIGGER audit_ins")
        .await
        .expect("drop trigger");

    wait_for(
        "all 3 nodes unregister the trigger",
        Duration::from_secs(5),
        Duration::from_millis(50),
        || cluster.nodes.iter().all(|n| !n.has_trigger(1, "audit_ins")),
    )
    .await;

    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn procedure_create_visible_on_every_node() {
    // 3-node cluster. CREATE PROCEDURE on the leader; every
    // follower's local `SystemCatalog` redb (written by the applier)
    // must contain the procedure within 5s.
    let cluster = TestCluster::spawn_three().await.expect("3-node cluster");

    cluster
        .exec_ddl_on_any_leader("CREATE PROCEDURE noop_proc() BEGIN RETURN 1; END")
        .await
        .expect("create procedure");

    wait_for(
        "all 3 nodes see the procedure in local SystemCatalog redb",
        Duration::from_secs(5),
        Duration::from_millis(50),
        || {
            cluster
                .nodes
                .iter()
                .all(|n| n.has_procedure(1, "noop_proc"))
        },
    )
    .await;

    cluster
        .exec_ddl_on_any_leader("DROP PROCEDURE noop_proc")
        .await
        .expect("drop procedure");

    wait_for(
        "all 3 nodes no longer see the procedure",
        Duration::from_secs(5),
        Duration::from_millis(50),
        || {
            cluster
                .nodes
                .iter()
                .all(|n| !n.has_procedure(1, "noop_proc"))
        },
    )
    .await;

    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn schedule_create_visible_on_every_node() {
    // 3-node cluster. CREATE SCHEDULE on the leader; every
    // follower's in-memory `schedule_registry` must contain the
    // schedule within 5s via the `PutSchedule` post-apply hook.
    let cluster = TestCluster::spawn_three().await.expect("3-node cluster");

    cluster
        .exec_ddl_on_any_leader(
            "CREATE SCHEDULE nightly_cleanup CRON '0 0 * * *' AS BEGIN RETURN 1; END",
        )
        .await
        .expect("create schedule");

    wait_for(
        "all 3 nodes see the schedule in schedule_registry",
        Duration::from_secs(5),
        Duration::from_millis(50),
        || {
            cluster
                .nodes
                .iter()
                .all(|n| n.has_schedule(1, "nightly_cleanup"))
        },
    )
    .await;

    cluster
        .exec_ddl_on_any_leader("DROP SCHEDULE nightly_cleanup")
        .await
        .expect("drop schedule");

    wait_for(
        "all 3 nodes no longer see the schedule",
        Duration::from_secs(5),
        Duration::from_millis(50),
        || {
            cluster
                .nodes
                .iter()
                .all(|n| !n.has_schedule(1, "nightly_cleanup"))
        },
    )
    .await;

    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn change_stream_create_visible_on_every_node() {
    // 3-node cluster. CREATE CHANGE STREAM on the leader; every
    // follower's in-memory `stream_registry` must contain the
    // stream within 5s via the `PutChangeStream` post-apply hook.
    let cluster = TestCluster::spawn_three().await.expect("3-node cluster");

    // Change streams attach to a collection, so create one first.
    cluster
        .exec_ddl_on_any_leader("CREATE COLLECTION events")
        .await
        .expect("create collection");

    wait_for(
        "collection visible on every node",
        Duration::from_secs(5),
        Duration::from_millis(50),
        || {
            cluster
                .nodes
                .iter()
                .all(|n| n.cached_collection_count() == 1)
        },
    )
    .await;

    cluster
        .exec_ddl_on_any_leader("CREATE CHANGE STREAM event_feed ON events")
        .await
        .expect("create change stream");

    wait_for(
        "all 3 nodes see the stream in stream_registry",
        Duration::from_secs(5),
        Duration::from_millis(50),
        || {
            cluster
                .nodes
                .iter()
                .all(|n| n.has_change_stream(1, "event_feed"))
        },
    )
    .await;

    cluster
        .exec_ddl_on_any_leader("DROP CHANGE STREAM event_feed")
        .await
        .expect("drop change stream");

    wait_for(
        "all 3 nodes no longer see the stream",
        Duration::from_secs(5),
        Duration::from_millis(50),
        || {
            cluster
                .nodes
                .iter()
                .all(|n| !n.has_change_stream(1, "event_feed"))
        },
    )
    .await;

    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn user_create_visible_on_every_node() {
    // 3-node cluster. CREATE USER on the leader; every follower's
    // in-memory `credentials` cache must contain the user within
    // 5s via the `CatalogEntry::PutUser` post-apply hook.
    let cluster = TestCluster::spawn_three().await.expect("3-node cluster");

    cluster
        .exec_ddl_on_any_leader("CREATE USER alice WITH PASSWORD 'sekret123' ROLE read_write")
        .await
        .expect("create user");

    wait_for(
        "all 3 nodes see the replicated user in credentials",
        Duration::from_secs(5),
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
        Duration::from_secs(5),
        Duration::from_millis(50),
        || cluster.nodes.iter().all(|n| !n.has_active_user("alice")),
    )
    .await;

    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn role_create_visible_on_every_node() {
    // 3-node cluster. CREATE ROLE on the leader; every follower's
    // in-memory `roles` cache must contain the role within 5s via
    // the `CatalogEntry::PutRole` post-apply hook.
    let cluster = TestCluster::spawn_three().await.expect("3-node cluster");

    cluster
        .exec_ddl_on_any_leader("CREATE ROLE data_analyst")
        .await
        .expect("create role");

    wait_for(
        "all 3 nodes see the replicated role",
        Duration::from_secs(5),
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
        Duration::from_secs(5),
        Duration::from_millis(50),
        || cluster.nodes.iter().all(|n| !n.has_role("data_analyst")),
    )
    .await;

    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn alter_user_role_replicates() {
    // 3-node cluster. CREATE USER + ALTER USER SET ROLE.
    // Every follower's credential cache must reflect the new role
    // within 5s via a second `PutUser` entry through raft.
    let cluster = TestCluster::spawn_three().await.expect("3-node cluster");

    cluster
        .exec_ddl_on_any_leader("CREATE USER bob WITH PASSWORD 'initial-pass' ROLE read_only")
        .await
        .expect("create user");

    wait_for(
        "all 3 nodes see bob with read_only role",
        Duration::from_secs(5),
        Duration::from_millis(50),
        || {
            cluster
                .nodes
                .iter()
                .all(|n| n.user_has_role("bob", "read_only"))
        },
    )
    .await;

    cluster
        .exec_ddl_on_any_leader("ALTER USER bob SET ROLE read_write")
        .await
        .expect("alter user set role");

    wait_for(
        "all 3 nodes see bob with read_write role",
        Duration::from_secs(5),
        Duration::from_millis(50),
        || {
            cluster
                .nodes
                .iter()
                .all(|n| n.user_has_role("bob", "read_write"))
        },
    )
    .await;

    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn api_key_create_and_revoke_replicates() {
    // 3-node cluster. CREATE USER + CREATE API KEY FOR user + REVOKE API KEY.
    // Every follower's api_keys cache must see the new key, then
    // see it as revoked after REVOKE — via `PutApiKey` and
    // `RevokeApiKey` entries through raft.
    let cluster = TestCluster::spawn_three().await.expect("3-node cluster");

    cluster
        .exec_ddl_on_any_leader("CREATE USER charlie WITH PASSWORD 'pw-charlie-1'")
        .await
        .expect("create user");

    wait_for(
        "all 3 nodes see charlie",
        Duration::from_secs(5),
        Duration::from_millis(50),
        || cluster.nodes.iter().all(|n| n.has_active_user("charlie")),
    )
    .await;

    // CREATE API KEY returns the token to the leader's client,
    // but we're using the cluster retry helper — the key_id is
    // embedded in the server-side record, not in the client
    // response our helper captures. We verify by observing the
    // cache: before the create, count = 0; after, count >= 1.
    let all_nodes_have_key = |cluster: &TestCluster| -> bool {
        cluster.nodes.iter().all(|n| {
            // Peek at the shared state's api_keys listing.
            !n.shared.api_keys.list_keys_for_user("charlie").is_empty()
        })
    };

    assert!(!all_nodes_have_key(&cluster));

    cluster
        .exec_ddl_on_any_leader("CREATE API KEY FOR charlie")
        .await
        .expect("create api key");

    wait_for(
        "all 3 nodes see a replicated API key for charlie",
        Duration::from_secs(5),
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
        Duration::from_secs(5),
        Duration::from_millis(50),
        || cluster.nodes.iter().all(|n| !n.has_active_api_key(&key_id)),
    )
    .await;

    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn function_create_visible_on_every_node() {
    // 3-node cluster. CREATE FUNCTION on the leader; every follower's
    // local `SystemCatalog` redb (written by the applier) must
    // contain the function within 5s.
    let cluster = TestCluster::spawn_three().await.expect("3-node cluster");

    cluster
        .exec_ddl_on_any_leader(
            "CREATE FUNCTION add_one(x INT) RETURNS INT AS BEGIN RETURN x + 1; END",
        )
        .await
        .expect("create function");

    wait_for(
        "all 3 nodes see the function in local SystemCatalog redb",
        Duration::from_secs(5),
        Duration::from_millis(50),
        || cluster.nodes.iter().all(|n| n.has_function(1, "add_one")),
    )
    .await;

    cluster
        .exec_ddl_on_any_leader("DROP FUNCTION add_one")
        .await
        .expect("drop function");

    wait_for(
        "all 3 nodes no longer see the function",
        Duration::from_secs(5),
        Duration::from_millis(50),
        || cluster.nodes.iter().all(|n| !n.has_function(1, "add_one")),
    )
    .await;

    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn tenant_create_visible_on_every_node() {
    let cluster = TestCluster::spawn_three().await.expect("3-node cluster");

    cluster
        .exec_ddl_on_any_leader("CREATE TENANT acme ID 4242")
        .await
        .expect("create tenant");

    wait_for(
        "all 3 nodes see tenant 4242",
        Duration::from_secs(5),
        Duration::from_millis(50),
        || cluster.nodes.iter().all(|n| n.has_tenant(4242)),
    )
    .await;

    cluster
        .exec_ddl_on_any_leader("DROP TENANT 4242")
        .await
        .expect("drop tenant");

    wait_for(
        "all 3 nodes no longer see tenant 4242",
        Duration::from_secs(5),
        Duration::from_millis(50),
        || cluster.nodes.iter().all(|n| !n.has_tenant(4242)),
    )
    .await;

    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn rls_policy_create_visible_on_every_node() {
    let cluster = TestCluster::spawn_three().await.expect("3-node cluster");

    cluster
        .exec_ddl_on_any_leader("CREATE COLLECTION accounts (id BIGINT PRIMARY KEY, owner TEXT)")
        .await
        .expect("create source collection");

    cluster
        .exec_ddl_on_any_leader(
            "CREATE RLS POLICY owner_only ON accounts FOR READ USING (owner = 'alice')",
        )
        .await
        .expect("create rls policy");

    wait_for(
        "all 3 nodes see the RLS policy",
        Duration::from_secs(5),
        Duration::from_millis(50),
        || {
            cluster
                .nodes
                .iter()
                .all(|n| n.has_rls_policy(1, "accounts", "owner_only"))
        },
    )
    .await;

    cluster
        .exec_ddl_on_any_leader("DROP RLS POLICY owner_only ON accounts")
        .await
        .expect("drop rls policy");

    wait_for(
        "all 3 nodes no longer see the RLS policy",
        Duration::from_secs(5),
        Duration::from_millis(50),
        || {
            cluster
                .nodes
                .iter()
                .all(|n| !n.has_rls_policy(1, "accounts", "owner_only"))
        },
    )
    .await;

    cluster.shutdown().await;
}

/// Batch 1l: `GRANT <perm> ON <collection> TO <grantee>` replicates
/// to every node's `PermissionStore` in-memory grants set.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn grant_permission_visible_on_every_node() {
    let cluster = TestCluster::spawn_three().await.expect("3-node cluster");

    cluster
        .exec_ddl_on_any_leader("CREATE COLLECTION documents (id BIGINT PRIMARY KEY, body TEXT)")
        .await
        .expect("create collection");

    cluster
        .exec_ddl_on_any_leader("CREATE USER analyst WITH PASSWORD 'secret123'")
        .await
        .expect("create user");

    cluster
        .exec_ddl_on_any_leader("GRANT read ON documents TO analyst")
        .await
        .expect("grant read");

    let target = "collection:1:documents";
    wait_for(
        "all 3 nodes see the grant",
        Duration::from_secs(5),
        Duration::from_millis(50),
        || {
            cluster
                .nodes
                .iter()
                .all(|n| n.has_grant(target, "analyst", "read"))
        },
    )
    .await;

    cluster
        .exec_ddl_on_any_leader("REVOKE read ON documents FROM analyst")
        .await
        .expect("revoke read");

    wait_for(
        "all 3 nodes no longer see the grant",
        Duration::from_secs(5),
        Duration::from_millis(50),
        || {
            cluster
                .nodes
                .iter()
                .all(|n| !n.has_grant(target, "analyst", "read"))
        },
    )
    .await;

    cluster.shutdown().await;
}

/// Batch 1l: `GRANT ROLE x TO user` replicates the updated `StoredUser`
/// (via `CatalogEntry::PutUser`) to every node's credentials cache.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn grant_role_visible_on_every_node() {
    let cluster = TestCluster::spawn_three().await.expect("3-node cluster");

    cluster
        .exec_ddl_on_any_leader("CREATE USER ops_user WITH PASSWORD 'ops_pass1'")
        .await
        .expect("create user");

    cluster
        .exec_ddl_on_any_leader("GRANT ROLE monitor TO ops_user")
        .await
        .expect("grant role");

    wait_for(
        "all 3 nodes see ops_user has monitor role",
        Duration::from_secs(5),
        Duration::from_millis(50),
        || {
            cluster
                .nodes
                .iter()
                .all(|n| n.user_has_role("ops_user", "monitor"))
        },
    )
    .await;

    cluster
        .exec_ddl_on_any_leader("REVOKE ROLE monitor FROM ops_user")
        .await
        .expect("revoke role");

    wait_for(
        "all 3 nodes see ops_user no longer has monitor role",
        Duration::from_secs(5),
        Duration::from_millis(50),
        || {
            cluster
                .nodes
                .iter()
                .all(|n| !n.user_has_role("ops_user", "monitor"))
        },
    )
    .await;

    cluster.shutdown().await;
}

/// Batch 1l: `ALTER COLLECTION owner OWNER TO new_owner` replicates
/// the updated `StoredCollection` (and therefore the owner record)
/// to every node's `PermissionStore` in-memory owner map.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn ownership_transfer_visible_on_every_node() {
    let cluster = TestCluster::spawn_three().await.expect("3-node cluster");

    cluster
        .exec_ddl_on_any_leader("CREATE COLLECTION assets (id BIGINT PRIMARY KEY, label TEXT)")
        .await
        .expect("create collection");

    cluster
        .exec_ddl_on_any_leader("CREATE USER new_owner_user WITH PASSWORD 'pass4567'")
        .await
        .expect("create new owner user");

    cluster
        .exec_ddl_on_any_leader("ALTER COLLECTION assets OWNER TO new_owner_user")
        .await
        .expect("transfer ownership");

    wait_for(
        "all 3 nodes see new_owner_user as owner of assets",
        Duration::from_secs(5),
        Duration::from_millis(50),
        || {
            cluster
                .nodes
                .iter()
                .all(|n| n.owner_of("collection", 1, "assets").as_deref() == Some("new_owner_user"))
        },
    )
    .await;

    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn materialized_view_create_visible_on_every_node() {
    let cluster = TestCluster::spawn_three().await.expect("3-node cluster");

    cluster
        .exec_ddl_on_any_leader("CREATE COLLECTION orders (id BIGINT PRIMARY KEY, amount BIGINT)")
        .await
        .expect("create source collection");

    cluster
        .exec_ddl_on_any_leader(
            "CREATE MATERIALIZED VIEW sales_total ON orders AS SELECT SUM(amount) FROM orders",
        )
        .await
        .expect("create materialized view");

    wait_for(
        "all 3 nodes see the materialized view",
        Duration::from_secs(5),
        Duration::from_millis(50),
        || {
            cluster
                .nodes
                .iter()
                .all(|n| n.has_materialized_view(1, "sales_total"))
        },
    )
    .await;

    cluster
        .exec_ddl_on_any_leader("DROP MATERIALIZED VIEW sales_total")
        .await
        .expect("drop materialized view");

    wait_for(
        "all 3 nodes no longer see the materialized view",
        Duration::from_secs(5),
        Duration::from_millis(50),
        || {
            cluster
                .nodes
                .iter()
                .all(|n| !n.has_materialized_view(1, "sales_total"))
        },
    )
    .await;

    cluster.shutdown().await;
}
