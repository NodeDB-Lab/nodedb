// SPDX-License-Identifier: BUSL-1.1
//! Database lifecycle across a 3-node cluster.
//!
//! - `DROP DATABASE ... CASCADE` on one node leaves no row of the database in
//!   any node's catalog, and the database stays dropped after every node
//!   restarts and replays the metadata log.
//! - Database ids come from the replicated metadata log: every node agrees
//!   on each id, and no id repeats across a full restart.

use crate::common;

use std::time::Duration;

use common::cluster_harness::shared_steps::use_database;
use common::cluster_harness::{TestCluster, TestClusterNode};
use nodedb_types::DatabaseId;

const TENANT: u64 = 1;
const DROPPED: &str = "dl_dropped";

/// The id every node's catalog holds for `name`. Panics when a node lacks
/// the database or two nodes disagree on its id.
fn agreed_database_id(cluster: &TestCluster, name: &str) -> DatabaseId {
    let ids: Vec<DatabaseId> = cluster
        .nodes
        .iter()
        .map(|node| {
            node.shared
                .credentials
                .catalog()
                .get_database_id_by_name(name)
                .expect("look up database id")
                .unwrap_or_else(|| panic!("node {} lacks database '{name}'", node.node_id))
        })
        .collect();
    assert!(
        ids.windows(2).all(|pair| pair[0] == pair[1]),
        "nodes disagree on the id of database '{name}': {ids:?}"
    );
    ids[0]
}

/// Every catalog row this node still holds for database `id`, named by kind.
fn rows_for_database(node: &TestClusterNode, name: &str, id: DatabaseId) -> Vec<String> {
    let catalog = node.shared.credentials.catalog();
    let db = id.as_u64();
    let mut rows = Vec::new();
    if catalog
        .get_database_id_by_name(name)
        .expect("look up database name")
        .is_some()
    {
        rows.push(format!("databases_by_name:{name}"));
    }
    if catalog.get_database(id).expect("read database").is_some() {
        rows.push(format!("databases:{db}"));
    }
    for coll in catalog.load_all_collections(id).expect("load collections") {
        rows.push(format!("collection:{}", coll.name));
    }
    for seq in catalog
        .load_sequences_in_database(db)
        .expect("load sequences")
    {
        rows.push(format!("sequence:{}", seq.name));
    }
    for owner in catalog
        .load_all_owners()
        .expect("load owners")
        .into_iter()
        .filter(|owner| owner.database_id == db)
    {
        rows.push(format!("owner:{}:{}", owner.object_type, owner.object_name));
    }
    if node.shared.sequence_registry.exists(db, TENANT, "dl_seq") {
        rows.push("sequence_registry:dl_seq".to_string());
    }
    rows
}

/// Poll every node until it holds no row of database `id`.
async fn assert_dropped_everywhere(cluster: &TestCluster, id: DatabaseId, stage: &str) {
    cluster
        .wait_for_full_apply_convergence(Duration::from_secs(30))
        .await;
    for node in &cluster.nodes {
        // The collection purge finishes in the apply's post-apply lane, so
        // poll briefly for it before reporting what is left.
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        let mut left = rows_for_database(node, DROPPED, id);
        while !left.is_empty() && std::time::Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(50)).await;
            left = rows_for_database(node, DROPPED, id);
        }
        assert!(
            left.is_empty(),
            "{stage}: node {} still holds rows of dropped database {}: {left:?}",
            node.node_id,
            id.as_u64()
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn drop_database_cascade_removes_rows_on_every_node_and_survives_restart() {
    let cluster = TestCluster::spawn_three()
        .await
        .expect("spawn 3-node cluster");

    cluster
        .exec_ddl_on_any_leader(&format!("CREATE DATABASE {DROPPED}"))
        .await
        .unwrap_or_else(|e| panic!("CREATE DATABASE: {e}"));
    let dropped_id = agreed_database_id(&cluster, DROPPED);

    use_database(&cluster, DROPPED).await;
    for ddl in [
        "CREATE COLLECTION dl_docs (id TEXT PRIMARY KEY, content TEXT) \
         WITH (engine='document_strict')",
        "CREATE COLLECTION dl_kv (key STRING PRIMARY KEY, value STRING) WITH (engine='kv')",
        "CREATE SEQUENCE dl_seq START 1",
    ] {
        cluster
            .exec_ddl_on_any_leader(ddl)
            .await
            .unwrap_or_else(|e| panic!("{ddl}: {e}"));
    }
    cluster.nodes[0]
        .exec("INSERT INTO dl_docs (id, content) VALUES ('k1', 'v1')")
        .await
        .unwrap_or_else(|e| panic!("INSERT: {e}"));
    for node in &cluster.nodes {
        assert!(
            !rows_for_database(node, DROPPED, dropped_id).is_empty(),
            "node {} must hold the database's rows before the drop",
            node.node_id
        );
    }
    use_database(&cluster, "default").await;

    cluster
        .exec_ddl_on_any_leader(&format!("DROP DATABASE {DROPPED} CASCADE"))
        .await
        .unwrap_or_else(|e| panic!("DROP DATABASE CASCADE: {e}"));
    assert_dropped_everywhere(&cluster, dropped_id, "after the drop").await;

    let cluster = cluster
        .restart_all()
        .await
        .unwrap_or_else(|e| panic!("restart every node: {e}"));
    assert_dropped_everywhere(&cluster, dropped_id, "after every node restarted").await;

    // An id issued after the restart never reuses the dropped database's id.
    cluster
        .exec_ddl_on_any_leader("CREATE DATABASE dl_after_restart")
        .await
        .unwrap_or_else(|e| panic!("CREATE DATABASE after restart: {e}"));
    let after = agreed_database_id(&cluster, "dl_after_restart");
    assert!(
        after.as_u64() > dropped_id.as_u64(),
        "database id {} issued after the restart does not exceed the dropped id {}",
        after.as_u64(),
        dropped_id.as_u64()
    );

    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn database_ids_agree_across_nodes_and_never_repeat_after_restart() {
    let cluster = TestCluster::spawn_three()
        .await
        .expect("spawn 3-node cluster");

    let mut issued = Vec::new();
    for name in ["dl_ids_a", "dl_ids_b"] {
        cluster
            .exec_ddl_on_any_leader(&format!("CREATE DATABASE {name}"))
            .await
            .unwrap_or_else(|e| panic!("CREATE DATABASE {name}: {e}"));
        issued.push(agreed_database_id(&cluster, name));
    }
    // Ids come from this cluster's id space, above the 30 counter bits.
    let space = issued[0].as_u64() >> 30;
    assert_ne!(
        space,
        0,
        "database id {} has no id space",
        issued[0].as_u64()
    );
    assert_eq!(issued[1].as_u64() >> 30, space);
    cluster
        .wait_for_full_apply_convergence(Duration::from_secs(30))
        .await;
    // Every node advanced its hwm, so any node that leads next continues
    // past every id already issued.
    for node in &cluster.nodes {
        let hwm = node.shared.database_registry.current_hwm();
        assert!(
            hwm >= issued[1].as_u64(),
            "node {} hwm {hwm} lags the replicated reservation of {}",
            node.node_id,
            issued[1].as_u64()
        );
    }

    let cluster = cluster
        .restart_all()
        .await
        .unwrap_or_else(|e| panic!("restart every node: {e}"));
    for name in ["dl_ids_c", "dl_ids_d"] {
        cluster
            .exec_ddl_on_any_leader(&format!("CREATE DATABASE {name}"))
            .await
            .unwrap_or_else(|e| panic!("CREATE DATABASE {name}: {e}"));
        issued.push(agreed_database_id(&cluster, name));
    }
    for name in ["dl_ids_a", "dl_ids_b"] {
        agreed_database_id(&cluster, name);
    }

    let mut unique: Vec<u64> = issued.iter().map(|id| id.as_u64()).collect();
    unique.sort_unstable();
    unique.dedup();
    assert_eq!(
        unique.len(),
        issued.len(),
        "a database id repeated across the restart: {issued:?}"
    );
    assert!(
        issued
            .windows(2)
            .all(|pair| pair[0].as_u64() < pair[1].as_u64()),
        "database ids are not increasing across the restart: {issued:?}"
    );

    cluster.shutdown().await;
}
