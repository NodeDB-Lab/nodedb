// SPDX-License-Identifier: BUSL-1.1
//! `MOVE TENANT` on a three-node cluster with two Data-Plane cores per node.
//!
//! A collection's home vShard hashes its database. Moving a collection to
//! another database therefore moves its rows to another vShard: in general
//! another core and another Raft group. The test seeds several collections,
//! so the source homes spread across vShards, moves them, and reads every row
//! back through the target database from each node. Each node must then hold
//! nothing under the source key and no pending reclaim of it.

use std::collections::BTreeSet;
use std::time::Duration;

use nodedb::control::security::sessions::{KillReason, SessionParams};
use nodedb_types::id::VShardId;
use nodedb_types::{CollectionKey, DatabaseId};

use crate::common;
use common::cluster_harness::TestCluster;
use common::cluster_harness::shared_steps::{database_id, db_detail, use_database};

const SOURCE: &str = "mt_cl_src";
const TARGET: &str = "mt_cl_tgt";
const MOVED_TENANT: &str = "mt_cl_owner";
const ROWS: usize = 5;
const DOC_COLLECTIONS: [&str; 3] = ["mt_docs_a", "mt_docs_b", "mt_docs_c"];
const KV_COLLECTIONS: [&str; 3] = ["mt_kv_a", "mt_kv_b", "mt_kv_c"];

/// Create every collection in `database`, with the same schema in each.
async fn create_collections(cluster: &TestCluster, database: &str) {
    use_database(cluster, database).await;
    for name in DOC_COLLECTIONS {
        let ddl = format!(
            "CREATE COLLECTION {name} (id TEXT PRIMARY KEY, content TEXT) \
             WITH (engine='document_strict')"
        );
        cluster
            .exec_ddl_on_any_leader(&ddl)
            .await
            .unwrap_or_else(|e| panic!("{ddl} in {database}: {e}"));
    }
    for name in KV_COLLECTIONS {
        let ddl = format!(
            "CREATE COLLECTION {name} (key STRING PRIMARY KEY, value STRING) WITH (engine='kv')"
        );
        cluster
            .exec_ddl_on_any_leader(&ddl)
            .await
            .unwrap_or_else(|e| panic!("{ddl} in {database}: {e}"));
    }
}

/// Fill every source collection through node 0. Each row's text names its
/// collection and its key.
async fn fill_source(cluster: &TestCluster) {
    use_database(cluster, SOURCE).await;
    for i in 0..ROWS {
        for name in DOC_COLLECTIONS {
            let sql = format!("INSERT INTO {name} (id, content) VALUES ('k{i}', '{name}-{i}')");
            cluster.nodes[0]
                .client
                .simple_query(&sql)
                .await
                .unwrap_or_else(|e| panic!("{sql}: {}", db_detail(&e)));
        }
        for name in KV_COLLECTIONS {
            let sql = format!("INSERT INTO {name} (key, value) VALUES ('k{i}', '{name}-{i}')");
            cluster.nodes[0]
                .client
                .simple_query(&sql)
                .await
                .unwrap_or_else(|e| panic!("{sql}: {}", db_detail(&e)));
        }
    }
}

/// The first column of every row `sql` returns on node `node_idx`.
async fn first_column(cluster: &TestCluster, node_idx: usize, sql: &str) -> Vec<String> {
    let messages = cluster.nodes[node_idx]
        .client
        .simple_query(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql} on node {node_idx}: {}", db_detail(&e)));
    messages
        .into_iter()
        .filter_map(|message| match message {
            tokio_postgres::SimpleQueryMessage::Row(row) => row.get(0).map(str::to_owned),
            _ => None,
        })
        .collect()
}

fn every_collection() -> impl Iterator<Item = &'static str> {
    DOC_COLLECTIONS.into_iter().chain(KV_COLLECTIONS)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn move_tenant_rows_are_readable_through_the_target_from_every_node() {
    let cluster = TestCluster::spawn_three_with_cores(2)
        .await
        .expect("cluster");

    for database in [SOURCE, TARGET] {
        cluster
            .exec_ddl_on_any_leader(&format!("CREATE DATABASE {database}"))
            .await
            .unwrap_or_else(|e| panic!("CREATE DATABASE {database}: {e}"));
    }
    create_collections(&cluster, SOURCE).await;
    create_collections(&cluster, TARGET).await;
    fill_source(&cluster).await;
    cluster
        .wait_for_full_apply_convergence(Duration::from_secs(10))
        .await;

    // The source homes spread across vShards, and the moved rows change
    // vShard: they must travel to the target homes.
    let source_id = database_id(&cluster.nodes[0], SOURCE);
    let target_id = database_id(&cluster.nodes[0], TARGET);
    let home = |db: DatabaseId, name: &str| {
        VShardId::from_collection(CollectionKey::from_bare(db, name)).as_u32()
    };
    let source_homes: BTreeSet<u32> = every_collection().map(|n| home(source_id, n)).collect();
    assert!(
        source_homes.len() > 1,
        "the source collections must home to more than one vShard: {source_homes:?}"
    );
    assert!(
        every_collection().any(|n| home(source_id, n) != home(target_id, n)),
        "at least one collection must change vShard when it moves"
    );

    use_database(&cluster, "default").await;
    cluster
        .exec_ddl_on_any_leader(&format!("CREATE TENANT {MOVED_TENANT} ID 77"))
        .await
        .unwrap_or_else(|e| panic!("CREATE TENANT {MOVED_TENANT}: {e}"));
    // One session of the moved tenant on every node, and one of another tenant.
    let session = |tenant_id: u64| SessionParams {
        user_id: 5,
        username: "moved_user".to_string(),
        db_user: "moved_user".to_string(),
        peer_addr: "127.0.0.1:1".to_string(),
        protocol: "test".to_string(),
        auth_method: "password".to_string(),
        tenant_id,
        credential_version: 0,
        current_database: None,
        token_expiry_ms: None,
        api_key_id: None,
    };
    let moved_sessions: Vec<_> = cluster
        .nodes
        .iter()
        .map(|node| {
            node.shared
                .session_registry
                .register("moved", &session(77))
                .expect("register")
        })
        .collect();
    let other_sessions: Vec<_> = cluster
        .nodes
        .iter()
        .map(|node| {
            node.shared
                .session_registry
                .register("other", &session(78))
                .expect("register")
        })
        .collect();

    cluster
        .exec_ddl_on_any_leader(&format!(
            "MOVE TENANT {MOVED_TENANT} FROM {SOURCE} TO {TARGET}"
        ))
        .await
        .unwrap_or_else(|e| panic!("MOVE TENANT {MOVED_TENANT}: {e}"));
    cluster
        .wait_for_full_apply_convergence(Duration::from_secs(10))
        .await;

    // The cutover's post-apply ends the tenant's sessions on every node.
    for (node_idx, kill) in moved_sessions.iter().enumerate() {
        assert_eq!(*kill.borrow(), KillReason::AdminKill, "node {node_idx}");
    }
    for (node_idx, kill) in other_sessions.iter().enumerate() {
        assert_eq!(*kill.borrow(), KillReason::Alive, "node {node_idx}");
    }

    // Every row, read through the target from every node.
    use_database(&cluster, TARGET).await;
    let expected_keys: Vec<String> = (0..ROWS).map(|i| format!("k{i}")).collect();
    for node_idx in 0..cluster.nodes.len() {
        for name in DOC_COLLECTIONS {
            let mut ids = first_column(&cluster, node_idx, &format!("SELECT id FROM {name}")).await;
            ids.sort();
            assert_eq!(ids, expected_keys, "{TARGET}.{name} on node {node_idx}");
            for i in 0..ROWS {
                let sql = format!("SELECT content FROM {name} WHERE id = 'k{i}'");
                assert_eq!(
                    first_column(&cluster, node_idx, &sql).await,
                    vec![format!("{name}-{i}")],
                    "{sql} on node {node_idx}"
                );
            }
        }
        for name in KV_COLLECTIONS {
            let count =
                first_column(&cluster, node_idx, &format!("SELECT COUNT(*) FROM {name}")).await;
            assert_eq!(
                count,
                vec![ROWS.to_string()],
                "{TARGET}.{name} on node {node_idx}"
            );
            for i in 0..ROWS {
                let sql = format!("SELECT value FROM {name} WHERE key = 'k{i}'");
                assert_eq!(
                    first_column(&cluster, node_idx, &sql).await,
                    vec![format!("{name}-{i}")],
                    "{sql} on node {node_idx}"
                );
            }
        }
    }

    // The source namespace is gone, and every node reclaimed its storage.
    use_database(&cluster, SOURCE).await;
    for (node_idx, node) in cluster.nodes.iter().enumerate() {
        for name in every_collection() {
            let sql = format!("SELECT COUNT(*) FROM {name}");
            if let Ok(messages) = node.client.simple_query(&sql).await {
                let count: Vec<String> = messages
                    .into_iter()
                    .filter_map(|m| match m {
                        tokio_postgres::SimpleQueryMessage::Row(row) => {
                            row.get(0).map(str::to_owned)
                        }
                        _ => None,
                    })
                    .collect();
                assert!(
                    count.is_empty() || count == vec!["0".to_string()],
                    "{SOURCE}.{name} on node {node_idx} must hold no rows, got {count:?}"
                );
            }
        }
        let pending = node
            .shared
            .credentials
            .catalog()
            .load_pending_reclaim_queue()
            .expect("pending-reclaim read");
        let owed: Vec<&str> = pending
            .iter()
            .filter(|entry| entry.database_id == source_id.as_u64())
            .map(|entry| entry.name.as_str())
            .collect();
        assert!(
            owed.is_empty(),
            "node {node_idx} must reclaim the source storage without a pending retry: {owed:?}"
        );
        let source_rows = node
            .shared
            .credentials
            .catalog()
            .load_all_collections(source_id)
            .expect("catalog read");
        assert!(
            source_rows.iter().all(|c| !c.is_active),
            "node {node_idx} must hold no active source collection"
        );
    }

    cluster.shutdown().await;
}
