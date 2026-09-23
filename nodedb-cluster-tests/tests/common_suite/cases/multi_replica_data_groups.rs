// SPDX-License-Identifier: BUSL-1.1
//! Data Raft groups must be genuinely multi-replica.
//!
//! ## What this guards
//!
//! A cluster bootstraps with one founding node and others join. Data groups
//! (ids 1..N, vshard-partitioned) must become real RF-way replicas: every
//! node a voter of every group (for an N <= RF cluster), every node locally
//! applying the group's committed writes, and the shared routing table the
//! data plane reads converging to that membership on every node.
//!
//! Two historical bugs this pins down:
//!   1. A bootstrap clamp forced the stored replication factor to 1, so data
//!      groups stayed single-voter and joiners never replicated their data.
//!   2. Every node held two separate routing tables — the Raft coordinator's
//!      private copy (updated by conf-changes) and the `Arc<RwLock>` the data
//!      plane reads (frozen at the join-time snapshot). Committed
//!      AddLearner/PromoteLearner changes never reached the data-plane view,
//!      so membership was permanently frozen and divergent across nodes.
//!
//! ## Shape
//!
//!  1. Spawn a 3-node cluster (RF defaults to 3), create a `document_strict`
//!     collection, insert rows via one node, converge.
//!  2. Assert the collection's data group has all three nodes as VOTERS on
//!     EVERY node's routing view (no learners left, no divergence).
//!  3. Kill the data group's LEADER and assert both survivors still serve the
//!     full row set — the only confound-free proof that the followers locally
//!     replicated the data rather than routing reads to a single owner.

use crate::common;
use common::cluster_harness::TestCluster;

use std::time::{Duration, Instant};

use nodedb_types::DatabaseId;

const COLL: &str = "mr_data_group";
const ROW_COUNT: u32 = 5;

fn pg_detail(e: &tokio_postgres::Error) -> String {
    if let Some(db) = e.as_db_error() {
        format!("{}: {}", db.code().code(), db.message())
    } else {
        format!("{e}")
    }
}

/// `SELECT COUNT(*)`, retrying transient catch-up errors until `timeout`.
async fn count_rows(client: &tokio_postgres::Client, timeout: Duration) -> Result<usize, String> {
    let deadline = Instant::now() + timeout;
    loop {
        match client
            .simple_query(&format!("SELECT COUNT(*) FROM {COLL}"))
            .await
        {
            Ok(rows) => {
                for msg in rows {
                    if let tokio_postgres::SimpleQueryMessage::Row(r) = msg
                        && let Some(s) = r.get(0)
                    {
                        return Ok(s.parse::<usize>().expect("COUNT(*) parse"));
                    }
                }
                return Err("COUNT(*) returned no rows".to_string());
            }
            Err(ref e) => {
                if Instant::now() < deadline {
                    tokio::time::sleep(Duration::from_millis(150)).await;
                    continue;
                }
                return Err(pg_detail(e));
            }
        }
    }
}

/// Sorted voter list for `group_id` as seen by `node`'s shared routing table.
fn voters_seen_by(node: &common::cluster_harness::TestClusterNode, group_id: u64) -> Vec<u64> {
    let routing = node
        .shared
        .cluster_routing
        .as_ref()
        .expect("cluster_routing")
        .read()
        .unwrap_or_else(|p| p.into_inner());
    let mut v = routing
        .group_info(group_id)
        .map(|i| i.members.clone())
        .unwrap_or_default();
    v.sort_unstable();
    v
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn data_group_is_multi_replica_and_survives_leader_loss() {
    let cluster = TestCluster::spawn_three()
        .await
        .expect("spawn 3-node cluster");

    cluster
        .exec_ddl_on_any_leader(&format!(
            "CREATE COLLECTION {COLL} \
             (id TEXT PRIMARY KEY, payload TEXT) WITH (engine='document_strict')"
        ))
        .await
        .expect("CREATE COLLECTION");

    for i in 0..ROW_COUNT {
        cluster.nodes[0]
            .client
            .simple_query(&format!(
                "INSERT INTO {COLL} (id, payload) VALUES ('row-{i}', 'payload-{i}')"
            ))
            .await
            .unwrap_or_else(|e| panic!("insert row-{i}: {}", pg_detail(&e)));
    }

    cluster
        .wait_for_full_apply_convergence(Duration::from_secs(15))
        .await;

    // Resolve the collection's data group.
    let vshard = nodedb_cluster::routing::vshard_for_collection(DatabaseId::DEFAULT, COLL);
    let group_id = {
        let routing = cluster.nodes[0]
            .shared
            .cluster_routing
            .as_ref()
            .expect("cluster_routing")
            .read()
            .unwrap_or_else(|p| p.into_inner());
        routing
            .group_for_vshard(vshard)
            .expect("collection vshard mapped to a group")
    };
    assert!(
        group_id != 0,
        "collection must map to a data group, not metadata"
    );

    // Every node's routing view must converge to all three nodes as voters
    // (no learners left, no divergence). Bounded poll for the promotion
    // conf-changes to commit + apply through the shared routing table.
    let deadline = Instant::now() + Duration::from_secs(20);
    let all_voters = loop {
        let converged = cluster
            .nodes
            .iter()
            .all(|n| voters_seen_by(n, group_id) == vec![1, 2, 3]);
        if converged {
            break true;
        }
        if Instant::now() >= deadline {
            break false;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    };
    assert!(
        all_voters,
        "data group {group_id} did not converge to [1,2,3] voters on every node; \
         views: {:?}",
        cluster
            .nodes
            .iter()
            .map(|n| (n.node_id, voters_seen_by(n, group_id)))
            .collect::<Vec<_>>()
    );

    // Kill the data group's LEADER. Reading from a survivor afterward is the
    // only confound-free proof of local replication: had the data lived only
    // on a single owner, killing the leader would lose it.
    let group_leader = {
        let routing = cluster.nodes[0]
            .shared
            .cluster_routing
            .as_ref()
            .expect("cluster_routing")
            .read()
            .unwrap_or_else(|p| p.into_inner());
        routing.group_info(group_id).map(|i| i.leader).unwrap_or(0)
    };
    assert!(group_leader != 0, "data group {group_id} has no leader");

    let mut nodes = cluster.nodes;
    let leader_idx = nodes
        .iter()
        .position(|n| n.node_id == group_leader)
        .expect("leader node present");
    nodes.remove(leader_idx).shutdown().await;

    // Survivors re-elect a new leader; give the group a moment to settle.
    tokio::time::sleep(Duration::from_secs(3)).await;

    for node in &nodes {
        let n = count_rows(&node.client, Duration::from_secs(20))
            .await
            .unwrap_or_else(|e| {
                panic!(
                    "survivor node {} could not serve rows after leader death: {e} \
                     => data group was NOT multi-replica",
                    node.node_id
                )
            });
        assert_eq!(
            n, ROW_COUNT as usize,
            "survivor node {} served {n} rows, expected {ROW_COUNT}",
            node.node_id
        );
    }

    for node in nodes {
        node.shutdown().await;
    }
}

const TXN_COLL: &str = "mr_txn_commit";

/// Rows every replica holds once the explicit transaction commits.
const TXN_EXPECTED: [(&str, &str); 4] = [
    ("seed-0", "updated"),
    ("seed-1", "seed"),
    ("txn-0", "inserted-0"),
    ("txn-1", "inserted-1"),
];

/// Leader of `group_id` as `node`'s routing table records it, or `0`.
fn routing_leader(node: &common::cluster_harness::TestClusterNode, group_id: u64) -> u64 {
    let routing = node
        .shared
        .cluster_routing
        .as_ref()
        .expect("cluster_routing")
        .read()
        .unwrap_or_else(|p| p.into_inner());
    routing.group_info(group_id).map(|i| i.leader).unwrap_or(0)
}

/// True when `node` leads `group_id` by its own Raft state.
fn leads(node: &common::cluster_harness::TestClusterNode, group_id: u64) -> bool {
    node.all_group_leaders().contains(&(group_id, node.node_id))
}

/// Index of the node that leads `group_id` by its own Raft state and by every
/// node's routing table.
async fn group_leader_index(
    nodes: &[common::cluster_harness::TestClusterNode],
    group_id: u64,
) -> usize {
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let found = nodes.iter().position(|n| {
            leads(n, group_id)
                && nodes
                    .iter()
                    .all(|m| routing_leader(m, group_id) == n.node_id)
        });
        if let Some(idx) = found {
            return idx;
        }
        if Instant::now() >= deadline {
            panic!("data group {group_id} has no leader agreed by every node within 20s");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// This node's local document entries for `TXN_COLL`, keyed by storage key.
async fn local_txn_documents(
    node: &common::cluster_harness::TestClusterNode,
) -> std::collections::BTreeMap<String, Vec<u8>> {
    let bytes = node
        .create_tenant_snapshot(nodedb_types::TenantId::new(1))
        .await;
    assert!(
        !bytes.is_empty(),
        "node {} returned an empty tenant snapshot",
        node.node_id
    );
    let snapshot: nodedb::types::TenantDataSnapshot =
        zerompk::from_msgpack(&bytes).expect("decode TenantDataSnapshot");
    let marker = format!(":{TXN_COLL}:");
    snapshot
        .documents
        .into_iter()
        .filter(|(key, _)| key.contains(&marker))
        .collect()
}

/// `(id, payload)` rows of `TXN_COLL` served through `client`, sorted by id.
async fn served_txn_rows(client: &tokio_postgres::Client) -> Result<Vec<(String, String)>, String> {
    let msgs = client
        .simple_query(&format!("SELECT id, payload FROM {TXN_COLL}"))
        .await
        .map_err(|e| pg_detail(&e))?;
    let mut rows: Vec<(String, String)> = msgs
        .iter()
        .filter_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(r) => Some((
                r.get("id").unwrap_or_default().to_owned(),
                r.get("payload").unwrap_or_default().to_owned(),
            )),
            _ => None,
        })
        .collect();
    rows.sort();
    Ok(rows)
}

/// Spawn 3 nodes, seed `TXN_COLL`, and commit one single-shard explicit
/// transaction on the node that leads the collection's data group. Returns
/// the cluster, the group id, and the leader's index.
async fn commit_single_shard_txn_on_group_leader() -> (TestCluster, u64, usize) {
    let cluster = TestCluster::spawn_three()
        .await
        .expect("spawn 3-node cluster");
    cluster
        .exec_ddl_on_any_leader(&format!(
            "CREATE COLLECTION {TXN_COLL} WITH (engine='document_schemaless')"
        ))
        .await
        .expect("CREATE COLLECTION");
    for id in ["seed-0", "seed-1"] {
        cluster.nodes[0]
            .client
            .simple_query(&format!(
                "INSERT INTO {TXN_COLL} (id, payload) VALUES ('{id}', 'seed')"
            ))
            .await
            .unwrap_or_else(|e| panic!("seed {id}: {}", pg_detail(&e)));
    }
    cluster
        .wait_for_full_apply_convergence(Duration::from_secs(15))
        .await;

    let group_id = cluster.nodes[0]
        .group_id_for_collection(TXN_COLL)
        .expect("collection vshard mapped to a group");
    let leader_idx = group_leader_index(&cluster.nodes, group_id).await;
    let leader = &cluster.nodes[leader_idx];

    // One vShard, run on its leader: the commit takes the local single-shard path.
    leader
        .client
        .simple_query(&format!(
            "BEGIN; \
             INSERT INTO {TXN_COLL} (id, payload) VALUES ('txn-0', 'inserted-0'); \
             INSERT INTO {TXN_COLL} (id, payload) VALUES ('txn-1', 'inserted-1'); \
             UPDATE {TXN_COLL} SET payload = 'updated' WHERE id = 'seed-0'; \
             COMMIT"
        ))
        .await
        .unwrap_or_else(|e| panic!("single-shard COMMIT on the leader: {}", pg_detail(&e)));
    assert!(
        leads(leader, group_id),
        "node {} lost leadership of group {group_id} during the commit; \
         the commit did not run on the group leader",
        leader.node_id
    );
    cluster
        .wait_for_full_apply_convergence(Duration::from_secs(15))
        .await;
    (cluster, group_id, leader_idx)
}

/// A single-shard explicit transaction committed on the data-group leader
/// reaches every replica's local state.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn single_shard_txn_committed_on_group_leader_reaches_every_replica() {
    let (cluster, _group_id, leader_idx) = commit_single_shard_txn_on_group_leader().await;
    let leader = &cluster.nodes[leader_idx];

    let expected: Vec<(String, String)> = TXN_EXPECTED
        .iter()
        .map(|(id, p)| ((*id).to_owned(), (*p).to_owned()))
        .collect();
    let served = served_txn_rows(&leader.client)
        .await
        .expect("read committed rows on the leader");
    assert_eq!(served, expected, "the leader must serve the committed rows");
    let leader_docs = local_txn_documents(leader).await;
    assert_eq!(
        leader_docs.len(),
        TXN_EXPECTED.len(),
        "the leader must hold every committed row locally"
    );

    for follower in cluster.nodes.iter().filter(|n| n.node_id != leader.node_id) {
        let deadline = Instant::now() + Duration::from_secs(15);
        let follower_docs = loop {
            let docs = local_txn_documents(follower).await;
            if docs == leader_docs || Instant::now() >= deadline {
                break docs;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        };
        let missing: Vec<&String> = leader_docs
            .keys()
            .filter(|k| follower_docs.get(*k) != leader_docs.get(*k))
            .collect();
        assert!(
            missing.is_empty() && follower_docs.len() == leader_docs.len(),
            "follower {} local state differs from leader {} after the committed \
             transaction; rows missing or different on the follower: {missing:?}",
            follower.node_id,
            leader.node_id
        );
    }

    for node in cluster.nodes {
        node.shutdown().await;
    }
}

/// A single-shard explicit transaction committed on the data-group leader
/// survives the loss of that leader.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn single_shard_txn_committed_on_group_leader_survives_leader_loss() {
    let (cluster, group_id, leader_idx) = commit_single_shard_txn_on_group_leader().await;

    let mut nodes = cluster.nodes;
    nodes.remove(leader_idx).shutdown().await;

    // A survivor takes over the data group.
    let deadline = Instant::now() + Duration::from_secs(20);
    while !nodes.iter().any(|n| leads(n, group_id)) {
        if Instant::now() >= deadline {
            panic!("no survivor took over data group {group_id} within 20s");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    let expected: Vec<(String, String)> = TXN_EXPECTED
        .iter()
        .map(|(id, p)| ((*id).to_owned(), (*p).to_owned()))
        .collect();
    for node in &nodes {
        let deadline = Instant::now() + Duration::from_secs(20);
        let served = loop {
            match served_txn_rows(&node.client).await {
                Ok(rows) => break rows,
                Err(e) if Instant::now() >= deadline => {
                    panic!("survivor {} could not serve rows: {e}", node.node_id)
                }
                Err(_) => tokio::time::sleep(Duration::from_millis(150)).await,
            }
        };
        assert_eq!(
            served, expected,
            "survivor {} must serve the committed transaction after the leader is lost",
            node.node_id
        );
    }

    for node in nodes {
        node.shutdown().await;
    }
}
