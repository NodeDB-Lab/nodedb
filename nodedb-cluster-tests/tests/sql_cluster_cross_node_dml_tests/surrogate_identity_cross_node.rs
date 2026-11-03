// SPDX-License-Identifier: BUSL-1.1

//! A key has one surrogate cluster-wide, whichever node writes it.
//!
//! With a replication factor of 1, a document lives on its collection's home
//! vShard and a graph edge on its endpoints' key vShards, often on three
//! different nodes. Each document here is written through one node, and edges
//! to the same keys through the two others. Every node that binds a key must
//! bind it to the one surrogate the collection home minted, so a RAG fusion,
//! which joins vector hits to graph nodes by surrogate, answers what a single
//! node answers.

use std::collections::{BTreeSet, HashMap};
use std::time::Duration;

use nodedb_types::id::VShardId;
use nodedb_types::{CollectionKey, DatabaseId, TenantId};

use crate::common::cluster_harness::{TestCluster, wait_for};
use crate::common::pgwire_harness::TestServer;

const COLL: &str = "sid_xnode";
const MIN_NODES: usize = 24;

/// Names `k0, k1, …`, enough that their key vShards cover every data group.
fn node_names(groups: &BTreeSet<u64>, group_of: &HashMap<u32, u64>) -> Vec<String> {
    let mut out = Vec::new();
    let mut covered = BTreeSet::new();
    let mut i = 0usize;
    while out.len() < MIN_NODES || covered != *groups {
        let name = format!("k{i}");
        let vshard = VShardId::from_key(name.as_bytes()).as_u32();
        covered.insert(group_of.get(&vshard).copied().unwrap_or(0));
        out.push(name);
        i += 1;
    }
    out
}

fn doc_insert(i: usize, name: &str) -> String {
    let angle = i as f64 * 0.13;
    format!(
        "INSERT INTO {COLL} (id, embedding) VALUES ('{name}', ARRAY[{:.6}, {:.6}, 0.0])",
        angle.cos(),
        angle.sin()
    )
}

fn edge_insert(src: &str, dst: &str) -> String {
    format!("GRAPH INSERT EDGE IN '{COLL}' FROM '{src}' TO '{dst}' TYPE 'rel'")
}

/// The surrogate each node's catalog binds `pk` to, for the nodes that bind it.
fn bindings(cluster: &TestCluster, pk: &str) -> Vec<(usize, u32)> {
    cluster
        .nodes
        .iter()
        .enumerate()
        .filter_map(|(idx, node)| {
            node.shared
                .credentials
                .catalog()
                .get_surrogate_for_pk(
                    CollectionKey::from_bare(DatabaseId::DEFAULT, COLL),
                    TenantId::new(1),
                    pk.as_bytes(),
                )
                .ok()
                .flatten()
                .map(|s| (idx, s.as_u32()))
        })
        .collect()
}

/// The hits of a fusion, as `{"results": [one object per row]}`. A NULL
/// column is `null`, and a column that reads as a number is a number, except
/// `node_id`.
async fn fusion_result(client: &tokio_postgres::Client, sql: &str) -> serde_json::Value {
    let msgs = client
        .simple_query(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
    let mut hits = Vec::new();
    for msg in &msgs {
        let tokio_postgres::SimpleQueryMessage::Row(row) = msg else {
            continue;
        };
        let mut hit = serde_json::Map::new();
        for (i, column) in row.columns().iter().enumerate() {
            let name = column.name();
            let value = match row.get(i) {
                None => serde_json::Value::Null,
                Some(text) if name == "node_id" => serde_json::Value::from(text),
                Some(text) => text
                    .parse::<i64>()
                    .map(serde_json::Value::from)
                    .or_else(|_| text.parse::<f64>().map(serde_json::Value::from))
                    .unwrap_or_else(|_| serde_json::Value::from(text)),
            };
            hit.insert(name.to_string(), value);
        }
        hits.push(serde_json::Value::Object(hit));
    }
    assert!(!hits.is_empty(), "{sql}: no result row");
    serde_json::json!({ "results": hits })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn documents_and_edges_through_different_nodes_share_one_surrogate() {
    let cluster = TestCluster::spawn_three_with_replication_factor(1)
        .await
        .expect("3-node RF1 cluster");
    let ddl = [
        format!("CREATE COLLECTION {COLL}"),
        format!("CREATE VECTOR INDEX idx_{COLL}_emb ON {COLL} METRIC cosine DIM 3"),
    ];
    for statement in &ddl {
        cluster
            .exec_ddl_on_any_leader(statement)
            .await
            .unwrap_or_else(|e| panic!("{statement}: {e}"));
    }
    wait_for(
        "all 3 nodes see the collection",
        Duration::from_secs(10),
        Duration::from_millis(50),
        || {
            cluster
                .nodes
                .iter()
                .all(|n| n.cached_collection_count() >= 1)
        },
    )
    .await;
    let (groups, group_of): (BTreeSet<u64>, HashMap<u32, u64>) = {
        let routing = cluster.nodes[0]
            .shared
            .cluster_routing
            .as_ref()
            .expect("cluster_routing")
            .read()
            .unwrap_or_else(|p| p.into_inner());
        let groups: BTreeSet<u64> = routing
            .group_ids()
            .into_iter()
            .filter(|g| *g != 0)
            .collect();
        let mut map = HashMap::new();
        for &g in &groups {
            for vs in routing.vshards_for_group(g) {
                map.insert(vs, g);
            }
        }
        (groups, map)
    };
    wait_for(
        "each data group has exactly one replica",
        Duration::from_secs(30),
        Duration::from_millis(50),
        || {
            groups.iter().all(|&g| {
                cluster
                    .nodes
                    .iter()
                    .filter(|node| node.replicates_data_group(g))
                    .count()
                    == 1
            })
        },
    )
    .await;
    cluster
        .wait_for_full_apply_convergence(Duration::from_secs(20))
        .await;

    // Key i: its document through node i % 3, an edge out of it through node
    // (i + 1) % 3, and an edge into it through node (i + 2) % 3.
    let names = node_names(&groups, &group_of);
    let n = names.len();
    let mut writes: Vec<(usize, String)> = Vec::new();
    for (i, name) in names.iter().enumerate() {
        writes.push((i % 3, doc_insert(i, name)));
        writes.push(((i + 1) % 3, edge_insert(name, &names[(i + 1) % n])));
        writes.push(((i + 2) % 3, edge_insert(&names[(i + n - 3) % n], name)));
    }

    let reference = TestServer::start().await;
    for statement in &ddl {
        reference
            .exec(statement)
            .await
            .unwrap_or_else(|e| panic!("reference {statement}: {e}"));
    }
    for (node, statement) in &writes {
        reference
            .exec(statement)
            .await
            .unwrap_or_else(|e| panic!("reference {statement}: {e}"));
        cluster.nodes[*node]
            .client
            .simple_query(statement)
            .await
            .unwrap_or_else(|e| panic!("node {node}: {statement}: {e}"));
    }
    cluster
        .wait_for_full_apply_convergence(Duration::from_secs(20))
        .await;

    for name in &names {
        let bound = bindings(&cluster, name);
        assert!(
            !bound.is_empty(),
            "some node binds '{name}': its collection home at least"
        );
        let distinct: BTreeSet<u32> = bound.iter().map(|(_, s)| *s).collect();
        assert_eq!(
            distinct.len(),
            1,
            "every node binds '{name}' to one surrogate, got {bound:?}"
        );
    }

    let sql = format!(
        "GRAPH RAG FUSION ON {COLL} QUERY ARRAY[1.0, 0.0, 0.0] VECTOR_FIELD 'embedding' \
         VECTOR_TOP_K 4 EXPANSION_DEPTH 2 EDGE_LABEL 'rel' FINAL_TOP_K 20 RRF_K (60.0, 10.0)"
    );
    let expected = fusion_result(&reference.client, &sql).await;
    assert!(
        expected["results"]
            .as_array()
            .is_some_and(|r| r.iter().any(|row| row["hop_distance"].is_number())),
        "the reference fusion reaches graph nodes from its vector hits: {expected}"
    );
    for (idx, node) in cluster.nodes.iter().enumerate() {
        assert_eq!(
            fusion_result(&node.client, &sql).await,
            expected,
            "node {idx}: the fusion must equal the single-node answer"
        );
    }

    cluster.shutdown().await;
}

const RACE_COLL: &str = "sid_race";

/// The index of the node leading `key`'s collection home vShard.
fn home_index(cluster: &TestCluster, key: CollectionKey<'_>) -> usize {
    let leader = cluster.nodes[0]
        .shared
        .cluster_routing
        .as_ref()
        .expect("cluster_routing")
        .read()
        .unwrap_or_else(|p| p.into_inner())
        .leader_for_vshard(VShardId::from_collection(key).as_u32())
        .expect("the collection home has a leader");
    cluster
        .nodes
        .iter()
        .position(|node| node.node_id == leader)
        .expect("the home leader is a cluster node")
}

/// Two coordinators that are not the key's home resolve a new key at the same
/// time. Both obtain the one value the home bound, and a third lookup, on a
/// coordinator that never resolved the key, returns it before any write
/// carrying the key applies.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn racing_coordinators_obtain_the_homes_one_surrogate() {
    let cluster = TestCluster::spawn_three_with_replication_factor(1)
        .await
        .expect("3-node RF1 cluster");
    cluster
        .exec_ddl_on_any_leader(&format!("CREATE COLLECTION {RACE_COLL}"))
        .await
        .unwrap_or_else(|e| panic!("CREATE COLLECTION {RACE_COLL}: {e}"));
    wait_for(
        "every data group has a leader",
        Duration::from_secs(30),
        Duration::from_millis(50),
        || {
            cluster.nodes[0]
                .all_group_leaders()
                .iter()
                .all(|(_, leader)| *leader != 0)
        },
    )
    .await;
    cluster
        .wait_for_full_apply_convergence(Duration::from_secs(20))
        .await;

    let key = CollectionKey::from_bare(DatabaseId::DEFAULT, RACE_COLL);
    let tenant = TenantId::new(1);
    let home = home_index(&cluster, key);
    let a = (home + 1) % 3;
    let b = (home + 2) % 3;

    for round in 0..8 {
        let pk = format!("race-{round}");
        let spawn_assign = |idx: usize| {
            let shared = std::sync::Arc::clone(&cluster.nodes[idx].shared);
            let pk = pk.clone();
            tokio::spawn(async move {
                shared
                    .surrogate_assigner
                    .assign(
                        CollectionKey::from_bare(DatabaseId::DEFAULT, RACE_COLL),
                        tenant,
                        pk.as_bytes(),
                    )
                    .await
            })
        };
        let (on_a, on_b) = tokio::join!(spawn_assign(a), spawn_assign(b));
        let on_a = on_a
            .expect("join a")
            .unwrap_or_else(|e| panic!("assign on node {a}: {e}"));
        let on_b = on_b
            .expect("join b")
            .unwrap_or_else(|e| panic!("assign on node {b}: {e}"));
        assert_eq!(
            on_a, on_b,
            "both coordinators plan '{pk}' with one surrogate"
        );
        let at_home = cluster.nodes[home]
            .shared
            .surrogate_assigner
            .lookup_bound(key, tenant, pk.as_bytes())
            .expect("home catalog read");
        assert_eq!(
            at_home,
            Some(on_a),
            "the coordinators plan with the home's bind"
        );
    }

    // A key only node `a` resolved: node `b` looks it up before anything
    // writes it, and gets the home's value.
    let shared = std::sync::Arc::clone(&cluster.nodes[a].shared);
    let assigned = tokio::spawn(async move {
        shared
            .surrogate_assigner
            .assign(
                CollectionKey::from_bare(DatabaseId::DEFAULT, RACE_COLL),
                tenant,
                b"lookup-key",
            )
            .await
    })
    .await
    .expect("join")
    .expect("assign on a");
    let shared = std::sync::Arc::clone(&cluster.nodes[b].shared);
    let looked_up = tokio::spawn(async move {
        shared
            .surrogate_assigner
            .lookup_many(
                CollectionKey::from_bare(DatabaseId::DEFAULT, RACE_COLL),
                tenant,
                &[b"lookup-key".as_slice()],
            )
            .await
    })
    .await
    .expect("join")
    .expect("lookup on b")
    .into_iter()
    .next()
    .flatten();
    assert_eq!(
        looked_up,
        Some(assigned),
        "a lookup on another coordinator returns the home's bind before any apply"
    );

    cluster.shutdown().await;
}
