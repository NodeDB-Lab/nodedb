// SPDX-License-Identifier: BUSL-1.1

//! A node delete too large for one sequencer entry commits as one
//! multi-part Calvin transaction.
//!
//! A row of an edge-bearing collection is a graph node. Deleting it
//! tombstones every edge incident on it, in the delete's own transaction.
//! The transaction spans every vShard that homes an edge end. Past 64
//! vShards or 1 MiB of plans, the sequencer carries its plans as parts. The
//! delete stays atomic: every lock is held from the header to the last
//! part's apply, and a reader sees all of its edges or none of them.

use std::collections::BTreeSet;

use crate::harness::TestServer;

/// The nodes `GRAPH NEIGHBORS` returns for `node` in `collection`.
async fn neighbors(
    client: &tokio_postgres::Client,
    collection: &str,
    node: &str,
    direction: &str,
) -> Vec<String> {
    let messages = client
        .simple_query(&format!(
            "GRAPH NEIGHBORS IN '{collection}' OF '{node}' DIRECTION {direction}"
        ))
        .await
        .expect("GRAPH NEIGHBORS");
    let mut out = Vec::new();
    for message in messages {
        let tokio_postgres::SimpleQueryMessage::Row(row) = message else {
            continue;
        };
        let Some(text) = row.get(0) else {
            continue;
        };
        let parsed: Vec<serde_json::Value> = serde_json::from_str(text).unwrap_or_default();
        out.extend(
            parsed
                .iter()
                .filter_map(|entry| entry.get("node").and_then(|v| v.as_str()))
                .map(str::to_owned),
        );
    }
    out
}

/// Insert edge documents `(id, from, to)`, 100 per statement. Each insert
/// is a Calvin transaction bound to the epoch cadence, so one statement per
/// edge makes a large setup take minutes.
async fn insert_edges(server: &TestServer, collection: &str, edges: &[(String, String, String)]) {
    for batch in edges.chunks(100) {
        let rows: Vec<String> = batch
            .iter()
            .map(|(id, from, to)| {
                format!("{{ id: '{id}', _from: '{from}', _to: '{to}', _type: 'l' }}")
            })
            .collect();
        server
            .exec(&format!("INSERT INTO {collection} [{}]", rows.join(", ")))
            .await
            .unwrap_or_else(|e| panic!("insert {} edges: {e}", batch.len()));
    }
}

async fn insert_node(server: &TestServer, collection: &str, id: &str) {
    server
        .exec(&format!(
            "INSERT INTO {collection} {{ id: '{id}', kind: 'node' }}"
        ))
        .await
        .unwrap_or_else(|e| panic!("insert node {id}: {e}"));
}

/// 200 nodes, each with an edge to its own peer and one to a shared hub,
/// deleted by one statement. The edge ends home on well over 64 vShards,
/// so the delete is a multi-part transaction. A reader polling the hub's
/// in-neighbours during the delete sees all 200 or none.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_node_delete_over_many_vshards_is_atomic_to_a_reader() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION mp_g WITH (engine='document_schemaless')")
        .await
        .unwrap();
    const NODES: usize = 200;
    let mut edges: Vec<(String, String, String)> = Vec::with_capacity(2 * NODES);
    for i in 0..NODES {
        insert_node(&server, "mp_g", &format!("n_{i}")).await;
        edges.push((format!("peer_{i}"), format!("n_{i}"), format!("p_{i}")));
        edges.push((format!("hub_{i}"), format!("n_{i}"), "hub".to_string()));
    }
    insert_edges(&server, "mp_g", &edges).await;
    assert_eq!(
        neighbors(&server.client, "mp_g", "hub", "in").await.len(),
        NODES
    );

    // The reader reads inside a transaction: the hub's in-edges on the hub's
    // home, and the out-edges of sampled nodes on their own homes. Reads take
    // no snapshot. COMMIT validates the read set and refuses one a write
    // moved with 40001, so the reader retries. A committed read transaction
    // saw one state across every vShard: the delete wholly before or wholly
    // after it.
    let (reader, _reader_task) = server.connect_as("nodedb", "nodedb").await.unwrap();
    let sampled: Vec<String> = [0, NODES / 3, NODES / 2, NODES - 1]
        .iter()
        .map(|i| format!("n_{i}"))
        .collect();
    let (stop_tx, mut stop_rx) = tokio::sync::oneshot::channel::<()>();
    let poll = tokio::spawn(async move {
        let mut committed: BTreeSet<(usize, Vec<usize>)> = BTreeSet::new();
        loop {
            reader.simple_query("BEGIN").await.expect("BEGIN");
            let hub = neighbors(&reader, "mp_g", "hub", "in").await.len();
            let mut nodes = Vec::with_capacity(sampled.len());
            for node in &sampled {
                nodes.push(neighbors(&reader, "mp_g", node, "out").await.len());
            }
            match reader.simple_query("COMMIT").await {
                Ok(_) => {
                    committed.insert((hub, nodes));
                }
                Err(error)
                    if error.code()
                        == Some(&tokio_postgres::error::SqlState::T_R_SERIALIZATION_FAILURE) =>
                {
                    let _ = reader.simple_query("ROLLBACK").await;
                }
                Err(error) => panic!("COMMIT: {error:?}"),
            }
            if stop_rx.try_recv().is_ok() {
                return committed;
            }
        }
    });

    server
        .exec("DELETE FROM mp_g WHERE kind = 'node'")
        .await
        .expect("the multi-part node delete commits");
    let _ = stop_tx.send(());
    let committed = poll.await.expect("reader task");
    assert!(!committed.is_empty(), "no read transaction committed");
    for (hub, nodes) in &committed {
        let before = *hub == NODES && nodes.iter().all(|&count| count == 2);
        let after = *hub == 0 && nodes.iter().all(|&count| count == 0);
        assert!(
            before || after,
            "a committed read saw a partial delete: hub in-degree {hub}, node out-degrees {nodes:?}"
        );
    }

    assert!(
        neighbors(&server.client, "mp_g", "hub", "in")
            .await
            .is_empty()
    );
    for i in [0, NODES / 2, NODES - 1] {
        let node = format!("n_{i}");
        assert!(
            neighbors(&server.client, "mp_g", &node, "out")
                .await
                .is_empty(),
            "{node} keeps an out-edge"
        );
        let peer = format!("p_{i}");
        assert!(
            neighbors(&server.client, "mp_g", &peer, "in")
                .await
                .is_empty(),
            "{peer} keeps an in-edge"
        );
    }
    let left = server
        .query_text("SELECT id FROM mp_g WHERE kind = 'node'")
        .await
        .unwrap();
    assert!(left.is_empty(), "node rows remain: {left:?}");
}

/// A hub with 500 neighbours is deleted with every incident edge.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_hub_with_500_neighbours_is_deleted_with_its_edges() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION mp_hub WITH (engine='document_schemaless')")
        .await
        .unwrap();
    insert_node(&server, "mp_hub", "hub").await;
    const NEIGHBOURS: usize = 500;
    let edges: Vec<(String, String, String)> = (0..NEIGHBOURS)
        .map(|i| (format!("e_{i}"), format!("m_{i}"), "hub".to_string()))
        .collect();
    insert_edges(&server, "mp_hub", &edges).await;
    assert_eq!(
        neighbors(&server.client, "mp_hub", "hub", "in").await.len(),
        NEIGHBOURS
    );

    server
        .exec("DELETE FROM mp_hub WHERE kind = 'node'")
        .await
        .expect("the hub delete commits");

    assert!(
        neighbors(&server.client, "mp_hub", "hub", "in")
            .await
            .is_empty()
    );
    for i in [0, NEIGHBOURS / 2, NEIGHBOURS - 1] {
        let node = format!("m_{i}");
        assert!(
            neighbors(&server.client, "mp_hub", &node, "out")
                .await
                .is_empty(),
            "{node} keeps its edge to the deleted hub"
        );
    }
}

/// A node delete whose plans exceed 1 MiB, one sequencer entry, commits.
/// Long node keys make each edge delete large.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_node_delete_with_plans_over_one_mebibyte_commits() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION mp_big WITH (engine='document_schemaless')")
        .await
        .unwrap();
    insert_node(&server, "mp_big", "hub").await;
    // 700 keys of 2 KB put 1.4 MB of keys in the plans before any other
    // field. Few long keys keep the setup to seven insert statements.
    let pad = "k".repeat(2_000);
    const NEIGHBOURS: usize = 700;
    let edges: Vec<(String, String, String)> = (0..NEIGHBOURS)
        .map(|i| (format!("e_{i}"), format!("{pad}_{i}"), "hub".to_string()))
        .collect();
    insert_edges(&server, "mp_big", &edges).await;

    server
        .exec("DELETE FROM mp_big WHERE kind = 'node'")
        .await
        .expect("the delete of over 1 MiB of plans commits");

    assert!(
        neighbors(&server.client, "mp_big", "hub", "in")
            .await
            .is_empty()
    );
    let sample = format!("{pad}_{}", NEIGHBOURS - 1);
    assert!(
        neighbors(&server.client, "mp_big", &sample, "out")
            .await
            .is_empty()
    );
}
