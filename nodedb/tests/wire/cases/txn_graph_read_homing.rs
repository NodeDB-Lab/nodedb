// SPDX-License-Identifier: BUSL-1.1

//! A graph read inside a transaction block homes its read-set entries on
//! every vShard its core's CSR partition holds, written or not.
//!
//! The edges the read scans sit on a vShard other than the collection's home
//! vShard, and they predate the WAL window: a graceful restart checkpoints
//! and truncates the log, so no write of that vShard reaches the restarted
//! node's WAL. A concurrent edge on that vShard after the block's snapshot
//! must still fail the block's COMMIT with `40001`.

use nodedb_types::id::{DatabaseId, VShardId};

use crate::harness::TestServer;

const PASSWORD: &str = "txn-graph-homing-3";
const GRAPH: &str = "homing_graph";

/// The home vShard of `GRAPH`.
fn graph_home() -> VShardId {
    nodedb_types::CollectionKey::from_bare(DatabaseId::DEFAULT, GRAPH).vshard()
}

/// Three node keys on one vShard that is not the graph's home vShard: the
/// first edge's endpoints and the concurrent edge's destination.
fn off_home_nodes() -> (String, String, String) {
    let home = graph_home();
    let first = (0u32..)
        .map(|i| format!("homing_src_{i}"))
        .find(|key| VShardId::from_key(key.as_bytes()) != home)
        .expect("a key off the home vShard");
    let vshard = VShardId::from_key(first.as_bytes());
    let mut same = (0u32..)
        .map(|i| format!("homing_peer_{i}"))
        .filter(|key| VShardId::from_key(key.as_bytes()) == vshard);
    let second = same.next().expect("a second key on the vShard");
    let third = same.next().expect("a third key on the vShard");
    (first, second, third)
}

fn sqlstate(error: &tokio_postgres::Error) -> Option<String> {
    error.as_db_error().map(|e| e.code().code().to_string())
}

async fn exec(client: &tokio_postgres::Client, sql: &str) {
    client
        .simple_query(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_concurrent_edge_on_a_vshard_older_than_the_wal_aborts_a_graph_read() {
    let (src, dst, rival) = off_home_nodes();

    let server = TestServer::start().await;
    server
        .exec(&format!("CREATE COLLECTION {GRAPH}"))
        .await
        .expect("CREATE COLLECTION graph");
    server
        .exec("CREATE COLLECTION homing_side")
        .await
        .expect("CREATE COLLECTION side");
    server
        .exec(&format!(
            "GRAPH INSERT EDGE IN '{GRAPH}' FROM '{src}' TO '{dst}' TYPE 'l'"
        ))
        .await
        .expect("seed edge");
    server
        .exec(&format!("CREATE USER homing_b PASSWORD '{PASSWORD}'"))
        .await
        .expect("CREATE USER");
    server
        .exec("GRANT ROLE readwrite TO homing_b")
        .await
        .expect("GRANT ROLE");

    // A graceful shutdown runs a final checkpoint cycle, which truncates the
    // WAL below it: the seeded edge lives in checkpointed state only.
    let (server, dir) = server.take_dir();
    server.graceful_shutdown().await;
    let (server, _dir) = TestServer::open_on_path(dir).await;

    let (b, handle) = server
        .connect_as("homing_b", PASSWORD)
        .await
        .expect("second connection");

    let a = &server.client;
    exec(a, "BEGIN").await;
    let rows = a
        .simple_query(&format!("MATCH (x)-[:l]->(y) IN '{GRAPH}' RETURN x, y"))
        .await
        .expect("MATCH in the block");
    assert!(
        rows.iter()
            .any(|m| matches!(m, tokio_postgres::SimpleQueryMessage::Row(_))),
        "the block sees the seeded edge"
    );
    exec(a, "INSERT INTO homing_side (id, n) VALUES ('s', 1)").await;

    // A concurrent edge on the seeded edge's vShard commits after the
    // block's snapshot.
    exec(
        &b,
        &format!("GRAPH INSERT EDGE IN '{GRAPH}' FROM '{src}' TO '{rival}' TYPE 'l'"),
    )
    .await;

    let error = a
        .simple_query("COMMIT")
        .await
        .expect_err("the block read the vShard the concurrent edge wrote");
    assert_eq!(sqlstate(&error).as_deref(), Some("40001"), "{error:?}");

    drop(b);
    handle.abort();
}
