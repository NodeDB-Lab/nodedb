// SPDX-License-Identifier: BUSL-1.1

//! The Control-Plane change stream serves every replicated write once, at
//! the same position, on every node.
//!
//! Every replica of a data group publishes the group's writes as its apply
//! loop settles them, in log order, at their log positions. So:
//!
//! - one write raises exactly one event per node: the node that proposed the
//!   write publishes nothing of its own;
//! - every node gives the event the same partition and position;
//! - a cursor taken on one node resumes on another at the next event.
//!
//! ## Test shape
//!
//! Bring up 3 nodes, subscribe on ALL of them, do ONE INSERT through one
//! node, and assert every node's subscription yields exactly one event at
//! one shared position: one `recv_sequenced` succeeds and a second one
//! TIMES OUT. Then page the stream on one node and resume the cursor on
//! another, and on every node after the whole cluster restarts.
//!
//! `ChangeStream::events_published()` is deliberately NOT used as the
//! counter. Counting what a subscriber receives is what a client sees.

use crate::common;
use common::cluster_harness::{TestCluster, TestClusterNode};

use std::time::Duration;

use nodedb::control::change_stream::{ChangeOperation, ReplaySnapshot, ReplayStart, Subscription};
use nodedb_types::{DatabaseId, TenantId};

const COLLECTION: &str = "cdc_once";

/// How long to wait for the write's event to reach a node.
const ARRIVAL_TIMEOUT: Duration = Duration::from_secs(10);

/// How long to wait to prove NO second event follows. A duplicate is
/// published in the same apply round as the original, so it arrives well
/// within this window.
const NO_DUPLICATE_WINDOW: Duration = Duration::from_secs(3);

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replicated_write_publishes_exactly_one_change_event_per_node() {
    let cluster = TestCluster::spawn_three()
        .await
        .expect("spawn 3-node cluster");

    cluster
        .exec_ddl_on_any_leader(&format!(
            "CREATE COLLECTION {COLLECTION} \
             (id TEXT PRIMARY KEY, payload TEXT) WITH (engine='document_strict')"
        ))
        .await
        .unwrap_or_else(|e| panic!("CREATE COLLECTION {COLLECTION}: {e}"));

    // Subscribe on every node BEFORE the write: the change stream is a
    // broadcast bus with no replay for a receiver that did not yet exist.
    let mut subs: Vec<Subscription> = cluster
        .nodes
        .iter()
        .map(|node| {
            node.shared
                .change_stream
                .subscribe(Some(COLLECTION.to_string()), None)
        })
        .collect();

    // Exactly ONE write, through ONE node. Every replica applies it.
    cluster.nodes[0]
        .client
        .simple_query(&format!(
            "INSERT INTO {COLLECTION} (id, payload) VALUES ('row-0', 'payload-0')"
        ))
        .await
        .unwrap_or_else(|e| panic!("insert row-0: {e}"));

    cluster
        .wait_for_full_apply_convergence(Duration::from_secs(15))
        .await;

    let mut positions = Vec::new();
    for (idx, sub) in subs.iter_mut().enumerate() {
        let event = match tokio::time::timeout(ARRIVAL_TIMEOUT, sub.recv_sequenced()).await {
            Ok(Ok(e)) => e,
            Ok(Err(e)) => panic!("node {idx}: change stream closed: {e}"),
            Err(_) => panic!("node {idx}: the replicated INSERT published no change event"),
        };
        assert_eq!(event.collection, COLLECTION, "node {idx}: wrong collection");
        assert_eq!(
            event.operation,
            ChangeOperation::Insert,
            "node {idx}: wrong operation kind"
        );
        positions.push((event.partition(), event.position()));

        // The publish-once guard: a second event for the same write means the
        // apply loop published per replica.
        match tokio::time::timeout(NO_DUPLICATE_WINDOW, sub.recv_filtered()).await {
            Err(_) => {}
            Ok(Ok(dup)) => panic!(
                "node {idx}: one INSERT produced a SECOND change event \
                 ({:?} on {} doc {})",
                dup.operation, dup.collection, dup.document_id
            ),
            Ok(Err(e)) => panic!("node {idx}: change stream closed: {e}"),
        }
    }

    assert!(
        positions.windows(2).all(|pair| pair[0] == pair[1]),
        "every node must give the write one shared position: {positions:?}"
    );

    drop(subs);
    cluster.shutdown().await;
}

/// An array write outside the default database publishes its event on every
/// node, in autocommit and in a transaction. The event names the array bare, in
/// its own database.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_array_write_in_a_named_database_publishes_its_event() {
    const DATABASE: &str = "cdc_named_db";
    const ARRAY: &str = "cdc_named_arr";
    let cluster = TestCluster::spawn_three()
        .await
        .expect("spawn 3-node cluster");
    cluster
        .exec_ddl_on_any_leader(&format!("CREATE DATABASE {DATABASE}"))
        .await
        .unwrap_or_else(|e| panic!("CREATE DATABASE {DATABASE}: {e}"));
    for node in &cluster.nodes {
        node.exec(&format!("USE DATABASE {DATABASE}"))
            .await
            .unwrap_or_else(|e| panic!("USE DATABASE {DATABASE}: {e}"));
    }
    cluster
        .exec_ddl_on_any_leader(&format!(
            "CREATE ARRAY {ARRAY} DIMS (x INT64 [0..15]) ATTRS (v INT64) TILE_EXTENTS (16) \
             CELL_ORDER ROW_MAJOR"
        ))
        .await
        .unwrap_or_else(|e| panic!("CREATE ARRAY {ARRAY}: {e}"));
    let database = cluster.nodes[0]
        .shared
        .credentials
        .catalog()
        .get_database_id_by_name(DATABASE)
        .expect("catalog")
        .expect("the database");

    let mut subs: Vec<Subscription> = cluster
        .nodes
        .iter()
        .map(|node| {
            node.shared
                .change_stream
                .subscribe_in_database(Some(ARRAY.to_string()), None, database)
        })
        .collect();

    cluster.nodes[0]
        .exec(&format!("INSERT INTO ARRAY {ARRAY} COORDS (0) VALUES (10)"))
        .await
        .unwrap_or_else(|e| panic!("insert: {e}"));
    let writer = &cluster.nodes[0];
    writer.exec("BEGIN").await.expect("begin");
    writer
        .exec(&format!("INSERT INTO ARRAY {ARRAY} COORDS (1) VALUES (11)"))
        .await
        .unwrap_or_else(|e| panic!("insert in a transaction: {e}"));
    writer.exec("COMMIT").await.expect("commit");
    cluster
        .wait_for_full_apply_convergence(Duration::from_secs(15))
        .await;

    for (idx, sub) in subs.iter_mut().enumerate() {
        for write in ["autocommit", "transaction"] {
            let event = match tokio::time::timeout(ARRIVAL_TIMEOUT, sub.recv_sequenced()).await {
                Ok(Ok(event)) => event,
                Ok(Err(e)) => panic!("node {idx}: change stream closed: {e}"),
                Err(_) => panic!("node {idx}: the {write} array write published no event"),
            };
            assert_eq!(event.collection, ARRAY, "node {idx}: {write}");
            assert_eq!(event.operation, ChangeOperation::Insert, "node {idx}");
        }
    }

    drop(subs);
    cluster.shutdown().await;
}

/// Rows the cursor tests write.
const ROWS: usize = 4;

/// Replay `node`'s change stream of [`COLLECTION`].
fn replay(node: &TestClusterNode, start: ReplayStart, limit: usize) -> ReplaySnapshot {
    node.shared
        .change_stream
        .query_changes_in_database(
            TenantId::new(1),
            DatabaseId::DEFAULT,
            Some(COLLECTION),
            start,
            limit,
        )
        .unwrap_or_else(|e| panic!("node {}: replay refused: {e:?}", node.node_id))
}

/// A 3-node cluster whose [`COLLECTION`] took [`ROWS`] writes through node 0,
/// once every node holds every write's event.
async fn cluster_with_rows() -> TestCluster {
    let cluster = TestCluster::spawn_three()
        .await
        .expect("spawn 3-node cluster");
    cluster
        .exec_ddl_on_any_leader(&format!(
            "CREATE COLLECTION {COLLECTION} \
             (id TEXT PRIMARY KEY, payload TEXT) WITH (engine='document_strict')"
        ))
        .await
        .unwrap_or_else(|e| panic!("CREATE COLLECTION {COLLECTION}: {e}"));
    for row in 0..ROWS {
        cluster.nodes[0]
            .client
            .simple_query(&format!(
                "INSERT INTO {COLLECTION} (id, payload) VALUES ('row-{row}', 'payload-{row}')"
            ))
            .await
            .unwrap_or_else(|e| panic!("insert row-{row}: {e}"));
    }
    cluster
        .wait_for_full_apply_convergence(Duration::from_secs(15))
        .await;
    let deadline = std::time::Instant::now() + ARRIVAL_TIMEOUT;
    while cluster
        .nodes
        .iter()
        .any(|node| replay(node, ReplayStart::Timestamp(0), ROWS).events.len() < ROWS)
    {
        assert!(
            std::time::Instant::now() < deadline,
            "every node must hold every write's event"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    cluster
}

/// The sorted document ids of two replays.
fn documents(first: &ReplaySnapshot, rest: &ReplaySnapshot) -> Vec<String> {
    let mut documents: Vec<String> = first
        .events
        .iter()
        .chain(rest.events.iter())
        .map(|change| change.document_id.as_str().to_owned())
        .collect();
    documents.sort();
    documents
}

fn every_row() -> Vec<String> {
    (0..ROWS).map(|row| format!("row-{row}")).collect()
}

/// A cursor from one node's replay resumes on another node at the next
/// event, and no event is served twice or skipped.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_change_cursor_from_one_node_resumes_on_another() {
    let cluster = cluster_with_rows().await;
    let first = replay(&cluster.nodes[0], ReplayStart::Timestamp(0), 2);
    assert!(first.has_more);
    let rest = replay(
        &cluster.nodes[1],
        ReplayStart::Cursor(first.cursor.clone()),
        ROWS,
    );
    assert_eq!(
        documents(&first, &rest),
        every_row(),
        "the cursor from node 0 must resume on node 1 with exactly the remaining events"
    );
    cluster.shutdown().await;
}

/// A cursor taken before every node restarts resumes after the restart at
/// the next event, with no reset: each node rebuilds its feeds from its
/// journal.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_change_cursor_resumes_after_the_cluster_restarts() {
    let cluster = cluster_with_rows().await;
    let first = replay(&cluster.nodes[0], ReplayStart::Timestamp(0), 2);
    assert!(first.has_more);

    let cluster = cluster.restart_all().await.expect("restart the cluster");
    for node in &cluster.nodes {
        // A refused cursor panics in `replay`: the restart must not reset it.
        let deadline = std::time::Instant::now() + ARRIVAL_TIMEOUT;
        let mut rest = replay(node, ReplayStart::Cursor(first.cursor.clone()), ROWS);
        while rest.events.len() < ROWS - first.events.len() && std::time::Instant::now() < deadline
        {
            tokio::time::sleep(Duration::from_millis(100)).await;
            rest = replay(node, ReplayStart::Cursor(first.cursor.clone()), ROWS);
        }
        assert_eq!(
            documents(&first, &rest),
            every_row(),
            "node {} must resume the pre-restart cursor with exactly the remaining events",
            node.node_id
        );
    }
    cluster.shutdown().await;
}
