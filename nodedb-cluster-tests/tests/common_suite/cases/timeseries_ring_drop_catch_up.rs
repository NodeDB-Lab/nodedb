// SPDX-License-Identifier: BUSL-1.1

//! Every replica drops the events of timeseries installs that store by
//! column name and reject a row at apply. WAL catch-up rebuilds exactly the
//! events of the rows that landed, with the images they were stored with.
//!
//! ## Test shape
//!
//! Bring up 3 nodes with a timeseries collection that declares only its
//! `ts` time key, and a change stream on it, and land one warm-up row. Park
//! every apply of the collection at a per-replica gate in the write funnel,
//! then send three raw ILP lines, one per node, through the native
//! `TimeseriesIngest` opcode: a fresh column comes only from a raw ILP line
//! (see `ts_native_ingest`). A parked apply holds every later entry of its
//! group, so only A reaches the gates. B goes out once every replica's gate
//! holds A, and C once every replica committed B, so the log orders them as
//! sent:
//!
//! - A gives the new column `extra` a float.
//! - B gives a new column `y` a float and names no `extra`.
//! - C gives `extra` a string.
//!
//! Each proposer resolves against a replica that applied none of them, so
//! the log carries all three as resolved. With the event ring dropping the
//! collection's events, release the applies. Every replica stores A exactly,
//! stores B by column name with `extra` empty, and rejects C's row. Then:
//!
//! - C's client reports its row rejected, and A's and B's report none;
//! - every replica holds the same three rows, and no core fail-stopped;
//! - every replica's change stream serves one event per stored row, none for
//!   C, and B's event carries `extra` as stored, empty, which the resolve's
//!   image of B never named.

#![cfg(feature = "failpoints")]

use crate::common;
use common::cluster_harness::shared_steps::{fail_stopped, local_timeseries_rows};
use common::cluster_harness::{TestCluster, TestClusterNode, wait_for};

use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use nodedb::event::cdc::consume::{ConsumeError, ConsumeParams, consume_local};
use nodedb_test_support::fail_point::{FailAction, FailGuard};
use nodedb_types::DatabaseId;

use super::ts_native_ingest::{
    ingest_native, ingest_until_accepted, native_session, rejection_warnings,
};

const COLLECTION: &str = "ts_ring_drop";
const STREAM: &str = "ts_ring_drop_feed";
const GROUP: &str = "ts_ring_drop_readers";
const TENANT: u64 = 1;

/// The warm-up row, A's row and B's row. C's row never lands.
const STORED_ROWS: usize = 3;

const CONVERGE: Duration = Duration::from_secs(30);
const STEP: Duration = Duration::from_millis(100);
const GATE_STEP: Duration = Duration::from_millis(20);

/// One replica's apply gate on the collection.
///
/// Every apply that reaches the gate parks until the release file exists.
/// Each arrival creates the arrival marker, so a test that removes the
/// marker learns when the next apply arrives.
struct ApplyGate {
    release: PathBuf,
    arrival: PathBuf,
    _guard: FailGuard,
}

impl ApplyGate {
    fn install(directory: &Path, node_id: u64) -> Self {
        let release = directory.join(format!("release-node{node_id}"));
        let arrival = directory.join(format!("release-node{node_id}.parked"));
        let guard = FailGuard::for_node(
            node_id,
            &format!("funnel::before_dispatch::{COLLECTION}"),
            FailAction::WaitForFile(release.clone()),
        );
        Self {
            release,
            arrival,
            _guard: guard,
        }
    }

    fn clear_arrival(&self) {
        match std::fs::remove_file(&self.arrival) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => panic!("remove {}: {error}", self.arrival.display()),
        }
    }

    fn arrived(&self) -> bool {
        self.arrival.exists()
    }

    fn release(&self) {
        std::fs::write(&self.release, b"release").expect("release the held applies");
    }
}

/// The commit index `node`'s Raft holds for `group_id`, or `0` when the
/// group is not hosted there.
fn group_commit_index(node: &TestClusterNode, group_id: u64) -> u64 {
    node.shared
        .cluster_observer
        .get()
        .and_then(|observer| observer.group_status.upgrade())
        .map(|status| status.group_statuses())
        .unwrap_or_default()
        .into_iter()
        .find(|group| group.group_id == group_id)
        .map_or(0, |group| group.commit_index)
}

/// The new values of the change events `node` serves.
fn event_values(node: &TestClusterNode) -> Vec<serde_json::Value> {
    let params = ConsumeParams {
        database_id: DatabaseId::DEFAULT,
        tenant_id: TENANT,
        stream_name: STREAM,
        group_name: GROUP,
        partition: None,
        limit: 100,
    };
    match consume_local(&node.shared, &params) {
        Ok(result) => result
            .events
            .into_iter()
            .map(|event| event.new_value.clone().unwrap_or(serde_json::Value::Null))
            .collect(),
        Err(ConsumeError::BufferEmpty(_)) => Vec::new(),
        Err(error) => panic!("node {}: consume failed: {error}", node.node_id),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn catch_up_after_an_apply_time_rejection_rebuilds_only_landed_rows() {
    let cluster = TestCluster::spawn_three()
        .await
        .expect("spawn 3-node cluster");
    cluster
        .exec_ddl_on_any_leader(&format!(
            "CREATE COLLECTION {COLLECTION} (ts BIGINT TIME_KEY) WITH (engine='timeseries')"
        ))
        .await
        .expect("create the timeseries collection");
    cluster
        .exec_ddl_on_any_leader(&format!("CREATE CHANGE STREAM {STREAM} ON {COLLECTION}"))
        .await
        .expect("create the change stream");
    cluster
        .exec_ddl_on_any_leader(&format!("CREATE CONSUMER GROUP {GROUP} ON {STREAM}"))
        .await
        .expect("create the consumer group");
    wait_for(
        "every node registers the stream and the group",
        CONVERGE,
        STEP,
        || {
            cluster.nodes.iter().all(|node| {
                node.has_change_stream(DatabaseId::DEFAULT, TENANT, STREAM)
                    && node
                        .shared
                        .group_registry
                        .get(DatabaseId::DEFAULT, TENANT, STREAM, GROUP)
                        .is_some()
            })
        },
    )
    .await;
    ingest_until_accepted(
        &cluster.nodes[0],
        COLLECTION,
        &format!("{COLLECTION} value=0.5 1000000000"),
    )
    .await;
    cluster.wait_for_full_apply_convergence(CONVERGE).await;

    // Each replica holds each apply of the collection at its own gate until
    // released. A gate marks each arrival, so the test sends a statement only
    // once every replica holds the one before it: the log then orders A, B
    // and C as sent.
    let gate_dir = tempfile::tempdir().expect("gate tempdir");
    let gates: Vec<ApplyGate> = cluster
        .nodes
        .iter()
        .map(|node| ApplyGate::install(gate_dir.path(), node.node_id))
        .collect();

    let lines = [
        format!("{COLLECTION} value=1.0,extra=1.5 2000000000"),
        format!("{COLLECTION} value=2.0,y=7.5 3000000000"),
        format!("{COLLECTION} value=3.0,extra=\"text\" 4000000000"),
    ];
    // A parked apply holds every later entry of its group, so only A reaches
    // the gates. B and C each go out once every replica committed the line
    // before it.
    let group_id = cluster.nodes[0]
        .group_id_for_collection(COLLECTION)
        .expect("the collection maps to a data group");
    let mut sent = Vec::new();
    for (label, (node, line)) in ["A", "B", "C"]
        .into_iter()
        .zip(cluster.nodes.iter().zip(lines))
    {
        for gate in &gates {
            gate.clear_arrival();
        }
        let committed_before: Vec<u64> = cluster
            .nodes
            .iter()
            .map(|node| group_commit_index(node, group_id))
            .collect();
        let mut session = native_session(node).await;
        sent.push(tokio::spawn(async move {
            ingest_native(&mut session, 1, COLLECTION, &line).await
        }));
        if label == "A" {
            wait_for("every replica holds A's apply", CONVERGE, GATE_STEP, || {
                gates.iter().all(ApplyGate::arrived)
            })
            .await;
        } else {
            wait_for(
                &format!("every replica commits {label}'s line"),
                CONVERGE,
                GATE_STEP,
                || {
                    cluster
                        .nodes
                        .iter()
                        .zip(&committed_before)
                        .all(|(node, before)| group_commit_index(node, group_id) > *before)
                },
            )
            .await;
        }
    }

    // The ring loses every event of the collection while the applies run.
    let dropped = FailGuard::fail(&format!("event::bus::drop::{COLLECTION}"), "ring full");
    for gate in &gates {
        gate.release();
    }
    let mut replies = Vec::new();
    for statement in sent {
        replies.push(statement.await.expect("statement task"));
    }

    let deadline = Instant::now() + CONVERGE;
    let mut per_node: Vec<(u64, Vec<String>)> = Vec::new();
    while Instant::now() < deadline {
        per_node.clear();
        for node in &cluster.nodes {
            per_node.push((
                node.node_id,
                local_timeseries_rows(node, TENANT, COLLECTION).await,
            ));
        }
        if per_node.iter().all(|(_, rows)| rows.len() == STORED_ROWS) {
            break;
        }
        tokio::time::sleep(STEP).await;
    }
    drop(dropped);
    drop(gates);

    assert!(
        rejection_warnings(&replies[0], COLLECTION).is_empty(),
        "A stored its row: {:?}",
        replies[0]
    );
    assert!(
        rejection_warnings(&replies[1], COLLECTION).is_empty(),
        "B stored its row: {:?}",
        replies[1]
    );
    let c_notices = rejection_warnings(&replies[2], COLLECTION);
    assert!(
        c_notices.iter().any(|notice| notice.contains("1 line(s)")),
        "C's client learns its row was rejected, got {c_notices:?}"
    );

    for (node_id, rows) in &per_node {
        assert_eq!(rows.len(), STORED_ROWS, "node {node_id} stores {rows:?}");
    }
    let (reference_node, reference) = &per_node[0];
    for (node_id, rows) in &per_node[1..] {
        assert_eq!(
            rows, reference,
            "node {node_id} stores other rows than node {reference_node}"
        );
    }
    for node in &cluster.nodes {
        assert!(
            !fail_stopped(node),
            "node {} fail-stopped a core",
            node.node_id
        );
    }

    wait_for(
        "every replica's catch-up serves one event per stored row",
        CONVERGE,
        STEP,
        || {
            cluster
                .nodes
                .iter()
                .all(|node| event_values(node).len() == STORED_ROWS)
        },
    )
    .await;
    for node in &cluster.nodes {
        let values = event_values(node);
        assert_eq!(
            values.len(),
            STORED_ROWS,
            "node {}: {values:?}",
            node.node_id
        );
        assert!(
            values
                .iter()
                .all(|value| !value.get("extra").is_some_and(serde_json::Value::is_string)),
            "node {}: an event carries C's rejected row: {values:?}",
            node.node_id
        );
        let b = values
            .iter()
            .find(|value| value.get("y").is_some_and(|y| !y.is_null()))
            .unwrap_or_else(|| panic!("node {}: no event for B: {values:?}", node.node_id));
        assert_eq!(
            b.get("extra"),
            Some(&serde_json::Value::Null),
            "node {}: B's event is not the row as stored: {b}",
            node.node_id
        );
    }

    cluster.shutdown().await;
}
