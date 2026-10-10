// SPDX-License-Identifier: BUSL-1.1

//! An AFTER trigger and a DEFINE EVENT action run exactly once per committed
//! row across a leader kill.
//!
//! Every replica of the source collection's group holds each inserted row's
//! action. Only the node that holds that group's leader lease fires, from a
//! replicated cursor. The action adds one to the row's tally, so an action
//! that ran twice leaves a tally of 2. Each scenario runs once with a trigger
//! body and once with a DEFINE EVENT THEN clause.
//!
//! Every node's firing is gated, so no election that moves the group's
//! leadership lets an unplanned node fire early.
//!
//! - `a_trigger_held_before_a_leader_kill_fires_once` keeps every node's
//!   firing parked while the rows apply everywhere, kills the source's
//!   leader, then releases the survivors. The new owner fires every row once
//!   from the cursor.
//! - `a_trigger_fired_before_a_leader_kill_does_not_fire_twice` lets only
//!   the leader fire, parks it before it commits the cursor, kills it, then
//!   releases the survivors. The new owner fires the rows again from the old
//!   cursor. Each body committed its event's key, so none runs twice.

#![cfg(feature = "failpoints")]

use crate::common;
use common::cluster_harness::shared_steps::{group_of, kill_node, leader_index_of};
use common::cluster_harness::{TestCluster, TestClusterNode, wait_for, wait_for_async};

use std::path::{Path, PathBuf};
use std::time::Duration;

use nodedb::event::cdc::CdcOffset;
use nodedb_test_support::fail_point::{FailAction, FailGuard};

const SRC: &str = "firing_failover_src";
const ROWS: [&str; 3] = ["r1", "r2", "r3"];

const CONVERGE: Duration = Duration::from_secs(30);
const STEP: Duration = Duration::from_millis(100);

/// How many of the source's trigger actions `node` holds for firing.
fn held(node: &TestClusterNode) -> usize {
    let Some(ledgers) = node.shared.sink_ledgers.get() else {
        return 0;
    };
    let ledger = &ledgers.actions.ledger;
    ledger
        .partitions()
        .unwrap_or_default()
        .into_iter()
        .map(|partition| {
            ledger
                .held_after(partition, CdcOffset::ZERO, 10_000)
                .map(|held| held.iter().filter(|(_, a)| a.collection == SRC).count())
                .unwrap_or(0)
        })
        .sum()
}

/// A tally collection in the source's group, so the body writes on the
/// firing node and commits its key there.
fn tally_name(cluster: &TestCluster) -> String {
    let probe = &cluster.nodes[0];
    let group = group_of(probe, SRC);
    (0..4096u32)
        .map(|i| format!("firing_failover_tally_{i}"))
        .find(|name| group_of(probe, name) == group)
        .unwrap_or_else(|| panic!("no tally name maps to {SRC}'s group {group}"))
}

/// One node's firing gate: its release file and the marker its first
/// arrival writes.
struct Gate {
    release: PathBuf,
    parked: PathBuf,
    _guard: FailGuard,
}

impl Gate {
    fn install(dir: &Path, point: &str, node_id: u64) -> Self {
        let release = dir.join(format!("{point}-node{node_id}"));
        let parked = dir.join(format!("{point}-node{node_id}.parked"));
        let guard = FailGuard::for_node(
            node_id,
            &format!("trigger::{point}"),
            FailAction::WaitForFile(release.clone()),
        );
        Self {
            release,
            parked,
            _guard: guard,
        }
    }

    fn release(&self) {
        std::fs::write(&self.release, b"release").expect("release a firing gate");
    }
}

/// Park every node's firing before anything is written.
async fn park_every_node(cluster: &TestCluster, dir: &Path) -> Vec<(u64, Gate)> {
    let gates: Vec<(u64, Gate)> = cluster
        .nodes
        .iter()
        .map(|node| {
            (
                node.node_id,
                Gate::install(dir, "before_firing", node.node_id),
            )
        })
        .collect();
    wait_for("every node's firing parks", CONVERGE, STEP, || {
        gates.iter().all(|(_, gate)| gate.parked.exists())
    })
    .await;
    gates
}

/// The action that adds one to a row's tally.
#[derive(Clone, Copy)]
enum Action {
    /// An AFTER trigger whose body runs the update.
    Trigger,
    /// A DEFINE EVENT whose THEN clause runs the update.
    Event,
}

/// Whether `node`'s catalog holds `action` on the source.
fn has_action(node: &TestClusterNode, action: Action) -> bool {
    match action {
        Action::Trigger => node.has_trigger(1, &format!("{SRC}_tally")),
        Action::Event => node
            .shared
            .credentials
            .catalog()
            .event_definitions(nodedb_types::DatabaseId::DEFAULT, 1, SRC)
            .is_some_and(|defs| !defs.is_empty()),
    }
}

/// A three-node cluster with the source, the tally seeded at 0 per row, and
/// `action`, which adds one to a row's tally.
async fn cluster_with_tally(action: Action) -> (TestCluster, String) {
    let cluster = TestCluster::spawn_three()
        .await
        .expect("spawn 3-node cluster");
    wait_for("every data group has a leader", CONVERGE, STEP, || {
        cluster.nodes[0]
            .all_group_leaders()
            .iter()
            .all(|(_, leader)| *leader != 0)
    })
    .await;
    let tally = tally_name(&cluster);
    for ddl in [
        format!(
            "CREATE COLLECTION {SRC} (id TEXT PRIMARY KEY, v BIGINT) \
             WITH (engine='document_strict')"
        ),
        format!(
            "CREATE COLLECTION {tally} (id TEXT PRIMARY KEY, n BIGINT) \
             WITH (engine='document_strict')"
        ),
    ] {
        cluster
            .exec_ddl_on_any_leader(&ddl)
            .await
            .unwrap_or_else(|e| panic!("{ddl}: {e}"));
    }
    for row in ROWS {
        cluster.nodes[0]
            .exec(&format!("INSERT INTO {tally} (id, n) VALUES ('{row}', 0)"))
            .await
            .unwrap_or_else(|e| panic!("seed tally {row}: {e}"));
    }
    let ddl = match action {
        Action::Trigger => format!(
            "CREATE TRIGGER {SRC}_tally AFTER INSERT ON {SRC} FOR EACH ROW \
             BEGIN UPDATE {tally} SET n = n + 1 WHERE id = NEW.id; END"
        ),
        Action::Event => format!(
            "DEFINE EVENT {SRC}_tally ON {SRC} WHEN INSERT \
             THEN UPDATE {tally} SET n = n + 1 WHERE id = $document_id"
        ),
    };
    cluster
        .exec_ddl_on_any_leader(&ddl)
        .await
        .unwrap_or_else(|e| panic!("{ddl}: {e}"));
    wait_for("every node registers the action", CONVERGE, STEP, || {
        cluster.nodes.iter().all(|node| has_action(node, action))
    })
    .await;
    (cluster, tally)
}

/// Insert every row through `node`.
async fn insert_rows(node: &TestClusterNode) {
    for (n, row) in ROWS.iter().enumerate() {
        node.exec(&format!("INSERT INTO {SRC} (id, v) VALUES ('{row}', {n})"))
            .await
            .unwrap_or_else(|e| panic!("insert {row}: {e}"));
    }
}

/// The text of a query error, with its SQLSTATE and message.
fn pg_detail(error: &tokio_postgres::Error) -> String {
    match error.as_db_error() {
        Some(db) => format!("{}: {}", db.code().code(), db.message()),
        None => format!("{error}"),
    }
}

/// The tally of every row as `node` reads it, in row order.
async fn read_tallies(node: &TestClusterNode, tally: &str) -> Result<Vec<String>, String> {
    let mut out = Vec::with_capacity(ROWS.len());
    for row in ROWS {
        let messages = node
            .client
            .simple_query(&format!("SELECT n FROM {tally} WHERE id = '{row}'"))
            .await
            .map_err(|e| format!("node {}: read tally {row}: {}", node.node_id, pg_detail(&e)))?;
        let n = messages
            .iter()
            .find_map(|message| match message {
                tokio_postgres::SimpleQueryMessage::Row(r) => r.get(0).map(str::to_owned),
                _ => None,
            })
            .unwrap_or_default();
        out.push(n);
    }
    Ok(out)
}

/// The tallies `node` reads once a read succeeds. A read that races a
/// leader change is retried; one that keeps failing fails the test with its
/// error.
async fn tallies(node: &TestClusterNode, tally: &str) -> Vec<String> {
    let deadline = tokio::time::Instant::now() + CONVERGE;
    loop {
        match read_tallies(node, tally).await {
            Ok(tallies) => return tallies,
            Err(error) if tokio::time::Instant::now() >= deadline => panic!("{error}"),
            Err(_) => tokio::time::sleep(STEP).await,
        }
    }
}

fn every_row(value: &str) -> Vec<String> {
    ROWS.iter().map(|_| value.to_owned()).collect()
}

/// Release the gates of the nodes still in `cluster`.
fn release_survivors(cluster: &TestCluster, gates: &[(u64, Gate)]) {
    for (node_id, gate) in gates {
        if cluster.nodes.iter().any(|node| node.node_id == *node_id) {
            gate.release();
        }
    }
}

/// The survivors read every row's tally at 1, and the firing cursor passes
/// every action on every survivor. Several firing passes later the tallies
/// still read 1.
async fn survivors_fired_every_row_once(cluster: &TestCluster, tally: &str) {
    let reader = &cluster.nodes[0];
    wait_for_async(
        "every row's trigger fired on the new owner",
        CONVERGE,
        STEP,
        || async { read_tallies(reader, tally).await.ok() == Some(every_row("1")) },
    )
    .await;
    wait_for(
        "the firing cursor passes every action on every survivor",
        CONVERGE,
        STEP,
        || cluster.nodes.iter().all(|node| held(node) == 0),
    )
    .await;
    tokio::time::sleep(Duration::from_millis(1_000)).await;
    for node in &cluster.nodes {
        assert_eq!(
            tallies(node, tally).await,
            every_row("1"),
            "node {}: a row's trigger fired twice or not at all",
            node.node_id
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn a_trigger_held_before_a_leader_kill_fires_once() {
    held_before_a_leader_kill_fires_once(Action::Trigger).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn a_trigger_fired_before_a_leader_kill_does_not_fire_twice() {
    fired_before_a_leader_kill_does_not_fire_twice(Action::Trigger).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn an_event_action_held_before_a_leader_kill_runs_once() {
    held_before_a_leader_kill_fires_once(Action::Event).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn an_event_action_run_before_a_leader_kill_does_not_run_twice() {
    fired_before_a_leader_kill_does_not_fire_twice(Action::Event).await;
}

async fn held_before_a_leader_kill_fires_once(action: Action) {
    let (mut cluster, tally) = cluster_with_tally(action).await;
    let gate_dir = tempfile::tempdir().expect("gate directory");
    let gates = park_every_node(&cluster, gate_dir.path()).await;

    let leader = leader_index_of(&cluster, SRC);
    insert_rows(&cluster.nodes[(leader + 1) % cluster.nodes.len()]).await;
    wait_for(
        "every replica holds every row's trigger action",
        CONVERGE,
        STEP,
        || cluster.nodes.iter().all(|node| held(node) == ROWS.len()),
    )
    .await;
    for node in &cluster.nodes {
        assert_eq!(
            tallies(node, &tally).await,
            every_row("0"),
            "node {}: no trigger fires while every owner is parked",
            node.node_id
        );
    }

    // Leadership can have moved since the insert: kill whoever leads now.
    let leader = leader_index_of(&cluster, SRC);
    kill_node(&mut cluster, leader).await;
    release_survivors(&cluster, &gates);
    survivors_fired_every_row_once(&cluster, &tally).await;
    cluster.shutdown().await;
}

async fn fired_before_a_leader_kill_does_not_fire_twice(action: Action) {
    let (mut cluster, tally) = cluster_with_tally(action).await;
    let gate_dir = tempfile::tempdir().expect("gate directory");
    let gates = park_every_node(&cluster, gate_dir.path()).await;

    let writer = (leader_index_of(&cluster, SRC) + 1) % cluster.nodes.len();
    insert_rows(&cluster.nodes[writer]).await;
    wait_for(
        "every replica holds every row's trigger action",
        CONVERGE,
        STEP,
        || cluster.nodes.iter().all(|node| held(node) == ROWS.len()),
    )
    .await;

    // Only the current leader fires, and it parks before its cursor commit.
    // That gate's release file is never created: the leader dies parked.
    let leader = leader_index_of(&cluster, SRC);
    let leader_id = cluster.nodes[leader].node_id;
    let commit_gate = Gate::install(gate_dir.path(), "before_cursor_commit", leader_id);
    let (_, leader_gate) = gates
        .iter()
        .find(|(node_id, _)| *node_id == leader_id)
        .expect("the leader's firing gate");
    leader_gate.release();
    wait_for(
        "the leader parks before its cursor commit",
        CONVERGE,
        Duration::from_millis(20),
        || commit_gate.parked.exists(),
    )
    .await;
    let reader = &cluster.nodes[(leader + 1) % cluster.nodes.len()];
    wait_for_async(
        "the leader's bodies committed every tally",
        CONVERGE,
        STEP,
        || async { read_tallies(reader, &tally).await.ok() == Some(every_row("1")) },
    )
    .await;
    for node in &cluster.nodes {
        assert!(
            held(node) > 0,
            "node {}: the cursor never passed the fired actions",
            node.node_id
        );
    }

    kill_node(&mut cluster, leader).await;
    release_survivors(&cluster, &gates);
    survivors_fired_every_row_once(&cluster, &tally).await;
    drop(commit_gate);
    cluster.shutdown().await;
}
