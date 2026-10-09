// SPDX-License-Identifier: BUSL-1.1

//! A Calvin transaction whose data-group leader dies between its vote and
//! its redo commits once on every replica.
//!
//! 1. A 3-node cluster holds two collections on distinct vShards. A session
//!    on a node that does not lead `a`'s data group runs
//!    `BEGIN; INSERT a; INSERT b; COMMIT` in strict cross-shard mode.
//! 2. The fail gate `calvin::before_redo_propose::<a>` holds `a`'s slice
//!    after its vote and resolve, before its leader proposes the redo.
//! 3. The leader of `a`'s data group is killed while the redo is held.
//! 4. Released, the new leader stages the held txn again, resolves it, and
//!    proposes its redo. Each collection holds its row exactly once on every
//!    surviving node.
//!
//! The session's node submits the txn to the sequencer leader and waits for
//! its answer. When the victim also leads the sequencer group, that answer
//! is lost with it: the COMMIT reports an unknown outcome (`57014`) and the
//! txn still commits once. Otherwise the COMMIT succeeds.
//!
//! Requires `--features failpoints`.

#![cfg(feature = "failpoints")]

use std::time::Duration;

use nodedb_types::fail_point::{FailAction, FailGuard};

use super::calvin_multishard_fixture::{Fixture, keyed_ddl};
use super::vshard_names::distinct_vshard_collections;
use crate::common::cluster_harness::shared_steps::{kill_node, leader_index_of, leader_of};
use crate::common::cluster_harness::{TestClusterNode, wait_for};

/// How long the test waits for the leader to hold the redo.
const HOLD_DEADLINE: Duration = Duration::from_secs(15);

/// A fresh session on `node` in strict cross-shard mode.
async fn strict_session(node: &TestClusterNode) -> tokio_postgres::Client {
    let (client, connection) = tokio_postgres::connect(
        &format!(
            "host=127.0.0.1 port={} user=nodedb dbname=default",
            node.pg_addr.port()
        ),
        tokio_postgres::NoTls,
    )
    .await
    .expect("connect a session");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
        .simple_query("SET cross_shard_txn = 'strict'")
        .await
        .expect("SET cross_shard_txn = strict");
    client
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn a_txn_whose_leader_dies_between_vote_and_redo_commits_once() {
    let (col_a, col_b) =
        distinct_vshard_collections("calvin_leader_change_0", "calvin_leader_change");
    let mut fx = Fixture::spawn(&[keyed_ddl(&col_a), keyed_ddl(&col_b)]).await;
    fx.wait_group_mounted(&col_a).await;
    fx.wait_group_mounted(&col_b).await;

    let victim = leader_index_of(&fx.cluster, &col_a);
    let session_node = (0..fx.cluster.nodes.len())
        .find(|idx| *idx != victim)
        .expect("a 3-node cluster has a node besides the leader");
    let victim_leads_sequencer = leader_of(
        &fx.cluster.nodes[session_node],
        nodedb_cluster::calvin::SEQUENCER_GROUP_ID,
    ) == fx.cluster.nodes[victim].node_id;
    let session = strict_session(&fx.cluster.nodes[session_node]).await;
    let txn = format!(
        "BEGIN; \
         INSERT INTO {col_a} (id, v) VALUES ('k1', 'a'); \
         INSERT INTO {col_b} (id, v) VALUES ('k2', 'b'); \
         COMMIT"
    );

    let gate_dir = tempfile::tempdir().expect("gate directory");
    let release = gate_dir.path().join("release");
    let parked = gate_dir.path().join("release.parked");
    let committed = {
        let _gate = FailGuard::install(
            &format!("calvin::before_redo_propose::{col_a}"),
            FailAction::WaitForFile(release.clone()),
        );
        let commit = session.simple_query(&txn);
        let change_leader = async {
            wait_for(
                "the leader holds the slice's redo after its vote",
                HOLD_DEADLINE,
                Duration::from_millis(20),
                || parked.exists(),
            )
            .await;
            kill_node(&mut fx.cluster, victim).await;
            std::fs::write(&release, b"release").expect("release the held redo");
        };
        let (committed, ()) = tokio::join!(commit, change_leader);
        committed
    };

    if victim_leads_sequencer {
        let error = committed.expect_err("the submit's answer died with the sequencer leader");
        let code = error.code().map(|code| code.code().to_owned());
        assert_eq!(
            code.as_deref(),
            Some("57014"),
            "a lost submit answer reports an unknown outcome, got: {error:?}"
        );
    } else {
        committed.expect("the transaction commits under the new leader");
    }
    fx.converge().await;
    for coll in [&col_a, &col_b] {
        fx.wait_rows_on_every_node(&format!("SELECT id FROM {coll}"), 1)
            .await;
    }

    fx.cluster.shutdown().await;
}
