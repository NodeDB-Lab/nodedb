// SPDX-License-Identifier: BUSL-1.1

//! A Calvin transaction staged against a collection that a purge and a
//! same-name create replaced never applies, in the new collection or in any
//! other one.
//!
//! 1. A 3-node cluster holds two collections on distinct vShards. A
//!    coordinator that is not the sequencer leader runs
//!    `BEGIN; INSERT a; INSERT b; COMMIT` in strict cross-shard mode.
//! 2. The fail gate `calvin::after_stamp::<a>` parks the transaction on the
//!    sequencer leader after its collection incarnations are stamped and
//!    before the sequencer admits it.
//! 3. `a` is purged and created again while the transaction is parked.
//! 4. Released, the transaction reaches every replica with `a`'s old
//!    incarnation. Every replica refuses it, and the global verdict aborts it:
//!    the client gets the retryable serialization failure, and neither
//!    collection holds a row on any node.
//! 5. The same transaction, retried, commits.
//!
//! Requires `--features failpoints`. File name contains "cluster" so nextest
//! applies the cluster test group.

#![cfg(feature = "failpoints")]

use std::time::Duration;

use nodedb_test_support::fail_point::{FailAction, FailGuard};
use tokio_postgres::error::SqlState;

use super::calvin_multishard_fixture::{Fixture, keyed_ddl};
use super::vshard_names::distinct_vshard_collections;
use crate::common::cluster_harness::{TestClusterNode, wait_for};

/// A fresh session on `node` in strict cross-shard mode, apart from the
/// harness client the DDL helpers use.
async fn strict_session(node: &TestClusterNode) -> tokio_postgres::Client {
    let (client, connection) = tokio_postgres::connect(
        &format!(
            "host=127.0.0.1 port={} user=nodedb dbname=default",
            node.pg_addr.port()
        ),
        tokio_postgres::NoTls,
    )
    .await
    .expect("connect a session to the coordinator");
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
async fn a_txn_staged_before_a_recreate_applies_nowhere() {
    let (col_a, col_b) = distinct_vshard_collections("calvin_superseded_0", "calvin_superseded");
    let fx = Fixture::spawn(&[keyed_ddl(&col_a), keyed_ddl(&col_b)]).await;
    fx.wait_group_mounted(&col_a).await;
    fx.wait_group_mounted(&col_b).await;

    let txn = format!(
        "BEGIN; \
         INSERT INTO {col_a} (id, v) VALUES ('k1', 'a'); \
         INSERT INTO {col_b} (id, v) VALUES ('k2', 'b'); \
         COMMIT"
    );

    let gate_dir = tempfile::tempdir().expect("gate directory");
    let release = gate_dir.path().join("release");
    let parked = gate_dir.path().join("release.parked");
    let session = strict_session(fx.coordinator()).await;
    let refused = {
        let _gate = FailGuard::install(
            &format!("calvin::after_stamp::{col_a}"),
            FailAction::WaitForFile(release.clone()),
        );
        let commit = session.simple_query(&txn);
        let recreate = async {
            wait_for(
                "the transaction parked after its incarnation stamp",
                Duration::from_secs(15),
                Duration::from_millis(20),
                || parked.exists(),
            )
            .await;
            for ddl in [format!("DROP COLLECTION {col_a} PURGE"), keyed_ddl(&col_a)] {
                fx.cluster
                    .exec_ddl_on_any_leader(&ddl)
                    .await
                    .unwrap_or_else(|e| panic!("{ddl}: {e}"));
            }
            std::fs::write(&release, b"release").expect("release the parked transaction");
        };
        let (refused, ()) = tokio::join!(commit, recreate);
        refused
    };

    let error = refused.expect_err("a transaction staged before the recreate must not commit");
    assert_eq!(
        error.code(),
        Some(&SqlState::T_R_SERIALIZATION_FAILURE),
        "the client retries a superseded transaction: {error:?}"
    );

    fx.converge().await;
    for coll in [&col_a, &col_b] {
        fx.wait_rows_on_every_node(&format!("SELECT id FROM {coll}"), 0)
            .await;
    }

    let retry = strict_session(fx.coordinator()).await;
    retry
        .simple_query(&txn)
        .await
        .expect("the retried transaction commits against the recreated collection");
    fx.converge().await;
    for coll in [&col_a, &col_b] {
        fx.wait_rows_on_every_node(&format!("SELECT id FROM {coll}"), 1)
            .await;
    }

    fx.cluster.shutdown().await;
}
