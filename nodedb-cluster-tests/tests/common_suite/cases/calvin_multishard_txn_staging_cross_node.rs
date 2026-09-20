// SPDX-License-Identifier: BUSL-1.1

//! Statements from a coordinator that does NOT own the target collection's
//! vShard, on pgwire and native.
//!
//! An in-block write from such a coordinator stages on the owner under the
//! session's transaction (the staging gate's leader forward), never applies
//! durably at statement time: ROLLBACK discards it on every node, and the
//! same session reads it back before COMMIT while every other session does
//! not. An autocommit write forwarded to the owner answers exactly what a
//! local dispatch answers: its own verb and count, one tag per statement,
//! and `RETURNING` rows as rows.

use std::time::Duration;

use nodedb_client::native::NativeClient;
use nodedb_client::native::pool::PoolConfig;
use tokio_postgres::SimpleQueryMessage;

use crate::common::cluster_harness::{
    TestCluster, TestClusterNode, is_no_serving_leader, wait_for, wait_for_async,
};
use crate::common::pgwire_harness::raw_pgwire::{RawPgConn, command_tags};

const CONVERGENCE: Duration = Duration::from_secs(15);

/// A 3-node cluster, the node leading `coll`'s Raft group, and a coordinator
/// that does not.
struct Fixture {
    cluster: TestCluster,
    owner: usize,
    coordinator: usize,
}

/// The leader `node` observes for the group owning `coll`'s vShard, `0`
/// while unknown.
fn owner_of(node: &TestClusterNode, coll: &str) -> u64 {
    let Some(group_id) = node.group_id_for_collection(coll) else {
        return 0;
    };
    node.all_group_leaders()
        .into_iter()
        .find(|(id, _)| *id == group_id)
        .map(|(_, leader)| leader)
        .unwrap_or(0)
}

impl Fixture {
    /// Spawn, create `coll` with `ddl`, and wait until every node agrees on
    /// the collection, on `coll`'s group leader, and on the sequencer leader.
    async fn spawn(coll: &str, ddl: &str) -> Self {
        let cluster = TestCluster::spawn_three().await.expect("3-node cluster");
        cluster
            .exec_ddl_on_any_leader(ddl)
            .await
            .unwrap_or_else(|e| panic!("{ddl}: {e}"));
        wait_for(
            "all 3 nodes see the collection",
            CONVERGENCE,
            Duration::from_millis(50),
            || {
                cluster
                    .nodes
                    .iter()
                    .all(|n| n.cached_collection_count() >= 1)
            },
        )
        .await;
        wait_for(
            "sequencer-group leader elected and visible on every node",
            CONVERGENCE,
            Duration::from_millis(50),
            || {
                let leader = cluster.nodes[0].sequencer_leader();
                leader != 0 && cluster.nodes.iter().all(|n| n.sequencer_leader() == leader)
            },
        )
        .await;
        wait_for(
            "collection group leader elected and visible on every node",
            CONVERGENCE,
            Duration::from_millis(50),
            || {
                let leader = owner_of(&cluster.nodes[0], coll);
                leader != 0 && cluster.nodes.iter().all(|n| owner_of(n, coll) == leader)
            },
        )
        .await;
        let leader = owner_of(&cluster.nodes[0], coll);
        let owner = cluster
            .nodes
            .iter()
            .position(|n| n.node_id == leader)
            .expect("the collection's group leader is a cluster node");
        let coordinator = cluster
            .nodes
            .iter()
            .position(|n| n.node_id != leader)
            .expect("a non-owner coordinator exists in a 3-node cluster");
        Self {
            cluster,
            owner,
            coordinator,
        }
    }

    fn owner(&self) -> &TestClusterNode {
        &self.cluster.nodes[self.owner]
    }

    fn coordinator(&self) -> &TestClusterNode {
        &self.cluster.nodes[self.coordinator]
    }

    async fn converge(&self) {
        self.cluster
            .wait_for_full_apply_convergence(CONVERGENCE)
            .await;
    }

    /// Row count of `sql` on every node, in node order.
    async fn rows_on_every_node(&self, sql: &str) -> Vec<usize> {
        let mut counts = Vec::with_capacity(self.cluster.nodes.len());
        for node in &self.cluster.nodes {
            counts.push(row_count(&node.client, sql).await);
        }
        counts
    }

    /// Wait until `sql` returns exactly `expected` rows on every node.
    async fn wait_rows_on_every_node(&self, sql: &str, expected: usize) {
        for (idx, node) in self.cluster.nodes.iter().enumerate() {
            wait_for_async(
                &format!("node {idx}: `{sql}` returns {expected} rows"),
                CONVERGENCE,
                Duration::from_millis(100),
                || async move {
                    match node.client.simple_query(sql).await {
                        Ok(msgs) => data_rows(&msgs) == expected,
                        Err(e) if is_no_serving_leader(&e) => false,
                        Err(e) => panic!("node {idx}: `{sql}`: {e}"),
                    }
                },
            )
            .await;
        }
    }
}

fn data_rows(msgs: &[SimpleQueryMessage]) -> usize {
    msgs.iter()
        .filter(|m| matches!(m, SimpleQueryMessage::Row(_)))
        .count()
}

async fn row_count(client: &tokio_postgres::Client, sql: &str) -> usize {
    let msgs = client
        .simple_query(sql)
        .await
        .unwrap_or_else(|e| panic!("`{sql}`: {e}"));
    data_rows(&msgs)
}

/// Send one statement on the raw connection and return its command tags.
async fn tags(conn: &mut RawPgConn, sql: &str) -> Vec<String> {
    command_tags(&conn.simple_query(sql).await)
}

/// The first column of every `DataRow` (`D`) in `messages`, as text.
fn first_cells(messages: &[(u8, Vec<u8>)]) -> Vec<String> {
    messages
        .iter()
        .filter(|(tag, _)| *tag == b'D')
        .map(|(_, body)| {
            // i16 column count, then per column an i32 length and its bytes.
            let len = i32::from_be_bytes([body[2], body[3], body[4], body[5]]);
            let cell = &body[6..6 + usize::try_from(len).expect("a non-null first cell")];
            String::from_utf8_lossy(cell).into_owned()
        })
        .collect()
}

/// Native `(rows_affected, command)` for one statement on `client`.
async fn native_outcome(client: &NativeClient, sql: &str) -> (u64, Option<String>) {
    let result = client
        .query(sql)
        .await
        .unwrap_or_else(|e| panic!("native `{sql}`: {e}"));
    (result.rows_affected, result.command)
}

/// A native client pinned to ONE connection, so `begin` / `query` /
/// `rollback` share one server session and one transaction.
fn pinned_native_client(node: &TestClusterNode) -> NativeClient {
    node.native_client_with(|base| PoolConfig {
        max_size: 1,
        ..base
    })
}

fn keyed_ddl(coll: &str) -> String {
    format!("CREATE COLLECTION {coll} (id TEXT PRIMARY KEY, v TEXT)")
}

fn insert_sql(coll: &str, id: &str, v: &str) -> String {
    format!("INSERT INTO {coll} (id, v) VALUES ('{id}', '{v}')")
}

fn select_by_v(coll: &str, v: &str) -> String {
    format!("SELECT id FROM {coll} WHERE v = '{v}'")
}

/// `BEGIN; INSERT; ROLLBACK` from a non-owner coordinator: the in-block
/// INSERT stages on the owner and answers its count, and ROLLBACK leaves no
/// row on any node.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn cross_node_transaction_rollback_discards_write_on_owner() {
    let coll = "txstage_rb_pg";
    let fx = Fixture::spawn(coll, &keyed_ddl(coll)).await;
    let mut conn = fx.coordinator().raw_pgwire().await;

    assert_eq!(tags(&mut conn, "BEGIN").await, vec!["BEGIN"]);
    assert_eq!(
        tags(&mut conn, &insert_sql(coll, "rb", "gone")).await,
        vec!["INSERT 0 1"],
        "in-block INSERT from a non-owner coordinator answers its staged count"
    );
    assert_eq!(tags(&mut conn, "ROLLBACK").await, vec!["ROLLBACK"]);

    fx.converge().await;
    assert_eq!(
        fx.rows_on_every_node(&select_by_v(coll, "gone")).await,
        vec![0; 3],
        "a rolled-back in-block INSERT must not land on any node"
    );

    fx.cluster.shutdown().await;
}

/// The session that staged a write on a remote owner reads it back before
/// COMMIT; another session on the owner does not until COMMIT lands.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn cross_node_transaction_reads_its_own_staged_write() {
    let coll = "txstage_ryow_pg";
    let fx = Fixture::spawn(coll, &keyed_ddl(coll)).await;
    let mut conn = fx.coordinator().raw_pgwire().await;

    assert_eq!(tags(&mut conn, "BEGIN").await, vec!["BEGIN"]);
    assert_eq!(
        tags(&mut conn, &insert_sql(coll, "ryow", "staged")).await,
        vec!["INSERT 0 1"]
    );

    let own = conn.simple_query(&select_by_v(coll, "staged")).await;
    assert_eq!(
        first_cells(&own),
        vec!["ryow".to_owned()],
        "the staging session reads its own write before COMMIT"
    );
    assert_eq!(
        row_count(&fx.owner().client, &select_by_v(coll, "staged")).await,
        0,
        "another session on the owner must not see the staged write"
    );

    assert_eq!(tags(&mut conn, "COMMIT").await, vec!["COMMIT"]);
    fx.converge().await;
    fx.wait_rows_on_every_node(&select_by_v(coll, "staged"), 1)
        .await;

    fx.cluster.shutdown().await;
}

/// Native twin of the rollback test.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn cross_node_transaction_rollback_discards_write_on_owner_native() {
    let coll = "txstage_rb_nat";
    let fx = Fixture::spawn(coll, &keyed_ddl(coll)).await;
    let driver = pinned_native_client(fx.coordinator());

    driver.begin().await.expect("native BEGIN");
    assert_eq!(
        native_outcome(&driver, &insert_sql(coll, "rb", "gone")).await,
        (1, Some("INSERT".to_owned())),
        "in-block native INSERT from a non-owner coordinator answers its staged count"
    );
    driver.rollback().await.expect("native ROLLBACK");

    fx.converge().await;
    assert_eq!(
        fx.rows_on_every_node(&select_by_v(coll, "gone")).await,
        vec![0; 3],
        "a rolled-back in-block native INSERT must not land on any node"
    );

    fx.cluster.shutdown().await;
}

/// Autocommit `UPDATE` / `DELETE` forwarded to the owner answer their own
/// verb and count: `UPDATE 1` / `DELETE 1` on the wire, `(1, Some(verb))`
/// on native.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn cross_node_update_and_delete_report_their_own_tags() {
    let coll = "txstage_upd_del";
    let fx = Fixture::spawn(coll, &keyed_ddl(coll)).await;
    let mut conn = fx.coordinator().raw_pgwire().await;
    let native = fx.coordinator().native_client();

    for id in ["k1", "k2", "k3", "k4"] {
        assert_eq!(
            tags(&mut conn, &insert_sql(coll, id, "seed")).await,
            vec!["INSERT 0 1"],
            "autocommit INSERT of {id} from a non-owner coordinator"
        );
    }
    fx.converge().await;

    assert_eq!(
        tags(
            &mut conn,
            &format!("UPDATE {coll} SET v = 'changed' WHERE id = 'k1'")
        )
        .await,
        vec!["UPDATE 1"],
        "pgwire UPDATE forwarded to the owner answers its own tag"
    );
    assert_eq!(
        tags(&mut conn, &format!("DELETE FROM {coll} WHERE id = 'k2'")).await,
        vec!["DELETE 1"],
        "pgwire DELETE forwarded to the owner answers its own tag"
    );
    assert_eq!(
        native_outcome(
            &native,
            &format!("UPDATE {coll} SET v = 'changed' WHERE id = 'k3'")
        )
        .await,
        (1, Some("UPDATE".to_owned())),
        "native UPDATE forwarded to the owner reports its verb and count"
    );
    assert_eq!(
        native_outcome(&native, &format!("DELETE FROM {coll} WHERE id = 'k4'")).await,
        (1, Some("DELETE".to_owned())),
        "native DELETE forwarded to the owner reports its verb and count"
    );

    fx.converge().await;
    fx.wait_rows_on_every_node(&select_by_v(coll, "changed"), 2)
        .await;
    fx.wait_rows_on_every_node(&format!("SELECT id FROM {coll}"), 2)
        .await;

    fx.cluster.shutdown().await;
}

/// `INSERT ... RETURNING id` forwarded to the owner answers the row plus the
/// one command tag a local dispatch answers with — never a folded tag in
/// place of the row, never one tag per forwarded payload.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn cross_node_insert_returning_emits_rows_not_a_folded_tag() {
    let coll = "txstage_returning";
    let fx = Fixture::spawn(coll, &keyed_ddl(coll)).await;
    let mut local = fx.owner().raw_pgwire().await;
    let mut forwarded = fx.coordinator().raw_pgwire().await;

    let on_owner = local
        .simple_query(&format!("{} RETURNING id", insert_sql(coll, "own", "x")))
        .await;
    let on_coordinator = forwarded
        .simple_query(&format!("{} RETURNING id", insert_sql(coll, "fwd", "x")))
        .await;

    assert_eq!(first_cells(&on_owner), vec!["own".to_owned()]);
    assert_eq!(
        first_cells(&on_coordinator),
        vec!["fwd".to_owned()],
        "the forwarded INSERT ... RETURNING answers its RETURNING row"
    );
    let local_tags = command_tags(&on_owner);
    assert_eq!(local_tags.len(), 1, "a local RETURNING answers one tag");
    assert_eq!(
        command_tags(&on_coordinator),
        local_tags,
        "the forwarded RETURNING answers the same one tag a local dispatch answers"
    );

    fx.converge().await;
    fx.wait_rows_on_every_node(&select_by_v(coll, "x"), 2).await;

    fx.cluster.shutdown().await;
}
