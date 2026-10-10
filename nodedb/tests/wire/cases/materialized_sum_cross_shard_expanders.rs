// SPDX-License-Identifier: BUSL-1.1

//! `INSERT ... SELECT`, `UPDATE ... FROM` and `MERGE` into the source of a
//! materialized sum whose target lives on another vShard.
//!
//! Each statement expands into point writes on the source. A balance on a
//! cross-shard target cannot fold on the source's core: that core owns no
//! target row. Each expansion ships the balance on its own task, homed on the
//! target's vShard, and COMMIT applies it with the source rows.
//!
//! The server runs two Data Plane cores, and the pair below lands on
//! different cores. A balance folded on the source's core then finds no
//! target row.

use nodedb_types::id::DatabaseId;
use tokio_postgres::SimpleQueryMessage;

use crate::harness::TestServer;

/// The sum's target collection.
const ACCOUNTS: &str = "ptv_acct";
/// The sum's source collection.
const POSTINGS: &str = "ptv_post";
/// Rows the statements copy or join from. Its home does not matter.
const FEED: &str = "ptv_feed";
/// Data Plane cores the server runs.
const CORES: usize = 2;

fn vshard(name: &str) -> u32 {
    nodedb_types::CollectionKey::from_bare(DatabaseId::DEFAULT, name)
        .vshard()
        .as_u32()
}

/// The premise every test rests on: the target and the source hash to
/// different vShards, and those vShards live on different cores.
#[test]
fn the_sum_pair_spans_two_vshards_and_two_cores() {
    assert_ne!(
        vshard(ACCOUNTS),
        vshard(POSTINGS),
        "the pair must plan a separate balance task"
    );
    assert_ne!(
        vshard(ACCOUNTS) as usize % CORES,
        vshard(POSTINGS) as usize % CORES,
        "a balance folded on the source's core must find no target row"
    );
}

/// Every `CommandComplete` count in `sql`'s simple-query response, in wire
/// order.
async fn command_counts(server: &TestServer, sql: &str) -> Vec<u64> {
    let messages = server
        .client
        .simple_query(sql)
        .await
        .unwrap_or_else(|e| panic!("run {sql}: {e:?}"));
    messages
        .into_iter()
        .filter_map(|m| match m {
            SimpleQueryMessage::CommandComplete(n) => Some(n),
            _ => None,
        })
        .collect()
}

async fn exec(server: &TestServer, sql: &str) {
    server
        .exec(sql)
        .await
        .unwrap_or_else(|e| panic!("run {sql}: {e}"));
}

/// Start a server and create the sum pair and the feed. Accounts `acc` and
/// `acc2` start empty. Posting `e0` puts 10 on `acc`, posting `e1` puts 5 on
/// `acc`.
async fn sum_fixture() -> TestServer {
    let server = TestServer::start_multicores(CORES).await;
    exec(
        &server,
        &format!(
            "CREATE COLLECTION {ACCOUNTS} (id TEXT PRIMARY KEY, owner TEXT) \
             WITH (engine='document_strict')"
        ),
    )
    .await;
    exec(
        &server,
        &format!(
            "CREATE COLLECTION {POSTINGS} (id TEXT PRIMARY KEY, account_id TEXT, amount TEXT) \
             WITH (engine='document_strict')"
        ),
    )
    .await;
    exec(
        &server,
        &format!(
            "ALTER COLLECTION {ACCOUNTS} ADD COLUMN balance TEXT \
             MATERIALIZED_SUM SOURCE {POSTINGS} \
             ON {POSTINGS}.account_id = {ACCOUNTS}.id VALUE {POSTINGS}.amount"
        ),
    )
    .await;
    exec(
        &server,
        &format!(
            "CREATE COLLECTION {FEED} (id TEXT PRIMARY KEY, account_id TEXT, amount TEXT) \
             WITH (engine='document_strict')"
        ),
    )
    .await;
    for account in ["acc", "acc2"] {
        exec(
            &server,
            &format!(
                "INSERT INTO {ACCOUNTS} (id, owner, balance) VALUES ('{account}', 'alice', '0')"
            ),
        )
        .await;
    }
    for (id, amount) in [("e0", "10"), ("e1", "5")] {
        exec(
            &server,
            &format!(
                "INSERT INTO {POSTINGS} (id, account_id, amount) VALUES ('{id}', 'acc', '{amount}')"
            ),
        )
        .await;
    }
    assert_eq!(
        balance(&server, "acc").await,
        "15",
        "the seed postings land"
    );
    server
}

/// Insert one feed row.
async fn feed(server: &TestServer, id: &str, account: &str, amount: &str) {
    exec(
        server,
        &format!(
            "INSERT INTO {FEED} (id, account_id, amount) VALUES ('{id}', '{account}', '{amount}')"
        ),
    )
    .await;
}

/// The stored balance of `account`.
async fn balance(server: &TestServer, account: &str) -> String {
    let rows = server
        .query_text(&format!(
            "SELECT balance FROM {ACCOUNTS} WHERE id = '{account}'"
        ))
        .await
        .unwrap_or_else(|e| panic!("read the balance of {account}: {e}"));
    match rows.as_slice() {
        [value] => value.clone(),
        other => panic!("one balance row for {account}, got {other:?}"),
    }
}

const INSERT_SELECT: &str = "INSERT INTO ptv_post (id, account_id, amount) \
                             SELECT id, account_id, amount FROM ptv_feed";

/// The amount of `e0` changes in place. `e1` moves from `acc` to `acc2`.
const UPDATE_FROM: &str = "UPDATE ptv_post SET amount = f.amount, account_id = f.account_id \
                           FROM ptv_feed f WHERE ptv_post.id = f.id";

/// Seed the feed `UPDATE_FROM` joins.
async fn update_feed(server: &TestServer) {
    feed(server, "e0", "acc", "25").await;
    feed(server, "e1", "acc2", "5").await;
}

/// `BEGIN; INSERT ... SELECT; COMMIT` credits each copied row's amount to its
/// cross-shard account, and tags only the copied rows.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_staged_insert_select_moves_a_cross_shard_sum() {
    let server = sum_fixture().await;
    feed(&server, "f1", "acc", "4").await;
    feed(&server, "f2", "acc2", "6").await;

    assert_eq!(
        command_counts(&server, &format!("BEGIN; {INSERT_SELECT}; COMMIT")).await,
        vec![0, 2, 0],
        "BEGIN, INSERT 0 2, COMMIT: a balance task adds no count"
    );

    assert_eq!(balance(&server, "acc").await, "19", "10 + 5 + 4");
    assert_eq!(balance(&server, "acc2").await, "6");
}

/// `BEGIN; UPDATE ... FROM; COMMIT` moves a cross-shard balance by the
/// amount's difference, and moves a whole posting between accounts when the
/// join column changes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_staged_update_from_moves_a_cross_shard_sum() {
    let server = sum_fixture().await;
    update_feed(&server).await;

    assert_eq!(
        command_counts(&server, &format!("BEGIN; {UPDATE_FROM}; COMMIT")).await,
        vec![0, 2, 0],
        "BEGIN, UPDATE 2, COMMIT: a balance task adds no count"
    );

    assert_eq!(balance(&server, "acc").await, "25", "e0 at 25, e1 left");
    assert_eq!(balance(&server, "acc2").await, "5", "e1 joined");
}

/// `BEGIN; MERGE; COMMIT` moves a cross-shard balance for its UPDATE arm and
/// its INSERT arm alike.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_staged_merge_moves_a_cross_shard_sum() {
    let server = sum_fixture().await;
    feed(&server, "e0", "acc", "30").await;
    feed(&server, "f1", "acc2", "7").await;

    exec(
        &server,
        &format!(
            "BEGIN; \
             MERGE INTO {POSTINGS} p USING {FEED} f ON p.id = f.id \
             WHEN MATCHED THEN UPDATE SET amount = f.amount \
             WHEN NOT MATCHED THEN INSERT (id, account_id, amount) \
                 VALUES (f.id, f.account_id, f.amount); \
             COMMIT"
        ),
    )
    .await;

    assert_eq!(balance(&server, "acc").await, "35", "e0 at 30, e1 at 5");
    assert_eq!(balance(&server, "acc2").await, "7", "f1 inserted");
}

/// Outside a transaction block the same statements run in an implicit
/// transaction, and move the cross-shard balance the same way.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn autocommit_insert_select_and_update_from_move_a_cross_shard_sum() {
    let server = sum_fixture().await;
    feed(&server, "f1", "acc2", "6").await;

    assert_eq!(
        command_counts(&server, INSERT_SELECT).await,
        vec![1],
        "INSERT 0 1: a balance task adds no count"
    );
    assert_eq!(balance(&server, "acc2").await, "6", "f1 copied");

    exec(&server, &format!("DELETE FROM {FEED} WHERE id = 'f1'")).await;
    update_feed(&server).await;
    assert_eq!(
        command_counts(&server, UPDATE_FROM).await,
        vec![2],
        "UPDATE 2: a balance task adds no count"
    );

    assert_eq!(balance(&server, "acc").await, "25", "e0 at 25, e1 left");
    assert_eq!(balance(&server, "acc2").await, "11", "f1 and e1");
}
