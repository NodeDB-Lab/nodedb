// SPDX-License-Identifier: BUSL-1.1

//! A best-effort `COMMIT` (`cross_shard_txn = 'best_effort_non_atomic'`)
//! whose block read a row that another session changes before the `COMMIT`.
//!
//! The block runs on the harness pgwire session. The concurrent change runs on
//! a native session outside any block, so no fail gate is needed.
//!
//! Best effort commits one Calvin transaction per written vShard, in vShard
//! order. The first group carries the block's reads, so every read is checked
//! before any group installs. A later group carries none and checks the rows it
//! writes. A stale read answers SQLSTATE 40001 and commits nothing. A row
//! written after the block's snapshot makes a later group answer 40001 after
//! the earlier groups committed, so 40001 does not prove that nothing committed.
//!
//! A block that settles a materialized sum commits its balance with the source
//! rows. Its balance equals the source sum whichever of the pair sorts first.

use nodedb_test_support::native_harness::{open_trust_session, send_sql};
use nodedb_types::id::DatabaseId;

use super::materialized_sum_cross_shard_expanders::{balance, sum_fixture};
use crate::harness::TestServer;

/// The SQLSTATE of a serialization conflict.
const SERIALIZATION_FAILURE: &str = "40001";

/// Data Plane cores the server runs.
const CORES: usize = 2;
/// The g7 sum's target, on vShard 578.
const ACCOUNTS: &str = "g7_acct";
/// The g7 sum's source, on vShard 469. It sorts before its target.
const POSTINGS: &str = "g7_src";
/// The shared sum's target. It sorts before its source.
const SHARED_ACCOUNTS: &str = "ptv_acct";
/// The shared sum's source.
const SHARED_POSTINGS: &str = "ptv_post";
/// A plain collection on the lower write vShard, 837.
const LOW: &str = "g7_low";
/// A plain collection on the higher write vShard, 977.
const HIGH: &str = "g7_high";
/// A plain collection on `HIGH`'s vShard that no block writes.
const PEER: &str = "g7_peer_cf";
/// A plain collection on a vShard no block writes, 973.
const FEED: &str = "g7_feed";
/// The block sets posting `e0` from 10 to 1. Settlement ships -9.
const SETTLE_E0: &str = "SET amount = '1' WHERE id = 'e0'";
/// The concurrent change sets `e0` from 10 to 20, and the balance to 25.
const MOVE_E0: &str = "SET amount = '20' WHERE id = 'e0'";

/// The sum of the `acc` postings of the shared sum, read from its source rows.
async fn source_sum(server: &TestServer) -> i64 {
    server
        .query_text(&format!(
            "SELECT amount FROM {SHARED_POSTINGS} WHERE account_id = 'acc'"
        ))
        .await
        .unwrap_or_else(|e| panic!("read the shared postings: {e}"))
        .iter()
        .map(|amount| {
            amount
                .parse::<i64>()
                .unwrap_or_else(|e| panic!("amount {amount}: {e}"))
        })
        .sum()
}

fn vshard(name: &str) -> u32 {
    nodedb_types::CollectionKey::from_bare(DatabaseId::DEFAULT, name)
        .vshard()
        .as_u32()
}

async fn run(server: &TestServer, sql: &str) {
    server
        .exec(sql)
        .await
        .unwrap_or_else(|e| panic!("run {sql}: {e}"));
}

/// The one text value `sql` answers.
async fn value(server: &TestServer, sql: &str) -> String {
    let rows = server
        .query_text(sql)
        .await
        .unwrap_or_else(|e| panic!("run {sql}: {e}"));
    match rows.as_slice() {
        [value] => value.clone(),
        other => panic!("one row for {sql}, got {other:?}"),
    }
}

/// Run `sql` on a new native session, outside any block.
async fn run_elsewhere(server: &TestServer, sql: &str) {
    let mut stream = open_trust_session(server.native_port, "nodedb").await;
    let reply = send_sql(&mut stream, 2, sql).await;
    if let Some(error) = reply.error {
        panic!(
            "the concurrent change {sql}: {}: {}",
            error.code, error.message
        );
    }
}

/// Put the harness session in best-effort mode and open a block.
async fn begin_best_effort(server: &TestServer) {
    run(server, "SET cross_shard_txn = 'best_effort_non_atomic'").await;
    assert_eq!(
        value(server, "SHOW cross_shard_txn").await,
        "best_effort_non_atomic",
        "the block runs in best-effort mode"
    );
    run(server, "BEGIN").await;
}

/// Assert that `commit` failed with `sqlstate`. Returns the error text.
fn expect_sqlstate(commit: Result<(), String>, sqlstate: &str) -> String {
    match commit {
        Ok(()) => panic!("COMMIT must fail with SQLSTATE {sqlstate}, it committed"),
        Err(error) => {
            assert!(
                error.contains(&format!("(SQLSTATE {sqlstate})")),
                "COMMIT must fail with SQLSTATE {sqlstate}, got {error}"
            );
            error
        }
    }
}

/// Assert that `commit` committed or answered a serialization conflict.
fn expect_commit_or_conflict(commit: Result<(), String>) {
    if let Err(error) = commit {
        assert!(
            error.contains(&format!("(SQLSTATE {SERIALIZATION_FAILURE})")),
            "COMMIT must commit or abort with a serialization conflict, got {error}"
        );
    }
}

/// A server with the g7 sum. Posting `e0` puts 10 on `acc`, `e1` puts 5.
async fn g7_sum_fixture() -> TestServer {
    let server = TestServer::start_multicores(CORES).await;
    run(
        &server,
        &format!(
            "CREATE COLLECTION {ACCOUNTS} (id TEXT PRIMARY KEY, owner TEXT) \
             WITH (engine='document_strict')"
        ),
    )
    .await;
    run(
        &server,
        &format!(
            "CREATE COLLECTION {POSTINGS} (id TEXT PRIMARY KEY, account_id TEXT, amount TEXT) \
             WITH (engine='document_strict')"
        ),
    )
    .await;
    run(
        &server,
        &format!(
            "ALTER COLLECTION {ACCOUNTS} ADD COLUMN balance TEXT \
             MATERIALIZED_SUM SOURCE {POSTINGS} \
             ON {POSTINGS}.account_id = {ACCOUNTS}.id VALUE {POSTINGS}.amount"
        ),
    )
    .await;
    run(
        &server,
        &format!("INSERT INTO {ACCOUNTS} (id, owner, balance) VALUES ('acc', 'alice', '0')"),
    )
    .await;
    for (id, amount) in [("e0", "10"), ("e1", "5")] {
        run(
            &server,
            &format!(
                "INSERT INTO {POSTINGS} (id, account_id, amount) VALUES ('{id}', 'acc', '{amount}')"
            ),
        )
        .await;
    }
    assert_eq!(g7_balance(&server).await, "15", "the seed postings land");
    server
}

async fn g7_balance(server: &TestServer) -> String {
    value(
        server,
        &format!("SELECT balance FROM {ACCOUNTS} WHERE id = 'acc'"),
    )
    .await
}

/// The sum of the `acc` postings, read from the g7 source rows.
async fn g7_source_sum(server: &TestServer) -> i64 {
    server
        .query_text(&format!(
            "SELECT amount FROM {POSTINGS} WHERE account_id = 'acc'"
        ))
        .await
        .unwrap_or_else(|e| panic!("read the g7 postings: {e}"))
        .iter()
        .map(|amount| {
            amount
                .parse::<i64>()
                .unwrap_or_else(|e| panic!("amount {amount}: {e}"))
        })
        .sum()
}

/// A server with `LOW`, `HIGH`, `PEER` and `FEED`. Row `r1` of `LOW` and
/// `HIGH`, `t1` of `PEER` and `f1` of `FEED` all hold `old`.
async fn group_fixture() -> TestServer {
    group_fixture_on(TestServer::start_multicores(CORES).await).await
}

/// [`group_fixture`] on a server the caller started.
async fn group_fixture_on(server: TestServer) -> TestServer {
    for (collection, id) in [(LOW, "r1"), (HIGH, "r1"), (PEER, "t1"), (FEED, "f1")] {
        run(
            &server,
            &format!(
                "CREATE COLLECTION {collection} (id TEXT PRIMARY KEY, v TEXT) \
                 WITH (engine='document_strict')"
            ),
        )
        .await;
        run(
            &server,
            &format!("INSERT INTO {collection} (id, v) VALUES ('{id}', 'old')"),
        )
        .await;
    }
    server
}

/// The `v` of row `id` in `collection`.
async fn row(server: &TestServer, collection: &str, id: &str) -> String {
    value(
        server,
        &format!("SELECT v FROM {collection} WHERE id = '{id}'"),
    )
    .await
}

/// Read row `id` of `collection` inside the open block, then set `r1` to
/// `new` in `LOW` and in `HIGH`.
async fn read_then_write_both_groups(server: &TestServer, collection: &str, id: &str) {
    assert_eq!(row(server, collection, id).await, "old", "the block read");
    for written in [LOW, HIGH] {
        run(
            server,
            &format!("UPDATE {written} SET v = 'new' WHERE id = 'r1'"),
        )
        .await;
    }
}

/// Rejects a fixture where the g7 source sorts last or the peer leaves the high vShard.
#[test]
fn the_fixture_vshards_sort_as_the_cells_need() {
    assert!(
        vshard(POSTINGS) < vshard(ACCOUNTS),
        "the g7 source sorts first"
    );
    assert_ne!(
        vshard(POSTINGS) as usize % CORES,
        vshard(ACCOUNTS) as usize % CORES,
        "the g7 pair spans two cores"
    );
    assert!(
        vshard(SHARED_ACCOUNTS) < vshard(SHARED_POSTINGS),
        "the shared target sorts first"
    );
    assert!(vshard(LOW) < vshard(HIGH), "the low group commits first");
    assert_eq!(vshard(PEER), vshard(HIGH), "a read of the peer joins HIGH");
    for written in [LOW, HIGH] {
        assert_ne!(
            vshard(FEED),
            vshard(written),
            "no block writes the feed's vShard"
        );
    }
}

/// Rejects a 40001 with no concurrent change. Both stored values are read
/// after the `COMMIT` answers, so a failure shows whether the source row or
/// the balance committed alone.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_best_effort_settlement_with_no_concurrent_change_commits() {
    let server = g7_sum_fixture().await;
    begin_best_effort(&server).await;
    run(&server, &format!("UPDATE {POSTINGS} {SETTLE_E0}")).await;
    let commit = server.exec("COMMIT").await;
    let stored_balance = g7_balance(&server).await;
    let stored_sum = g7_source_sum(&server).await;
    assert!(
        commit.is_ok(),
        "COMMIT must commit, got {commit:?}; balance {stored_balance}, source sum {stored_sum}"
    );
    assert_eq!(stored_balance, "6");
    assert_eq!(stored_sum, 6);
}

/// Rejects balance 16 against a source sum of 6 when the source vShard sorts first.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_best_effort_settlement_matches_the_sum_when_the_source_sorts_first() {
    let server = g7_sum_fixture().await;
    begin_best_effort(&server).await;
    run(&server, &format!("UPDATE {POSTINGS} {SETTLE_E0}")).await;
    run_elsewhere(&server, &format!("UPDATE {POSTINGS} {MOVE_E0}")).await;
    expect_commit_or_conflict(server.exec("COMMIT").await);
    let sum = g7_source_sum(&server).await;
    assert_eq!(g7_balance(&server).await, sum.to_string(), "balance = sum");
}

/// Rejects balance 16 against the source sum when the target vShard sorts first.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_best_effort_settlement_matches_the_sum_when_the_target_sorts_first() {
    let server = sum_fixture().await;
    begin_best_effort(&server).await;
    run(&server, &format!("UPDATE {SHARED_POSTINGS} {SETTLE_E0}")).await;
    run_elsewhere(&server, &format!("UPDATE {SHARED_POSTINGS} {MOVE_E0}")).await;
    expect_commit_or_conflict(server.exec("COMMIT").await);
    let sum = source_sum(&server).await;
    assert_eq!(
        balance(&server, "acc").await,
        sum.to_string(),
        "balance = sum"
    );
}

/// Rejects `new` in `g7_low` after a stale read on a vShard no group writes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stale_read_on_an_unwritten_vshard_aborts_before_any_group_commits() {
    let server = group_fixture().await;
    begin_best_effort(&server).await;
    read_then_write_both_groups(&server, FEED, "f1").await;
    run_elsewhere(
        &server,
        &format!("UPDATE {FEED} SET v = 'moved' WHERE id = 'f1'"),
    )
    .await;
    expect_sqlstate(server.exec("COMMIT").await, SERIALIZATION_FAILURE);
    assert_eq!(
        row(&server, LOW, "r1").await,
        "old",
        "the first group aborts"
    );
    assert_eq!(row(&server, HIGH, "r1").await, "old", "no later group runs");
}

/// Rejects `new` in `g7_low` after a stale read on the vShard of the later
/// group.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stale_read_on_a_later_groups_vshard_aborts_before_any_group_commits() {
    let server = group_fixture().await;
    begin_best_effort(&server).await;
    read_then_write_both_groups(&server, PEER, "t1").await;
    run_elsewhere(
        &server,
        &format!("UPDATE {PEER} SET v = 'moved' WHERE id = 't1'"),
    )
    .await;
    expect_sqlstate(server.exec("COMMIT").await, SERIALIZATION_FAILURE);
    assert_eq!(
        row(&server, LOW, "r1").await,
        "old",
        "the first group aborts"
    );
    assert_eq!(row(&server, HIGH, "r1").await, "old", "no later group runs");
}

/// Rejects a 40001 for a read of a row an earlier group writes: the later
/// group must commit.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_read_of_a_row_an_earlier_group_writes_commits() {
    let server = group_fixture().await;
    begin_best_effort(&server).await;
    read_then_write_both_groups(&server, LOW, "r1").await;
    run(&server, "COMMIT").await;
    assert_eq!(
        row(&server, LOW, "r1").await,
        "new",
        "the first group commits"
    );
    assert_eq!(
        row(&server, HIGH, "r1").await,
        "new",
        "the later group commits"
    );
}

/// How long the parked `COMMIT` and the concurrent change wait for an answer.
#[cfg(feature = "failpoints")]
const ANSWER_BOUND: std::time::Duration = std::time::Duration::from_secs(30);

/// The marker a gate writes on arrival, beside its release file.
#[cfg(feature = "failpoints")]
fn parked_marker(release: &std::path::Path) -> std::path::PathBuf {
    let mut marker = release.as_os_str().to_owned();
    marker.push(".parked");
    std::path::PathBuf::from(marker)
}

/// Wait until a transaction reports parked at the gate armed on `release`.
#[cfg(feature = "failpoints")]
async fn wait_parked(release: &std::path::Path) {
    let marker = parked_marker(release);
    for _ in 0..600 {
        if marker.exists() {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    panic!("the later group never reached calvin::after_stamp::{HIGH}");
}

/// Documents a limit: a row written after the block's snapshot makes a later
/// group answer 40001 after the first group committed. The first group
/// validates the block's reads before the change, and the later group validates
/// the row it writes after it.
#[cfg(feature = "failpoints")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_write_conflict_in_a_later_group_answers_40001_after_the_first_group_committed() {
    let gate_dir = tempfile::tempdir().expect("gate tempdir");
    let release = gate_dir.path().join("release-later-group");
    std::fs::write(&release, b"open").expect("open the gate for the fixture");
    let spec = format!(
        "calvin::after_stamp::{HIGH}=wait_file({})",
        release.display()
    );
    let server =
        group_fixture_on(TestServer::start_multicores_with_failpoints(CORES, &spec).await).await;
    let (block, _connection) = server
        .connect_as("nodedb", "nodedb")
        .await
        .expect("connect the block's session");
    for sql in [
        "SET cross_shard_txn = 'best_effort_non_atomic'".to_owned(),
        "BEGIN".to_owned(),
        format!("SELECT v FROM {HIGH} WHERE id = 'r1'"),
        format!("UPDATE {LOW} SET v = 'new' WHERE id = 'r1'"),
        format!("UPDATE {HIGH} SET v = 'new' WHERE id = 'r1'"),
    ] {
        block
            .simple_query(&sql)
            .await
            .unwrap_or_else(|e| panic!("run {sql}: {e}"));
    }
    match std::fs::remove_file(parked_marker(&release)) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => panic!("clear the marker an open pass left: {e}"),
    }
    std::fs::remove_file(&release).expect("arm the gate");
    let commit = tokio::spawn(async move {
        block
            .simple_query("COMMIT")
            .await
            .map(|_| ())
            .map_err(|e| e.code().map(|code| code.code().to_owned()))
    });
    wait_parked(&release).await;
    tokio::time::timeout(
        ANSWER_BOUND,
        run_elsewhere(
            &server,
            &format!("UPDATE {HIGH} SET v = 'moved' WHERE id = 'r1'"),
        ),
    )
    .await
    .expect("the concurrent change did not return while the later group was parked");
    std::fs::write(&release, b"release").expect("release the later group");
    let answer = tokio::time::timeout(ANSWER_BOUND, commit)
        .await
        .expect("the COMMIT did not answer after the release")
        .expect("the COMMIT task");
    assert_eq!(
        answer,
        Err(Some(SERIALIZATION_FAILURE.to_owned())),
        "the later group's write conflicts"
    );
    assert_eq!(
        row(&server, LOW, "r1").await,
        "new",
        "the first group committed"
    );
    assert_eq!(
        row(&server, HIGH, "r1").await,
        "moved",
        "the later group wrote nothing"
    );
}
