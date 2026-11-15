// SPDX-License-Identifier: BUSL-1.1

//! A period lock against a close of the period by another request.
//!
//! Each request is a `BEGIN .. COMMIT` block that writes two collections on
//! two vShards, so its COMMIT is one Calvin transaction. Without the block,
//! pgwire runs each statement of a simple query on its own, and a statement
//! on one vShard takes no Calvin route. The write plans first and reads the period open.
//! It parks before the sequencer admits it. The close is then sequenced, and
//! its install on the reference row's vShard is held at the redo proposal.
//! The write is released and sequenced after the close, so it must see the
//! period closed: the write is refused, and none of its rows apply.
//!
//! A close by a single-shard autocommit `UPDATE` takes no Calvin route. A
//! write parked at the stamp, whose period that `UPDATE` closes, must be
//! refused when it is released. This holds for a point insert, a bulk
//! update and a batch insert.
//!
//! A block that writes into a period closed before it began is refused. A
//! block that writes into a period and then closes it applies.
//!
//! A best-effort `COMMIT` submits one Calvin transaction per written vShard,
//! in vShard order, and stops at the first refusal. Its period-locked writes
//! are refused like those of a strict `COMMIT`.

#![cfg(feature = "failpoints")]

use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use nodedb_types::{CollectionKey, DatabaseId};

use crate::harness::TestServer;

/// The collections of the foreign-close cell. Their names are fixed: the
/// failpoint spec names them before the server starts.
const FC_PERIODS: &str = "pl_fc_periods";
const FC_ENTRIES: &str = "pl_fc_entries";

/// The period-locked collection of the best-effort cell.
const BE_ENTRIES: &str = "pl_be_entries";

/// A period collection, an entries collection and the period lock between them.
pub(super) async fn setup(server: &TestServer, periods: &str, entries: &str) {
    for sql in [
        format!(
            "CREATE COLLECTION {periods} (period_key TEXT PRIMARY KEY, status TEXT) \
             WITH (engine='document_schemaless')"
        ),
        format!(
            "CREATE COLLECTION {entries} (id TEXT PRIMARY KEY, fiscal_period TEXT, amount INT) \
             WITH (engine='document_schemaless')"
        ),
        format!(
            "ALTER COLLECTION {entries} ADD PERIOD LOCK ON fiscal_period \
             REFERENCES {periods}(period_key) STATUS status \
             ALLOW WRITE WHEN status IN ('OPEN')"
        ),
    ] {
        server
            .exec(&sql)
            .await
            .unwrap_or_else(|e| panic!("run {sql}: {e}"));
    }
}

/// Insert the period row `key` with `status`.
pub(super) async fn seed_period(server: &TestServer, periods: &str, key: &str, status: &str) {
    server
        .exec(&format!(
            "INSERT INTO {periods} (period_key, status) VALUES ('{key}', '{status}')"
        ))
        .await
        .unwrap_or_else(|e| panic!("seed period {key}: {e}"));
}

pub(super) fn vshard(name: &str) -> u32 {
    CollectionKey::from_bare(DatabaseId::DEFAULT, name)
        .vshard()
        .as_u32()
}

/// A name that starts with `prefix` and homes on a vShard none of `taken`
/// homes on.
pub(super) fn name_off(prefix: &str, taken: &[&str]) -> String {
    let used: Vec<u32> = taken.iter().map(|name| vshard(name)).collect();
    (0..)
        .map(|n| format!("{prefix}{n}"))
        .find(|name| !used.contains(&vshard(name)))
        .expect("a name on a free vShard")
}

pub(super) async fn create_side(server: &TestServer, name: &str) {
    server
        .exec(&format!(
            "CREATE COLLECTION {name} (id TEXT PRIMARY KEY, v INT) \
             WITH (engine='document_schemaless')"
        ))
        .await
        .unwrap_or_else(|e| panic!("create {name}: {e}"));
}

/// The marker a gate released by `release` writes when it parks a request.
fn parked_marker(release: &Path) -> PathBuf {
    let mut parked = release.as_os_str().to_owned();
    parked.push(".parked");
    PathBuf::from(parked)
}

/// Wait until the gate released by `release` parked a request.
async fn wait_parked(release: &Path, what: &str) {
    let parked = parked_marker(release);
    let deadline = Instant::now() + Duration::from_secs(30);
    while !parked.exists() {
        assert!(Instant::now() < deadline, "{what} never reached its gate");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// `sql` as one block: its COMMIT flushes every write through Calvin.
pub(super) fn block(sql: &str) -> String {
    format!("BEGIN; {sql}; COMMIT")
}

/// Run `statements` in order on one new connection. The first error ends the
/// run and is the answer.
pub(super) async fn spawn_statements(
    server: &TestServer,
    statements: Vec<String>,
) -> tokio::task::JoinHandle<Result<(), String>> {
    let (client, _connection) = server
        .connect_as("nodedb", "nodedb")
        .await
        .expect("connect a second client");
    tokio::spawn(async move {
        for sql in &statements {
            client
                .simple_query(sql)
                .await
                .map_err(|e| match e.as_db_error() {
                    Some(db) => format!("{} (SQLSTATE {})", db.message(), db.code().code()),
                    None => format!("{e:?}"),
                })?;
        }
        Ok(())
    })
}

/// Run `sql` as one request on its own connection.
pub(super) async fn spawn_request(
    server: &TestServer,
    sql: String,
) -> tokio::task::JoinHandle<Result<(), String>> {
    spawn_statements(server, vec![sql]).await
}

/// A name that starts with `prefix` and homes on a vShard above `floor`'s.
fn name_above(prefix: &str, floor: &str) -> String {
    let floor = vshard(floor);
    (0..1_000_000)
        .map(|n| format!("{prefix}{n}"))
        .find(|name| vshard(name) > floor)
        .expect("a name on a higher vShard")
}

/// A close sequenced before a write into its period, whose install on the
/// reference row's vShard lands after the write's stage. The write read the
/// period open, and the order puts it after the close: it must be refused.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_write_sequenced_after_a_foreign_close_of_its_period_is_refused() {
    const WRITE_RUNS_FOR: Duration = Duration::from_secs(3);

    assert_ne!(vshard(FC_PERIODS), vshard(FC_ENTRIES));
    let close_side = name_off("pl_fc_close_side", &[FC_PERIODS, FC_ENTRIES]);
    let write_side = name_off("pl_fc_write_side", &[FC_PERIODS, FC_ENTRIES]);
    let gate_dir = tempfile::tempdir().expect("gate tempdir");
    let release_write = gate_dir.path().join("release-write");
    let release_close = gate_dir.path().join("release-close");
    let server = TestServer::start_with_failpoints(&format!(
        "calvin::after_stamp::{FC_ENTRIES}=wait_file({}),\
         calvin::before_redo_propose::{FC_PERIODS}=wait_file({})",
        release_write.display(),
        release_close.display()
    ))
    .await;
    setup(&server, FC_PERIODS, FC_ENTRIES).await;
    seed_period(&server, FC_PERIODS, "2026-01", "OPEN").await;
    create_side(&server, &close_side).await;
    create_side(&server, &write_side).await;

    // The write plans and reads the period open, then parks before the
    // sequencer admits it.
    let write = spawn_request(
        &server,
        block(&format!(
            "INSERT INTO {FC_ENTRIES} (id, fiscal_period, amount) VALUES ('e1', '2026-01', 5); \
             INSERT INTO {write_side} (id, v) VALUES ('w1', 1)"
        )),
    )
    .await;
    wait_parked(&release_write, "the write").await;

    // The close is sequenced, and its slice on the reference row's vShard is
    // held before it installs.
    let close = spawn_request(
        &server,
        block(&format!(
            "UPDATE {FC_PERIODS} SET status = 'CLOSED' WHERE period_key = '2026-01'; \
             INSERT INTO {close_side} (id, v) VALUES ('c1', 1)"
        )),
    )
    .await;
    wait_parked(&release_close, "the close").await;

    // The write is sequenced after the close.
    std::fs::write(&release_write, b"release").expect("release the write");
    tokio::time::sleep(WRITE_RUNS_FOR).await;
    let write_answered_while_close_held = write.is_finished();

    std::fs::write(&release_close, b"release").expect("release the close");
    close
        .await
        .expect("close task")
        .unwrap_or_else(|e| panic!("the close must commit: {e}"));
    let written = write.await.expect("write task");
    eprintln!(
        "foreign close: write answered while the close was held: \
         {write_answered_while_close_held}, answer: {written:?}"
    );
    assert!(
        written.is_err(),
        "a write sequenced after the close of its period applied"
    );

    let status = server
        .query_text(&format!(
            "SELECT status FROM {FC_PERIODS} WHERE period_key = '2026-01'"
        ))
        .await
        .unwrap();
    assert_eq!(status, ["CLOSED"]);
    let rows = server
        .query_text(&format!("SELECT id FROM {FC_ENTRIES} WHERE id = 'e1'"))
        .await
        .unwrap();
    assert!(
        rows.is_empty(),
        "a refused write applies no entry: {rows:?}"
    );
    let rows = server
        .query_text(&format!("SELECT id FROM {write_side} WHERE id = 'w1'"))
        .await
        .unwrap();
    assert!(
        rows.is_empty(),
        "a refused write applies none of its rows: {rows:?}"
    );
}

/// The answer of a write refused after its period closed: the period-locked
/// refusal or a serialization conflict.
pub(super) fn is_period_refusal(message: &str) -> bool {
    message.to_lowercase().contains("period locked")
        || message.contains("23603")
        || message.contains("40001")
}

/// Park `write(entries, side)` at the stamp of `entries`, close its period
/// with a single-shard autocommit `UPDATE`, then release the write.
///
/// `seeded` rows `(id, amount)` are in the open period before the write. The
/// write must be refused, and neither collection may change.
async fn refused_after_an_autocommit_close(
    tag: &str,
    seeded: &[(&str, i64)],
    write: fn(&str, &str) -> String,
) {
    let entries = format!("pl_{tag}_entries");
    let periods = name_off(&format!("pl_{tag}_periods"), &[entries.as_str()]);
    let side = name_off(
        &format!("pl_{tag}_side"),
        &[entries.as_str(), periods.as_str()],
    );
    let gate_dir = tempfile::tempdir().expect("gate tempdir");
    let release = gate_dir.path().join("release-write");
    let server = TestServer::start_with_failpoints(&format!(
        "calvin::after_stamp::{entries}=wait_file({})",
        release.display()
    ))
    .await;
    setup(&server, &periods, &entries).await;
    seed_period(&server, &periods, "2026-01", "OPEN").await;
    create_side(&server, &side).await;
    for (id, amount) in seeded {
        server
            .exec(&format!(
                "INSERT INTO {entries} (id, fiscal_period, amount) VALUES ('{id}', '2026-01', {amount})"
            ))
            .await
            .unwrap_or_else(|e| panic!("seed {id}: {e}"));
    }
    let before = server
        .query_text_joined(&format!("SELECT id, amount FROM {entries} ORDER BY id"))
        .await
        .unwrap();

    let parked = spawn_request(&server, block(&write(&entries, &side))).await;
    wait_parked(&release, "the write").await;

    let closed = tokio::time::timeout(
        Duration::from_secs(10),
        server.exec(&format!(
            "UPDATE {periods} SET status = 'CLOSED' WHERE period_key = '2026-01'"
        )),
    )
    .await;
    assert!(
        matches!(closed, Ok(Ok(()))),
        "the close must commit while the write is parked: {closed:?}"
    );

    std::fs::write(&release, b"release").expect("release the write");
    let written = tokio::time::timeout(Duration::from_secs(30), parked)
        .await
        .expect("the released write must answer")
        .expect("write task");
    match &written {
        Ok(()) => panic!("a write sequenced after the close of its period applied"),
        Err(message) => assert!(
            is_period_refusal(message),
            "the refusal must name the period lock or a serialization conflict: {message}"
        ),
    }

    let status = server
        .query_text(&format!(
            "SELECT status FROM {periods} WHERE period_key = '2026-01'"
        ))
        .await
        .unwrap();
    assert_eq!(status, ["CLOSED"]);
    let after = server
        .query_text_joined(&format!("SELECT id, amount FROM {entries} ORDER BY id"))
        .await
        .unwrap();
    assert_eq!(after, before, "a refused write changes no entry");
    let rows = server
        .query_text(&format!("SELECT id FROM {side}"))
        .await
        .unwrap();
    assert!(
        rows.is_empty(),
        "a refused write applies none of its rows: {rows:?}"
    );
}

/// The foreign close is one point `UPDATE` on the reference vShard. It runs
/// no Calvin transaction.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_write_parked_at_the_stamp_is_refused_after_a_single_shard_autocommit_close() {
    refused_after_an_autocommit_close("ac1", &[], |entries, side| {
        format!(
            "INSERT INTO {entries} (id, fiscal_period, amount) VALUES ('e1', '2026-01', 5); \
             INSERT INTO {side} (id, v) VALUES ('w1', 1)"
        )
    })
    .await;
}

/// A bulk update names no row at plan time. The rows it touches are known
/// after the read.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_bulk_update_in_a_request_is_refused_after_a_foreign_close() {
    refused_after_an_autocommit_close("ac2", &[("e1", 10), ("e2", 20)], |entries, side| {
        format!(
            "UPDATE {entries} SET amount = amount + 1 WHERE fiscal_period = '2026-01'; \
             INSERT INTO {side} (id, v) VALUES ('w1', 1)"
        )
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_batch_insert_in_a_request_is_refused_after_a_foreign_close() {
    refused_after_an_autocommit_close("ac3", &[], |entries, side| {
        format!(
            "INSERT INTO {entries} (id, fiscal_period, amount) \
             VALUES ('e1', '2026-01', 5), ('e2', '2026-01', 6); \
             INSERT INTO {side} (id, v) VALUES ('w1', 1)"
        )
    })
    .await;
}

/// The answer to a block must come within the statement deadline. A halted
/// vShard answers `57014` after 30 s.
pub(super) const ORD_ANSWER_WITHIN: Duration = Duration::from_secs(20);

/// A block on two vShards that writes into a period closed before the block
/// began. The answer is a clean refusal, within the deadline, and no row
/// applies.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_block_that_writes_into_an_already_closed_period_is_refused() {
    let server = TestServer::start().await;
    let periods = "pl_cl_periods";
    let entries = name_off("pl_cl_entries", &[periods]);
    let side = name_off("pl_cl_side", &[periods, entries.as_str()]);
    setup(&server, periods, &entries).await;
    seed_period(&server, periods, "2026-01", "CLOSED").await;
    create_side(&server, &side).await;

    let write = spawn_request(
        &server,
        block(&format!(
            "INSERT INTO {entries} (id, fiscal_period, amount) VALUES ('e1', '2026-01', 5); \
             INSERT INTO {side} (id, v) VALUES ('w1', 1)"
        )),
    )
    .await;
    let written = tokio::time::timeout(ORD_ANSWER_WITHIN, write)
        .await
        .expect("the block must answer within the deadline")
        .expect("write task");
    match &written {
        Ok(()) => panic!("a block applied into a closed period"),
        Err(message) => assert!(
            is_period_refusal(message),
            "the refusal must name the period lock: {message}"
        ),
    }

    let rows = server
        .query_text(&format!("SELECT id FROM {entries}"))
        .await
        .unwrap();
    assert!(
        rows.is_empty(),
        "a refused block applies no entry: {rows:?}"
    );
    let rows = server
        .query_text(&format!("SELECT id FROM {side}"))
        .await
        .unwrap();
    assert!(rows.is_empty(), "a refused block applies no row: {rows:?}");
}

/// A best-effort `COMMIT` whose period another request closes. The side
/// collection homes above the entries, so the entries group commits first and
/// the refusal stops the run before the side group. The entries group parks
/// before the sequencer admits it, and a single-shard autocommit `UPDATE`
/// closes the period. The released group is refused, and no row applies.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_best_effort_commit_that_writes_into_a_period_another_request_closes_is_refused() {
    let periods = name_off("pl_be_periods", &[BE_ENTRIES]);
    let side = name_above("pl_be_side", BE_ENTRIES);
    let gate_dir = tempfile::tempdir().expect("gate tempdir");
    let release = gate_dir.path().join("release-write");
    let server = TestServer::start_with_failpoints(&format!(
        "calvin::after_stamp::{BE_ENTRIES}=wait_file({})",
        release.display()
    ))
    .await;
    setup(&server, &periods, BE_ENTRIES).await;
    seed_period(&server, &periods, "2026-01", "OPEN").await;
    create_side(&server, &side).await;

    let parked = spawn_statements(
        &server,
        vec![
            "SET cross_shard_txn = 'best_effort_non_atomic'".to_string(),
            block(&format!(
                "INSERT INTO {BE_ENTRIES} (id, fiscal_period, amount) \
                 VALUES ('e1', '2026-01', 5); \
                 INSERT INTO {side} (id, v) VALUES ('w1', 1)"
            )),
        ],
    )
    .await;
    wait_parked(&release, "the best-effort write").await;

    let closed = tokio::time::timeout(
        Duration::from_secs(10),
        server.exec(&format!(
            "UPDATE {periods} SET status = 'CLOSED' WHERE period_key = '2026-01'"
        )),
    )
    .await;
    assert!(
        matches!(closed, Ok(Ok(()))),
        "the close must commit while the write is parked: {closed:?}"
    );

    std::fs::write(&release, b"release").expect("release the write");
    let written = tokio::time::timeout(ORD_ANSWER_WITHIN, parked)
        .await
        .expect("the released write must answer within the deadline")
        .expect("write task");
    match &written {
        Ok(()) => panic!("a best-effort write applied after its period closed"),
        Err(message) => assert!(
            is_period_refusal(message),
            "the refusal must name the period lock or a serialization conflict: {message}"
        ),
    }

    let rows = server
        .query_text(&format!("SELECT id FROM {BE_ENTRIES}"))
        .await
        .unwrap();
    assert!(
        rows.is_empty(),
        "a refused group applies no entry: {rows:?}"
    );
    let rows = server
        .query_text(&format!("SELECT id FROM {side}"))
        .await
        .unwrap();
    assert!(
        rows.is_empty(),
        "the refusal stops the run before the side group: {rows:?}"
    );
}

/// `BEGIN; INSERT; UPDATE; COMMIT` on two vShards, ten rounds, one period
/// each. The write comes first, so both statements apply. A refusal or a
/// halted vShard in any round fails the cell.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_block_that_writes_into_a_period_then_closes_it_applies_ten_times() {
    let server = TestServer::start().await;
    let periods = "pl_blk_periods";
    let entries = name_off("pl_blk_entries", &[periods]);
    setup(&server, periods, &entries).await;
    for round in 0..10 {
        let key = format!("2026-{:02}", round + 1);
        seed_period(&server, periods, &key, "OPEN").await;
        for sql in [
            "BEGIN".to_string(),
            format!(
                "INSERT INTO {entries} (id, fiscal_period, amount) \
                 VALUES ('e{round}', '{key}', 5)"
            ),
            format!("UPDATE {periods} SET status = 'CLOSED' WHERE period_key = '{key}'"),
            "COMMIT".to_string(),
        ] {
            let answered = tokio::time::timeout(ORD_ANSWER_WITHIN, server.exec(&sql))
                .await
                .unwrap_or_else(|_| panic!("round {round}: `{sql}` did not answer in time"));
            answered.unwrap_or_else(|e| panic!("round {round}: `{sql}` must apply: {e}"));
        }
        let rows = server
            .query_text(&format!("SELECT id FROM {entries} WHERE id = 'e{round}'"))
            .await
            .unwrap();
        assert_eq!(rows, [format!("e{round}")], "round {round}");
    }
}
