// SPDX-License-Identifier: BUSL-1.1

//! A crash after an entry applied and before its group's applied floor is
//! saved loses no change event and publishes none twice.
//!
//! Each test arms `apply_loop::before_floor_save::<collection>` with a long
//! sleep. The point fires once boot's replay finished, for a pass that
//! published new events of the collection. The apply loop settles the
//! write's entry, publishes its change events, and sleeps before it journals
//! them and saves the floor. The test
//! reads the published events from the change feed, then kills the server
//! inside the sleep. After the restart the entry is delivered again as a
//! repeat, and its events stage again from the WAL. The feed then holds
//! each event exactly once, with the position, LSN and timestamp it had
//! before the crash. A second restart changes nothing.
//!
//! One test per source of change events:
//!
//! - a plan entry: a point insert, update and delete, each named by its plan;
//! - an inline transaction redo: rows named by the redo record;
//! - a Calvin cross-shard transaction: one slice per data group;
//! - a chunked redo: rows read back from the stream's chunks.
//!
//! Requires `--features failpoints`.

#![cfg(feature = "failpoints")]

mod crash_harness;

use std::time::Duration;

use crash_harness::vshards::{names_in_distinct_data_groups, single_node_data_group};
use crash_harness::{CrashHarness, diagnostics};

/// How long the armed apply loop sleeps before it saves the floor. The
/// test kills the server well inside it.
const FLOOR_SLEEP_MS: u64 = 120_000;

/// How long a poll waits for the expected events.
const POLL_DEADLINE: Duration = Duration::from_secs(60);

const CALVIN_READY_TIMEOUT: Duration = Duration::from_secs(30);

/// The redo entry limit of the chunked test: the smallest the config takes.
const ENTRY_LIMIT: usize = 64 * 1024;

/// One published change, as the poll reports it.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct Change {
    /// The event's position in its own partition: its data group's entry in
    /// the cursor that resumes right after it.
    position: String,
    document_id: String,
    operation: String,
    lsn: u64,
    timestamp_ms: u64,
}

impl Change {
    /// The change `value` reports for a collection whose writes apply in
    /// data group `group`.
    ///
    /// The poll's `cursor` is a resume token, not the event's identity. The
    /// replay raises it past every event it walks in ring order, the events
    /// of other partitions and other collections included. A restart
    /// rebuilds the ring feed by feed from the journal, then appends what the
    /// apply loop settles, so the order across partitions differs and other
    /// partitions' entries in the token differ with it. The entry of the
    /// event's own partition is the event's position: a partition's events
    /// sit in the ring in ascending position.
    fn of(value: &serde_json::Value, group: u32) -> Self {
        let text = |field: &str| {
            value[field]
                .as_str()
                .unwrap_or_else(|| panic!("change field {field} is text: {value}"))
                .to_owned()
        };
        let number = |field: &str| {
            value[field]
                .as_u64()
                .unwrap_or_else(|| panic!("change field {field} is a number: {value}"))
        };
        let cursor = text("cursor");
        let own = format!("g{group}@");
        let position = cursor
            .trim_start_matches("v2:")
            .split(',')
            .find_map(|entry| entry.strip_prefix(own.as_str()))
            .unwrap_or_else(|| panic!("cursor {cursor} names no position in group {group}"))
            .to_owned();
        Self {
            position,
            document_id: text("document_id"),
            operation: text("operation"),
            lsn: number("lsn"),
            timestamp_ms: number("timestamp_ms"),
        }
    }
}

/// A server whose config file lowers the redo entry limit, so a large
/// commit travels as a chunked redo.
fn chunked_harness() -> CrashHarness {
    let h = CrashHarness::new();
    let config = h.data_dir().join("change_feed_floor.toml");
    std::fs::write(
        &config,
        format!("[tuning.calvin]\nmax_redo_entry_bytes = {ENTRY_LIMIT}\n"),
    )
    .expect("write config file");
    let path = config.to_string_lossy().to_string();
    h.with_env("NODEDB_CONFIG", &path)
}

async fn api_token(h: &CrashHarness) -> String {
    h.query_col("CREATE API KEY FOR nodedb", "api_key")
        .await
        .into_iter()
        .next()
        .expect("CREATE API KEY returns the token row")
}

/// Poll the change feed of `coll` until it holds `expected` events or the
/// wait ends. Returns every event, sorted by its position.
async fn poll_changes(h: &CrashHarness, token: &str, coll: &str, expected: usize) -> Vec<Change> {
    let client = reqwest::Client::new();
    let deadline = tokio::time::Instant::now() + POLL_DEADLINE;
    loop {
        let body: serde_json::Value = client
            .get(format!(
                "http://127.0.0.1:{}/v1/cdc/{coll}/poll?since_ms=0&limit=10000",
                h.http_port
            ))
            .bearer_auth(token)
            .send()
            .await
            .expect("poll the change feed")
            .json()
            .await
            .expect("a JSON change page");
        let changes = body["changes"].as_array().cloned().unwrap_or_default();
        if changes.len() >= expected || tokio::time::Instant::now() >= deadline {
            assert_eq!(body["has_more"], serde_json::json!(false), "{body}");
            let group = single_node_data_group(coll);
            let mut changes: Vec<Change> = changes
                .iter()
                .map(|change| Change::of(change, group))
                .collect();
            changes.sort();
            return changes;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Run `statements` in order on their own connection. The server dies while
/// the last statement can still wait, so the caller reads the result only
/// when the events it waits for never publish.
fn spawn_statements(
    h: &CrashHarness,
    statements: Vec<String>,
) -> tokio::task::JoinHandle<Result<(), String>> {
    let conn_str = h.pgwire_conn_str();
    tokio::spawn(async move {
        let (client, connection) = tokio_postgres::connect(&conn_str, tokio_postgres::NoTls)
            .await
            .map_err(|e| format!("connect: {e}"))?;
        tokio::spawn(connection);
        for statement in &statements {
            client.simple_query(statement).await.map_err(|e| {
                let detail = e
                    .as_db_error()
                    .map(|db| format!("{}: {}", db.code().code(), db.message()))
                    .unwrap_or_else(|| e.to_string());
                format!("{statement}: {detail}")
            })?;
        }
        Ok(())
    })
}

/// The events one collection's feed holds once the crashed statements
/// applied.
struct Expected<'a> {
    collection: &'a str,
    count: usize,
    /// Whether the floor-save sleep is armed for the collection. The armed
    /// pass blocks the apply loop, so the entries of an unarmed collection
    /// settle before it, in the same pass, or after the crash.
    armed: bool,
}

/// What one crash test runs: the collections it creates, the statements
/// the server runs before it is armed, the statements whose entries it
/// crashes inside, and the events each collection's feed then holds.
struct CrashCase<'a> {
    harness: CrashHarness,
    create: Vec<String>,
    before: Vec<String>,
    crashed: Vec<String>,
    expected: Vec<Expected<'a>>,
}

/// Run `case`: write, crash after the events published and before the
/// floor save, restart twice, and compare the feed each time.
async fn crash_before_the_floor_save(case: CrashCase<'_>) {
    let CrashCase {
        harness: mut h,
        create,
        before,
        crashed,
        expected,
    } = case;
    h.spawn();
    h.wait_ready();
    // A write the Calvin scheduler orders needs its sequencer.
    h.wait_for_calvin_ready(CALVIN_READY_TIMEOUT).await;
    for statement in create.iter().chain(&before) {
        h.exec(statement).await;
    }
    let token = api_token(&h).await;

    // Arm the floor-save sleep for the written collections on a fresh boot.
    // The point fires only once that boot's replay finished.
    let armed: Vec<String> = expected
        .iter()
        .filter(|expected| expected.armed)
        .map(|expected| {
            format!(
                "apply_loop::before_floor_save::{}=sleep({FLOOR_SLEEP_MS})",
                expected.collection
            )
        })
        .collect();
    h.kill_9();
    h.set_env("NODEDB_FAILPOINTS", &armed.join(","));
    h.reopen();
    // A write the Calvin scheduler orders needs its sequencer.
    h.wait_for_calvin_ready(CALVIN_READY_TIMEOUT).await;
    let mut writer = spawn_statements(&h, crashed);

    let mut live = Vec::new();
    for expected in expected.iter().filter(|expected| expected.armed) {
        let coll = expected.collection;
        let changes = poll_changes(&h, &token, coll, expected.count).await;
        if changes.len() != expected.count {
            let writer_outcome = if writer.is_finished() {
                format!("{:?}", (&mut writer).await)
            } else {
                "the statements still run".to_owned()
            };
            panic!(
                "the entry's events never published before the floor save on {coll}: {} of {} \
                 events; writer: {writer_outcome}\n{}",
                changes.len(),
                expected.count,
                diagnostics::log_tail_section(&h.server_log())
            );
        }
    }
    // The armed pass sleeps now. Every feed holds what published before it.
    for expected in &expected {
        live.push(poll_changes(&h, &token, expected.collection, 0).await);
    }
    // Inside the sleep: the entries applied and published, and no floor
    // covers them.
    h.kill_9();
    writer.abort();
    h.clear_env("NODEDB_FAILPOINTS");

    for restart in ["first", "second"] {
        h.reopen();
        h.wait_for_calvin_ready(CALVIN_READY_TIMEOUT).await;
        for (expected, before_crash) in expected.iter().zip(&live) {
            let coll = expected.collection;
            let changes = poll_changes(&h, &token, coll, expected.count).await;
            assert_eq!(
                changes.len(),
                expected.count,
                "after the {restart} restart, the feed of {coll} lost an event or holds one \
                 twice\n{}",
                diagnostics::log_tail_section(&h.server_log())
            );
            // Every event published before the crash keeps its position,
            // LSN and timestamp.
            for change in before_crash {
                assert!(
                    changes.contains(change),
                    "after the {restart} restart, the feed of {coll} holds {change:?} at \
                     another position, LSN or timestamp: {changes:?}\n{}",
                    diagnostics::log_tail_section(&h.server_log())
                );
            }
        }
        h.kill_9();
    }
}

fn armed(collection: &str, count: usize) -> Expected<'_> {
    Expected {
        collection,
        count,
        armed: true,
    }
}

fn document_collection(coll: &str) -> String {
    format!(
        "CREATE COLLECTION {coll} (id STRING PRIMARY KEY, value STRING) \
         WITH (engine='document_strict')"
    )
}

#[tokio::test(flavor = "multi_thread")]
async fn a_point_insert_publishes_once_across_a_crash_before_its_floor() {
    crash_before_the_floor_save(CrashCase {
        harness: CrashHarness::new(),
        create: vec![document_collection("plan_insert")],
        before: Vec::new(),
        crashed: vec!["INSERT INTO plan_insert (id, value) VALUES ('p1', 'a')".into()],
        expected: vec![armed("plan_insert", 1)],
    })
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_point_update_publishes_once_across_a_crash_before_its_floor() {
    crash_before_the_floor_save(CrashCase {
        harness: CrashHarness::new(),
        create: vec![document_collection("plan_update")],
        before: vec!["INSERT INTO plan_update (id, value) VALUES ('p1', 'a')".into()],
        crashed: vec!["UPDATE plan_update SET value = 'b' WHERE id = 'p1'".into()],
        expected: vec![armed("plan_update", 2)],
    })
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_point_delete_publishes_once_across_a_crash_before_its_floor() {
    crash_before_the_floor_save(CrashCase {
        harness: CrashHarness::new(),
        create: vec![document_collection("plan_delete")],
        before: vec!["INSERT INTO plan_delete (id, value) VALUES ('p1', 'a')".into()],
        crashed: vec!["DELETE FROM plan_delete WHERE id = 'p1'".into()],
        expected: vec![armed("plan_delete", 2)],
    })
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn an_inline_redo_publishes_once_across_a_crash_before_its_floor() {
    crash_before_the_floor_save(CrashCase {
        harness: CrashHarness::new(),
        create: vec![document_collection("inline_redo")],
        before: Vec::new(),
        crashed: vec![
            "BEGIN".into(),
            "INSERT INTO inline_redo (id, value) VALUES ('r1', 'a'), ('r2', 'b')".into(),
            "INSERT INTO inline_redo (id, value) VALUES ('r3', 'c')".into(),
            "COMMIT".into(),
        ],
        expected: vec![armed("inline_redo", 3)],
    })
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_calvin_transaction_publishes_once_across_a_crash_before_its_floors() {
    let [left, right] = names_in_distinct_data_groups(["calvin_left", "calvin_right"]);
    crash_before_the_floor_save(CrashCase {
        harness: CrashHarness::new(),
        create: vec![document_collection(&left), document_collection(&right)],
        before: Vec::new(),
        crashed: vec![
            "BEGIN".into(),
            format!("INSERT INTO {left} (id, value) VALUES ('c1', 'a')"),
            format!("INSERT INTO {right} (id, value) VALUES ('c2', 'b')"),
            "COMMIT".into(),
        ],
        expected: vec![
            armed(&left, 1),
            Expected {
                collection: &right,
                count: 1,
                armed: false,
            },
        ],
    })
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_chunked_redo_publishes_once_across_a_crash_before_its_floor() {
    const STATEMENTS: usize = 6;
    const ROWS_PER_STATEMENT: usize = 100;
    let filler = "x".repeat(400);
    let mut crashed = vec!["BEGIN".to_string()];
    for statement in 0..STATEMENTS {
        let values: Vec<String> = (0..ROWS_PER_STATEMENT)
            .map(|row| format!("('k{}', '{filler}')", statement * ROWS_PER_STATEMENT + row))
            .collect();
        crashed.push(format!(
            "INSERT INTO chunked_redo (id, value) VALUES {}",
            values.join(", ")
        ));
    }
    crashed.push("COMMIT".into());
    crash_before_the_floor_save(CrashCase {
        harness: chunked_harness(),
        create: vec![document_collection("chunked_redo")],
        before: Vec::new(),
        crashed,
        expected: vec![armed("chunked_redo", STATEMENTS * ROWS_PER_STATEMENT)],
    })
    .await;
}
