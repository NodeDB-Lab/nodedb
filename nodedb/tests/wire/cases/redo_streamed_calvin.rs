// SPDX-License-Identifier: BUSL-1.1

//! A cross-shard transaction commits through the Calvin scheduler, and each
//! participant slice's redo passes `tuning.calvin.max_redo_entry_bytes`.
//! Each slice's leader reads the slice's resolve from its core in parts and
//! proposes them as chunk entries while it reads.
//!
//! - Every row of both slices reads back with its own value, live and after
//!   a WAL-only restart.
//! - A stream held before its second chunk on both participants shows no
//!   row of either slice: a read during the hold answers at once with a
//!   count of 0. Once released, the transaction installs whole. Requires
//!   `--features failpoints`.

use std::collections::BTreeMap;

use super::backup_support::names_on_two_vshards;
use crate::harness::TestServer;

/// The redo entry limit the server boots with: the smallest the config takes.
const ENTRY_LIMIT: usize = 64 * 1024;

/// Rows one statement inserts into one collection.
const ROWS_PER_STATEMENT: usize = 100;

/// Statements the transaction runs per collection. With 400-byte values each
/// slice's redo holds several times `ENTRY_LIMIT`.
const STATEMENTS: usize = 4;

fn value_of(id: &str) -> String {
    format!("{id}-{}", "y".repeat(400))
}

/// The statements of one transaction that inserts `STATEMENTS *
/// ROWS_PER_STATEMENT` rows into each of `colls`.
fn transaction(colls: &[&str]) -> Vec<String> {
    let mut statements = vec!["BEGIN".to_owned()];
    for coll in colls {
        for statement in 0..STATEMENTS {
            let values: Vec<String> = (0..ROWS_PER_STATEMENT)
                .map(|row| {
                    let id = format!("r{}", statement * ROWS_PER_STATEMENT + row);
                    format!("('{id}', '{}')", value_of(&id))
                })
                .collect();
            statements.push(format!(
                "INSERT INTO {coll} (id, value) VALUES {}",
                values.join(", ")
            ));
        }
    }
    statements.push("COMMIT".to_owned());
    statements
}

async fn create(srv: &TestServer, coll: &str) {
    srv.exec(&format!(
        "CREATE COLLECTION {coll} (id STRING PRIMARY KEY, value STRING) \
         WITH (engine='document_strict')"
    ))
    .await
    .unwrap_or_else(|e| panic!("create {coll}: {e}"));
}

/// Every row of `coll`, by id.
async fn rows_of(srv: &TestServer, coll: &str) -> BTreeMap<String, String> {
    srv.query_rows(&format!("SELECT id, value FROM {coll}"))
        .await
        .expect("SELECT rows")
        .into_iter()
        .map(|row| (row[0].clone(), row[1].clone()))
        .collect()
}

/// Assert `coll` holds every row of the transaction, each with its value.
async fn assert_rows(srv: &TestServer, coll: &str, when: &str) {
    let rows = rows_of(srv, coll).await;
    assert_eq!(
        rows.len(),
        STATEMENTS * ROWS_PER_STATEMENT,
        "{when}: every row of '{coll}' installs exactly once"
    );
    for n in 0..STATEMENTS * ROWS_PER_STATEMENT {
        let id = format!("r{n}");
        assert_eq!(
            rows.get(&id).map(String::as_str),
            Some(value_of(&id).as_str()),
            "{when}: row {id} of '{coll}' reads back with its own value"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_calvin_commit_streams_each_slice_redo_in_several_chunks() {
    let srv = TestServer::start_with_max_redo_entry_bytes(ENTRY_LIMIT).await;
    let (first, second) = names_on_two_vshards("calvin_stream");
    create(&srv, &first).await;
    create(&srv, &second).await;

    for sql in transaction(&[&first, &second]) {
        srv.exec(&sql)
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
    }
    assert_rows(&srv, &first, "live").await;
    assert_rows(&srv, &second, "live").await;

    let (srv, dir) = srv.take_dir();
    srv.graceful_shutdown().await;
    let (srv, _dir) = TestServer::open_on_path_with_max_redo_entry_bytes(dir, ENTRY_LIMIT).await;
    assert_rows(&srv, &first, "after restart").await;
    assert_rows(&srv, &second, "after restart").await;
}

/// Both slices' streams hold before their second chunk. While they hold, no
/// participant shows a row: the first chunk of each slice installs nothing.
/// The committed transaction cannot drop its redo, so once released every
/// row of both slices installs.
#[cfg(feature = "failpoints")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_held_slice_stream_shows_no_row_until_it_completes() {
    use std::time::Duration;

    /// How long the hold is watched.
    const HELD_FOR: Duration = Duration::from_millis(1500);
    /// How long one read during the hold gets before the test fails.
    const READ_DEADLINE: Duration = Duration::from_secs(5);

    let (first, second) = names_on_two_vshards("calvin_held");
    let gate_dir = tempfile::tempdir().expect("gate tempdir");
    let release = gate_dir.path().join("release-stream");
    let spec = [&first, &second]
        .iter()
        .map(|coll| {
            format!(
                "calvin::before_second_chunk::{coll}=wait_file({})",
                release.display()
            )
        })
        .collect::<Vec<_>>()
        .join(",");
    let srv = TestServer::start_with_max_redo_entry_bytes_and_failpoints(ENTRY_LIMIT, &spec).await;
    create(&srv, &first).await;
    create(&srv, &second).await;

    let (committer, _committer_conn) = srv
        .connect_as("nodedb", "nodedb")
        .await
        .expect("connect the committer");
    let statements = transaction(&[&first, &second]);
    let commit = tokio::spawn(async move {
        for sql in &statements {
            committer
                .simple_query(sql)
                .await
                .map_err(|e| format!("{sql}: {e}"))?;
        }
        Ok::<(), String>(())
    });

    let mut parked = release.clone().into_os_string();
    parked.push(".parked");
    let parked = std::path::PathBuf::from(parked);
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    while !parked.exists() {
        assert!(
            std::time::Instant::now() < deadline,
            "no slice stream reached its gate"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    tokio::time::sleep(HELD_FOR).await;
    assert!(!commit.is_finished(), "the COMMIT was not held");

    // A read during the hold answers at once and counts 0 for each slice.
    // The read waits only for this node to apply its group through the read
    // index. The held stream's first chunk applied as held bytes, which
    // install nothing, and the slice's staged rows live in its own overlay,
    // which no other read sees. A read that does not answer fails the test.
    let (reader, _reader_conn) = srv
        .connect_as("nodedb", "nodedb")
        .await
        .expect("connect the reader");
    for coll in [&first, &second] {
        let messages = tokio::time::timeout(
            READ_DEADLINE,
            reader.simple_query(&format!("SELECT COUNT(*) FROM {coll}")),
        )
        .await
        .unwrap_or_else(|_| panic!("a read of '{coll}' during the hold did not answer"))
        .expect("COUNT(*)");
        let count = messages
            .into_iter()
            .find_map(|message| match message {
                tokio_postgres::SimpleQueryMessage::Row(row) => row.get(0).map(str::to_owned),
                _ => None,
            })
            .expect("one COUNT(*) row");
        assert_eq!(count, "0", "a held stream shows no row of '{coll}'");
    }
    assert!(
        !commit.is_finished(),
        "the COMMIT stays held while the reads run"
    );

    std::fs::write(&release, b"release").expect("release the streams");
    commit
        .await
        .expect("commit task")
        .unwrap_or_else(|e| panic!("commit: {e}"));
    assert_rows(&srv, &first, "released").await;
    assert_rows(&srv, &second, "released").await;

    let (srv, dir) = srv.take_dir();
    srv.graceful_shutdown().await;
    let (srv, _dir) = TestServer::open_on_path_with_max_redo_entry_bytes(dir, ENTRY_LIMIT).await;
    assert_rows(&srv, &first, "after restart").await;
    assert_rows(&srv, &second, "after restart").await;
}
