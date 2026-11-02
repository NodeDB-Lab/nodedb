// SPDX-License-Identifier: BUSL-1.1

//! The version cleanup pass removes superseded versions once the cleanup
//! horizon passes them, and releases their memory charge. The current read
//! never changes, and an open block keeps reading its snapshot across a
//! pass.
//!
//! The server runs the pass every 200 ms with a history window of about
//! 15 s and a snapshot age limit of 5 s (`start_with_fast_version_cleanup`).

use std::time::{Duration, Instant};

use crate::harness::TestServer;

const PASSWORD: &str = "version-cleanup-3";

/// How long the test waits for the horizon to pass the overwritten history:
/// the 15 s window plus pass and clock slack.
const PURGE_WAIT: Duration = Duration::from_secs(45);

/// Bytes of each overwritten value.
const VALUE_BYTES: usize = 400;

/// Overwrites of the one row.
const OVERWRITES: usize = 300;

/// How long an open block stays open while passes run: several 200 ms
/// passes, well inside the 5 s snapshot age limit.
const BLOCK_SPAN: Duration = Duration::from_millis(1_200);

/// The bytes the governor charges the schemaless document engine.
async fn document_bytes(server: &TestServer) -> i64 {
    let rows = server.query_rows("SHOW MEMORY").await.expect("SHOW MEMORY");
    rows.iter()
        .find(|row| row.first().map(String::as_str) == Some("DocumentSchemaless"))
        .and_then(|row| row.get(1))
        .and_then(|bytes| bytes.parse::<i64>().ok())
        .unwrap_or_else(|| panic!("SHOW MEMORY has no DocumentSchemaless row: {rows:?}"))
}

fn value(round: usize) -> String {
    format!("{round:04}").repeat(VALUE_BYTES / 4)
}

/// Many overwrites of one row grow its history. Once the horizon passes
/// them, the pass removes them and releases their charge, and the current
/// read still answers the last value.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_pass_removes_overwritten_history_and_keeps_the_current_row() {
    let server = TestServer::start_with_fast_version_cleanup().await;
    server
        .exec("CREATE COLLECTION vc_plain")
        .await
        .expect("CREATE COLLECTION");
    server
        .exec(&format!(
            "INSERT INTO vc_plain (id, payload) VALUES ('r', '{}')",
            value(0)
        ))
        .await
        .expect("INSERT");
    let before = document_bytes(&server).await;
    for round in 1..=OVERWRITES {
        server
            .exec(&format!(
                "UPDATE vc_plain SET payload = '{}' WHERE id = 'r'",
                value(round)
            ))
            .await
            .unwrap_or_else(|e| panic!("UPDATE {round}: {e}"));
    }
    let grown = document_bytes(&server).await;
    let history = i64::try_from(OVERWRITES * VALUE_BYTES).expect("history bytes");
    assert!(
        grown - before >= history,
        "the overwrites charge their history: {before} -> {grown}"
    );

    let started = Instant::now();
    let mut now = grown;
    while started.elapsed() < PURGE_WAIT {
        now = document_bytes(&server).await;
        if grown - now >= history {
            break;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    assert!(
        grown - now >= history,
        "the pass releases the purged history: {grown} -> {now} after {:?}",
        started.elapsed()
    );

    let current = server
        .query_text("SELECT payload FROM vc_plain WHERE id = 'r'")
        .await
        .expect("SELECT");
    assert_eq!(
        current,
        vec![value(OVERWRITES)],
        "the current read is unchanged"
    );
}

/// A block's snapshot keeps the version it read across cleanup passes,
/// while another session overwrites the row.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_open_block_keeps_its_version_across_a_pass() {
    let server = TestServer::start_with_fast_version_cleanup().await;
    server
        .exec(&format!("CREATE USER vc_writer PASSWORD '{PASSWORD}'"))
        .await
        .expect("CREATE USER");
    server
        .exec("GRANT ROLE readwrite TO vc_writer")
        .await
        .expect("GRANT ROLE");
    let (writer, handle) = server
        .connect_as("vc_writer", PASSWORD)
        .await
        .expect("second connection");
    server
        .exec("CREATE COLLECTION vc_snap")
        .await
        .expect("CREATE COLLECTION");
    server
        .exec("INSERT INTO vc_snap (id, n) VALUES ('s', 0)")
        .await
        .expect("INSERT");

    let read = "SELECT n FROM vc_snap WHERE id = 's'";
    server.exec("BEGIN").await.expect("BEGIN");
    assert_eq!(
        server.query_text(read).await.expect("first read"),
        vec!["0"]
    );
    let opened = Instant::now();
    for n in 1..=50 {
        writer
            .simple_query(&format!("UPDATE vc_snap SET n = {n} WHERE id = 's'"))
            .await
            .unwrap_or_else(|e| panic!("UPDATE {n}: {e}"));
    }
    // Several passes run, inside the snapshot age limit. The span counts
    // from the first read, so slow UPDATEs do not stretch the block.
    tokio::time::sleep(BLOCK_SPAN.saturating_sub(opened.elapsed())).await;
    assert_eq!(
        server.query_text(read).await.expect("second read"),
        vec!["0"],
        "the block still reads its snapshot"
    );
    server.exec("COMMIT").await.expect("COMMIT");
    assert_eq!(server.query_text(read).await.expect("after"), vec!["50"]);
    drop(writer);
    handle.abort();
}
