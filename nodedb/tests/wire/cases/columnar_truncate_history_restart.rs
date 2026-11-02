// SPDX-License-Identifier: BUSL-1.1

//! A columnar, spatial or timeseries `TRUNCATE` ends every row at its
//! ordinal and keeps the rows as history. A read before the truncate still
//! sees them: an `AS OF SYSTEM TIME` read, and a transaction snapshot pinned
//! before it. The server is then killed, so no final checkpoint runs, and a
//! restart replays the WAL: the history survives the restart.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::harness::TestServer;

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock after epoch")
        .as_millis() as i64
}

/// Sorted ids `sql` returns.
async fn ids(srv: &TestServer, sql: &str) -> Vec<String> {
    let mut ids: Vec<String> = srv
        .query_rows(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .into_iter()
        .map(|row| row[0].clone())
        .collect();
    ids.sort();
    ids
}

/// The collections under test and the row each insert writes.
struct Case {
    collection: &'static str,
    create: &'static str,
    insert: fn(&str, &str) -> String,
}

fn columnar_insert(collection: &str, id: &str) -> String {
    format!("INSERT INTO {collection} (id, n) VALUES ('{id}', 1)")
}

fn spatial_insert(collection: &str, id: &str) -> String {
    format!("INSERT INTO {collection} (id, loc) VALUES ('{id}', ST_MakePoint(1.0, 1.0))")
}

fn timeseries_insert(collection: &str, id: &str) -> String {
    format!("INSERT INTO {collection} (id, ts, v) VALUES ('{id}', 1000, 1)")
}

const CASES: [Case; 3] = [
    Case {
        collection: "th_columnar",
        create: "CREATE COLLECTION th_columnar COLUMNS (id TEXT, n INT) \
                 WITH (engine='columnar')",
        insert: columnar_insert,
    },
    Case {
        collection: "th_spatial",
        create: "CREATE COLLECTION th_spatial COLUMNS (id TEXT, loc GEOMETRY) \
                 WITH (engine='spatial')",
        insert: spatial_insert,
    },
    Case {
        collection: "th_timeseries",
        create: "CREATE COLLECTION th_timeseries COLUMNS (id TEXT, ts BIGINT TIME_KEY, v INT) \
                 WITH (engine='timeseries')",
        insert: timeseries_insert,
    },
];

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn truncate_history_survives_a_restart_that_replays_the_wal() {
    let srv = TestServer::start().await;
    for case in &CASES {
        srv.exec(case.create)
            .await
            .unwrap_or_else(|e| panic!("create {}: {e}", case.collection));
        for id in ["a", "b"] {
            srv.exec(&(case.insert)(case.collection, id))
                .await
                .unwrap_or_else(|e| panic!("seed {id}: {e}"));
        }
    }

    // A transaction snapshot pinned before the truncate.
    let (reader, _reader_task) = srv.connect_as("nodedb", "nodedb").await.unwrap();
    reader.simple_query("BEGIN").await.expect("BEGIN");
    for case in &CASES {
        let pinned = reader
            .simple_query(&format!("SELECT id FROM {}", case.collection))
            .await
            .expect("pinning read");
        assert!(!pinned.is_empty());
    }

    // Margins on both sides of `before` keep millisecond rounding out.
    tokio::time::sleep(Duration::from_millis(50)).await;
    let before = now_ms();
    tokio::time::sleep(Duration::from_millis(50)).await;

    for case in &CASES {
        srv.exec(&format!("TRUNCATE {}", case.collection))
            .await
            .unwrap_or_else(|e| panic!("truncate {}: {e}", case.collection));
        srv.exec(&(case.insert)(case.collection, "z"))
            .await
            .unwrap_or_else(|e| panic!("post-truncate insert: {e}"));
    }

    // The snapshot pinned before the truncate still reads the rows.
    for case in &CASES {
        let rows = reader
            .simple_query(&format!("SELECT id FROM {} ORDER BY id", case.collection))
            .await
            .expect("snapshot read");
        let mut seen: Vec<String> = rows
            .iter()
            .filter_map(|msg| match msg {
                tokio_postgres::SimpleQueryMessage::Row(row) => row.get(0).map(str::to_string),
                _ => None,
            })
            .collect();
        seen.sort();
        assert_eq!(
            seen,
            ["a", "b"],
            "{}: a snapshot pinned before the truncate reads the rows",
            case.collection
        );
    }
    reader.simple_query("COMMIT").await.expect("COMMIT");

    for case in &CASES {
        assert_eq!(
            ids(
                &srv,
                &format!(
                    "SELECT id FROM {} AS OF SYSTEM TIME {before}",
                    case.collection
                )
            )
            .await,
            ["a", "b"],
            "{}: AS OF before the truncate, live",
            case.collection
        );
    }

    // Kill the server: a graceful shutdown runs a final checkpoint, a kill
    // does not, so the restart replays the WAL.
    let (srv, dir) = srv.take_dir();
    drop(srv);
    let (srv, _dir) = TestServer::open_on_path(dir).await;

    for case in &CASES {
        assert_eq!(
            ids(
                &srv,
                &format!(
                    "SELECT id FROM {} AS OF SYSTEM TIME {before}",
                    case.collection
                )
            )
            .await,
            ["a", "b"],
            "{}: AS OF before the truncate after a restart",
            case.collection
        );
        assert_eq!(
            ids(&srv, &format!("SELECT id FROM {}", case.collection)).await,
            ["z"],
            "{}: the current read sees only the row written after the truncate",
            case.collection
        );
    }
}
