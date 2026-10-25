// SPDX-License-Identifier: BUSL-1.1

//! Bitemporal node documents and graph edges share one system-time cut.
//!
//! Document versions are keyed by the system-time ordinal edges stamp from,
//! and a MATCH reads its node documents at the ordinal it reads its edges
//! at. A transaction stamps every document version with its one ordinal, so
//! a burst of versions committed before a read is under that read's cut,
//! however many versions the burst holds. A millisecond stamp raised one per
//! version ran ahead of the clock under such a burst, and the read's cut
//! dropped the versions it reached past.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::harness::TestServer;

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock after the epoch")
        .as_millis() as i64
}

/// Let the wall clock move past the last write's millisecond.
async fn tick() {
    tokio::time::sleep(Duration::from_millis(50)).await;
}

/// Rows staged in the burst transaction ahead of the node documents.
const BURST: usize = 1_000;

/// The node a MATCH binds as `a` when its document holds `role = 'admin'`
/// at `cut`, `None` for the current state.
async fn admins(server: &TestServer, cut: Option<i64>) -> Vec<String> {
    let as_of = cut.map_or(String::new(), |ms| format!(" AS OF SYSTEM TIME {ms}"));
    server
        .query_text(&format!(
            "MATCH (a)-[:knows]->(b) IN 'gdc_people'{as_of} WHERE a.role = 'admin' RETURN a"
        ))
        .await
        .expect("match admins")
}

/// The ids a document read `AS OF SYSTEM TIME cut` returns for `id`.
async fn ids_as_of(server: &TestServer, id: &str, cut: i64) -> Vec<String> {
    server
        .query_rows(&format!(
            "SELECT id FROM gdc_people AS OF SYSTEM TIME {cut} WHERE id = '{id}'"
        ))
        .await
        .expect("document read as of the cut")
        .into_iter()
        .map(|row| row[0].clone())
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_document_burst_stays_under_the_cut_its_edges_are_read_at() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION gdc_people WITH (engine='document_schemaless', bitemporal=true)")
        .await
        .expect("create collection");

    // One transaction: a burst of filler rows, then the two node documents,
    // which sort after every filler row and so stamp last.
    let mut values: Vec<String> = (0..BURST)
        .map(|i| format!("('f{i:05}', 'filler')"))
        .collect();
    values.push("('zz_alice', 'admin')".to_string());
    values.push("('zz_bob', 'user')".to_string());
    server.exec("BEGIN").await.expect("begin");
    server
        .exec(&format!(
            "INSERT INTO gdc_people (id, role) VALUES {}",
            values.join(", ")
        ))
        .await
        .expect("burst insert");
    server.exec("COMMIT").await.expect("commit");
    server
        .exec("GRAPH INSERT EDGE IN 'gdc_people' FROM 'zz_alice' TO 'zz_bob' TYPE 'knows'")
        .await
        .expect("insert edge");

    // The read follows the writes with no pause: every version above is
    // under its cut, documents and edge alike.
    let cut = now_ms();
    assert_eq!(
        admins(&server, Some(cut)).await,
        vec!["zz_alice".to_string()],
        "the node document committed before the cut is read at the edges' cut"
    );
    assert_eq!(
        ids_as_of(&server, "zz_alice", cut).await,
        vec!["zz_alice".to_string()],
        "a document read at the cut sees the committed burst"
    );

    // A later version is above the cut and leaves the cut's answer as it was.
    tick().await;
    server
        .exec("UPDATE gdc_people SET role = 'user' WHERE id = 'zz_alice'")
        .await
        .expect("demote alice");
    assert!(
        admins(&server, None).await.is_empty(),
        "alice is no admin now"
    );
    assert_eq!(
        admins(&server, Some(cut)).await,
        vec!["zz_alice".to_string()],
        "the cut answers the same after a later write"
    );

    // The same cut answers the same after a restart: replay installs the
    // committed ordinals.
    let (server, dir) = server.take_dir();
    server.graceful_shutdown().await;
    let (server, _dir) = TestServer::open_on_path(dir).await;
    assert_eq!(
        admins(&server, Some(cut)).await,
        vec!["zz_alice".to_string()],
        "the cut answers the same after a restart"
    );
    assert_eq!(
        ids_as_of(&server, "zz_alice", cut).await,
        vec!["zz_alice".to_string()],
        "a document read at the cut answers the same after a restart"
    );
    assert!(admins(&server, None).await.is_empty());
}

/// Every version a transaction writes carries its one system time, so the
/// audit log shows one `_ts_system` for the whole transaction.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn one_transaction_stamps_one_system_time() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION gdc_txn WITH (engine='document_schemaless', bitemporal=true)")
        .await
        .expect("create collection");
    let values: Vec<String> = (0..BURST).map(|i| format!("('r{i:05}', 'v')")).collect();
    server.exec("BEGIN").await.expect("begin");
    server
        .exec(&format!(
            "INSERT INTO gdc_txn (id, value) VALUES {}",
            values.join(", ")
        ))
        .await
        .expect("insert");
    server.exec("COMMIT").await.expect("commit");

    let rows = server
        .query_named_rows("SELECT * FROM gdc_txn AS OF SYSTEM TIME NULL")
        .await
        .expect("audit read");
    assert_eq!(rows.len(), BURST, "one version per row");
    let mut stamps: Vec<&str> = rows
        .iter()
        .filter_map(|row| row.get("_ts_system").map(String::as_str))
        .collect();
    stamps.dedup();
    assert_eq!(
        stamps.len(),
        1,
        "every version of the transaction carries one system time, got {stamps:?}"
    );
}

/// A document read at a time before every write and before this server's
/// oldest WAL time anchor answers no rows, never an error: a versioned read
/// compares version ordinals and never maps the time through the anchors.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_cut_before_every_write_reads_no_rows() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION gdc_early WITH (engine='document_schemaless', bitemporal=true)")
        .await
        .expect("create collection");
    server
        .exec("INSERT INTO gdc_early (id, value) VALUES ('r1', 'v')")
        .await
        .expect("insert");
    let rows = server
        .query_rows("SELECT id FROM gdc_early AS OF SYSTEM TIME 1000")
        .await
        .expect("a read before every write is no error");
    assert!(rows.is_empty(), "no row existed at the cut: {rows:?}");
}
