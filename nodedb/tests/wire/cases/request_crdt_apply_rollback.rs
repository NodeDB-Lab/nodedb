// SPDX-License-Identifier: BUSL-1.1

//! A request that holds `SELECT crdt_apply(..)` and a later statement that
//! fails leaves no CRDT row. The request is either refused whole or rolled
//! back whole. Over HTTP and native, the CRDT row stays absent.

use nodedb_test_support::native_harness::{open_trust_session, send_sql};

use super::wire_support::exec;
use crate::harness::TestServer;

const COLL: &str = "crdt_rollback";

/// Hex of a Loro delta that creates row `a` in `COLL`.
fn row_delta_hex() -> String {
    let doc = loro::LoroDoc::new();
    let empty = doc.oplog_vv();
    let row = doc
        .get_map(COLL)
        .insert_container("a", loro::LoroMap::new())
        .expect("row container");
    row.insert("id", "a").expect("id field");
    row.insert("status", "draft").expect("status field");
    doc.commit();
    hex::encode(
        doc.export(loro::ExportMode::updates(&empty))
            .expect("export loro updates"),
    )
}

/// A server with the CRDT collection and `cr_plain` holding key `dup`.
async fn fixture() -> TestServer {
    let server = TestServer::start().await;
    exec(
        &server,
        &format!("CREATE COLLECTION {COLL} (id TEXT PRIMARY KEY, status TEXT) WITH (crdt='true')"),
    )
    .await;
    exec(&server, "CREATE COLLECTION cr_plain (id TEXT PRIMARY KEY)").await;
    exec(&server, "INSERT INTO cr_plain (id) VALUES ('dup')").await;
    server
}

/// The apply statement, then an INSERT of the key `dup`, which fails.
fn failing_request(apply_first: bool) -> String {
    let apply = format!("SELECT crdt_apply('{COLL}', 'a', '{}')", row_delta_hex());
    let bad = "INSERT INTO cr_plain (id) VALUES ('dup')";
    if apply_first {
        format!("{apply}; {bad}")
    } else {
        format!("{bad}; {apply}")
    }
}

async fn crdt_rows(server: &TestServer) -> Vec<String> {
    server
        .query_text(&format!("SELECT id FROM {COLL}"))
        .await
        .unwrap_or_else(|e| panic!("read {COLL}: {e}"))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn http_request_with_a_failing_write_leaves_no_crdt_row() {
    for apply_first in [true, false] {
        let server = fixture().await;
        let sql = failing_request(apply_first);
        let (status, body) = server.http_query(&sql).await;
        assert_ne!(
            status, 200,
            "the duplicate key must fail the request: {body}"
        );
        assert_eq!(
            crdt_rows(&server).await,
            Vec::<String>::new(),
            "apply_first={apply_first}: the refused request left a CRDT row"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_request_with_a_failing_write_leaves_no_crdt_row() {
    for apply_first in [true, false] {
        let server = fixture().await;
        let mut stream = open_trust_session(server.native_port, "nodedb").await;
        let reply = send_sql(&mut stream, 2, &failing_request(apply_first)).await;
        assert!(
            reply.error.is_some(),
            "the duplicate key must fail the request: {reply:?}"
        );
        assert_eq!(
            crdt_rows(&server).await,
            Vec::<String>::new(),
            "apply_first={apply_first}: the refused request left a CRDT row"
        );
    }
}
