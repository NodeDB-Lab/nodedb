// SPDX-License-Identifier: BUSL-1.1

//! A native request of several statements keeps each statement apart. A line
//! comment that ends one statement does not swallow the next, when the
//! request commits the statements as one implicit transaction.

use nodedb_test_support::native_harness::{NativeTestServer, do_handshake, send_sql};

use nodedb_types::protocol::HelloFrame;
use nodedb_types::protocol::opcodes::ResponseStatus;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_line_comment_at_the_end_of_a_statement_leaves_the_next_statement_running() {
    let server = NativeTestServer::start().await;
    let (mut stream, _ack) = do_handshake(server.addr, &HelloFrame::current())
        .await
        .expect("handshake");

    let create = send_sql(
        &mut stream,
        1,
        "CREATE COLLECTION native_comment_rows (id TEXT PRIMARY KEY, n INT)",
    )
    .await;
    assert_eq!(create.status, ResponseStatus::Ok, "{create:?}");

    let request = "INSERT INTO native_comment_rows (id, n) VALUES ('a', 1) -- note\n; \
                   INSERT INTO native_comment_rows (id, n) VALUES ('b', 2); COMMIT";
    let answer = send_sql(&mut stream, 2, request).await;
    assert_eq!(answer.status, ResponseStatus::Ok, "{answer:?}");

    let rows = send_sql(
        &mut stream,
        3,
        "SELECT id FROM native_comment_rows ORDER BY id",
    )
    .await;
    assert_eq!(rows.status, ResponseStatus::Ok, "{rows:?}");
    let count = rows.rows.as_ref().map_or(0, Vec::len);
    assert_eq!(count, 2, "both inserts must apply: {rows:?}");

    server.shutdown().await;
}
