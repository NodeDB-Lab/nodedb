// SPDX-License-Identifier: BUSL-1.1

//! Transaction-control spellings over the native protocol.
//!
//! The native SQL dispatch classifies transaction control with the same
//! function pgwire uses. Every PostgreSQL spelling ends or opens a block, in
//! any letter case and with a trailing `;`. An aborted block admits only
//! transaction control: every other statement and opcode gets SQLSTATE 25P02.

use nodedb_test_support::native_harness::{NativeTestServer, do_handshake, send_request, send_sql};

use nodedb_types::protocol::opcodes::ResponseStatus;
use nodedb_types::protocol::text_fields::TextFields;
use nodedb_types::protocol::{HelloFrame, NativeResponse, OpCode};
use tokio::net::TcpStream;

/// A statement that fails at planning, which aborts an open block.
const FAILING_SELECT: &str = "SELECT * FROM native_txn_spelling_missing";

fn error_code(response: &NativeResponse) -> String {
    response
        .error
        .as_ref()
        .map(|e| e.code.clone())
        .unwrap_or_default()
}

/// Open a block, abort it with a failing statement, and check that the
/// block refuses an ordinary statement with 25P02.
async fn enter_failed_block(stream: &mut TcpStream, seq: &mut u64) {
    *seq += 1;
    let begin = send_sql(stream, *seq, "BEGIN").await;
    assert_eq!(begin.status, ResponseStatus::Ok, "BEGIN: {begin:?}");

    *seq += 1;
    let failing = send_sql(stream, *seq, FAILING_SELECT).await;
    assert_eq!(
        failing.status,
        ResponseStatus::Error,
        "the failing statement must error: {failing:?}"
    );

    *seq += 1;
    let refused = send_sql(stream, *seq, "SELECT 1").await;
    assert_eq!(error_code(&refused), "25P02", "{refused:?}");
}

/// Check the session is out of the block: an ordinary statement runs.
async fn assert_block_ended(stream: &mut TcpStream, seq: &mut u64, ended_by: &str) {
    *seq += 1;
    let after = send_sql(stream, *seq, "SELECT 1").await;
    assert_eq!(
        after.status,
        ResponseStatus::Ok,
        "`{ended_by}` must end the aborted block: {after:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rollback_spellings_end_an_aborted_block() {
    let server = NativeTestServer::start().await;
    let (mut stream, _ack) = do_handshake(server.addr, &HelloFrame::current())
        .await
        .expect("handshake");
    let mut seq = 0u64;

    for spelling in ["ROLLBACK TRANSACTION", "ABORT WORK", "rollback;", "Abort"] {
        enter_failed_block(&mut stream, &mut seq).await;
        seq += 1;
        let rollback = send_sql(&mut stream, seq, spelling).await;
        assert_eq!(
            rollback.status,
            ResponseStatus::Ok,
            "`{spelling}` in an aborted block: {rollback:?}"
        );
        assert_block_ended(&mut stream, &mut seq, spelling).await;
    }

    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn aborted_block_refuses_non_sql_opcodes() {
    let server = NativeTestServer::start().await;
    let (mut stream, _ack) = do_handshake(server.addr, &HelloFrame::current())
        .await
        .expect("handshake");
    let mut seq = 0u64;

    enter_failed_block(&mut stream, &mut seq).await;

    seq += 1;
    let show = send_request(
        &mut stream,
        seq,
        OpCode::Show,
        TextFields {
            key: Some("statement_timeout".into()),
            ..Default::default()
        },
    )
    .await;
    assert_eq!(error_code(&show), "25P02", "{show:?}");

    // `OpCode::Rollback` is transaction control and ends the block.
    seq += 1;
    let rollback = send_request(&mut stream, seq, OpCode::Rollback, TextFields::default()).await;
    assert_eq!(rollback.status, ResponseStatus::Ok, "{rollback:?}");
    assert_block_ended(&mut stream, &mut seq, "OpCode::Rollback").await;

    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn begin_and_commit_spellings_open_and_close_a_block() {
    let server = NativeTestServer::start().await;
    let (mut stream, _ack) = do_handshake(server.addr, &HelloFrame::current())
        .await
        .expect("handshake");
    let mut seq = 0u64;

    for (begin, commit) in [
        ("start transaction;", "COMMIT WORK"),
        ("Begin Work", "end transaction;"),
        (
            "START TRANSACTION ISOLATION LEVEL READ COMMITTED, READ WRITE",
            "commit",
        ),
    ] {
        seq += 1;
        let opened = send_sql(&mut stream, seq, begin).await;
        assert_eq!(opened.status, ResponseStatus::Ok, "`{begin}`: {opened:?}");

        // A savepoint needs an open block, so its success proves `begin`
        // opened one.
        seq += 1;
        let savepoint = send_sql(&mut stream, seq, "savepoint S1;").await;
        assert_eq!(
            savepoint.status,
            ResponseStatus::Ok,
            "SAVEPOINT after `{begin}`: {savepoint:?}"
        );
        seq += 1;
        let rewound = send_sql(&mut stream, seq, "rollback work to s1").await;
        assert_eq!(rewound.status, ResponseStatus::Ok, "{rewound:?}");
        seq += 1;
        let released = send_sql(&mut stream, seq, "RELEASE s1").await;
        assert_eq!(released.status, ResponseStatus::Ok, "{released:?}");

        seq += 1;
        let closed = send_sql(&mut stream, seq, commit).await;
        assert_eq!(closed.status, ResponseStatus::Ok, "`{commit}`: {closed:?}");

        // Outside a block a savepoint fails with 25P01, so `commit` closed it.
        seq += 1;
        let outside = send_sql(&mut stream, seq, "SAVEPOINT s2").await;
        assert_eq!(
            error_code(&outside),
            "25P01",
            "after `{commit}`: {outside:?}"
        );
    }

    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_isolation_level_opens_a_block() {
    let server = NativeTestServer::start().await;
    let (mut stream, _ack) = do_handshake(server.addr, &HelloFrame::current())
        .await
        .expect("handshake");

    let mut seq = 1;
    for level in [
        "SERIALIZABLE",
        "REPEATABLE READ",
        "READ COMMITTED",
        "READ UNCOMMITTED",
    ] {
        let begun = send_sql(
            &mut stream,
            seq,
            &format!("START TRANSACTION ISOLATION LEVEL {level}"),
        )
        .await;
        assert!(begun.error.is_none(), "{level}: {begun:?}");
        let ended = send_sql(&mut stream, seq + 1, "COMMIT").await;
        assert!(ended.error.is_none(), "{level}: {ended:?}");
        seq += 2;
    }

    // DEFERRABLE is refused and opens no block.
    let refused = send_sql(&mut stream, seq, "START TRANSACTION DEFERRABLE").await;
    assert_eq!(error_code(&refused), "0A000", "{refused:?}");
    let outside = send_sql(&mut stream, seq + 1, "SAVEPOINT s1").await;
    assert_eq!(error_code(&outside), "25P01", "{outside:?}");

    server.shutdown().await;
}
