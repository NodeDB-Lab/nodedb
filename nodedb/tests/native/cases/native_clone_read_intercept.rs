// SPDX-License-Identifier: BUSL-1.1

//! Native-protocol SELECT against a `Shadowed` clone must see the source
//! row: the clone CoW read-interception hook runs from the protocol-neutral
//! `shared::clone_read` module, reached from every dispatch entry point
//! through `intercept_and_authorize` — not only pgwire.
//!
//! Before the read-side hook existed, `intercept_and_authorize` had no
//! clone-read branch at all, so a native SELECT against a Shadowed clone's
//! target (which holds zero rows until a write copies one up) dispatched
//! straight through and returned zero rows. This test fails without the fix
//! and passes with it.
//!
//! Setup runs over pgwire; the read under test runs over native — see
//! `native_clone_write_intercept.rs` for why the clone-scoped session
//! authenticates its database on `OpCode::Auth` rather than `USE DATABASE`.

use nodedb_test_support::native_harness::{do_handshake, send_request, send_sql};
use nodedb_test_support::pgwire_harness::TestServer;

use nodedb_types::protocol::opcodes::ResponseStatus;
use nodedb_types::protocol::text_fields::TextFields;
use nodedb_types::protocol::{AuthMethod, HelloFrame, OpCode};
use nodedb_types::value::Value;

/// A native SELECT against a Shadowed clone whose target never received any
/// write must still return the row that lives only in the clone's source.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_select_on_shadowed_clone_reads_source_row() {
    let srv = TestServer::start().await;

    srv.exec("CREATE DATABASE ncr_src")
        .await
        .expect("create source database");
    srv.exec("USE DATABASE ncr_src")
        .await
        .expect("use source database");
    srv.exec("CREATE COLLECTION docs (id TEXT PRIMARY KEY, v INT)")
        .await
        .expect("create source collection");
    srv.exec("INSERT INTO docs (id, v) VALUES ('a', 1)")
        .await
        .expect("seed source row");

    srv.exec("USE DATABASE default")
        .await
        .expect("use default database");
    srv.exec("CLONE DATABASE ncr_tgt FROM ncr_src")
        .await
        .expect("clone database (Shadowed by default)");

    // The read under test: a native SELECT against `docs.a`, which was
    // never copied into `ncr_tgt` — the clone's target holds zero rows for
    // this collection, so the row is visible only through the clone's
    // source chain-walk.
    let trust_username = srv
        .shared
        .credentials
        .configured_trust_superuser()
        .expect("read configured trust superuser")
        .expect("harness runs in AuthMode::Trust");

    let native_addr = format!("127.0.0.1:{}", srv.native_port)
        .parse()
        .expect("native addr");
    let (mut native_stream, _ack) = do_handshake(native_addr, &HelloFrame::current())
        .await
        .expect("native handshake");

    let auth = send_request(
        &mut native_stream,
        1,
        OpCode::Auth,
        TextFields {
            auth: Some(AuthMethod::Trust {
                username: trust_username,
            }),
            database: Some("ncr_tgt".into()),
            ..Default::default()
        },
    )
    .await;
    assert_eq!(
        auth.status,
        ResponseStatus::Ok,
        "native session must authenticate straight into the clone database: {auth:?}"
    );

    let read = send_sql(&mut native_stream, 2, "SELECT v FROM docs WHERE id = 'a'").await;
    assert_eq!(
        read.status,
        ResponseStatus::Ok,
        "native SELECT on a Shadowed clone must succeed: {read:?}"
    );
    let rows = read.rows.expect("clone read must return rows, not None");
    assert_eq!(
        rows.len(),
        1,
        "the source-only row must be visible through the clone: {rows:?}"
    );
    assert_eq!(
        rows[0][0],
        Value::Integer(1),
        "the clone read must return the source row's value: {rows:?}"
    );

    drop(native_stream);
    srv.graceful_shutdown().await;
}

/// A native full scan and a native `UNION` over a Shadowed clone return the
/// source-only rows. Both plans carry Exchange nodes, which the statement
/// path resolves only after the clone read runs.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_scan_and_union_on_shadowed_clone_read_source_rows() {
    let srv = TestServer::start().await;
    for sql in [
        "CREATE DATABASE ncu_src",
        "USE DATABASE ncu_src",
        "CREATE COLLECTION docs (id TEXT PRIMARY KEY, v INT)",
        "CREATE COLLECTION notes (id TEXT PRIMARY KEY, v INT)",
        "INSERT INTO docs (id, v) VALUES ('a', 1)",
        "INSERT INTO docs (id, v) VALUES ('b', 2)",
        "INSERT INTO notes (id, v) VALUES ('n', 3)",
        "USE DATABASE default",
        "CLONE DATABASE ncu_tgt FROM ncu_src",
    ] {
        srv.exec(sql).await.unwrap_or_else(|e| panic!("{sql}: {e}"));
    }

    let trust_username = srv
        .shared
        .credentials
        .configured_trust_superuser()
        .expect("read configured trust superuser")
        .expect("harness runs in AuthMode::Trust");
    let native_addr = format!("127.0.0.1:{}", srv.native_port)
        .parse()
        .expect("native addr");
    let (mut native_stream, _ack) = do_handshake(native_addr, &HelloFrame::current())
        .await
        .expect("native handshake");
    let auth = send_request(
        &mut native_stream,
        1,
        OpCode::Auth,
        TextFields {
            auth: Some(AuthMethod::Trust {
                username: trust_username,
            }),
            database: Some("ncu_tgt".into()),
            ..Default::default()
        },
    )
    .await;
    assert_eq!(auth.status, ResponseStatus::Ok, "native auth: {auth:?}");

    let cases = [
        ("SELECT v FROM docs", vec![1, 2]),
        (
            "SELECT v FROM docs UNION SELECT v FROM notes",
            vec![1, 2, 3],
        ),
    ];
    for (seq, (sql, expected)) in (2u64..).zip(cases) {
        let read = send_sql(&mut native_stream, seq, sql).await;
        assert_eq!(read.status, ResponseStatus::Ok, "{sql}: {read:?}");
        let rows = read.rows.unwrap_or_else(|| panic!("{sql}: no rows"));
        let mut values: Vec<i64> = rows
            .iter()
            .map(|row| match &row[0] {
                Value::Integer(v) => *v,
                other => panic!("{sql}: non-integer cell {other:?}"),
            })
            .collect();
        values.sort_unstable();
        assert_eq!(
            values, expected,
            "{sql} reads the source-only rows through the clone"
        );
    }

    drop(native_stream);
    srv.graceful_shutdown().await;
}
