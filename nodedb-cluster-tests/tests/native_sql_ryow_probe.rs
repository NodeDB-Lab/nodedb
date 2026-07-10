// SPDX-License-Identifier: BUSL-1.1

//! REGRESSION GUARDS for two native (MessagePack) explicit-transaction
//! behaviours that were broken on `upstream/main` and fixed on this branch.
//! Upstream had NO native explicit-txn wire coverage
//! (`single_node_calvin_graph_txn.rs` drives BEGIN over pgwire), so these paths
//! were unguarded; both tests assert the CORRECT post-fix behaviour.
//!
//! GAP A — first-frame BEGIN must buffer. Previously `handle_begin` ->
//! `run_begin` -> `sessions.begin(addr)` used
//! `write_session(addr,..).unwrap_or(Ok(()))`; with NO session yet for the addr
//! it silently returned `Ok(())` WITHOUT setting `InBlock`, so a BEGIN issued
//! before any SQL no-oped and the next INSERT autocommitted. `handle_begin` now
//! calls `ensure_session` before `run_begin`, so the session transitions to
//! `InBlock` even when BEGIN is the first post-auth frame.
//!
//! GAP B (U1) — in-txn SQL read-your-own-writes. Native SQL reads route through
//! `sql_gateway.rs::dispatch_task_via_gateway`, which previously built a
//! `GatewayQueryContext { tenant_id, trace_id, database_id }` with NO `txn_id`
//! and called `gw.execute(&gw_ctx, plan)`, dropping the connection's `txn_id`
//! before the Data Plane so the per-txn staging overlay-merge never fired. It
//! now propagates `txn_id` into `GatewayQueryContext` so the Data Plane resolves
//! this transaction's staging overlay. The pgwire twin
//! (`nodedb/tests/sql_transactions_staged_point_writes.rs`) asserts `["42"]`.
//!
//! The first test guards GAP A; the second WARMS UP the session (a SQL read
//! triggers `ensure_session`) so a subsequent BEGIN buffers, then guards the
//! GAP B RYOW path. A failure here signals a REGRESSION of either fix.

mod common;

use std::time::Duration;

use common::cluster_harness::TestClusterNode;
use nodedb_client::native::connection::NativeConnection;
use nodedb_types::protocol::AuthMethod;
use tokio_postgres::SimpleQueryMessage;

/// Single-node harness with collection `t` created and settled.
async fn setup() -> TestClusterNode {
    let node = TestClusterNode::spawn(1, vec![])
        .await
        .expect("spawn single-node cluster");
    tokio::time::sleep(Duration::from_millis(300)).await;

    node.exec(
        "CREATE COLLECTION t \
         (id STRING NOT NULL PRIMARY KEY, n INT) WITH (engine='document_strict')",
    )
    .await
    .expect("CREATE COLLECTION t");
    tokio::time::sleep(Duration::from_millis(150)).await;

    node
}

/// One pinned native connection (explicit txns are peer-address-scoped, so the
/// whole transaction must ride a single socket).
async fn native_conn(node: &TestClusterNode) -> NativeConnection {
    let mut conn = NativeConnection::connect(&format!("127.0.0.1:{}", node.native_port))
        .await
        .expect("connect native socket");
    conn.authenticate(
        AuthMethod::Trust {
            username: "admin".into(),
        },
        None,
    )
    .await
    .expect("native trust auth");
    conn
}

/// `n` for `id` read over the INDEPENDENT pgwire session (durable state, not the
/// txn connection's overlay). `None` when absent.
async fn durable_n(node: &TestClusterNode, id: &str) -> Option<String> {
    let msgs = node
        .client
        .simple_query(&format!("SELECT n FROM t WHERE id = '{id}'"))
        .await
        .expect("pgwire durable read");
    msgs.iter().find_map(|m| match m {
        SimpleQueryMessage::Row(r) => r.get("n").map(str::to_string),
        _ => None,
    })
}

/// GAP A: a BEGIN issued as the first post-auth frame does NOT buffer; the
/// following INSERT autocommits (visible to an independent session pre-COMMIT).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_first_frame_begin_does_not_buffer_probe() {
    let node = setup().await;
    let mut conn = native_conn(&node).await;

    // BEGIN is the very first post-auth frame -> no session exists yet.
    conn.begin().await.expect("BEGIN");
    conn.execute_sql("INSERT INTO t (id, n) VALUES ('ff', 7)")
        .await
        .expect("INSERT after first-frame BEGIN");

    let leaked = durable_n(&node, "ff").await;
    assert_eq!(
        leaked, None,
        "GAP A GUARD: first-frame BEGIN must buffer the write (invisible \
         pre-COMMIT). leaked={leaked:?}. Some(\"7\") -> the INSERT \
         autocommitted (first-frame BEGIN no-oped) -> GAP A REGRESSED."
    );

    node.shutdown().await;
}

/// GAP B (U1): with the session warmed up (so BEGIN buffers), the transaction's
/// OWN native SQL read must observe the staged write (read-your-own-writes).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_in_txn_sql_select_reads_own_write_probe() {
    let node = setup().await;
    let mut conn = native_conn(&node).await;

    // WARM UP: a SQL statement triggers `ensure_session` (sql.rs), creating the
    // session so the following BEGIN can transition it to InBlock.
    conn.execute_sql("SELECT n FROM t WHERE id = 'warmup'")
        .await
        .expect("warm-up read (creates native session)");

    conn.begin().await.expect("BEGIN");
    conn.execute_sql("INSERT INTO t (id, n) VALUES ('a', 42)")
        .await
        .expect("in-tx INSERT");

    // Precondition for a meaningful RYOW probe: the write must actually be
    // buffered (invisible to an independent session). If this trips, staging is
    // broken even with a warm session (a broader gap than U1).
    assert_eq!(
        durable_n(&node, "a").await,
        None,
        "warmed in-block write must be invisible to other sessions mid-transaction"
    );

    // THE U1 PROBE: the transaction's OWN native SQL read.
    let res = conn
        .execute_sql("SELECT n FROM t WHERE id = 'a'")
        .await
        .expect("in-tx read");
    let seen = res
        .rows
        .first()
        .and_then(|r| r.first())
        .and_then(|v| v.as_i64());

    assert_eq!(
        seen,
        Some(42),
        "GAP B (U1) GUARD: native in-txn SQL SELECT must read-its-own-write \
         (Some(42)); seen={seen:?}, rows={:?}. None -> native RYOW \
         REGRESSED (gateway dropped txn_id).",
        res.rows
    );

    conn.commit().await.expect("COMMIT");
    node.shutdown().await;
}
