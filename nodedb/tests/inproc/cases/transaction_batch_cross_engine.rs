// SPDX-License-Identifier: BUSL-1.1

//! A committed transaction must atomically commit or atomically roll back
//! across engine pairs. This file covers KV paired with every other engine.
//! Each transaction stages its plans, resolves them into one redo record,
//! and installs the record (`commit_plans`).
//!
//! Pairs already covered by executor_tests/test_transaction_matrix.rs and
//! test_transaction_matrix_kv.rs are not duplicated here.
//! Columnar/Timeseries/CRDT pairs: see transaction_batch_cross_engine_mixed.rs.

use nodedb_test_support::tx_batch_helpers::*;

use nodedb::bridge::envelope::{ErrorCode, Status};

// ── Shared helpers ────────────────────────────────────────────────────────────

fn assert_no_rb_fail(resp: &nodedb::bridge::envelope::Response) {
    assert!(
        !matches!(
            resp.error_code.as_deref(),
            Some(ErrorCode::RollbackFailed { .. })
        ),
        "rollback itself must succeed; got {:?}",
        resp.error_code
    );
}

// ── KV × Vector ──────────────────────────────────────────────────────────────

#[test]
fn commit_kv_vector() {
    let (mut core, mut tx, mut rx, _dir) = make_core();

    let resp = commit_plans(
        &mut core,
        &mut tx,
        &mut rx,
        vec![kv_put(b"k1", b"v1"), vector_direct_insert("vp", 101)],
        10,
    );
    assert_eq!(resp.status, Status::Ok);
    let r = send_raw(&mut core, &mut tx, &mut rx, kv_get(b"k1"));
    assert_eq!(&*r.payload, b"v1");
}

#[test]
fn rollback_kv_on_refusal() {
    let (mut core, mut tx, mut rx, _dir) = make_core();
    send_ok(&mut core, &mut tx, &mut rx, kv_put(b"k1", b"original"));

    let resp = commit_plans(
        &mut core,
        &mut tx,
        &mut rx,
        with_unique_refusal(vec![kv_put(b"k1", b"modified")]),
        20,
    );
    assert_eq!(resp.status, Status::Error);
    assert_no_rb_fail(&resp);
    let r = send_raw(&mut core, &mut tx, &mut rx, kv_get(b"k1"));
    assert_eq!(&*r.payload, b"original");
}

// ── KV × Graph ───────────────────────────────────────────────────────────────

#[test]
fn commit_kv_graph() {
    let (mut core, mut tx, mut rx, _dir) = make_core();

    let resp = commit_plans(
        &mut core,
        &mut tx,
        &mut rx,
        vec![kv_put(b"kg1", b"val"), edge_put("g", "a", "b")],
        30,
    );
    assert_eq!(resp.status, Status::Ok);
    let r = send_raw(&mut core, &mut tx, &mut rx, kv_get(b"kg1"));
    assert_eq!(&*r.payload, b"val");
}

#[test]
fn rollback_kv_graph_on_refusal() {
    let (mut core, mut tx, mut rx, _dir) = make_core();
    send_ok(&mut core, &mut tx, &mut rx, kv_put(b"k2", b"original"));

    let resp = commit_plans(
        &mut core,
        &mut tx,
        &mut rx,
        with_unique_refusal(vec![kv_put(b"k2", b"modified"), edge_put("g", "x", "y")]),
        40,
    );
    assert_eq!(resp.status, Status::Error);
    assert_no_rb_fail(&resp);
    let r = send_raw(&mut core, &mut tx, &mut rx, kv_get(b"k2"));
    assert_eq!(&*r.payload, b"original");
    assert_edge_absent(&mut core, &mut tx, &mut rx, "g", "x");
}

// ── KV × Columnar ─────────────────────────────────────────────────────────────

#[test]
fn commit_kv_columnar() {
    let (mut core, mut tx, mut rx, _dir) = make_core();

    let resp = commit_plans(
        &mut core,
        &mut tx,
        &mut rx,
        vec![kv_put(b"kc1", b"val"), columnar_insert("metrics", "r1", 10)],
        50,
    );
    assert_eq!(resp.status, Status::Ok);
    let r = send_raw(&mut core, &mut tx, &mut rx, kv_get(b"kc1"));
    assert_eq!(&*r.payload, b"val");
}

#[test]
fn rollback_kv_columnar_on_refusal() {
    let (mut core, mut tx, mut rx, _dir) = make_core();
    send_ok(&mut core, &mut tx, &mut rx, kv_put(b"k3", b"original"));

    let resp = commit_plans(
        &mut core,
        &mut tx,
        &mut rx,
        with_unique_refusal(vec![
            kv_put(b"k3", b"modified"),
            columnar_insert("metrics", "r1", 10),
        ]),
        60,
    );
    assert_eq!(resp.status, Status::Error);
    assert_no_rb_fail(&resp);
    let r = send_raw(&mut core, &mut tx, &mut rx, kv_get(b"k3"));
    assert_eq!(&*r.payload, b"original");
    assert_columnar_count(&mut core, &mut tx, &mut rx, "metrics", 0);
}

// ── KV × Timeseries ──────────────────────────────────────────────────────────

#[test]
fn commit_kv_timeseries() {
    let (mut core, mut tx, mut rx, _dir) = make_core();
    let ilp = "temp,host=s1 value=1.0 1000000000\n";

    let resp = commit_plans(
        &mut core,
        &mut tx,
        &mut rx,
        vec![kv_put(b"kt1", b"val"), timeseries_ingest("temp", ilp)],
        70,
    );
    assert_eq!(resp.status, Status::Ok);
    let r = send_raw(&mut core, &mut tx, &mut rx, kv_get(b"kt1"));
    assert_eq!(&*r.payload, b"val");
    assert_ts_count(&mut core, &mut tx, &mut rx, "temp", 1);
}

#[test]
fn rollback_kv_timeseries_on_refusal() {
    let (mut core, mut tx, mut rx, _dir) = make_core();
    send_ok(&mut core, &mut tx, &mut rx, kv_put(b"k4", b"original"));
    let ilp = "temp,host=s1 value=1.0 1000000000\n";

    let resp = commit_plans(
        &mut core,
        &mut tx,
        &mut rx,
        with_unique_refusal(vec![
            kv_put(b"k4", b"modified"),
            timeseries_ingest("temp", ilp),
        ]),
        80,
    );
    assert_eq!(resp.status, Status::Error);
    assert_no_rb_fail(&resp);
    let r = send_raw(&mut core, &mut tx, &mut rx, kv_get(b"k4"));
    assert_eq!(&*r.payload, b"original");
    assert_ts_count(&mut core, &mut tx, &mut rx, "temp", 0);
}

// ── KV × CRDT ─────────────────────────────────────────────────────────────────

#[test]
fn rollback_kv_crdt_on_refusal() {
    let (mut core, mut tx, mut rx, _dir) = make_core();
    send_ok(&mut core, &mut tx, &mut rx, kv_put(b"k5", b"original"));

    let resp = commit_plans(
        &mut core,
        &mut tx,
        &mut rx,
        with_unique_refusal(vec![
            kv_put(b"k5", b"modified"),
            crdt_upsert("crdt_coll", "doc1", 301),
        ]),
        90,
    );
    assert_eq!(resp.status, Status::Error);
    assert_no_rb_fail(&resp);
    let r = send_raw(&mut core, &mut tx, &mut rx, kv_get(b"k5"));
    assert_eq!(&*r.payload, b"original");
}
