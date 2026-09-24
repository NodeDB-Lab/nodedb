// SPDX-License-Identifier: BUSL-1.1

//! Committed-transaction atomicity tests for Columnar, Timeseries, and CRDT
//! engine pairs. Pairs with KV are in transaction_batch_cross_engine.rs.

use nodedb_test_support::tx_batch_helpers::*;

use nodedb::bridge::envelope::{ErrorCode, Status};

fn seed_conflict_doc(
    core: &mut nodedb::data::executor::core_loop::CoreLoop,
    tx: &mut nodedb_bridge::buffer::Producer<nodedb::bridge::dispatch::BridgeRequest>,
    rx: &mut nodedb_bridge::buffer::Consumer<nodedb::bridge::dispatch::BridgeResponse>,
    coll: &str,
    doc_id: &str,
) {
    send_ok(core, tx, rx, doc_put(coll, doc_id, b"seed"));
}

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

// ── Columnar × Vector ─────────────────────────────────────────────────────────

#[test]
fn commit_columnar_vector() {
    let (mut core, mut tx, mut rx, _dir) = make_core();

    let resp = commit_plans(
        &mut core,
        &mut tx,
        &mut rx,
        vec![
            columnar_insert("metrics", "r1", 10),
            vector_direct_insert("vp", 101),
        ],
        10,
    );
    assert_eq!(resp.status, Status::Ok);
    assert_columnar_count(&mut core, &mut tx, &mut rx, "metrics", 1);
}

#[test]
fn rollback_columnar_on_refusal() {
    let (mut core, mut tx, mut rx, _dir) = make_core();

    let resp = commit_plans(
        &mut core,
        &mut tx,
        &mut rx,
        with_unique_refusal(vec![columnar_insert("metrics", "r1", 10)]),
        20,
    );
    assert_eq!(resp.status, Status::Error);
    assert_no_rb_fail(&resp);
    assert_columnar_count(&mut core, &mut tx, &mut rx, "metrics", 0);
}

// ── Columnar × Graph ──────────────────────────────────────────────────────────

#[test]
fn rollback_columnar_graph_on_refusal() {
    let (mut core, mut tx, mut rx, _dir) = make_core();

    let resp = commit_plans(
        &mut core,
        &mut tx,
        &mut rx,
        with_unique_refusal(vec![
            columnar_insert("metrics", "r1", 10),
            edge_put("g", "p", "q"),
        ]),
        30,
    );
    assert_eq!(resp.status, Status::Error);
    assert_no_rb_fail(&resp);
    assert_columnar_count(&mut core, &mut tx, &mut rx, "metrics", 0);
    assert_edge_absent(&mut core, &mut tx, &mut rx, "g", "p");
}

// ── Columnar × Timeseries ─────────────────────────────────────────────────────

#[test]
fn rollback_columnar_timeseries_on_refusal() {
    let (mut core, mut tx, mut rx, _dir) = make_core();
    seed_conflict_doc(&mut core, &mut tx, &mut rx, "docs", "conflict");
    let ilp = "temp,host=s1 value=1.0 1000000000\n";

    let resp = commit_plans(
        &mut core,
        &mut tx,
        &mut rx,
        vec![
            columnar_insert("metrics", "r1", 10),
            timeseries_ingest("temp", ilp),
            doc_conflict("docs", "conflict"),
        ],
        40,
    );
    assert_eq!(resp.status, Status::Error);
    assert_no_rb_fail(&resp);
    assert_columnar_count(&mut core, &mut tx, &mut rx, "metrics", 0);
    assert_ts_count(&mut core, &mut tx, &mut rx, "temp", 0);
}

// ── Columnar × CRDT ──────────────────────────────────────────────────────────

#[test]
fn rollback_columnar_crdt_on_refusal() {
    let (mut core, mut tx, mut rx, _dir) = make_core();

    let resp = commit_plans(
        &mut core,
        &mut tx,
        &mut rx,
        with_unique_refusal(vec![
            columnar_insert("metrics", "r1", 10),
            crdt_upsert("crdt_coll", "doc1", 301),
        ]),
        50,
    );
    assert_eq!(resp.status, Status::Error);
    assert_no_rb_fail(&resp);
    assert_columnar_count(&mut core, &mut tx, &mut rx, "metrics", 0);
}

// ── Timeseries × Vector ───────────────────────────────────────────────────────

#[test]
fn commit_timeseries_vector() {
    let (mut core, mut tx, mut rx, _dir) = make_core();
    let ilp = "cpu,host=s1 value=0.5 1000000000\n";

    let resp = commit_plans(
        &mut core,
        &mut tx,
        &mut rx,
        vec![
            timeseries_ingest("cpu", ilp),
            vector_direct_insert("vp", 101),
        ],
        60,
    );
    assert_eq!(resp.status, Status::Ok);
    assert_ts_count(&mut core, &mut tx, &mut rx, "cpu", 1);
}

#[test]
fn rollback_timeseries_on_refusal() {
    let (mut core, mut tx, mut rx, _dir) = make_core();
    let ilp = "cpu,host=s1 value=0.5 1000000000\n";

    let resp = commit_plans(
        &mut core,
        &mut tx,
        &mut rx,
        with_unique_refusal(vec![timeseries_ingest("cpu", ilp)]),
        70,
    );
    assert_eq!(resp.status, Status::Error);
    assert_no_rb_fail(&resp);
    assert_ts_count(&mut core, &mut tx, &mut rx, "cpu", 0);
}

// ── Timeseries × Graph ────────────────────────────────────────────────────────

#[test]
fn rollback_timeseries_graph_on_refusal() {
    let (mut core, mut tx, mut rx, _dir) = make_core();
    let ilp = "cpu,host=s1 value=0.5 1000000000\n";

    let resp = commit_plans(
        &mut core,
        &mut tx,
        &mut rx,
        with_unique_refusal(vec![timeseries_ingest("cpu", ilp), edge_put("g", "m", "n")]),
        80,
    );
    assert_eq!(resp.status, Status::Error);
    assert_no_rb_fail(&resp);
    assert_ts_count(&mut core, &mut tx, &mut rx, "cpu", 0);
    assert_edge_absent(&mut core, &mut tx, &mut rx, "g", "m");
}

// ── Timeseries × CRDT ─────────────────────────────────────────────────────────

#[test]
fn rollback_timeseries_crdt_on_refusal() {
    let (mut core, mut tx, mut rx, _dir) = make_core();
    let ilp = "cpu,host=s1 value=0.5 1000000000\n";

    let resp = commit_plans(
        &mut core,
        &mut tx,
        &mut rx,
        with_unique_refusal(vec![
            timeseries_ingest("cpu", ilp),
            crdt_upsert("crdt_coll", "doc1", 301),
        ]),
        90,
    );
    assert_eq!(resp.status, Status::Error);
    assert_no_rb_fail(&resp);
    assert_ts_count(&mut core, &mut tx, &mut rx, "cpu", 0);
}

// ── CRDT × Doc ────────────────────────────────────────────────────────────────

#[test]
fn rollback_crdt_doc_on_refusal() {
    let (mut core, mut tx, mut rx, _dir) = make_core();
    seed_conflict_doc(&mut core, &mut tx, &mut rx, "docs", "conflict");
    send_ok(
        &mut core,
        &mut tx,
        &mut rx,
        doc_put("docs", "sentinel", b"original"),
    );

    let resp = commit_plans(
        &mut core,
        &mut tx,
        &mut rx,
        vec![
            crdt_upsert("crdt_coll", "doc1", 301),
            doc_put("docs", "sentinel", b"modified"),
            doc_conflict("docs", "conflict"),
        ],
        100,
    );
    assert_eq!(resp.status, Status::Error);
    assert_no_rb_fail(&resp);

    let r = send_raw(&mut core, &mut tx, &mut rx, doc_get("docs", "sentinel"));
    assert_eq!(r.status, Status::Ok);
    assert_eq!(&*r.payload, b"original");
}

// ── CRDT × Graph ──────────────────────────────────────────────────────────────

#[test]
fn rollback_crdt_graph_on_refusal() {
    let (mut core, mut tx, mut rx, _dir) = make_core();

    let resp = commit_plans(
        &mut core,
        &mut tx,
        &mut rx,
        with_unique_refusal(vec![
            crdt_upsert("crdt_coll", "doc1", 301),
            edge_put("g", "c1", "c2"),
        ]),
        110,
    );
    assert_eq!(resp.status, Status::Error);
    assert_no_rb_fail(&resp);
    assert_edge_absent(&mut core, &mut tx, &mut rx, "g", "c1");
}

// ── CRDT × KV ─────────────────────────────────────────────────────────────────

#[test]
fn rollback_crdt_kv_on_refusal() {
    let (mut core, mut tx, mut rx, _dir) = make_core();
    seed_conflict_doc(&mut core, &mut tx, &mut rx, "docs", "conflict");

    let resp = commit_plans(
        &mut core,
        &mut tx,
        &mut rx,
        vec![
            crdt_upsert("crdt_coll", "doc1", 301),
            kv_put(b"ck1", b"should_not_persist"),
            doc_conflict("docs", "conflict"),
        ],
        120,
    );
    assert_eq!(resp.status, Status::Error);
    assert_no_rb_fail(&resp);
    assert_kv_absent(&mut core, &mut tx, &mut rx, b"ck1");
}

// ── CRDT × Columnar ───────────────────────────────────────────────────────────

#[test]
fn rollback_crdt_columnar_on_refusal() {
    let (mut core, mut tx, mut rx, _dir) = make_core();

    let resp = commit_plans(
        &mut core,
        &mut tx,
        &mut rx,
        with_unique_refusal(vec![
            crdt_upsert("crdt_coll", "doc1", 301),
            columnar_insert("metrics", "r1", 10),
        ]),
        130,
    );
    assert_eq!(resp.status, Status::Error);
    assert_no_rb_fail(&resp);
    assert_columnar_count(&mut core, &mut tx, &mut rx, "metrics", 0);
}

// ── CRDT × Timeseries ─────────────────────────────────────────────────────────

#[test]
fn rollback_crdt_timeseries_on_refusal() {
    let (mut core, mut tx, mut rx, _dir) = make_core();
    let ilp = "cpu,host=s1 value=0.5 1000000000\n";

    let resp = commit_plans(
        &mut core,
        &mut tx,
        &mut rx,
        with_unique_refusal(vec![
            crdt_upsert("crdt_coll", "doc1", 301),
            timeseries_ingest("cpu", ilp),
        ]),
        140,
    );
    assert_eq!(resp.status, Status::Error);
    assert_no_rb_fail(&resp);
    assert_ts_count(&mut core, &mut tx, &mut rx, "cpu", 0);
}
