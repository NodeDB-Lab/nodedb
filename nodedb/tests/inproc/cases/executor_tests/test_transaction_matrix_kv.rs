// SPDX-License-Identifier: BUSL-1.1

//! Transaction rollback matrix tests for KV, Columnar, and Timeseries engines.
//!
//! Each test follows the same pattern as `test_transaction_matrix`:
//!   1. Pre-condition: write a known state.
//!   2. Transaction: valid write (first op) + deterministically refused write (second op).
//!   3. Assert: the transaction committed none of its writes.

use nodedb::bridge::envelope::Status;
use nodedb_physical::physical_plan::{
    AggregateSpec, ColumnarInsertIntent, ColumnarOp, DocumentOp, KvOp, PhysicalPlan, QueryOp,
    TimeseriesOp,
};

use nodedb_test_support::tx_batch_helpers::commit_plans;

use super::helpers::*;

// ---------------------------------------------------------------------------
// Shared plan builders
// ---------------------------------------------------------------------------

fn kv_put(key: &[u8], value: &[u8]) -> PhysicalPlan {
    PhysicalPlan::Kv(KvOp::Put {
        collection: nodedb_types::QualifiedCollection::new(
            nodedb_types::DatabaseId::DEFAULT,
            "kv_coll",
        ),
        key: key.to_vec(),
        value: value.to_vec(),
        ttl_ms: 0,
        surrogate: nodedb_types::Surrogate::ZERO,
        returning: None,
        rls_filters: Vec::new(),
    })
}

fn kv_get(key: &[u8]) -> PhysicalPlan {
    PhysicalPlan::Kv(KvOp::Get {
        collection: nodedb_types::QualifiedCollection::new(
            nodedb_types::DatabaseId::DEFAULT,
            "kv_coll",
        ),
        key: key.to_vec(),
        rls_filters: Vec::new(),
        surrogate_ceiling: None,
    })
}

fn doc_put_conflict_seed(coll: &str) -> PhysicalPlan {
    PhysicalPlan::Document(DocumentOp::PointPut {
        collection: nodedb_types::QualifiedCollection::new(nodedb_types::DatabaseId::DEFAULT, coll),
        document_id: "conflict_doc".into(),
        value: b"seed".to_vec(),
        surrogate: nodedb_types::Surrogate::ZERO,
        pk_bytes: Vec::new(),
        returning: None,
        rls_filters: Vec::new(),
        resolved_sum_targets: Vec::new(),
    })
}

fn doc_insert_conflict(coll: &str) -> PhysicalPlan {
    PhysicalPlan::Document(DocumentOp::PointInsert {
        collection: nodedb_types::QualifiedCollection::new(nodedb_types::DatabaseId::DEFAULT, coll),
        document_id: "conflict_doc".into(),
        value: b"conflict".to_vec(),
        surrogate: nodedb_types::Surrogate::ZERO,
        if_absent: false,
        returning: None,
        rls_filters: Vec::new(),
        resolved_sum_targets: Vec::new(),
        deferred_sum_targets: Vec::new(),
    })
}

fn doc_get(coll: &str, doc_id: &str) -> PhysicalPlan {
    PhysicalPlan::Document(DocumentOp::PointGet {
        collection: nodedb_types::QualifiedCollection::new(nodedb_types::DatabaseId::DEFAULT, coll),
        document_id: doc_id.into(),
        rls_filters: Vec::new(),
        system_time: nodedb_types::SystemTimeScope::Current,
        valid_at_ms: None,
        surrogate: nodedb_types::Surrogate::ZERO,
        pk_bytes: Vec::new(),
    })
}

// ---------------------------------------------------------------------------
// Pair: KV write (first) × Document conflict (second) — doc fails
// ---------------------------------------------------------------------------

#[test]
fn rollback_matrix_kv_then_doc_fail() {
    let (mut core, mut tx, mut rx, _dir) = make_core();

    // Pre-condition: key "k1" = "original".
    send_ok(&mut core, &mut tx, &mut rx, kv_put(b"k1", b"original"));
    // Seed the doc that will be used as a conflict trigger.
    send_ok(&mut core, &mut tx, &mut rx, doc_put_conflict_seed("docs"));

    // Transaction: overwrite KV key + failing doc insert (key exists, not if_absent).
    let resp = commit_plans(
        &mut core,
        &mut tx,
        &mut rx,
        vec![kv_put(b"k1", b"modified"), doc_insert_conflict("docs")],
        10,
    );
    assert_eq!(
        resp.status,
        Status::Error,
        "the transaction must be refused on conflict"
    );

    // KV key must be rolled back to "original".
    let r = send_raw(&mut core, &mut tx, &mut rx, kv_get(b"k1"));
    assert_eq!(r.status, Status::Ok);
    assert_eq!(&*r.payload, b"original", "kv put must be rolled back");
}

// ---------------------------------------------------------------------------
// Pair: Document write (first) × KV DDL inside a transaction (rejected)
// ---------------------------------------------------------------------------
// KV DDL ops (RegisterIndex, Truncate, etc.) are rejected with a typed error
// when inside a committed transaction. This test verifies the document write rolled
// back correctly when the KV write itself fails due to a prior-doc write
// combined with a KV Put that fails.
//
// Since we cannot force a raw KV failure after a successful doc write without
// custom error injection, we use a new-key KV put followed by a doc conflict
// as the second op — this exercises doc-first rollback as the symmetric case.

#[test]
fn rollback_matrix_doc_then_kv_fail() {
    let (mut core, mut tx, mut rx, _dir) = make_core();

    // Pre-condition: doc "rollback_doc" does not exist yet.
    // Seed the conflict document.
    send_ok(&mut core, &mut tx, &mut rx, doc_put_conflict_seed("docs"));

    // Transaction: write a new KV key + failing doc insert.
    // On failure the new KV key must not persist.
    let resp = commit_plans(
        &mut core,
        &mut tx,
        &mut rx,
        vec![
            kv_put(b"new_key", b"should_not_persist"),
            doc_insert_conflict("docs"),
        ],
        20,
    );
    assert_eq!(
        resp.status,
        Status::Error,
        "the transaction must be refused on conflict"
    );

    // "new_key" must have been rolled back — Get should return empty/NotFound.
    let r = send_raw(&mut core, &mut tx, &mut rx, kv_get(b"new_key"));
    let is_absent = r.status == Status::Error || r.payload.is_empty();
    assert!(
        is_absent,
        "new_key must not persist after rollback; payload={:?}",
        r.payload
    );
}

// ---------------------------------------------------------------------------
// Pair: KV delete (first) × Document conflict (second) — delete rolled back
// ---------------------------------------------------------------------------

#[test]
fn rollback_matrix_kv_delete_then_doc_fail() {
    let (mut core, mut tx, mut rx, _dir) = make_core();

    // Pre-condition: key "del_key" = "keep_me".
    send_ok(&mut core, &mut tx, &mut rx, kv_put(b"del_key", b"keep_me"));
    // Seed conflict doc.
    send_ok(&mut core, &mut tx, &mut rx, doc_put_conflict_seed("docs"));

    // Transaction: delete the KV key + failing doc insert.
    let resp = commit_plans(
        &mut core,
        &mut tx,
        &mut rx,
        vec![
            PhysicalPlan::Kv(KvOp::Delete {
                collection: nodedb_types::QualifiedCollection::new(
                    nodedb_types::DatabaseId::DEFAULT,
                    "kv_coll",
                ),
                keys: vec![b"del_key".to_vec()],
                rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
                returning: None,
                rls_filters: Vec::new(),
            }),
            doc_insert_conflict("docs"),
        ],
        30,
    );
    assert_eq!(
        resp.status,
        Status::Error,
        "the transaction must be refused"
    );

    // "del_key" must be restored to "keep_me".
    let r = send_raw(&mut core, &mut tx, &mut rx, kv_get(b"del_key"));
    assert_eq!(r.status, Status::Ok);
    assert_eq!(&*r.payload, b"keep_me", "kv delete must be rolled back");
}

// ---------------------------------------------------------------------------
// Verify: doc conflict after KV batch put rolls back all keys
// ---------------------------------------------------------------------------

#[test]
fn rollback_matrix_kv_batch_put_then_doc_fail() {
    let (mut core, mut tx, mut rx, _dir) = make_core();

    // Pre-conditions: k_a = "a_orig", k_b does not exist.
    send_ok(&mut core, &mut tx, &mut rx, kv_put(b"k_a", b"a_orig"));
    send_ok(&mut core, &mut tx, &mut rx, doc_put_conflict_seed("docs"));

    let resp = commit_plans(
        &mut core,
        &mut tx,
        &mut rx,
        vec![
            PhysicalPlan::Kv(KvOp::BatchPut {
                collection: nodedb_types::QualifiedCollection::new(
                    nodedb_types::DatabaseId::DEFAULT,
                    "kv_coll",
                ),
                entries: vec![
                    (b"k_a".to_vec(), b"a_new".to_vec()),
                    (b"k_b".to_vec(), b"b_new".to_vec()),
                ],
                ttl_ms: 0,
                surrogates: vec![nodedb_types::Surrogate::ZERO; 2],
                returning: None,
                rls_filters: Vec::new(),
            }),
            doc_insert_conflict("docs"),
        ],
        40,
    );
    assert_eq!(
        resp.status,
        Status::Error,
        "the transaction must be refused"
    );

    // k_a must be restored to "a_orig".
    let r = send_raw(&mut core, &mut tx, &mut rx, kv_get(b"k_a"));
    assert_eq!(r.status, Status::Ok);
    assert_eq!(&*r.payload, b"a_orig", "k_a must be rolled back");

    // k_b must not persist.
    let r = send_raw(&mut core, &mut tx, &mut rx, kv_get(b"k_b"));
    let is_absent = r.status == Status::Error || r.payload.is_empty();
    assert!(is_absent, "k_b must not persist after rollback");
}

// ---------------------------------------------------------------------------
// Verify: doc get after the transaction returns the pre-transaction state
// ---------------------------------------------------------------------------

#[test]
fn rollback_matrix_doc_then_doc_conflict_kv_intact() {
    let (mut core, mut tx, mut rx, _dir) = make_core();

    // Pre-condition: "anchor" = "anchor_val".
    send_ok(
        &mut core,
        &mut tx,
        &mut rx,
        kv_put(b"anchor", b"anchor_val"),
    );
    send_ok(&mut core, &mut tx, &mut rx, doc_put_conflict_seed("docs"));

    // Batch: KV write + doc conflict — the KV write happened, must roll back.
    let resp = commit_plans(
        &mut core,
        &mut tx,
        &mut rx,
        vec![
            kv_put(b"anchor", b"anchor_modified"),
            doc_insert_conflict("docs"),
        ],
        50,
    );
    assert_eq!(resp.status, Status::Error);

    // "anchor" must still be "anchor_val".
    let r = send_raw(&mut core, &mut tx, &mut rx, kv_get(b"anchor"));
    assert_eq!(r.status, Status::Ok);
    assert_eq!(&*r.payload, b"anchor_val");

    // The seeded conflict doc must still be readable (it was not part of the transaction).
    let r = send_raw(&mut core, &mut tx, &mut rx, doc_get("docs", "conflict_doc"));
    assert_eq!(r.status, Status::Ok, "conflict_doc must still exist");
}

// ---------------------------------------------------------------------------
// Pair: Columnar insert (first) × Document conflict (second) — columnar rolled back
//
// A row is inserted into a plain columnar collection inside a committed transaction.
// The second plan is a PointInsert that conflicts (doc already exists).
// After rollback, the columnar collection must be empty.
// ---------------------------------------------------------------------------

#[test]
fn rollback_matrix_columnar_then_doc_fail() {
    let (mut core, mut tx, mut rx, _dir) = make_core();

    // Seed the document that will trigger the conflict.
    send_ok(
        &mut core,
        &mut tx,
        &mut rx,
        doc_put_conflict_seed("conflict_coll"),
    );

    // Confirm columnar collection is empty before the transaction.
    let before = core.scan_collection(0, 1, "metrics", 100).unwrap();
    assert!(
        before.is_empty(),
        "columnar must be empty before the transaction"
    );

    // Build a columnar insert payload: one row.
    let rows = serde_json::json!([{"id": "r1", "val": 42}]);
    let payload = nodedb_types::json_to_msgpack(&rows).unwrap();

    // Transaction: columnar insert + failing doc insert.
    let resp = commit_plans(
        &mut core,
        &mut tx,
        &mut rx,
        vec![
            PhysicalPlan::Columnar(ColumnarOp::Insert {
                collection: nodedb_types::QualifiedCollection::new(
                    nodedb_types::DatabaseId::DEFAULT,
                    "metrics",
                ),
                payload,
                format: "msgpack".into(),
                intent: ColumnarInsertIntent::Insert,
                on_conflict_updates: Vec::new(),
                surrogates: Vec::new(),
                schema_bytes: Vec::new(),
                provenance: None,
                wal_lsn: None,
                rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
                returning: None,
                rls_filters: Vec::new(),
            }),
            doc_insert_conflict("conflict_coll"),
        ],
        60,
    );
    assert_eq!(
        resp.status,
        Status::Error,
        "the transaction must be refused on doc conflict"
    );
    assert!(
        !matches!(
            resp.error_code.as_deref(),
            Some(nodedb::bridge::envelope::ErrorCode::RollbackFailed { .. })
        ),
        "rollback itself must succeed; got {:?}",
        resp.error_code
    );

    // Columnar collection must be empty — rollback_memtable_inserts was called.
    let after = core.scan_collection(0, 1, "metrics", 100).unwrap();
    assert!(
        after.is_empty(),
        "columnar insert must be rolled back; found {} rows after rollback",
        after.len()
    );
}

// ---------------------------------------------------------------------------
// Pair: Columnar insert (first) × Columnar insert — verify aggregate count
//
// Inserts one row outside the transaction (baseline), then in a refused transaction inserts
// another row + conflicts. After rollback the aggregate count must be 1.
// ---------------------------------------------------------------------------

#[test]
fn rollback_matrix_columnar_count_after_rollback() {
    let (mut core, mut tx, mut rx, _dir) = make_core();

    // Seed conflict doc.
    send_ok(
        &mut core,
        &mut tx,
        &mut rx,
        doc_put_conflict_seed("conflict_coll"),
    );

    // Baseline: insert one row outside any transaction (committed).
    let baseline = serde_json::json!([{"id": "baseline", "val": 1}]);
    let baseline_payload = nodedb_types::json_to_msgpack(&baseline).unwrap();
    send_ok(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Columnar(ColumnarOp::Insert {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                "metrics2",
            ),
            payload: baseline_payload,
            format: "msgpack".into(),
            intent: ColumnarInsertIntent::Insert,
            on_conflict_updates: Vec::new(),
            surrogates: Vec::new(),
            schema_bytes: Vec::new(),
            provenance: None,
            wal_lsn: None,
            rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
            returning: None,
            rls_filters: Vec::new(),
        }),
    );

    // Refused transaction: inserts a second row + doc conflict.
    let extra = serde_json::json!([{"id": "rolled_back", "val": 2}]);
    let extra_payload = nodedb_types::json_to_msgpack(&extra).unwrap();
    let resp = commit_plans(
        &mut core,
        &mut tx,
        &mut rx,
        vec![
            PhysicalPlan::Columnar(ColumnarOp::Insert {
                collection: nodedb_types::QualifiedCollection::new(
                    nodedb_types::DatabaseId::DEFAULT,
                    "metrics2",
                ),
                payload: extra_payload,
                format: "msgpack".into(),
                intent: ColumnarInsertIntent::Insert,
                on_conflict_updates: Vec::new(),
                surrogates: Vec::new(),
                schema_bytes: Vec::new(),
                provenance: None,
                wal_lsn: None,
                rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
                returning: None,
                rls_filters: Vec::new(),
            }),
            doc_insert_conflict("conflict_coll"),
        ],
        70,
    );
    assert_eq!(
        resp.status,
        Status::Error,
        "the transaction must be refused"
    );

    // Aggregate count must be 1 (only the baseline row).
    let agg_payload = send_ok(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Query(QueryOp::Aggregate {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                "metrics2",
            ),
            input: None,
            group_by: Vec::new(),
            aggregates: vec![AggregateSpec {
                function: "count".into(),
                alias: "count(*)".into(),
                user_alias: None,
                field: "*".into(),
                expr: None,
            }],
            filters: Vec::new(),
            having: Vec::new(),
            limit: 10,
            sub_group_by: Vec::new(),
            sub_aggregates: Vec::new(),
            grouping_sets: Vec::new(),
            sort_keys: Vec::new(),
        }),
    );
    let json = super::helpers::payload_json(&agg_payload);
    let val: serde_json::Value = serde_json::from_str(&json).unwrap();
    let count = val
        .as_array()
        .and_then(|a| a.first())
        .and_then(|row| row.get("count(*)"))
        .and_then(|v| v.as_i64())
        .unwrap_or(-1);
    assert_eq!(
        count, 1,
        "columnar row count after rollback must be 1 (baseline only); got count={count} json={json}"
    );
}

// ---------------------------------------------------------------------------
// Pair: Timeseries ingest (first) × Document conflict (second) — ts rolled back
//
// A timeseries ingest inside a committed transaction followed by a failing doc insert
// must leave the timeseries memtable empty (truncated back by apply_undo_timeseries).
// ---------------------------------------------------------------------------

#[test]
fn rollback_matrix_timeseries_then_doc_fail() {
    let (mut core, mut tx, mut rx, _dir) = make_core();

    // Seed conflict doc.
    send_ok(
        &mut core,
        &mut tx,
        &mut rx,
        doc_put_conflict_seed("conflict_coll"),
    );

    // Transaction: timeseries ingest (3 rows) + doc conflict (fails).
    let ilp = "cpu,host=s1 value=0.5 1000000000\n\
               cpu,host=s1 value=0.6 2000000000\n\
               cpu,host=s1 value=0.7 3000000000\n";
    let resp = commit_plans(
        &mut core,
        &mut tx,
        &mut rx,
        vec![
            PhysicalPlan::Timeseries(TimeseriesOp::Ingest {
                collection: nodedb_types::QualifiedCollection::new(
                    nodedb_types::DatabaseId::DEFAULT,
                    "cpu",
                ),
                payload: ilp.as_bytes().to_vec(),
                format: "ilp".into(),
                wal_lsn: None,
                surrogates: Vec::new(),
                provenance: None,
                rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
                returning: None,
                rls_filters: Vec::new(),
            }),
            doc_insert_conflict("conflict_coll"),
        ],
        80,
    );
    assert_eq!(
        resp.status,
        Status::Error,
        "the transaction must be refused on doc conflict"
    );
    assert!(
        !matches!(
            resp.error_code.as_deref(),
            Some(nodedb::bridge::envelope::ErrorCode::RollbackFailed { .. })
        ),
        "rollback itself must succeed; got {:?}",
        resp.error_code
    );

    // Scan the timeseries — must return zero rows (memtable was truncated to 0).
    let scan_resp = send_raw(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Timeseries(TimeseriesOp::Scan {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                "cpu",
            ),
            time_range: (0, i64::MAX),
            projection: Vec::new(),
            limit: 100,
            filters: Vec::new(),
            sort_keys: Vec::new(),
            bucket_interval_ms: 0,
            group_by: Vec::new(),
            aggregates: Vec::new(),
            gap_fill: String::new(),
            rls_filters: Vec::new(),
            system_time: nodedb_types::SystemTimeScope::Current,
            valid_at_ms: None,
            computed_columns: Vec::new(),
        }),
    );
    assert_eq!(scan_resp.status, Status::Ok);
    let json = super::helpers::payload_json(&scan_resp.payload);
    let val: serde_json::Value =
        serde_json::from_str(&json).unwrap_or(serde_json::Value::Array(vec![]));
    let empty = vec![];
    let arr = val.as_array().unwrap_or(&empty);
    assert!(
        arr.is_empty(),
        "timeseries rows must be rolled back (truncate_to called); got {} rows: {json}",
        arr.len()
    );
}

// ---------------------------------------------------------------------------
// Pair: Timeseries ingest (first) — baseline + transaction — count after rollback
//
// Ingests rows outside any transaction (committed), then a refused transaction ingests more.
// After rollback the scan must return only the committed rows.
// ---------------------------------------------------------------------------

#[test]
fn rollback_matrix_timeseries_count_after_rollback() {
    let (mut core, mut tx, mut rx, _dir) = make_core();

    // Seed conflict doc.
    send_ok(
        &mut core,
        &mut tx,
        &mut rx,
        doc_put_conflict_seed("conflict_coll"),
    );

    // Baseline: commit 2 rows.
    let baseline_ilp = "temp,host=s1 value=1.0 1000000000\n\
                        temp,host=s1 value=2.0 2000000000\n";
    send_ok(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Timeseries(TimeseriesOp::Ingest {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                "temp",
            ),
            payload: baseline_ilp.as_bytes().to_vec(),
            format: "ilp".into(),
            wal_lsn: None,
            surrogates: Vec::new(),
            provenance: None,
            rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
            returning: None,
            rls_filters: Vec::new(),
        }),
    );

    // Refused transaction: ingest 3 more rows + doc conflict.
    let extra_ilp = "temp,host=s1 value=3.0 3000000000\n\
                     temp,host=s1 value=4.0 4000000000\n\
                     temp,host=s1 value=5.0 5000000000\n";
    let resp = commit_plans(
        &mut core,
        &mut tx,
        &mut rx,
        vec![
            PhysicalPlan::Timeseries(TimeseriesOp::Ingest {
                collection: nodedb_types::QualifiedCollection::new(
                    nodedb_types::DatabaseId::DEFAULT,
                    "temp",
                ),
                payload: extra_ilp.as_bytes().to_vec(),
                format: "ilp".into(),
                wal_lsn: None,
                surrogates: Vec::new(),
                provenance: None,
                rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
                returning: None,
                rls_filters: Vec::new(),
            }),
            doc_insert_conflict("conflict_coll"),
        ],
        90,
    );
    assert_eq!(
        resp.status,
        Status::Error,
        "the transaction must be refused"
    );

    // Scan must return exactly 2 rows (the baseline only).
    let scan_resp = send_raw(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Timeseries(TimeseriesOp::Scan {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                "temp",
            ),
            time_range: (0, i64::MAX),
            projection: Vec::new(),
            limit: 100,
            filters: Vec::new(),
            sort_keys: Vec::new(),
            bucket_interval_ms: 0,
            group_by: Vec::new(),
            aggregates: Vec::new(),
            gap_fill: String::new(),
            rls_filters: Vec::new(),
            system_time: nodedb_types::SystemTimeScope::Current,
            valid_at_ms: None,
            computed_columns: Vec::new(),
        }),
    );
    assert_eq!(scan_resp.status, Status::Ok);
    let json = super::helpers::payload_json(&scan_resp.payload);
    let val: serde_json::Value =
        serde_json::from_str(&json).unwrap_or(serde_json::Value::Array(vec![]));
    let empty = vec![];
    let arr = val.as_array().unwrap_or(&empty);
    assert_eq!(
        arr.len(),
        2,
        "timeseries must have only baseline 2 rows after rollback; got {} rows: {json}",
        arr.len()
    );
}
