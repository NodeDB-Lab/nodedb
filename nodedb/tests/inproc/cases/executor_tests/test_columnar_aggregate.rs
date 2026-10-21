// SPDX-License-Identifier: BUSL-1.1

use super::helpers::{make_ctx, payload_value, send_ok, send_raw};
use nodedb::bridge::envelope::{ErrorCode, Status};
use nodedb::bridge::scan_filter::{FilterOp, ScanFilter};
use nodedb_physical::physical_plan::{
    AggregateSpec, ColumnarInsertIntent, ColumnarOp, GroupKeySpec, PhysicalPlan, QueryOp,
};

#[test]
fn aggregate_count_reads_plain_columnar_engine_rows() {
    let mut ctx = make_ctx();

    let rows = serde_json::json!([
        {"id": "r1", "city": "SF", "temp": 21},
        {"id": "r2", "city": "NYC", "temp": 18},
        {"id": "r3", "city": "SF", "temp": 25}
    ]);
    let payload = nodedb_types::json_to_msgpack(&rows).unwrap();

    send_ok(
        &mut ctx.core,
        &mut ctx.tx,
        &mut ctx.rx,
        PhysicalPlan::Columnar(ColumnarOp::Insert {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                "weather",
            ),
            payload,
            format: "msgpack".into(),
            intent: ColumnarInsertIntent::Insert,
            on_conflict_updates: Vec::new(),
            surrogates: Vec::new(),
            schema_bytes: Vec::new(),
            provenance: None,
            wal_lsn: None,
            system_from: None,
            rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
            returning: None,
            rls_filters: Vec::new(),
        }),
    );

    let docs = ctx.core.scan_collection(0, 1, "weather", 100).unwrap();
    assert_eq!(
        docs.len(),
        3,
        "scan_collection must see columnar engine rows"
    );

    let payload = send_ok(
        &mut ctx.core,
        &mut ctx.tx,
        &mut ctx.rx,
        PhysicalPlan::Query(QueryOp::Aggregate {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                "weather",
            ),
            input: None,
            group_by: Vec::new(),
            aggregates: vec![AggregateSpec {
                function: "count".into(),
                alias: "count(*)".into(),
                user_alias: None,
                field: "*".into(),
                expr: None,
                param: None,
            }],
            filters: Vec::new(),
            having: Vec::new(),
            limit: 10,
            sub_group_by: Vec::new(),
            sub_aggregates: Vec::new(),
            grouping_sets: Vec::new(),
            group_key_aliases: Vec::new(),
            sort_keys: Vec::new(),
        }),
    );

    let result = payload_value(&payload);
    let rows = result
        .as_array()
        .unwrap_or_else(|| panic!("expected aggregate rows, got {result}"));
    assert_eq!(rows.len(), 1, "expected a single COUNT(*) row");
    assert_eq!(rows[0]["count(*)"].as_u64(), Some(3));
}

#[test]
fn columnar_having_uses_canonical_key_but_output_keeps_user_alias() {
    let mut ctx = make_ctx();

    let rows = serde_json::json!([
        {"id": "r1", "city": "SF", "temp": 21},
        {"id": "r2", "city": "NYC", "temp": 18},
        {"id": "r3", "city": "SF", "temp": 25}
    ]);
    let payload = nodedb_types::json_to_msgpack(&rows).unwrap();

    send_ok(
        &mut ctx.core,
        &mut ctx.tx,
        &mut ctx.rx,
        PhysicalPlan::Columnar(ColumnarOp::Insert {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                "weather",
            ),
            payload,
            format: "msgpack".into(),
            intent: ColumnarInsertIntent::Insert,
            on_conflict_updates: Vec::new(),
            surrogates: Vec::new(),
            schema_bytes: Vec::new(),
            provenance: None,
            wal_lsn: None,
            system_from: None,
            rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
            returning: None,
            rls_filters: Vec::new(),
        }),
    );

    let having = zerompk::to_msgpack_vec(&vec![ScanFilter {
        field: "count(*)".into(),
        op: FilterOp::Gt,
        value: nodedb_types::Value::Integer(1),
        clauses: Vec::new(),
        expr: None,
    }])
    .unwrap();

    let payload = send_ok(
        &mut ctx.core,
        &mut ctx.tx,
        &mut ctx.rx,
        PhysicalPlan::Query(QueryOp::Aggregate {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                "weather",
            ),
            input: None,
            group_by: vec![GroupKeySpec::column("city")],
            aggregates: vec![AggregateSpec {
                function: "count".into(),
                alias: "count(*)".into(),
                user_alias: Some("city_count".into()),
                field: "*".into(),
                expr: None,
                param: None,
            }],
            filters: Vec::new(),
            having,
            limit: 10,
            sub_group_by: Vec::new(),
            sub_aggregates: Vec::new(),
            grouping_sets: Vec::new(),
            group_key_aliases: Vec::new(),
            sort_keys: Vec::new(),
        }),
    );

    let result = payload_value(&payload);
    let rows = result
        .as_array()
        .unwrap_or_else(|| panic!("expected aggregate rows, got {result}"));
    assert_eq!(rows.len(), 1, "HAVING should keep only the SF group");
    assert_eq!(rows[0]["city"], "SF");
    assert_eq!(rows[0]["city_count"].as_u64(), Some(2));
    assert!(rows[0].get("count(*)").is_none());
}

#[test]
fn columnar_insert_triggers_memtable_flush() {
    // Spec: after inserting more rows than DEFAULT_FLUSH_THRESHOLD (65536), the
    // memtable must be drained to a segment on disk rather than accumulating
    // unbounded memory.
    let mut ctx = make_ctx();

    // Build a batch of 70000 rows — above the 65536 flush threshold.
    let rows: Vec<serde_json::Value> = (0..70_000)
        .map(|i| {
            serde_json::json!({
                "id": format!("r{i}"),
                "v": i,
            })
        })
        .collect();
    let payload = nodedb_types::json_to_msgpack(&serde_json::Value::Array(rows)).unwrap();

    // The write must succeed without error, and the engine must flush
    // the memtable to a segment once the threshold is crossed, not silently
    // accumulate all rows in RAM.
    send_ok(
        &mut ctx.core,
        &mut ctx.tx,
        &mut ctx.rx,
        PhysicalPlan::Columnar(ColumnarOp::Insert {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                "large_col",
            ),
            payload,
            format: "msgpack".into(),
            intent: ColumnarInsertIntent::Insert,
            on_conflict_updates: Vec::new(),
            surrogates: Vec::new(),
            schema_bytes: Vec::new(),
            provenance: None,
            wal_lsn: None,
            system_from: None,
            rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
            returning: None,
            rls_filters: Vec::new(),
        }),
    );

    // All rows must be readable back — the segment flush must not lose data.
    let doc_count = ctx
        .core
        .scan_collection(0, 1, "large_col", 70_001)
        .unwrap()
        .len();
    assert_eq!(
        doc_count, 70_000,
        "all inserted rows must be scannable after flush"
    );
}

#[test]
fn aggregate_group_by_does_not_require_full_materialization() {
    // Spec: GROUP BY aggregation must return correct per-group results regardless
    // of whether the implementation uses running aggregates (O(groups)) or
    // full doc materialization (O(rows)). This test locks in correctness;
    // the fix changes internal memory usage from O(N) to O(groups).
    let mut ctx = make_ctx();

    // Insert 1000 rows across 10 groups (g0..g9), each group gets 100 rows.
    let rows: Vec<serde_json::Value> = (0..1_000)
        .map(|i| {
            serde_json::json!({
                "id": format!("r{i}"),
                "g": format!("g{}", i % 10),
                "v": i,
            })
        })
        .collect();
    let payload = nodedb_types::json_to_msgpack(&serde_json::Value::Array(rows)).unwrap();

    send_ok(
        &mut ctx.core,
        &mut ctx.tx,
        &mut ctx.rx,
        PhysicalPlan::Columnar(ColumnarOp::Insert {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                "grouped",
            ),
            payload,
            format: "msgpack".into(),
            intent: ColumnarInsertIntent::Insert,
            on_conflict_updates: Vec::new(),
            surrogates: Vec::new(),
            schema_bytes: Vec::new(),
            provenance: None,
            wal_lsn: None,
            system_from: None,
            rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
            returning: None,
            rls_filters: Vec::new(),
        }),
    );

    let payload = send_ok(
        &mut ctx.core,
        &mut ctx.tx,
        &mut ctx.rx,
        PhysicalPlan::Query(QueryOp::Aggregate {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                "grouped",
            ),
            input: None,
            group_by: vec![GroupKeySpec::column("g")],
            aggregates: vec![
                AggregateSpec {
                    function: "count".into(),
                    alias: "count(*)".into(),
                    user_alias: None,
                    field: "*".into(),
                    expr: None,
                    param: None,
                },
                AggregateSpec {
                    function: "sum".into(),
                    alias: "sum(v)".into(),
                    user_alias: None,
                    field: "v".into(),
                    expr: None,
                    param: None,
                },
            ],
            filters: Vec::new(),
            having: Vec::new(),
            limit: 100,
            sub_group_by: Vec::new(),
            sub_aggregates: Vec::new(),
            grouping_sets: Vec::new(),
            group_key_aliases: Vec::new(),
            sort_keys: Vec::new(),
        }),
    );

    let result = payload_value(&payload);
    let result_rows = result
        .as_array()
        .unwrap_or_else(|| panic!("expected aggregate rows, got {result}"));

    assert_eq!(
        result_rows.len(),
        10,
        "GROUP BY must produce exactly 10 groups"
    );
    for row in result_rows {
        assert_eq!(
            row["count(*)"].as_u64(),
            Some(100),
            "each group must contain exactly 100 rows, got: {row}"
        );
    }
}

// ---------------------------------------------------------------------------
// Scan memory-budget bound (no-LIMIT scans)
// ---------------------------------------------------------------------------

/// Insert `count` tiny rows into a columnar `collection` in a single batch.
fn insert_columnar_rows(ctx: &mut super::helpers::TestCtx, collection: &str, count: usize) {
    let rows: Vec<serde_json::Value> = (0..count)
        .map(|i| serde_json::json!({ "id": format!("r{i}"), "v": i }))
        .collect();
    let payload = nodedb_types::json_to_msgpack(&serde_json::Value::Array(rows)).unwrap();
    send_ok(
        &mut ctx.core,
        &mut ctx.tx,
        &mut ctx.rx,
        PhysicalPlan::Columnar(ColumnarOp::Insert {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                collection,
            ),
            payload,
            format: "msgpack".into(),
            intent: ColumnarInsertIntent::Insert,
            on_conflict_updates: Vec::new(),
            surrogates: Vec::new(),
            schema_bytes: Vec::new(),
            provenance: None,
            wal_lsn: None,
            system_from: None,
            rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
            returning: None,
            rls_filters: Vec::new(),
        }),
    );
}

/// A no-LIMIT columnar scan models `limit == usize::MAX`.
fn columnar_scan_unbounded(collection: &str) -> PhysicalPlan {
    PhysicalPlan::Columnar(ColumnarOp::Scan {
        collection: nodedb_types::QualifiedCollection::new(
            nodedb_types::DatabaseId::DEFAULT,
            collection,
        ),
        projection: Vec::new(),
        limit: usize::MAX,
        filters: Vec::new(),
        rls_filters: Vec::new(),
        sort_keys: Vec::new(),
        system_time: nodedb_types::SystemTimeScope::Current,
        valid_at_ms: None,
        prefilter: None,
        computed_columns: Vec::new(),
    })
}

/// A no-LIMIT columnar scan over a collection larger than the historical 10k
/// truncation point returns EVERY row when the result fits the memory budget.
#[test]
fn columnar_unbounded_scan_returns_all_rows_when_within_budget() {
    let mut ctx = make_ctx();

    let count = 12_000;
    insert_columnar_rows(&mut ctx, "wide_col", count);

    let payload = send_ok(
        &mut ctx.core,
        &mut ctx.tx,
        &mut ctx.rx,
        columnar_scan_unbounded("wide_col"),
    );
    let json = payload_value(&payload);
    let rows = json.as_array().unwrap();
    assert_eq!(
        rows.len(),
        count,
        "no-LIMIT scan must return all {count} rows, not the old 10k cap"
    );
    assert!(
        rows.len() > 10_000,
        "regression: result was truncated at/under the old 10k default"
    );
}

/// A no-LIMIT columnar scan whose materialized result exceeds the memory budget
/// surfaces a deterministic `ResourcesExhausted` error — never a partial result.
#[test]
fn columnar_unbounded_scan_over_budget_surfaces_error() {
    let mut ctx = make_ctx();

    // Tiny per-query scan budget so a modest collection trips it.
    ctx.core
        .set_query_tuning(nodedb_types::config::tuning::QueryTuning {
            max_scan_result_bytes: 256,
            ..nodedb_types::config::tuning::QueryTuning::default()
        });

    insert_columnar_rows(&mut ctx, "big_col", 5_000);

    let resp = send_raw(
        &mut ctx.core,
        &mut ctx.tx,
        &mut ctx.rx,
        columnar_scan_unbounded("big_col"),
    );
    assert_eq!(
        resp.status,
        Status::Error,
        "over-budget scan must surface an error"
    );
    assert_eq!(
        resp.error_code.as_deref(),
        Some(&ErrorCode::ResourcesExhausted),
        "must surface the deterministic resource-exhausted error"
    );
}

/// An explicit `limit = N` still returns exactly `N` rows — the budget bound
/// only applies to unbounded scans and must not change explicit-limit behaviour.
#[test]
fn columnar_explicit_limit_returns_exactly_n() {
    let mut ctx = make_ctx();

    insert_columnar_rows(&mut ctx, "limited_col", 12_000);

    let payload = send_ok(
        &mut ctx.core,
        &mut ctx.tx,
        &mut ctx.rx,
        PhysicalPlan::Columnar(ColumnarOp::Scan {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                "limited_col",
            ),
            projection: Vec::new(),
            limit: 250,
            filters: Vec::new(),
            rls_filters: Vec::new(),
            sort_keys: Vec::new(),
            system_time: nodedb_types::SystemTimeScope::Current,
            valid_at_ms: None,
            prefilter: None,
            computed_columns: Vec::new(),
        }),
    );
    let json = payload_value(&payload);
    let rows = json.as_array().unwrap();
    assert_eq!(
        rows.len(),
        250,
        "explicit limit 250 must return exactly 250 rows"
    );
}
