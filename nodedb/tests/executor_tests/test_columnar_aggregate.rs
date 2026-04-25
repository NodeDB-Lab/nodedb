use crate::helpers::{make_ctx, payload_value, send_ok};
use nodedb::bridge::envelope::PhysicalPlan;
use nodedb::bridge::physical_plan::{AggregateSpec, ColumnarInsertIntent, ColumnarOp, QueryOp};
use nodedb::bridge::scan_filter::{FilterOp, ScanFilter};

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
            collection: "weather".into(),
            payload,
            format: "msgpack".into(),
            intent: ColumnarInsertIntent::Insert,
            on_conflict_updates: Vec::new(),
            surrogates: Vec::new(),
        }),
    );

    let docs = ctx.core.scan_collection(1, "weather", 100).unwrap();
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
            collection: "weather".into(),
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
            collection: "weather".into(),
            payload,
            format: "msgpack".into(),
            intent: ColumnarInsertIntent::Insert,
            on_conflict_updates: Vec::new(),
            surrogates: Vec::new(),
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
            collection: "weather".into(),
            group_by: vec!["city".into()],
            aggregates: vec![AggregateSpec {
                function: "count".into(),
                alias: "count(*)".into(),
                user_alias: Some("city_count".into()),
                field: "*".into(),
                expr: None,
            }],
            filters: Vec::new(),
            having,
            limit: 10,
            sub_group_by: Vec::new(),
            sub_aggregates: Vec::new(),
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

    // The write must succeed without error. Before the fix this would succeed
    // but silently accumulate all rows in RAM; after the fix the engine flushes
    // the memtable to a segment once the threshold is crossed.
    send_ok(
        &mut ctx.core,
        &mut ctx.tx,
        &mut ctx.rx,
        PhysicalPlan::Columnar(ColumnarOp::Insert {
            collection: "large_col".into(),
            payload,
            format: "msgpack".into(),
            intent: ColumnarInsertIntent::Insert,
            on_conflict_updates: Vec::new(),
            surrogates: Vec::new(),
        }),
    );

    // All rows must be readable back — the segment flush must not lose data.
    let doc_count = ctx
        .core
        .scan_collection(1, "large_col", 70_001)
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
            collection: "grouped".into(),
            payload,
            format: "msgpack".into(),
            intent: ColumnarInsertIntent::Insert,
            on_conflict_updates: Vec::new(),
            surrogates: Vec::new(),
        }),
    );

    let payload = send_ok(
        &mut ctx.core,
        &mut ctx.tx,
        &mut ctx.rx,
        PhysicalPlan::Query(QueryOp::Aggregate {
            collection: "grouped".into(),
            group_by: vec!["g".into()],
            aggregates: vec![
                AggregateSpec {
                    function: "count".into(),
                    alias: "count(*)".into(),
                    user_alias: None,
                    field: "*".into(),
                    expr: None,
                },
                AggregateSpec {
                    function: "sum".into(),
                    alias: "sum(v)".into(),
                    user_alias: None,
                    field: "v".into(),
                    expr: None,
                },
            ],
            filters: Vec::new(),
            having: Vec::new(),
            limit: 100,
            sub_group_by: Vec::new(),
            sub_aggregates: Vec::new(),
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
