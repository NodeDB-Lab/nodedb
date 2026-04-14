use crate::helpers::{make_ctx, payload_value, send_ok};
use nodedb::bridge::envelope::PhysicalPlan;
use nodedb::bridge::physical_plan::{AggregateSpec, ColumnarOp, QueryOp};
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
