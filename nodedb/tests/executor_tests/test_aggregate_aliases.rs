use crate::helpers::{make_ctx, payload_value, send_ok};
use nodedb::bridge::envelope::PhysicalPlan;
use nodedb::bridge::physical_plan::{AggregateSpec, DocumentOp, QueryOp};
use nodedb::bridge::scan_filter::{FilterOp, ScanFilter};

#[test]
fn aggregate_output_uses_user_alias_but_having_reads_canonical_key() {
    let mut ctx = make_ctx();

    for (id, department, score) in [
        ("u1", "tools", 10),
        ("u2", "tools", 20),
        ("u3", "sales", 30),
    ] {
        let doc = nodedb_types::json_to_msgpack(&serde_json::json!({
            "id": id,
            "department": department,
            "score": score,
        }))
        .unwrap();

        send_ok(
            &mut ctx.core,
            &mut ctx.tx,
            &mut ctx.rx,
            PhysicalPlan::Document(DocumentOp::PointPut {
                collection: "users".into(),
                document_id: id.into(),
                value: doc,
                surrogate: nodedb_types::Surrogate::ZERO,
                pk_bytes: Vec::new(),
            }),
        );
    }

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
            collection: "users".into(),
            group_by: vec!["department".into()],
            aggregates: vec![
                AggregateSpec {
                    function: "count".into(),
                    alias: "count(*)".into(),
                    user_alias: Some("dept_count".into()),
                    field: "*".into(),
                    expr: None,
                },
                AggregateSpec {
                    function: "avg".into(),
                    alias: "avg(score)".into(),
                    user_alias: Some("avg_score".into()),
                    field: "score".into(),
                    expr: None,
                },
            ],
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

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["department"], "tools");
    assert_eq!(rows[0]["dept_count"].as_u64(), Some(2));
    assert_eq!(rows[0]["avg_score"].as_f64(), Some(15.0));
    assert!(rows[0].get("count(*)").is_none());
    assert!(rows[0].get("avg(score)").is_none());
}
