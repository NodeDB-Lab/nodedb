// SPDX-License-Identifier: BUSL-1.1

use super::helpers::{make_ctx, payload_value, send_ok};
use nodedb::bridge::scan_filter::{FilterOp, ScanFilter};
use nodedb_physical::physical_plan::{
    AggregateSpec, DocumentOp, GroupKeySpec, PhysicalPlan, QueryOp,
};

#[test]
fn aggregate_output_uses_user_alias_but_having_reads_canonical_key() {
    let mut ctx = make_ctx();

    for (idx, (id, department, score)) in [
        ("u1", "tools", 10),
        ("u2", "tools", 20),
        ("u3", "sales", 30),
    ]
    .into_iter()
    .enumerate()
    {
        let surrogate = nodedb_types::Surrogate::new((idx as u32) + 1);
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
                collection: nodedb_types::QualifiedCollection::new(
                    nodedb_types::DatabaseId::DEFAULT,
                    "users",
                ),
                document_id: id.into(),
                value: doc,
                surrogate,
                pk_bytes: Vec::new(),
                returning: None,
                rls_filters: Vec::new(),
                resolved_sum_targets: Vec::new(),
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
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                "users",
            ),
            input: None,
            group_by: vec![GroupKeySpec::column("department")],
            aggregates: vec![
                AggregateSpec {
                    function: "count".into(),
                    alias: "count(*)".into(),
                    user_alias: Some("dept_count".into()),
                    field: "*".into(),
                    expr: None,
                    param: None,
                },
                AggregateSpec {
                    function: "avg".into(),
                    alias: "avg(score)".into(),
                    user_alias: Some("avg_score".into()),
                    field: "score".into(),
                    expr: None,
                    param: None,
                },
            ],
            filters: Vec::new(),
            system_time: nodedb_types::SystemTimeScope::Current,
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

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["department"], "tools");
    assert_eq!(rows[0]["dept_count"].as_u64(), Some(2));
    assert_eq!(rows[0]["avg_score"].as_f64(), Some(15.0));
    assert!(rows[0].get("count(*)").is_none());
    assert!(rows[0].get("avg(score)").is_none());
}

fn grouped_count_plan(user_alias: &str) -> PhysicalPlan {
    grouped_count_shape_plan(user_alias, 10, Vec::new())
}

fn computed_group_count_plan(function: &str) -> PhysicalPlan {
    PhysicalPlan::Query(QueryOp::Aggregate {
        collection: nodedb_types::QualifiedCollection::new(
            nodedb_types::DatabaseId::DEFAULT,
            "cache_alias_users",
        ),
        input: None,
        group_by: vec![GroupKeySpec {
            output_name: "group_0".into(),
            field: None,
            expr: Some(nodedb_query::expr::SqlExpr::Function {
                name: function.into(),
                args: vec![nodedb_query::expr::SqlExpr::Column("department".into())],
            }),
        }],
        aggregates: vec![AggregateSpec {
            function: "count".into(),
            alias: "count(*)".into(),
            user_alias: Some("row_count".into()),
            field: "*".into(),
            expr: None,
            param: None,
        }],
        filters: Vec::new(),
        system_time: nodedb_types::SystemTimeScope::Current,
        having: Vec::new(),
        limit: 10,
        sub_group_by: Vec::new(),
        sub_aggregates: Vec::new(),
        grouping_sets: Vec::new(),
        group_key_aliases: Vec::new(),
        sort_keys: Vec::new(),
    })
}

fn grouped_count_shape_plan(
    user_alias: &str,
    limit: usize,
    sort_keys: Vec<nodedb_physical::physical_plan::SortKeySpec>,
) -> PhysicalPlan {
    PhysicalPlan::Query(QueryOp::Aggregate {
        collection: nodedb_types::QualifiedCollection::new(
            nodedb_types::DatabaseId::DEFAULT,
            "cache_alias_users",
        ),
        input: None,
        group_by: vec![GroupKeySpec::column("department")],
        aggregates: vec![AggregateSpec {
            function: "count".into(),
            alias: "count(*)".into(),
            user_alias: Some(user_alias.into()),
            field: "*".into(),
            expr: None,
            param: None,
        }],
        filters: Vec::new(),
        system_time: nodedb_types::SystemTimeScope::Current,
        having: Vec::new(),
        limit,
        sub_group_by: Vec::new(),
        sub_aggregates: Vec::new(),
        grouping_sets: Vec::new(),
        group_key_aliases: Vec::new(),
        sort_keys,
    })
}

#[test]
fn aggregate_cache_separates_user_facing_output_aliases() {
    let mut ctx = make_ctx();

    for (idx, id) in ["u1", "u2"].into_iter().enumerate() {
        let doc = nodedb_types::json_to_msgpack(&serde_json::json!({
            "id": id,
            "department": "tools",
        }))
        .expect("encode document");
        send_ok(
            &mut ctx.core,
            &mut ctx.tx,
            &mut ctx.rx,
            PhysicalPlan::Document(DocumentOp::PointPut {
                collection: nodedb_types::QualifiedCollection::new(
                    nodedb_types::DatabaseId::DEFAULT,
                    "cache_alias_users",
                ),
                document_id: id.into(),
                value: doc,
                surrogate: nodedb_types::Surrogate::new((idx as u32) + 1),
                pk_bytes: Vec::new(),
                returning: None,
                rls_filters: Vec::new(),
                resolved_sum_targets: Vec::new(),
            }),
        );
    }

    let first = send_ok(
        &mut ctx.core,
        &mut ctx.tx,
        &mut ctx.rx,
        grouped_count_plan("first_count"),
    );
    let first_rows = payload_value(&first);
    assert_eq!(first_rows[0]["first_count"].as_u64(), Some(2));

    let second = send_ok(
        &mut ctx.core,
        &mut ctx.tx,
        &mut ctx.rx,
        grouped_count_plan("second_count"),
    );
    let second_rows = payload_value(&second);
    assert_eq!(
        second_rows[0]["second_count"].as_u64(),
        Some(2),
        "a cache hit must preserve the requesting query's output schema: {second_rows}"
    );
    assert!(second_rows[0].get("first_count").is_none());
    assert!(second_rows[0].get("count(*)").is_none());
}

#[test]
fn aggregate_cache_separates_computed_group_expressions() {
    let mut ctx = make_ctx();
    for (idx, department) in ["Alpha", "alpha"].into_iter().enumerate() {
        let id = format!("computed-{idx}");
        let doc = nodedb_types::json_to_msgpack(&serde_json::json!({
            "id": id,
            "department": department,
        }))
        .expect("encode document");
        send_ok(
            &mut ctx.core,
            &mut ctx.tx,
            &mut ctx.rx,
            PhysicalPlan::Document(DocumentOp::PointPut {
                collection: nodedb_types::QualifiedCollection::new(
                    nodedb_types::DatabaseId::DEFAULT,
                    "cache_alias_users",
                ),
                document_id: id,
                value: doc,
                surrogate: nodedb_types::Surrogate::new((idx as u32) + 1),
                pk_bytes: Vec::new(),
                returning: None,
                rls_filters: Vec::new(),
                resolved_sum_targets: Vec::new(),
            }),
        );
    }

    let lower = payload_value(&send_ok(
        &mut ctx.core,
        &mut ctx.tx,
        &mut ctx.rx,
        computed_group_count_plan("lower"),
    ));
    assert_eq!(lower[0]["group_0"], "alpha");
    assert_eq!(lower[0]["row_count"].as_u64(), Some(2));

    let upper = payload_value(&send_ok(
        &mut ctx.core,
        &mut ctx.tx,
        &mut ctx.rx,
        computed_group_count_plan("upper"),
    ));
    assert_eq!(upper[0]["group_0"], "ALPHA");
    assert_eq!(upper[0]["row_count"].as_u64(), Some(2));
}

#[test]
fn aggregate_cache_separates_limit_and_sort_shape() {
    let mut ctx = make_ctx();
    let mut surrogate = 1u32;
    for (department, count) in [("alpha", 1), ("beta", 2), ("gamma", 3)] {
        for index in 0..count {
            let id = format!("{department}-{index}");
            let doc = nodedb_types::json_to_msgpack(&serde_json::json!({
                "id": id,
                "department": department,
            }))
            .expect("encode document");
            send_ok(
                &mut ctx.core,
                &mut ctx.tx,
                &mut ctx.rx,
                PhysicalPlan::Document(DocumentOp::PointPut {
                    collection: nodedb_types::QualifiedCollection::new(
                        nodedb_types::DatabaseId::DEFAULT,
                        "cache_alias_users",
                    ),
                    document_id: id,
                    value: doc,
                    surrogate: nodedb_types::Surrogate::new(surrogate),
                    pk_bytes: Vec::new(),
                    returning: None,
                    rls_filters: Vec::new(),
                    resolved_sum_targets: Vec::new(),
                }),
            );
            surrogate += 1;
        }
    }

    let ascending = send_ok(
        &mut ctx.core,
        &mut ctx.tx,
        &mut ctx.rx,
        grouped_count_shape_plan(
            "row_count",
            1,
            vec![nodedb_physical::physical_plan::SortKeySpec::column(
                "row_count",
                true,
            )],
        ),
    );
    let ascending_rows = payload_value(&ascending);
    assert_eq!(ascending_rows.as_array().map(Vec::len), Some(1));
    assert_eq!(ascending_rows[0]["row_count"].as_u64(), Some(1));

    let descending = send_ok(
        &mut ctx.core,
        &mut ctx.tx,
        &mut ctx.rx,
        grouped_count_shape_plan(
            "row_count",
            2,
            vec![nodedb_physical::physical_plan::SortKeySpec::column(
                "row_count",
                false,
            )],
        ),
    );
    let descending_rows = payload_value(&descending);
    assert_eq!(descending_rows.as_array().map(Vec::len), Some(2));
    assert_eq!(descending_rows[0]["row_count"].as_u64(), Some(3));
    assert_eq!(descending_rows[1]["row_count"].as_u64(), Some(2));
}
