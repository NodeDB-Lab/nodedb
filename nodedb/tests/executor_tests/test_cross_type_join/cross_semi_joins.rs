// SPDX-License-Identifier: BUSL-1.1

//! Cross-join and semi-join tests with inline scalar aggregate subqueries.

use crate::helpers::{make_ctx, send_ok};
use nodedb::bridge::scan_filter::{FilterOp, ScanFilter};
use nodedb::data::executor::response_codec;
use nodedb_physical::physical_plan::{
    AggregateSpec, DocumentOp, JoinProjection, PhysicalPlan, QueryOp,
};

#[test]
fn cross_join_uses_inline_right_scalar_aggregate_for_post_filter() {
    let mut ctx = make_ctx();

    for (idx, (id, name, score)) in [
        ("u1", "Alice", 10.0),
        ("u2", "Bob", 7.0),
        ("u3", "Cara", 8.0),
    ]
    .into_iter()
    .enumerate()
    {
        let doc = nodedb_types::json_to_msgpack(&serde_json::json!({
            "id": id,
            "name": name,
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
                surrogate: nodedb_types::Surrogate::new((idx as u32) + 1),
                pk_bytes: Vec::new(),
            }),
        );
    }

    let post_filters = zerompk::to_msgpack_vec(&vec![ScanFilter {
        field: "score".into(),
        op: FilterOp::GtColumn,
        value: nodedb_types::Value::String("avg_score".into()),
        clauses: Vec::new(),
        expr: None,
    }])
    .unwrap();

    let payload = send_ok(
        &mut ctx.core,
        &mut ctx.tx,
        &mut ctx.rx,
        PhysicalPlan::Query(QueryOp::HashJoin {
            left_collection: "users".into(),
            right_collection: "users".into(),
            left_alias: None,
            right_alias: None,
            on: Vec::new(),
            join_type: "cross".into(),
            limit: 100,
            post_group_by: Vec::new(),
            post_aggregates: Vec::new(),
            projection: vec![JoinProjection {
                source: "name".into(),
                output: "name".into(),
            }],
            computed_projection: Vec::new(),
            join_filters: Vec::new(),
            post_filters,
            left_input: None,
            right_input: Some(Box::new(PhysicalPlan::Query(QueryOp::Aggregate {
                collection: "users".into(),
                input: None,
                group_by: Vec::new(),
                aggregates: vec![AggregateSpec {
                    function: "avg".into(),
                    alias: "avg(score)".into(),
                    user_alias: Some("avg_score".into()),
                    field: "score".into(),
                    expr: None,
                }],
                filters: Vec::new(),
                having: Vec::new(),
                limit: 1,
                sub_group_by: Vec::new(),
                sub_aggregates: Vec::new(),
                grouping_sets: Vec::new(),
                sort_keys: Vec::new(),
            }))),
            left_bitmap: None,
            right_bitmap: None,
        }),
    );

    let json = response_codec::decode_payload_to_json(&payload);
    let parsed: Vec<serde_json::Value> =
        serde_json::from_str(&json).unwrap_or_else(|e| panic!("invalid JSON: {e}\nraw: {json}"));

    assert_eq!(
        parsed.len(),
        1,
        "expected only Alice above average, got {json}"
    );
    assert_eq!(parsed[0]["name"], "Alice");
}

#[test]
fn cross_join_uses_unaliased_scalar_aggregate_key_for_post_filter() {
    let mut ctx = make_ctx();

    for (idx, (id, amount)) in [
        ("o1", 30.0_f64),
        ("o2", 99.99_f64),
        ("o3", 30.0_f64),
        ("o4", 25.0_f64),
        ("o5", 50.0_f64),
    ]
    .into_iter()
    .enumerate()
    {
        let doc = nodedb_types::json_to_msgpack(&serde_json::json!({
            "id": id,
            "amount": amount,
        }))
        .unwrap();
        send_ok(
            &mut ctx.core,
            &mut ctx.tx,
            &mut ctx.rx,
            PhysicalPlan::Document(DocumentOp::PointPut {
                collection: "orders".into(),
                document_id: id.into(),
                value: doc,
                surrogate: nodedb_types::Surrogate::new((idx as u32) + 1),
                pk_bytes: Vec::new(),
            }),
        );
    }

    let post_filters = zerompk::to_msgpack_vec(&vec![ScanFilter {
        field: "amount".into(),
        op: FilterOp::GtColumn,
        value: nodedb_types::Value::String("avg(amount)".into()),
        clauses: Vec::new(),
        expr: None,
    }])
    .unwrap();

    let payload = send_ok(
        &mut ctx.core,
        &mut ctx.tx,
        &mut ctx.rx,
        PhysicalPlan::Query(QueryOp::HashJoin {
            left_collection: "orders".into(),
            right_collection: "orders".into(),
            left_alias: None,
            right_alias: None,
            on: Vec::new(),
            join_type: "cross".into(),
            limit: 100,
            post_group_by: Vec::new(),
            post_aggregates: Vec::new(),
            projection: vec![JoinProjection {
                source: "id".into(),
                output: "id".into(),
            }],
            computed_projection: Vec::new(),
            join_filters: Vec::new(),
            post_filters,
            left_input: None,
            right_input: Some(Box::new(PhysicalPlan::Query(QueryOp::Aggregate {
                collection: "orders".into(),
                input: None,
                group_by: Vec::new(),
                aggregates: vec![AggregateSpec {
                    function: "avg".into(),
                    alias: "avg(amount)".into(),
                    user_alias: None,
                    field: "amount".into(),
                    expr: None,
                }],
                filters: Vec::new(),
                having: Vec::new(),
                limit: 1,
                sub_group_by: Vec::new(),
                sub_aggregates: Vec::new(),
                grouping_sets: Vec::new(),
                sort_keys: Vec::new(),
            }))),
            left_bitmap: None,
            right_bitmap: None,
        }),
    );

    let json = response_codec::decode_payload_to_json(&payload);
    let parsed: Vec<serde_json::Value> =
        serde_json::from_str(&json).unwrap_or_else(|e| panic!("invalid JSON: {e}\nraw: {json}"));

    let ids: Vec<&str> = parsed
        .iter()
        .filter_map(|row| row.get("id").and_then(|v| v.as_str()))
        .collect();
    assert_eq!(ids, vec!["o2", "o5"]);
}

#[test]
fn semi_join_uses_nested_scalar_subquery_result_as_inline_right() {
    let mut ctx = make_ctx();

    for (idx, (id, name)) in [("u1", "Alice"), ("u2", "Bob Updated"), ("u3", "Carol")]
        .into_iter()
        .enumerate()
    {
        let doc = nodedb_types::json_to_msgpack(&serde_json::json!({
            "id": id,
            "name": name,
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
                surrogate: nodedb_types::Surrogate::new((idx as u32) + 1),
                pk_bytes: Vec::new(),
            }),
        );
    }

    for (idx, (id, user_id, amount)) in [
        ("o1", "u1", 30.0_f64),
        ("o2", "u2", 99.99_f64),
        ("o3", "u3", 30.0_f64),
        ("o4", "u1", 25.0_f64),
        ("o5", "u2", 50.0_f64),
    ]
    .into_iter()
    .enumerate()
    {
        let doc = nodedb_types::json_to_msgpack(&serde_json::json!({
            "id": id,
            "user_id": user_id,
            "amount": amount,
        }))
        .unwrap();
        send_ok(
            &mut ctx.core,
            &mut ctx.tx,
            &mut ctx.rx,
            PhysicalPlan::Document(DocumentOp::PointPut {
                collection: "orders".into(),
                document_id: id.into(),
                value: doc,
                surrogate: nodedb_types::Surrogate::new((idx as u32) + 1),
                pk_bytes: Vec::new(),
            }),
        );
    }

    let inner_post_filters = zerompk::to_msgpack_vec(&vec![ScanFilter {
        field: "amount".into(),
        op: FilterOp::GtColumn,
        value: nodedb_types::Value::String("avg(amount)".into()),
        clauses: Vec::new(),
        expr: None,
    }])
    .unwrap();

    let payload = send_ok(
        &mut ctx.core,
        &mut ctx.tx,
        &mut ctx.rx,
        PhysicalPlan::Query(QueryOp::HashJoin {
            left_collection: "users".into(),
            right_collection: "orders".into(),
            left_alias: None,
            right_alias: None,
            on: vec![("id".into(), "user_id".into())],
            join_type: "semi".into(),
            limit: 100,
            post_group_by: Vec::new(),
            post_aggregates: Vec::new(),
            projection: Vec::new(),
            computed_projection: Vec::new(),
            join_filters: Vec::new(),
            post_filters: Vec::new(),
            left_input: None,
            right_input: Some(Box::new(PhysicalPlan::Query(QueryOp::HashJoin {
                left_collection: "orders".into(),
                right_collection: "orders".into(),
                left_alias: None,
                right_alias: None,
                on: Vec::new(),
                join_type: "cross".into(),
                limit: 100,
                post_group_by: Vec::new(),
                post_aggregates: Vec::new(),
                projection: vec![JoinProjection {
                    source: "user_id".into(),
                    output: "user_id".into(),
                }],
                computed_projection: Vec::new(),
                join_filters: Vec::new(),
                post_filters: inner_post_filters,
                left_input: None,
                right_input: Some(Box::new(PhysicalPlan::Query(QueryOp::Aggregate {
                    collection: "orders".into(),
                    input: None,
                    group_by: Vec::new(),
                    aggregates: vec![AggregateSpec {
                        function: "avg".into(),
                        alias: "avg(amount)".into(),
                        user_alias: None,
                        field: "amount".into(),
                        expr: None,
                    }],
                    filters: Vec::new(),
                    having: Vec::new(),
                    limit: 1,
                    sub_group_by: Vec::new(),
                    sub_aggregates: Vec::new(),
                    grouping_sets: Vec::new(),
                    sort_keys: Vec::new(),
                }))),
                left_bitmap: None,
                right_bitmap: None,
            }))),
            left_bitmap: None,
            right_bitmap: None,
        }),
    );

    let json = response_codec::decode_payload_to_json(&payload);
    let parsed: Vec<serde_json::Value> =
        serde_json::from_str(&json).unwrap_or_else(|e| panic!("invalid JSON: {e}\nraw: {json}"));

    assert_eq!(parsed.len(), 1, "expected only u2 to match, got {json}");
    let row = parsed[0].as_object().unwrap();
    let id = row
        .get("id")
        .or_else(|| row.get("users.id"))
        .and_then(|v| v.as_str());
    let name = row
        .get("name")
        .or_else(|| row.get("users.name"))
        .and_then(|v| v.as_str());
    assert_eq!(id, Some("u2"), "unexpected row shape: {json}");
    assert_eq!(name, Some("Bob Updated"), "unexpected row shape: {json}");
}
