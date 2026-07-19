// SPDX-License-Identifier: BUSL-1.1

//! Inline hash join test: two pre-computed payloads joined with qualified left-side keys.

use crate::helpers::{make_ctx, send_ok};
use nodedb::data::executor::response_codec;
use nodedb_physical::physical_plan::{DocumentOp, PhysicalPlan, QueryOp};

use super::basic_scans::build_msgpack_map;

#[test]
fn inline_hash_join_honors_qualified_left_keys() {
    let mut ctx = make_ctx();

    for (idx, (id, name)) in [("u1", "Alice"), ("u2", "Bob")].into_iter().enumerate() {
        let doc = build_msgpack_map(&[("id", id), ("name", name)]);
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

    for (idx, (id, user_id, item)) in [("o1", "u1", "Book"), ("o2", "u2", "Pen")]
        .into_iter()
        .enumerate()
    {
        let doc = build_msgpack_map(&[("id", id), ("user_id", user_id), ("item", item)]);
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

    for (idx, (id, order_id, amount)) in [("p1", "o1", "10"), ("p2", "o2", "25")]
        .into_iter()
        .enumerate()
    {
        let doc = build_msgpack_map(&[("id", id), ("order_id", order_id), ("amount", amount)]);
        send_ok(
            &mut ctx.core,
            &mut ctx.tx,
            &mut ctx.rx,
            PhysicalPlan::Document(DocumentOp::PointPut {
                collection: "payments".into(),
                document_id: id.into(),
                value: doc,
                surrogate: nodedb_types::Surrogate::new((idx as u32) + 1),
                pk_bytes: Vec::new(),
            }),
        );
    }

    let left_data = send_ok(
        &mut ctx.core,
        &mut ctx.tx,
        &mut ctx.rx,
        PhysicalPlan::Query(QueryOp::HashJoin {
            left_collection: "users".into(),
            right_collection: "orders".into(),
            left_alias: None,
            right_alias: None,
            on: vec![("id".into(), "user_id".into())],
            join_type: "inner".into(),
            limit: 100,
            post_group_by: Vec::new(),
            post_aggregates: Vec::new(),
            projection: Vec::new(),
            computed_projection: Vec::new(),
            join_filters: Vec::new(),
            post_filters: Vec::new(),
            left_input: None,
            right_input: None,
            left_bitmap: None,
            right_bitmap: None,
        }),
    );

    let right_data = send_ok(
        &mut ctx.core,
        &mut ctx.tx,
        &mut ctx.rx,
        PhysicalPlan::Document(DocumentOp::Scan {
            collection: "payments".into(),
            filters: Vec::new(),
            limit: 100,
            offset: 0,
            sort_keys: Vec::new(),
            distinct: false,
            projection: Vec::new(),
            computed_columns: Vec::new(),
            window_functions: Vec::new(),
            system_time: nodedb_types::SystemTimeScope::Current,
            valid_at_ms: None,
            prefilter: None,
        }),
    );

    let payload = send_ok(
        &mut ctx.core,
        &mut ctx.tx,
        &mut ctx.rx,
        PhysicalPlan::Query(QueryOp::HashJoin {
            left_collection: String::new(),
            right_collection: String::new(),
            left_alias: None,
            right_alias: None,
            on: vec![("orders.id".into(), "order_id".into())],
            join_type: "inner".into(),
            limit: 100,
            post_group_by: Vec::new(),
            post_aggregates: Vec::new(),
            projection: Vec::new(),
            computed_projection: Vec::new(),
            join_filters: Vec::new(),
            post_filters: Vec::new(),
            left_input: Some(Box::new(PhysicalPlan::Query(QueryOp::ProviderScan {
                provider: None,
                rows: response_codec::flatten_to_relational_rows(&left_data),
                filters: Vec::new(),
                projection: Vec::new(),
                sort_keys: Vec::new(),
                limit: None,
                offset: 0,
                distinct: false,
            }))),
            right_input: Some(Box::new(PhysicalPlan::Query(QueryOp::ProviderScan {
                provider: None,
                rows: response_codec::flatten_to_relational_rows(&right_data),
                filters: Vec::new(),
                projection: Vec::new(),
                sort_keys: Vec::new(),
                limit: None,
                offset: 0,
                distinct: false,
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
        2,
        "expected 2 inline join rows, got {}. json={json}",
        parsed.len()
    );

    for row in &parsed {
        let obj = row.as_object().unwrap();
        assert!(obj.contains_key("users.id"), "missing users.id: {row}");
        assert!(obj.contains_key("orders.id"), "missing orders.id: {row}");
        assert!(
            !obj.keys().any(|k| k.starts_with("inline_left.")),
            "inline join should preserve existing left keys: {row}"
        );
    }
}
