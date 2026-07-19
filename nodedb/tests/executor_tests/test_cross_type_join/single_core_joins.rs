// SPDX-License-Identifier: BUSL-1.1

//! Single-core hash join and self-join tests.

use crate::helpers::{make_ctx, send_ok};
use nodedb::bridge::scan_filter::{FilterOp, ScanFilter};
use nodedb::data::executor::response_codec;
use nodedb_physical::physical_plan::{DocumentOp, JoinProjection, KvOp, PhysicalPlan, QueryOp};

use super::basic_scans::build_msgpack_map;

#[test]
fn single_core_cross_type_hash_join() {
    // Both collections on same core — tests join matching across KV and schemaless.
    let mut ctx = make_ctx();

    // Insert schemaless docs.
    for (idx, (id, name)) in [("d1", "Alice"), ("d3", "Carol"), ("d4", "Dave")]
        .into_iter()
        .enumerate()
    {
        let doc = build_msgpack_map(&[("id", id), ("name", name)]);
        send_ok(
            &mut ctx.core,
            &mut ctx.tx,
            &mut ctx.rx,
            PhysicalPlan::Document(DocumentOp::PointPut {
                collection: "docs".into(),
                document_id: id.into(),
                value: doc,
                surrogate: nodedb_types::Surrogate::new((idx as u32) + 1),
                pk_bytes: Vec::new(),
            }),
        );
    }

    // Insert KV entries (value = typed columns, no key inside).
    for (key, theme, lang) in [("d1", "dark", "en"), ("d3", "light", "fr")] {
        let value = build_msgpack_map(&[("theme", theme), ("lang", lang)]);
        send_ok(
            &mut ctx.core,
            &mut ctx.tx,
            &mut ctx.rx,
            PhysicalPlan::Kv(KvOp::Put {
                collection: "prefs".into(),
                key: key.as_bytes().to_vec(),
                value,
                ttl_ms: 0,
                surrogate: nodedb_types::Surrogate::ZERO,
            }),
        );
    }

    // Verify scan_collection works for both.
    let docs = ctx.core.scan_collection(0, 1, "docs", 100).unwrap();
    assert_eq!(docs.len(), 3, "docs: expected 3, got {}", docs.len());
    let prefs = ctx.core.scan_collection(0, 1, "prefs", 100).unwrap();
    assert_eq!(prefs.len(), 2, "prefs: expected 2, got {}", prefs.len());

    // Execute hash join: docs INNER JOIN prefs ON docs.id = prefs.key
    let payload = send_ok(
        &mut ctx.core,
        &mut ctx.tx,
        &mut ctx.rx,
        PhysicalPlan::Query(QueryOp::HashJoin {
            left_collection: "docs".into(),
            right_collection: "prefs".into(),
            left_alias: None,
            right_alias: None,
            on: vec![("id".into(), "key".into())],
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

    let json = response_codec::decode_payload_to_json(&payload);
    eprintln!("hash join result: {json}");
    let parsed: Vec<serde_json::Value> =
        serde_json::from_str(&json).unwrap_or_else(|e| panic!("invalid JSON: {e}\nraw: {json}"));

    // d1 and d3 match, d4 has no prefs → INNER JOIN = 2 rows.
    assert_eq!(
        parsed.len(),
        2,
        "expected 2 inner join rows, got {}. json={json}",
        parsed.len()
    );

    // Verify each result has prefixed keys from both sides.
    for row in &parsed {
        let obj = row.as_object().unwrap();
        assert!(
            obj.keys().any(|k| k.starts_with("docs.")),
            "missing docs.* keys: {row}"
        );
        assert!(
            obj.keys().any(|k| k.starts_with("prefs.")),
            "missing prefs.* keys: {row}"
        );
    }
}

#[test]
fn single_core_left_join_with_nulls() {
    let mut ctx = make_ctx();

    for (idx, (id, name)) in [("d1", "Alice"), ("d3", "Carol"), ("d4", "Dave")]
        .into_iter()
        .enumerate()
    {
        let doc = build_msgpack_map(&[("id", id), ("name", name)]);
        send_ok(
            &mut ctx.core,
            &mut ctx.tx,
            &mut ctx.rx,
            PhysicalPlan::Document(DocumentOp::PointPut {
                collection: "docs".into(),
                document_id: id.into(),
                value: doc,
                surrogate: nodedb_types::Surrogate::new((idx as u32) + 1),
                pk_bytes: Vec::new(),
            }),
        );
    }

    for (key, theme) in [("d1", "dark"), ("d3", "light")] {
        let value = build_msgpack_map(&[("theme", theme)]);
        send_ok(
            &mut ctx.core,
            &mut ctx.tx,
            &mut ctx.rx,
            PhysicalPlan::Kv(KvOp::Put {
                collection: "prefs".into(),
                key: key.as_bytes().to_vec(),
                value,
                ttl_ms: 0,
                surrogate: nodedb_types::Surrogate::ZERO,
            }),
        );
    }

    let payload = send_ok(
        &mut ctx.core,
        &mut ctx.tx,
        &mut ctx.rx,
        PhysicalPlan::Query(QueryOp::HashJoin {
            left_collection: "docs".into(),
            right_collection: "prefs".into(),
            left_alias: None,
            right_alias: None,
            on: vec![("id".into(), "key".into())],
            join_type: "left".into(),
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

    let json = response_codec::decode_payload_to_json(&payload);
    eprintln!("left join result: {json}");
    let parsed: Vec<serde_json::Value> =
        serde_json::from_str(&json).unwrap_or_else(|e| panic!("invalid JSON: {e}\nraw: {json}"));

    // d1 matches, d3 matches, d4 has no prefs → LEFT JOIN = 3 rows.
    assert_eq!(
        parsed.len(),
        3,
        "expected 3 left join rows, got {}. json={json}",
        parsed.len()
    );
}

#[test]
fn single_core_self_join_respects_aliases_in_filter_and_projection() {
    let mut ctx = make_ctx();

    for (idx, (id, name, dept)) in [
        ("1", "Alice", "eng"),
        ("2", "Bob", "eng"),
        ("3", "Cara", "pm"),
    ]
    .iter()
    .enumerate()
    {
        let doc = build_msgpack_map(&[("id", id), ("name", name), ("dept", dept)]);
        send_ok(
            &mut ctx.core,
            &mut ctx.tx,
            &mut ctx.rx,
            PhysicalPlan::Document(DocumentOp::PointPut {
                collection: "employees".into(),
                document_id: (*id).into(),
                value: doc,
                surrogate: nodedb_types::Surrogate::new((idx as u32) + 1),
                pk_bytes: id.as_bytes().to_vec(),
            }),
        );
    }

    let post_filters = zerompk::to_msgpack_vec(&vec![ScanFilter {
        field: "a.id".into(),
        op: FilterOp::LtColumn,
        value: nodedb_types::Value::String("b.id".into()),
        clauses: Vec::new(),
        expr: None,
    }])
    .unwrap();

    let payload = send_ok(
        &mut ctx.core,
        &mut ctx.tx,
        &mut ctx.rx,
        PhysicalPlan::Query(QueryOp::HashJoin {
            left_collection: "employees".into(),
            right_collection: "employees".into(),
            left_alias: Some("a".into()),
            right_alias: Some("b".into()),
            on: vec![("dept".into(), "dept".into())],
            join_type: "inner".into(),
            limit: 100,
            post_group_by: Vec::new(),
            post_aggregates: Vec::new(),
            projection: vec![
                JoinProjection {
                    source: "a.name".into(),
                    output: "emp1".into(),
                },
                JoinProjection {
                    source: "b.name".into(),
                    output: "emp2".into(),
                },
                JoinProjection {
                    source: "a.dept".into(),
                    output: "a.dept".into(),
                },
            ],
            computed_projection: Vec::new(),
            join_filters: Vec::new(),
            post_filters,
            left_input: None,
            right_input: None,
            left_bitmap: None,
            right_bitmap: None,
        }),
    );

    let json = response_codec::decode_payload_to_json(&payload);
    let parsed: Vec<serde_json::Value> =
        serde_json::from_str(&json).unwrap_or_else(|e| panic!("invalid JSON: {e}\nraw: {json}"));

    assert_eq!(parsed.len(), 1, "expected one eng pair, got {json}");
    let row = parsed[0].as_object().unwrap();
    assert_eq!(row.get("emp1").and_then(|v| v.as_str()), Some("Alice"));
    assert_eq!(row.get("emp2").and_then(|v| v.as_str()), Some("Bob"));
    assert_eq!(row.get("a.dept").and_then(|v| v.as_str()), Some("eng"));
}

#[test]
fn single_core_self_join_star_keeps_both_sides() {
    let mut ctx = make_ctx();

    for (id, name, dept, level) in [("emp1", "Alice", "eng", 5), ("emp2", "Bob", "eng", 4)] {
        let doc = nodedb_types::json_to_msgpack(&serde_json::json!({
            "id": id,
            "name": name,
            "dept": dept,
            "level": level
        }))
        .unwrap();
        send_ok(
            &mut ctx.core,
            &mut ctx.tx,
            &mut ctx.rx,
            PhysicalPlan::Document(DocumentOp::PointPut {
                collection: "employees".into(),
                document_id: id.into(),
                value: doc,
                surrogate: nodedb_types::Surrogate::ZERO,
                pk_bytes: Vec::new(),
            }),
        );
    }

    let payload = send_ok(
        &mut ctx.core,
        &mut ctx.tx,
        &mut ctx.rx,
        PhysicalPlan::Query(QueryOp::HashJoin {
            left_collection: "employees".into(),
            right_collection: "employees".into(),
            left_alias: Some("a".into()),
            right_alias: Some("b".into()),
            on: vec![("dept".into(), "dept".into())],
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

    let json = response_codec::decode_payload_to_json(&payload);
    let parsed: Vec<serde_json::Value> =
        serde_json::from_str(&json).unwrap_or_else(|e| panic!("invalid JSON: {e}\nraw: {json}"));

    assert!(!parsed.is_empty(), "expected self join rows, got {json}");
    let row = parsed[0].as_object().unwrap();
    assert!(row.contains_key("a.id"), "missing a.id in {json}");
    assert!(row.contains_key("a.name"), "missing a.name in {json}");
    assert!(row.contains_key("a.dept"), "missing a.dept in {json}");
    assert!(row.contains_key("a.level"), "missing a.level in {json}");
    assert!(row.contains_key("b.id"), "missing b.id in {json}");
    assert!(row.contains_key("b.name"), "missing b.name in {json}");
    assert!(row.contains_key("b.dept"), "missing b.dept in {json}");
    assert!(row.contains_key("b.level"), "missing b.level in {json}");
}

#[test]
fn schemaless_self_join_matches_on_canonicalized_object_fields() {
    let mut ctx = make_ctx();

    for (idx, (id, user_id, item)) in [
        ("o1", "u1", "book"),
        ("o3", "u1", "pen"),
        ("o2", "u2", "pad"),
    ]
    .iter()
    .enumerate()
    {
        let mut obj = std::collections::HashMap::new();
        // Inline `id` into the document body so projections of `a.id`
        // surface the user-facing order id rather than the substrate
        // row key (hex of surrogate). The substrate retrofit moved
        // doc-id-as-key out of the `id` column slot, so tests that
        // assert on the user PK have to write it as a payload field.
        obj.insert("id".to_string(), nodedb_types::Value::String((*id).into()));
        obj.insert(
            "user_id".to_string(),
            nodedb_types::Value::String((*user_id).into()),
        );
        obj.insert(
            "item".to_string(),
            nodedb_types::Value::String((*item).into()),
        );
        let tagged = zerompk::to_msgpack_vec(&nodedb_types::Value::Object(obj)).unwrap();

        send_ok(
            &mut ctx.core,
            &mut ctx.tx,
            &mut ctx.rx,
            PhysicalPlan::Document(DocumentOp::PointPut {
                collection: "orders".into(),
                document_id: (*id).into(),
                value: tagged,
                surrogate: nodedb_types::Surrogate::new((idx as u32) + 1),
                pk_bytes: id.as_bytes().to_vec(),
            }),
        );
    }

    let post_filters = zerompk::to_msgpack_vec(&vec![ScanFilter {
        field: "a.id".into(),
        op: FilterOp::LtColumn,
        value: nodedb_types::Value::String("b.id".into()),
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
            left_alias: Some("a".into()),
            right_alias: Some("b".into()),
            on: vec![("user_id".into(), "user_id".into())],
            join_type: "inner".into(),
            limit: 100,
            post_group_by: Vec::new(),
            post_aggregates: Vec::new(),
            projection: vec![
                JoinProjection {
                    source: "a.id".into(),
                    output: "order1".into(),
                },
                JoinProjection {
                    source: "b.id".into(),
                    output: "order2".into(),
                },
                JoinProjection {
                    source: "a.user_id".into(),
                    output: "a.user_id".into(),
                },
            ],
            computed_projection: Vec::new(),
            join_filters: Vec::new(),
            post_filters,
            left_input: None,
            right_input: None,
            left_bitmap: None,
            right_bitmap: None,
        }),
    );

    let json = response_codec::decode_payload_to_json(&payload);
    let parsed: Vec<serde_json::Value> =
        serde_json::from_str(&json).unwrap_or_else(|e| panic!("invalid JSON: {e}\nraw: {json}"));

    assert_eq!(parsed.len(), 1, "expected one user_id pair, got {json}");
    let row = parsed[0].as_object().unwrap();
    assert_eq!(row.get("order1").and_then(|v| v.as_str()), Some("o1"));
    assert_eq!(row.get("order2").and_then(|v| v.as_str()), Some("o3"));
    assert_eq!(row.get("a.user_id").and_then(|v| v.as_str()), Some("u1"));
}
