//! Integration test: cross-type JOIN pipeline.
//!
//! Tests the full pipeline from KV/doc storage through scan, merge, encode, decode.
//! Includes single-core hash join and multi-core broadcast join scenarios.

use crate::helpers::{make_ctx, make_ctx_with_id, send_ok};
use nodedb::bridge::envelope::PhysicalPlan;
use nodedb::bridge::physical_plan::{
    AggregateSpec, DocumentOp, EnforcementOptions, JoinProjection, KvOp, QueryOp, StorageMode,
};
use nodedb::bridge::scan_filter::{FilterOp, ScanFilter};
use nodedb::data::executor::handlers::join;
use nodedb::data::executor::response_codec;
use nodedb_types::columnar::{ColumnDef, ColumnType, StrictSchema};

/// Build a standard msgpack map from string key-value pairs.
fn build_msgpack_map(fields: &[(&str, &str)]) -> Vec<u8> {
    let mut map = serde_json::Map::new();
    for (k, v) in fields {
        map.insert(k.to_string(), serde_json::Value::String(v.to_string()));
    }
    nodedb_types::json_to_msgpack(&serde_json::Value::Object(map)).unwrap()
}

// ── Single-core roundtrips ──

#[test]
fn kv_put_scan_roundtrip() {
    let mut ctx = make_ctx();

    let value1 = build_msgpack_map(&[("theme", "dark"), ("lang", "en")]);
    send_ok(
        &mut ctx.core,
        &mut ctx.tx,
        &mut ctx.rx,
        PhysicalPlan::Kv(KvOp::Put {
            collection: "prefs".into(),
            key: b"d1".to_vec(),
            value: value1,
            ttl_ms: 0,
        }),
    );

    let value2 = build_msgpack_map(&[("theme", "light"), ("lang", "fr")]);
    send_ok(
        &mut ctx.core,
        &mut ctx.tx,
        &mut ctx.rx,
        PhysicalPlan::Kv(KvOp::Put {
            collection: "prefs".into(),
            key: b"d3".to_vec(),
            value: value2,
            ttl_ms: 0,
        }),
    );

    let kv_docs = ctx.core.scan_collection(1, "prefs", 100).unwrap();
    assert_eq!(
        kv_docs.len(),
        2,
        "expected 2 KV entries, got {}",
        kv_docs.len()
    );

    for (doc_id, bytes) in &kv_docs {
        let json = nodedb_types::json_from_msgpack(bytes).unwrap();
        let obj = json.as_object().unwrap();
        assert!(
            obj.contains_key("key"),
            "doc {doc_id} missing 'key': {json}"
        );
        assert!(
            obj.contains_key("theme"),
            "doc {doc_id} missing 'theme': {json}"
        );
    }
}

#[test]
fn document_scan_preserves_kv_rows_when_collection_has_strict_config() {
    let mut ctx = make_ctx();

    send_ok(
        &mut ctx.core,
        &mut ctx.tx,
        &mut ctx.rx,
        PhysicalPlan::Document(DocumentOp::Register {
            collection: "prefs".into(),
            index_paths: Vec::new(),
            crdt_enabled: false,
            storage_mode: StorageMode::Strict {
                schema: StrictSchema {
                    columns: vec![
                        ColumnDef::required("key", ColumnType::String).with_primary_key(),
                        ColumnDef::required("theme", ColumnType::String),
                        ColumnDef::nullable("lang", ColumnType::String),
                    ],
                    version: 1,
                    dropped_columns: Vec::new(),
                },
            },
            enforcement: Box::new(EnforcementOptions::default()),
        }),
    );

    let value = build_msgpack_map(&[("theme", "dark"), ("lang", "en")]);
    send_ok(
        &mut ctx.core,
        &mut ctx.tx,
        &mut ctx.rx,
        PhysicalPlan::Kv(KvOp::Put {
            collection: "prefs".into(),
            key: b"d1".to_vec(),
            value,
            ttl_ms: 0,
        }),
    );

    let payload = send_ok(
        &mut ctx.core,
        &mut ctx.tx,
        &mut ctx.rx,
        PhysicalPlan::Document(DocumentOp::Scan {
            collection: "prefs".into(),
            filters: Vec::new(),
            limit: 100,
            offset: 0,
            sort_keys: Vec::new(),
            distinct: false,
            projection: Vec::new(),
            computed_columns: Vec::new(),
            window_functions: Vec::new(),
        }),
    );

    let json = response_codec::decode_payload_to_json(&payload);
    let parsed: Vec<serde_json::Value> =
        serde_json::from_str(&json).unwrap_or_else(|e| panic!("invalid JSON: {e}\nraw: {json}"));
    assert_eq!(parsed.len(), 1, "expected 1 row, got {json}");

    let data = parsed[0]["data"]
        .as_object()
        .unwrap_or_else(|| panic!("expected object data, got {}", parsed[0]["data"]));
    assert_eq!(data.get("key").and_then(|v| v.as_str()), Some("d1"));
    assert_eq!(data.get("theme").and_then(|v| v.as_str()), Some("dark"));
    assert_eq!(data.get("lang").and_then(|v| v.as_str()), Some("en"));
}

#[test]
fn schemaless_put_scan_roundtrip() {
    let mut ctx = make_ctx();

    let doc1 = build_msgpack_map(&[("id", "d1"), ("name", "Alice")]);
    send_ok(
        &mut ctx.core,
        &mut ctx.tx,
        &mut ctx.rx,
        PhysicalPlan::Document(DocumentOp::PointPut {
            collection: "docs".into(),
            document_id: "d1".into(),
            value: doc1,
        }),
    );

    let doc2 = build_msgpack_map(&[("id", "d3"), ("name", "Carol")]);
    send_ok(
        &mut ctx.core,
        &mut ctx.tx,
        &mut ctx.rx,
        PhysicalPlan::Document(DocumentOp::PointPut {
            collection: "docs".into(),
            document_id: "d3".into(),
            value: doc2,
        }),
    );

    let docs = ctx.core.scan_collection(1, "docs", 100).unwrap();
    assert_eq!(docs.len(), 2, "expected 2 docs, got {}", docs.len());

    for (doc_id, bytes) in &docs {
        let json = nodedb_types::json_from_msgpack(bytes).unwrap();
        let obj = json.as_object().unwrap();
        assert!(obj.contains_key("id"), "doc {doc_id} missing 'id': {json}");
        assert!(
            obj.contains_key("name"),
            "doc {doc_id} missing 'name': {json}"
        );
    }
}

#[test]
fn merge_encode_decode_roundtrip() {
    let left = build_msgpack_map(&[("id", "d1"), ("name", "Alice")]);
    let right = build_msgpack_map(&[("key", "d1"), ("theme", "dark")]);

    let merged = join::merge_join_docs_binary(&left, Some(&right), "docs", "prefs");
    let merged_json = nodedb_types::json_from_msgpack(&merged).unwrap();
    let obj = merged_json.as_object().unwrap();
    assert!(
        obj.contains_key("docs.id"),
        "missing docs.id: {merged_json}"
    );
    assert!(
        obj.contains_key("prefs.theme"),
        "missing prefs.theme: {merged_json}"
    );

    let rows = vec![merged.clone(), merged.clone(), merged];
    let encoded = response_codec::encode_binary_rows(&rows);
    let json_str = response_codec::decode_payload_to_json(&encoded);
    assert!(json_str.starts_with('['), "expected JSON array: {json_str}");

    let parsed: Vec<serde_json::Value> = serde_json::from_str(&json_str).unwrap();
    assert_eq!(parsed.len(), 3);
}

#[test]
fn broadcast_merge_preserves_nonempty() {
    let row = build_msgpack_map(&[("docs.id", "d1"), ("prefs.theme", "dark")]);
    let nonempty_encoded = response_codec::encode_binary_rows(&[row]);
    let nonempty_json = response_codec::decode_payload_to_json(&nonempty_encoded);

    let mut all_elements: Vec<String> = Vec::new();
    // 22 empty cores
    for _ in 0..22 {
        let inner = "";
        if !inner.trim().is_empty() {
            all_elements.push(inner.to_string());
        }
    }
    // 1 core with data
    if nonempty_json.starts_with('[') && nonempty_json.ends_with(']') {
        let inner = &nonempty_json[1..nonempty_json.len() - 1];
        if !inner.trim().is_empty() {
            all_elements.push(inner.to_string());
        }
    }

    let merged = format!("[{}]", all_elements.join(","));
    let parsed: Vec<serde_json::Value> = serde_json::from_str(&merged).unwrap();
    assert_eq!(parsed.len(), 1, "expected 1 row, got {}", parsed.len());
}

// ── Single-core cross-type hash join ──

#[test]
fn single_core_cross_type_hash_join() {
    // Both collections on same core — tests join matching across KV and schemaless.
    let mut ctx = make_ctx();

    // Insert schemaless docs.
    for (id, name) in [("d1", "Alice"), ("d3", "Carol"), ("d4", "Dave")] {
        let doc = build_msgpack_map(&[("id", id), ("name", name)]);
        send_ok(
            &mut ctx.core,
            &mut ctx.tx,
            &mut ctx.rx,
            PhysicalPlan::Document(DocumentOp::PointPut {
                collection: "docs".into(),
                document_id: id.into(),
                value: doc,
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
            }),
        );
    }

    // Verify scan_collection works for both.
    let docs = ctx.core.scan_collection(1, "docs", 100).unwrap();
    assert_eq!(docs.len(), 3, "docs: expected 3, got {}", docs.len());
    let prefs = ctx.core.scan_collection(1, "prefs", 100).unwrap();
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
            post_filters: Vec::new(),
            inline_left: None,
            inline_right: None,
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

    for (id, name) in [("d1", "Alice"), ("d3", "Carol"), ("d4", "Dave")] {
        let doc = build_msgpack_map(&[("id", id), ("name", name)]);
        send_ok(
            &mut ctx.core,
            &mut ctx.tx,
            &mut ctx.rx,
            PhysicalPlan::Document(DocumentOp::PointPut {
                collection: "docs".into(),
                document_id: id.into(),
                value: doc,
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
            post_filters: Vec::new(),
            inline_left: None,
            inline_right: None,
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

    for (id, name, dept) in [
        ("1", "Alice", "eng"),
        ("2", "Bob", "eng"),
        ("3", "Cara", "pm"),
    ] {
        let doc = build_msgpack_map(&[("id", id), ("name", name), ("dept", dept)]);
        send_ok(
            &mut ctx.core,
            &mut ctx.tx,
            &mut ctx.rx,
            PhysicalPlan::Document(DocumentOp::PointPut {
                collection: "employees".into(),
                document_id: id.into(),
                value: doc,
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
            post_filters,
            inline_left: None,
            inline_right: None,
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
            post_filters: Vec::new(),
            inline_left: None,
            inline_right: None,
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

    for (id, user_id, item) in [
        ("o1", "u1", "book"),
        ("o3", "u1", "pen"),
        ("o2", "u2", "pad"),
    ] {
        let mut obj = std::collections::HashMap::new();
        obj.insert(
            "user_id".to_string(),
            nodedb_types::Value::String(user_id.into()),
        );
        obj.insert("item".to_string(), nodedb_types::Value::String(item.into()));
        let tagged = zerompk::to_msgpack_vec(&nodedb_types::Value::Object(obj)).unwrap();

        send_ok(
            &mut ctx.core,
            &mut ctx.tx,
            &mut ctx.rx,
            PhysicalPlan::Document(DocumentOp::PointPut {
                collection: "orders".into(),
                document_id: id.into(),
                value: tagged,
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
            post_filters,
            inline_left: None,
            inline_right: None,
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

#[test]
fn cross_join_uses_inline_right_scalar_aggregate_for_post_filter() {
    let mut ctx = make_ctx();

    for (id, name, score) in [
        ("u1", "Alice", 10.0),
        ("u2", "Bob", 7.0),
        ("u3", "Cara", 8.0),
    ] {
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
            post_filters,
            inline_left: None,
            inline_right: Some(Box::new(PhysicalPlan::Query(QueryOp::Aggregate {
                collection: "users".into(),
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
            }))),
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

    for (id, amount) in [
        ("o1", 30.0_f64),
        ("o2", 99.99_f64),
        ("o3", 30.0_f64),
        ("o4", 25.0_f64),
        ("o5", 50.0_f64),
    ] {
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
            post_filters,
            inline_left: None,
            inline_right: Some(Box::new(PhysicalPlan::Query(QueryOp::Aggregate {
                collection: "orders".into(),
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
            }))),
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

    for (id, name) in [("u1", "Alice"), ("u2", "Bob Updated"), ("u3", "Carol")] {
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
            }),
        );
    }

    for (id, user_id, amount) in [
        ("o1", "u1", 30.0_f64),
        ("o2", "u2", 99.99_f64),
        ("o3", "u3", 30.0_f64),
        ("o4", "u1", 25.0_f64),
        ("o5", "u2", 50.0_f64),
    ] {
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
            post_filters: Vec::new(),
            inline_left: None,
            inline_right: Some(Box::new(PhysicalPlan::Query(QueryOp::HashJoin {
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
                post_filters: inner_post_filters,
                inline_left: None,
                inline_right: Some(Box::new(PhysicalPlan::Query(QueryOp::Aggregate {
                    collection: "orders".into(),
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
                }))),
            }))),
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

// ── Multi-core broadcast join tests ──
//
// Simulate the two-phase broadcast join: Phase 1 scans the right (small)
// side from one core, Phase 2 sends BroadcastJoin to the left (large) core
// with the serialized right-side data embedded.

#[test]
fn multi_core_broadcast_inner_join() {
    // Core 0: holds schemaless docs (left/large side).
    let mut core0 = make_ctx_with_id(0);
    // Core 1: holds KV prefs (right/small side).
    let mut core1 = make_ctx_with_id(1);

    // Insert docs on core 0.
    for (id, name) in [("d1", "Alice"), ("d3", "Carol"), ("d4", "Dave")] {
        let doc = build_msgpack_map(&[("id", id), ("name", name)]);
        send_ok(
            &mut core0.core,
            &mut core0.tx,
            &mut core0.rx,
            PhysicalPlan::Document(DocumentOp::PointPut {
                collection: "docs".into(),
                document_id: id.into(),
                value: doc,
            }),
        );
    }

    // Insert KV prefs on core 1.
    for (key, theme, lang) in [("d1", "dark", "en"), ("d3", "light", "fr")] {
        let value = build_msgpack_map(&[("theme", theme), ("lang", lang)]);
        send_ok(
            &mut core1.core,
            &mut core1.tx,
            &mut core1.rx,
            PhysicalPlan::Kv(KvOp::Put {
                collection: "prefs".into(),
                key: key.as_bytes().to_vec(),
                value,
                ttl_ms: 0,
            }),
        );
    }

    // Phase 1: Scan prefs from core 1 via DocumentScan (same as broadcast_raw).
    let phase1_payload = send_ok(
        &mut core1.core,
        &mut core1.tx,
        &mut core1.rx,
        PhysicalPlan::Document(DocumentOp::Scan {
            collection: "prefs".into(),
            filters: Vec::new(),
            limit: 100,
            offset: 0,
            sort_keys: Vec::new(),
            distinct: false,
            projection: Vec::new(),
            computed_columns: Vec::new(),
            window_functions: Vec::new(),
        }),
    );

    eprintln!(
        "phase1 broadcast_data: {} bytes, hex: {:02x?}",
        phase1_payload.len(),
        &phase1_payload[..phase1_payload.len().min(80)]
    );
    assert!(
        !phase1_payload.is_empty(),
        "Phase 1 scan returned empty payload"
    );

    // Phase 2: Send BroadcastJoin to core 0 with phase1 data.
    let join_payload = send_ok(
        &mut core0.core,
        &mut core0.tx,
        &mut core0.rx,
        PhysicalPlan::Query(QueryOp::BroadcastJoin {
            large_collection: "docs".into(),
            small_collection: "prefs".into(),
            large_alias: None,
            small_alias: None,
            broadcast_data: phase1_payload,
            on: vec![("id".into(), "key".into())],
            join_type: "inner".into(),
            limit: 100,
            post_group_by: Vec::new(),
            post_aggregates: Vec::new(),
            projection: Vec::new(),
            post_filters: Vec::new(),
        }),
    );

    let json = response_codec::decode_payload_to_json(&join_payload);
    eprintln!("broadcast inner join result: {json}");
    let parsed: Vec<serde_json::Value> =
        serde_json::from_str(&json).unwrap_or_else(|e| panic!("invalid JSON: {e}\nraw: {json}"));

    // d1 and d3 match, d4 has no prefs → INNER JOIN = 2 rows.
    assert_eq!(
        parsed.len(),
        2,
        "expected 2 broadcast inner join rows, got {}. json={json}",
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
fn multi_core_broadcast_left_join() {
    let mut core0 = make_ctx_with_id(0);
    let mut core1 = make_ctx_with_id(1);

    for (id, name) in [("d1", "Alice"), ("d3", "Carol"), ("d4", "Dave")] {
        let doc = build_msgpack_map(&[("id", id), ("name", name)]);
        send_ok(
            &mut core0.core,
            &mut core0.tx,
            &mut core0.rx,
            PhysicalPlan::Document(DocumentOp::PointPut {
                collection: "docs".into(),
                document_id: id.into(),
                value: doc,
            }),
        );
    }

    for (key, theme) in [("d1", "dark"), ("d3", "light")] {
        let value = build_msgpack_map(&[("theme", theme)]);
        send_ok(
            &mut core1.core,
            &mut core1.tx,
            &mut core1.rx,
            PhysicalPlan::Kv(KvOp::Put {
                collection: "prefs".into(),
                key: key.as_bytes().to_vec(),
                value,
                ttl_ms: 0,
            }),
        );
    }

    // Phase 1: scan small side.
    let phase1_payload = send_ok(
        &mut core1.core,
        &mut core1.tx,
        &mut core1.rx,
        PhysicalPlan::Document(DocumentOp::Scan {
            collection: "prefs".into(),
            filters: Vec::new(),
            limit: 100,
            offset: 0,
            sort_keys: Vec::new(),
            distinct: false,
            projection: Vec::new(),
            computed_columns: Vec::new(),
            window_functions: Vec::new(),
        }),
    );

    // Phase 2: broadcast join.
    let join_payload = send_ok(
        &mut core0.core,
        &mut core0.tx,
        &mut core0.rx,
        PhysicalPlan::Query(QueryOp::BroadcastJoin {
            large_collection: "docs".into(),
            small_collection: "prefs".into(),
            large_alias: None,
            small_alias: None,
            broadcast_data: phase1_payload,
            on: vec![("id".into(), "key".into())],
            join_type: "left".into(),
            limit: 100,
            post_group_by: Vec::new(),
            post_aggregates: Vec::new(),
            projection: Vec::new(),
            post_filters: Vec::new(),
        }),
    );

    let json = response_codec::decode_payload_to_json(&join_payload);
    eprintln!("broadcast left join result: {json}");
    let parsed: Vec<serde_json::Value> =
        serde_json::from_str(&json).unwrap_or_else(|e| panic!("invalid JSON: {e}\nraw: {json}"));

    // d1 matches, d3 matches, d4 has no prefs → LEFT JOIN = 3 rows.
    assert_eq!(
        parsed.len(),
        3,
        "expected 3 broadcast left join rows, got {}. json={json}",
        parsed.len()
    );
}

#[test]
fn multi_core_broadcast_merge_simulation() {
    // Simulates what broadcast_to_all_cores does: collect encoded payloads
    // from multiple cores and merge them into one JSON array.
    let mut core0 = make_ctx_with_id(0);
    let mut core1 = make_ctx_with_id(1);

    // Docs split across cores: d1,d3 on core0, d4 on core1.
    for (id, name) in [("d1", "Alice"), ("d3", "Carol")] {
        let doc = build_msgpack_map(&[("id", id), ("name", name)]);
        send_ok(
            &mut core0.core,
            &mut core0.tx,
            &mut core0.rx,
            PhysicalPlan::Document(DocumentOp::PointPut {
                collection: "docs".into(),
                document_id: id.into(),
                value: doc,
            }),
        );
    }
    {
        let doc = build_msgpack_map(&[("id", "d4"), ("name", "Dave")]);
        send_ok(
            &mut core1.core,
            &mut core1.tx,
            &mut core1.rx,
            PhysicalPlan::Document(DocumentOp::PointPut {
                collection: "docs".into(),
                document_id: "d4".into(),
                value: doc,
            }),
        );
    }

    // KV prefs on both cores (simulating distributed data).
    {
        let value = build_msgpack_map(&[("theme", "dark"), ("lang", "en")]);
        send_ok(
            &mut core0.core,
            &mut core0.tx,
            &mut core0.rx,
            PhysicalPlan::Kv(KvOp::Put {
                collection: "prefs".into(),
                key: b"d1".to_vec(),
                value,
                ttl_ms: 0,
            }),
        );
    }
    {
        let value = build_msgpack_map(&[("theme", "light"), ("lang", "fr")]);
        send_ok(
            &mut core1.core,
            &mut core1.tx,
            &mut core1.rx,
            PhysicalPlan::Kv(KvOp::Put {
                collection: "prefs".into(),
                key: b"d3".to_vec(),
                value,
                ttl_ms: 0,
            }),
        );
    }

    // Phase 1: scan prefs from BOTH cores and concatenate raw payloads.
    let scan_plan = PhysicalPlan::Document(DocumentOp::Scan {
        collection: "prefs".into(),
        filters: Vec::new(),
        limit: 100,
        offset: 0,
        sort_keys: Vec::new(),
        distinct: false,
        projection: Vec::new(),
        computed_columns: Vec::new(),
        window_functions: Vec::new(),
    });
    let payload0 = send_ok(
        &mut core0.core,
        &mut core0.tx,
        &mut core0.rx,
        scan_plan.clone(),
    );
    let payload1 = send_ok(&mut core1.core, &mut core1.tx, &mut core1.rx, scan_plan);

    // Concatenate raw payloads (same as broadcast_raw).
    let mut broadcast_data = Vec::new();
    broadcast_data.extend_from_slice(&payload0);
    broadcast_data.extend_from_slice(&payload1);
    eprintln!(
        "combined broadcast_data: {} bytes (core0={}, core1={})",
        broadcast_data.len(),
        payload0.len(),
        payload1.len()
    );

    // Phase 2: BroadcastJoin on each core, then merge results.
    let join_plan = |data: Vec<u8>| {
        PhysicalPlan::Query(QueryOp::BroadcastJoin {
            large_collection: "docs".into(),
            small_collection: "prefs".into(),
            large_alias: None,
            small_alias: None,
            broadcast_data: data,
            on: vec![("id".into(), "key".into())],
            join_type: "inner".into(),
            limit: 100,
            post_group_by: Vec::new(),
            post_aggregates: Vec::new(),
            projection: Vec::new(),
            post_filters: Vec::new(),
        })
    };

    let result0 = send_ok(
        &mut core0.core,
        &mut core0.tx,
        &mut core0.rx,
        join_plan(broadcast_data.clone()),
    );
    let result1 = send_ok(
        &mut core1.core,
        &mut core1.tx,
        &mut core1.rx,
        join_plan(broadcast_data),
    );

    // Merge results the same way broadcast_to_all_cores does.
    let mut all_elements: Vec<String> = Vec::new();
    for payload in [&result0, &result1] {
        if payload.is_empty() {
            continue;
        }
        let json_text = response_codec::decode_payload_to_json(payload);
        if json_text.starts_with('[') && json_text.ends_with(']') {
            let inner = &json_text[1..json_text.len() - 1];
            if !inner.trim().is_empty() {
                all_elements.push(inner.to_string());
            }
        } else if !json_text.is_empty() && json_text != "null" {
            all_elements.push(json_text);
        }
    }
    let merged = format!("[{}]", all_elements.join(","));
    eprintln!("merged broadcast result: {merged}");

    let parsed: Vec<serde_json::Value> = serde_json::from_str(&merged)
        .unwrap_or_else(|e| panic!("invalid merged JSON: {e}\nraw: {merged}"));

    // d1 on core0 matches prefs.d1, d3 on core0 matches prefs.d3 → 2 rows from core0.
    // d4 on core1 has no matching pref → 0 rows from core1.
    // Total INNER JOIN = 2.
    assert_eq!(
        parsed.len(),
        2,
        "expected 2 merged broadcast join rows, got {}. merged={merged}",
        parsed.len()
    );
}

#[test]
fn inline_hash_join_honors_qualified_left_keys() {
    let mut ctx = make_ctx();

    for (id, name) in [("u1", "Alice"), ("u2", "Bob")] {
        let doc = build_msgpack_map(&[("id", id), ("name", name)]);
        send_ok(
            &mut ctx.core,
            &mut ctx.tx,
            &mut ctx.rx,
            PhysicalPlan::Document(DocumentOp::PointPut {
                collection: "users".into(),
                document_id: id.into(),
                value: doc,
            }),
        );
    }

    for (id, user_id, item) in [("o1", "u1", "Book"), ("o2", "u2", "Pen")] {
        let doc = build_msgpack_map(&[("id", id), ("user_id", user_id), ("item", item)]);
        send_ok(
            &mut ctx.core,
            &mut ctx.tx,
            &mut ctx.rx,
            PhysicalPlan::Document(DocumentOp::PointPut {
                collection: "orders".into(),
                document_id: id.into(),
                value: doc,
            }),
        );
    }

    for (id, order_id, amount) in [("p1", "o1", "10"), ("p2", "o2", "25")] {
        let doc = build_msgpack_map(&[("id", id), ("order_id", order_id), ("amount", amount)]);
        send_ok(
            &mut ctx.core,
            &mut ctx.tx,
            &mut ctx.rx,
            PhysicalPlan::Document(DocumentOp::PointPut {
                collection: "payments".into(),
                document_id: id.into(),
                value: doc,
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
            post_filters: Vec::new(),
            inline_left: None,
            inline_right: None,
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
        }),
    );

    let payload = send_ok(
        &mut ctx.core,
        &mut ctx.tx,
        &mut ctx.rx,
        PhysicalPlan::Query(QueryOp::InlineHashJoin {
            left_data,
            right_data,
            right_alias: None,
            on: vec![("orders.id".into(), "order_id".into())],
            join_type: "inner".into(),
            limit: 100,
            projection: Vec::new(),
            post_filters: Vec::new(),
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
