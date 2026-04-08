//! Integration test: cross-type JOIN pipeline.
//!
//! Tests the full pipeline from KV/doc storage through scan, merge, encode, decode.
//! Includes single-core hash join and multi-core broadcast join scenarios.

use crate::helpers::{make_ctx, make_ctx_with_id, send_ok};
use nodedb::bridge::envelope::PhysicalPlan;
use nodedb::bridge::physical_plan::{DocumentOp, EnforcementOptions, KvOp, QueryOp, StorageMode};
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
