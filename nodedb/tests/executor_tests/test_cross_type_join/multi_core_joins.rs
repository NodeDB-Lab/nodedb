// SPDX-License-Identifier: BUSL-1.1

//! Multi-core broadcast join and inline hash join tests.

use crate::helpers::{make_ctx_with_id, send_ok};
use nodedb::data::executor::response_codec;
use nodedb_physical::physical_plan::{DocumentOp, KvOp, PhysicalPlan, QueryOp};

use super::basic_scans::build_msgpack_map;

#[test]
fn multi_core_broadcast_inner_join() {
    // Core 0: holds schemaless docs (left/large side).
    let mut core0 = make_ctx_with_id(0);
    // Core 1: holds KV prefs (right/small side).
    let mut core1 = make_ctx_with_id(1);

    // Insert docs on core 0.
    for (idx, (id, name)) in [("d1", "Alice"), ("d3", "Carol"), ("d4", "Dave")]
        .into_iter()
        .enumerate()
    {
        let doc = build_msgpack_map(&[("id", id), ("name", name)]);
        send_ok(
            &mut core0.core,
            &mut core0.tx,
            &mut core0.rx,
            PhysicalPlan::Document(DocumentOp::PointPut {
                collection: "docs".into(),
                document_id: id.into(),
                value: doc,
                surrogate: nodedb_types::Surrogate::new((idx as u32) + 1),
                pk_bytes: Vec::new(),
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
                surrogate: nodedb_types::Surrogate::ZERO,
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
            system_time: nodedb_types::SystemTimeScope::Current,
            valid_at_ms: None,
            prefilter: None,
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

    // Phase 2: Send a HashJoin to core 0 with the small side embedded as a
    // ProviderScan (the post-resolution shape the coordinator now produces:
    // large side scanned locally per core, small side broadcast inline).
    let join_payload = send_ok(
        &mut core0.core,
        &mut core0.tx,
        &mut core0.rx,
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
            right_input: Some(Box::new(PhysicalPlan::Query(QueryOp::ProviderScan {
                provider: None,
                rows: response_codec::flatten_to_relational_rows(&phase1_payload),
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

    for (idx, (id, name)) in [("d1", "Alice"), ("d3", "Carol"), ("d4", "Dave")]
        .into_iter()
        .enumerate()
    {
        let doc = build_msgpack_map(&[("id", id), ("name", name)]);
        send_ok(
            &mut core0.core,
            &mut core0.tx,
            &mut core0.rx,
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
            &mut core1.core,
            &mut core1.tx,
            &mut core1.rx,
            PhysicalPlan::Kv(KvOp::Put {
                collection: "prefs".into(),
                key: key.as_bytes().to_vec(),
                value,
                ttl_ms: 0,
                surrogate: nodedb_types::Surrogate::ZERO,
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
            system_time: nodedb_types::SystemTimeScope::Current,
            valid_at_ms: None,
            prefilter: None,
        }),
    );

    // Phase 2: broadcast join (small side embedded as ProviderScan).
    let join_payload = send_ok(
        &mut core0.core,
        &mut core0.tx,
        &mut core0.rx,
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
            right_input: Some(Box::new(PhysicalPlan::Query(QueryOp::ProviderScan {
                provider: None,
                rows: response_codec::flatten_to_relational_rows(&phase1_payload),
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
    for (idx, (id, name)) in [("d1", "Alice"), ("d3", "Carol")].into_iter().enumerate() {
        let doc = build_msgpack_map(&[("id", id), ("name", name)]);
        send_ok(
            &mut core0.core,
            &mut core0.tx,
            &mut core0.rx,
            PhysicalPlan::Document(DocumentOp::PointPut {
                collection: "docs".into(),
                document_id: id.into(),
                value: doc,
                surrogate: nodedb_types::Surrogate::new((idx as u32) + 1),
                pk_bytes: Vec::new(),
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
                surrogate: nodedb_types::Surrogate::new(1),
                pk_bytes: Vec::new(),
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
                surrogate: nodedb_types::Surrogate::ZERO,
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
                surrogate: nodedb_types::Surrogate::ZERO,
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
        system_time: nodedb_types::SystemTimeScope::Current,
        valid_at_ms: None,
        prefilter: None,
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

    // Phase 2: HashJoin on each core with the small side embedded as a
    // ProviderScan, then merge results.
    let join_plan = |data: Vec<u8>| {
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
            right_input: Some(Box::new(PhysicalPlan::Query(QueryOp::ProviderScan {
                provider: None,
                rows: response_codec::flatten_to_relational_rows(&data),
                filters: Vec::new(),
                projection: Vec::new(),
                sort_keys: Vec::new(),
                limit: None,
                offset: 0,
                distinct: false,
            }))),
            left_bitmap: None,
            right_bitmap: None,
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
