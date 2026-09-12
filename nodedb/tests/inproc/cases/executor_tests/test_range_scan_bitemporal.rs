// SPDX-License-Identifier: BUSL-1.1

//! Native-protocol RANGE scan on `bitemporal=true` document collections.
//!
//! A bitemporal collection keeps every write on the versioned redb table, so
//! the plain INDEXES / DOCUMENTS tables `execute_range_scan` probed are empty.
//! Before the bitemporal branch was added, `DocumentOp::RangeScan` returned
//! ZERO rows (silent, not an error). These tests exercise both storage modes
//! (`document_schemaless` + `document_strict`) and assert the current-version
//! rows in `[lower, upper)` come back, that UPDATE moves a row in/out of range,
//! that a DELETE tombstone is respected, and that the scan never silently
//! falls through to the empty plain-table fallback.

use nodedb::bridge::envelope::Status;
use nodedb_physical::physical_plan::{DocumentOp, EnforcementOptions, PhysicalPlan, StorageMode};
use nodedb_types::columnar::{ColumnDef, ColumnType, StrictSchema};

use super::helpers::{TestCtx, make_ctx};

/// Build an all-string MessagePack map — the write value both storage modes
/// accept (strict re-encodes it to a Binary Tuple via the registered schema).
fn msgpack_map(fields: &[(&str, &str)]) -> Vec<u8> {
    let mut map = serde_json::Map::new();
    for (k, v) in fields {
        map.insert(k.to_string(), serde_json::Value::String(v.to_string()));
    }
    nodedb_types::json_to_msgpack(&serde_json::Value::Object(map)).unwrap()
}

fn register_schemaless_bitemporal(ctx: &mut TestCtx, collection: &str) {
    let resp = super::helpers::send_raw(
        &mut ctx.core,
        &mut ctx.tx,
        &mut ctx.rx,
        PhysicalPlan::Document(DocumentOp::Register {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                collection,
            ),
            indexes: Vec::new(),
            crdt_enabled: false,
            storage_mode: StorageMode::Schemaless,
            enforcement: Box::new(EnforcementOptions::default()),
            bitemporal: true,
            conflict_policy: None,
            timeseries: None,
            vector_primary: None,
        }),
    );
    assert_eq!(resp.status, Status::Ok, "register schemaless bitemporal");
}

fn register_strict_bitemporal(ctx: &mut TestCtx, collection: &str) {
    let resp = super::helpers::send_raw(
        &mut ctx.core,
        &mut ctx.tx,
        &mut ctx.rx,
        PhysicalPlan::Document(DocumentOp::Register {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                collection,
            ),
            indexes: Vec::new(),
            crdt_enabled: false,
            storage_mode: StorageMode::Strict {
                // A bitemporal strict schema must carry the three reserved
                // timestamp columns in slots 0/1/2 ahead of the user columns —
                // exactly what the real `CREATE ... WITH (bitemporal=true)` DDL
                // builds. `new_bitemporal` prepends them and sets `bitemporal`.
                schema: StrictSchema::new_bitemporal(vec![
                    ColumnDef::required("id", ColumnType::String).with_primary_key(),
                    ColumnDef::required("score", ColumnType::String),
                ])
                .expect("build bitemporal strict schema"),
            },
            enforcement: Box::new(EnforcementOptions::default()),
            bitemporal: true,
            conflict_policy: None,
            timeseries: None,
            vector_primary: None,
        }),
    );
    assert_eq!(resp.status, Status::Ok, "register strict bitemporal");
}

fn put(ctx: &mut TestCtx, collection: &str, doc_id: &str, value: Vec<u8>, surrogate: u32) {
    let resp = super::helpers::send_raw(
        &mut ctx.core,
        &mut ctx.tx,
        &mut ctx.rx,
        PhysicalPlan::Document(DocumentOp::PointPut {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                collection,
            ),
            document_id: doc_id.into(),
            value,
            surrogate: nodedb_types::Surrogate::new(surrogate),
            pk_bytes: doc_id.as_bytes().to_vec(),
            returning: None,
            rls_filters: Vec::new(),
            resolved_sum_targets: Vec::new(),
        }),
    );
    assert_eq!(resp.status, Status::Ok, "PointPut {doc_id}");
}

fn delete(ctx: &mut TestCtx, collection: &str, doc_id: &str, surrogate: u32) {
    let resp = super::helpers::send_raw(
        &mut ctx.core,
        &mut ctx.tx,
        &mut ctx.rx,
        PhysicalPlan::Document(DocumentOp::PointDelete {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                collection,
            ),
            document_id: doc_id.into(),
            surrogate: nodedb_types::Surrogate::new(surrogate),
            pk_bytes: doc_id.as_bytes().to_vec(),
            returning: None,
            rls_filters: Vec::new(),
            rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
            resolved_sum_targets: Vec::new(),
        }),
    );
    assert_eq!(resp.status, Status::Ok, "PointDelete {doc_id}");
}

/// Run a RANGE scan and return the `score` values of the returned rows,
/// in the order the handler produced them (ascending by `score`).
fn range_scan_scores(
    ctx: &mut TestCtx,
    collection: &str,
    lower: Option<&[u8]>,
    upper: Option<&[u8]>,
) -> Vec<String> {
    let resp = super::helpers::send_raw(
        &mut ctx.core,
        &mut ctx.tx,
        &mut ctx.rx,
        PhysicalPlan::Document(DocumentOp::RangeScan {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                collection,
            ),
            field: "score".into(),
            lower: lower.map(|b| b.to_vec()),
            upper: upper.map(|b| b.to_vec()),
            limit: 100,
            rls_filters: Vec::new(),
        }),
    );
    assert_eq!(resp.status, Status::Ok, "RangeScan status");
    let json = nodedb::data::executor::response_codec::decode_payload_to_json(&resp.payload);
    let rows: Vec<serde_json::Value> = serde_json::from_str(&json).unwrap_or_else(|_| Vec::new());
    rows.iter()
        .filter_map(|row| {
            // Rows are `{ "id": ..., "data": { "score": ... } }`.
            row.get("data")
                .and_then(|d| d.get("score"))
                .and_then(|s| s.as_str())
                .map(|s| s.to_string())
        })
        .collect()
}

/// Seed four rows with zero-padded numeric-string scores so lexical byte
/// ordering (what the secondary index uses) matches numeric ordering.
fn seed_four(ctx: &mut TestCtx, collection: &str) {
    let rows = [("d1", "010"), ("d2", "020"), ("d3", "030"), ("d4", "040")];
    for (i, (id, score)) in rows.iter().enumerate() {
        let value = msgpack_map(&[("id", id), ("score", score)]);
        put(ctx, collection, id, value, (i + 1) as u32);
    }
}

fn assert_range_basics(ctx: &mut TestCtx, collection: &str) {
    // [020, 040) → 020, 030 (inclusive lower, exclusive upper).
    let scores = range_scan_scores(ctx, collection, Some(b"020"), Some(b"040"));
    assert_eq!(
        scores,
        vec!["020".to_string(), "030".to_string()],
        "bitemporal range [020,040) must return the two in-range CURRENT rows \
         (empty here = the bug)"
    );

    // Unbounded scan returns all four current rows — proves the versioned
    // branch is used and never falls through to the empty plain-table
    // fallback (a no-plain-index-hit case).
    let all = range_scan_scores(ctx, collection, None, None);
    assert_eq!(
        all,
        vec![
            "010".to_string(),
            "020".to_string(),
            "030".to_string(),
            "040".to_string()
        ],
        "unbounded bitemporal range scan must return all current rows"
    );
}

#[test]
fn schemaless_bitemporal_range_scan_returns_in_range_rows() {
    let mut ctx = make_ctx();
    register_schemaless_bitemporal(&mut ctx, "events");
    seed_four(&mut ctx, "events");
    assert_range_basics(&mut ctx, "events");
}

#[test]
fn strict_bitemporal_range_scan_returns_in_range_rows() {
    let mut ctx = make_ctx();
    register_strict_bitemporal(&mut ctx, "events");
    seed_four(&mut ctx, "events");
    assert_range_basics(&mut ctx, "events");
}

#[test]
fn schemaless_bitemporal_update_moves_row_in_range() {
    let mut ctx = make_ctx();
    register_schemaless_bitemporal(&mut ctx, "events");
    seed_four(&mut ctx, "events");

    // The versioned row identity is the SURROGATE (a PointPut keys its
    // version by `StorageKey::for_surrogate(surrogate)`), so an UPDATE must reuse
    // the row's original surrogate to append a new version of the SAME row —
    // a fresh surrogate would create a distinct row instead of superseding.
    // Seed surrogates are 1..4 for d1..d4.

    // Move d4 (040, out of [020,035)) into range by updating its score to 025.
    put(
        &mut ctx,
        "events",
        "d4",
        msgpack_map(&[("id", "d4"), ("score", "025")]),
        4,
    );
    let scores = range_scan_scores(&mut ctx, "events", Some(b"020"), Some(b"035"));
    assert_eq!(
        scores,
        vec!["020".to_string(), "025".to_string(), "030".to_string()],
        "range scan must reflect only the CURRENT version after UPDATE"
    );

    // Move d2 (020) OUT of range by updating its score to 099.
    put(
        &mut ctx,
        "events",
        "d2",
        msgpack_map(&[("id", "d2"), ("score", "099")]),
        2,
    );
    let scores = range_scan_scores(&mut ctx, "events", Some(b"020"), Some(b"035"));
    assert_eq!(
        scores,
        vec!["025".to_string(), "030".to_string()],
        "an updated-out-of-range row must not appear"
    );
}

#[test]
fn strict_bitemporal_update_moves_row_in_range() {
    let mut ctx = make_ctx();
    register_strict_bitemporal(&mut ctx, "events");
    seed_four(&mut ctx, "events");

    // Reuse d4's original surrogate (4) so the UPDATE supersedes the same
    // versioned row rather than creating a distinct one.
    put(
        &mut ctx,
        "events",
        "d4",
        msgpack_map(&[("id", "d4"), ("score", "025")]),
        4,
    );
    let scores = range_scan_scores(&mut ctx, "events", Some(b"020"), Some(b"035"));
    assert_eq!(
        scores,
        vec!["020".to_string(), "025".to_string(), "030".to_string()],
        "strict range scan must reflect only the CURRENT version after UPDATE"
    );
}

#[test]
fn schemaless_bitemporal_delete_excludes_tombstoned_row() {
    let mut ctx = make_ctx();
    register_schemaless_bitemporal(&mut ctx, "events");
    seed_four(&mut ctx, "events");

    // Delete d3 (030, in [020,040)) — its tombstone must exclude it.
    delete(&mut ctx, "events", "d3", 3);
    let scores = range_scan_scores(&mut ctx, "events", Some(b"020"), Some(b"040"));
    assert_eq!(
        scores,
        vec!["020".to_string()],
        "deleted in-range row must be excluded (tombstone respected)"
    );
}

#[test]
fn strict_bitemporal_delete_excludes_tombstoned_row() {
    let mut ctx = make_ctx();
    register_strict_bitemporal(&mut ctx, "events");
    seed_four(&mut ctx, "events");

    delete(&mut ctx, "events", "d3", 3);
    let scores = range_scan_scores(&mut ctx, "events", Some(b"020"), Some(b"040"));
    assert_eq!(
        scores,
        vec!["020".to_string()],
        "deleted in-range strict row must be excluded (tombstone respected)"
    );
}
