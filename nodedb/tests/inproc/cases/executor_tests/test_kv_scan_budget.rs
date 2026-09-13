// SPDX-License-Identifier: BUSL-1.1

//! Memory-budget bound for unbounded (no-LIMIT) KV scans.
//!
//! A SQL `SELECT * FROM <kv>` arrives as `KvOp::Scan { count: usize::MAX }`.
//! It must return every entry that fits the per-query memory budget and surface
//! a deterministic `ResourcesExhausted` error rather than silently truncating
//! when the result would exceed it. An explicit `count = N` is unaffected.

use nodedb::bridge::envelope::{ErrorCode, Status};
use nodedb_physical::physical_plan::{KvOp, PhysicalPlan};
use nodedb_types::{DatabaseId, QualifiedCollection};

use super::helpers::*;

/// Insert `count` tiny entries into `collection` via a single `BatchPut`.
fn batch_put_entries(ctx: &mut TestCtx, collection: &str, count: usize) {
    let entries: Vec<(Vec<u8>, Vec<u8>)> = (0..count)
        .map(|i| {
            let key = format!("k{i}").into_bytes();
            let value = format!("v{i}").into_bytes();
            (key, value)
        })
        .collect();
    let surrogates = vec![nodedb_types::Surrogate::ZERO; entries.len()];
    send_ok(
        &mut ctx.core,
        &mut ctx.tx,
        &mut ctx.rx,
        PhysicalPlan::Kv(KvOp::BatchPut {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, collection),
            entries,
            ttl_ms: 0,
            surrogates,
            returning: None,
            rls_filters: Vec::new(),
        }),
    );
}

/// A no-LIMIT KV scan models `count == usize::MAX`.
fn kv_scan(collection: &str, count: usize) -> PhysicalPlan {
    PhysicalPlan::Kv(KvOp::Scan {
        collection: QualifiedCollection::new(DatabaseId::DEFAULT, collection),
        cursor: Vec::new(),
        count,
        filters: Vec::new(),
        match_pattern: None,
        sort_keys: Vec::new(),
        surrogate_ceiling: None,
        projection: Vec::new(),
        computed_columns: Vec::new(),
    })
}

/// A no-LIMIT KV scan over more than the historical 10k truncation point
/// returns EVERY entry when the result fits the memory budget.
#[test]
fn kv_unbounded_scan_returns_all_entries_when_within_budget() {
    let mut ctx = make_ctx();

    let count = 12_000;
    batch_put_entries(&mut ctx, "wide_kv", count);

    let payload = send_ok(
        &mut ctx.core,
        &mut ctx.tx,
        &mut ctx.rx,
        kv_scan("wide_kv", usize::MAX),
    );
    let json = payload_value(&payload);
    let entries = json.as_array().unwrap();
    assert_eq!(
        entries.len(),
        count,
        "no-LIMIT scan must return all {count} entries, not the old 10k cap"
    );
    assert!(
        entries.len() > 10_000,
        "regression: result was truncated at/under the old 10k default"
    );
}

/// A no-LIMIT KV scan whose materialized result exceeds the memory budget
/// surfaces a deterministic `ResourcesExhausted` error — never a partial result.
#[test]
fn kv_unbounded_scan_over_budget_surfaces_error() {
    let mut ctx = make_ctx();

    // Tiny per-query scan budget so a modest collection trips it.
    ctx.core
        .set_query_tuning(nodedb_types::config::tuning::QueryTuning {
            max_scan_result_bytes: 256,
            ..nodedb_types::config::tuning::QueryTuning::default()
        });

    batch_put_entries(&mut ctx, "big_kv", 5_000);

    let resp = send_raw(
        &mut ctx.core,
        &mut ctx.tx,
        &mut ctx.rx,
        kv_scan("big_kv", usize::MAX),
    );
    assert_eq!(
        resp.status,
        Status::Error,
        "over-budget scan must surface an error"
    );
    assert_eq!(
        resp.error_code.as_deref(),
        Some(&ErrorCode::ResourcesExhausted),
        "must surface the deterministic resource-exhausted error"
    );
}

/// An explicit `count = N` still returns exactly `N` entries — the budget bound
/// only applies to unbounded scans and must not change explicit-count behaviour.
#[test]
fn kv_explicit_count_returns_exactly_n() {
    let mut ctx = make_ctx();

    batch_put_entries(&mut ctx, "limited_kv", 12_000);

    let payload = send_ok(
        &mut ctx.core,
        &mut ctx.tx,
        &mut ctx.rx,
        kv_scan("limited_kv", 250),
    );
    let json = payload_value(&payload);
    let entries = json.as_array().unwrap();
    assert_eq!(
        entries.len(),
        250,
        "explicit count 250 must return exactly 250 entries"
    );
}
