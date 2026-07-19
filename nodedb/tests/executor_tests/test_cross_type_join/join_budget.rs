// SPDX-License-Identifier: BUSL-1.1

//! Tests for the join-side memory-budget guards and the removal of the
//! silent 50,000-row-per-side cap.
//!
//! Verifies these properties:
//! 1. **Completeness past the old cap** — an inner join whose matching row
//!    sits beyond index 50,000 IS returned when the memory budget allows it.
//! 2. **Build-side spill completion (hash join, both sides local)** — when the
//!    build (right) side of a hash join over plain local scans exceeds
//!    `max_scan_result_bytes`, the join no longer errors: it streams the build
//!    side into a grace-hash partitioner that spills to disk, streams the probe
//!    side through it, and COMPLETES with the full, correct result set.
//! 3. **Deterministic error over budget (remaining cases)** — when a side that
//!    is NOT covered by the memory-bounded hash-join path exceeds the byte
//!    budget, the join still returns `ResourcesExhausted` (never a truncated
//!    success). This now covers both sides of the nested-loop and sort-merge
//!    handlers. The hash-join handler, when both sides are plain local scans,
//!    COMPLETES for an over-budget side of EITHER kind: an over-budget build
//!    (right) side spills to a grace-hash partitioner; an over-budget probe
//!    (left) side is streamed in bounded batches against the in-memory build
//!    index. (A no-LIMIT join whose OUTPUT exceeds the byte budget still
//!    surfaces `ResourcesExhausted` — see property-bound tests below.)

use nodedb::bridge::envelope::{ErrorCode, Status};
use nodedb::bridge::scan_filter::{FilterOp, ScanFilter};
use nodedb_physical::physical_plan::{KvOp, PhysicalPlan, QueryOp};

use crate::helpers::*;

// ── helpers ──────────────────────────────────────────────────────────────────

/// Insert `count` KV entries (`k0`…`k{count-1}`) with tiny payloads.
/// Returns the key of the last entry inserted.
fn batch_kv(ctx: &mut TestCtx, collection: &str, count: usize) -> String {
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
            collection: collection.into(),
            entries,
            ttl_ms: 0,
            surrogates,
        }),
    );
    format!("k{}", count - 1)
}

// ── 1. Completeness past the old 50k cap ─────────────────────────────────────

/// An inner join whose probe row sits beyond the old 50,000-row cap is still
/// returned when the memory budget allows.
///
/// Strategy: insert 51,000 KV entries on the left side and one matching entry
/// whose key is `k50999` (the last one). The right side has a single entry
/// with key `k50999` as well. With the old cap the probe never reached that
/// row; after the fix the full scan completes and the join matches.
#[test]
fn hash_join_completeness_past_50k_cap() {
    let mut ctx = make_ctx();

    // Left: 51,000 entries — the matching key is the very last one.
    let match_key = batch_kv(&mut ctx, "left_large", 51_000);

    // Right: one entry with the same key.
    send_ok(
        &mut ctx.core,
        &mut ctx.tx,
        &mut ctx.rx,
        PhysicalPlan::Kv(KvOp::Put {
            collection: "right_small".into(),
            key: match_key.as_bytes().to_vec(),
            value: b"match".to_vec(),
            ttl_ms: 0,
            surrogate: nodedb_types::Surrogate::ZERO,
        }),
    );

    // Hash join on the `key` field (the KV engine surfaces the key as `key`).
    let payload = send_ok(
        &mut ctx.core,
        &mut ctx.tx,
        &mut ctx.rx,
        PhysicalPlan::Query(QueryOp::HashJoin {
            left_collection: "left_large".into(),
            right_collection: "right_small".into(),
            left_alias: None,
            right_alias: None,
            on: vec![("key".into(), "key".into())],
            join_type: "inner".into(),
            // Use a large limit so the join itself doesn't cap results.
            limit: 1_000_000,
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

    let json = payload_value(&payload);
    let rows = json
        .as_array()
        .unwrap_or_else(|| panic!("expected JSON array, got {json}"));
    assert_eq!(
        rows.len(),
        1,
        "inner join must find the match that sits beyond index 50k; got {} rows",
        rows.len()
    );
}

/// Same completeness check for the sort-merge join handler.
#[test]
fn sort_merge_join_completeness_past_50k_cap() {
    let mut ctx = make_ctx();

    let match_key = batch_kv(&mut ctx, "smj_left", 51_000);
    send_ok(
        &mut ctx.core,
        &mut ctx.tx,
        &mut ctx.rx,
        PhysicalPlan::Kv(KvOp::Put {
            collection: "smj_right".into(),
            key: match_key.as_bytes().to_vec(),
            value: b"smatch".to_vec(),
            ttl_ms: 0,
            surrogate: nodedb_types::Surrogate::ZERO,
        }),
    );

    let payload = send_ok(
        &mut ctx.core,
        &mut ctx.tx,
        &mut ctx.rx,
        PhysicalPlan::Query(QueryOp::SortMergeJoin {
            left_collection: "smj_left".into(),
            right_collection: "smj_right".into(),
            on: vec![("key".into(), "key".into())],
            join_type: "inner".into(),
            limit: 1_000_000,
            pre_sorted: false,
        }),
    );

    let json = payload_value(&payload);
    let rows = json
        .as_array()
        .unwrap_or_else(|| panic!("expected JSON array, got {json}"));
    assert_eq!(
        rows.len(),
        1,
        "sort-merge join must find the match that sits beyond index 50k; got {} rows",
        rows.len()
    );
}

/// Same completeness check for the nested-loop join handler.
#[test]
fn nested_loop_join_completeness_past_50k_cap() {
    let mut ctx = make_ctx();

    let match_key = batch_kv(&mut ctx, "nlj_left", 51_000);
    send_ok(
        &mut ctx.core,
        &mut ctx.tx,
        &mut ctx.rx,
        PhysicalPlan::Kv(KvOp::Put {
            collection: "nlj_right".into(),
            key: match_key.as_bytes().to_vec(),
            value: b"nlmatch".to_vec(),
            ttl_ms: 0,
            surrogate: nodedb_types::Surrogate::ZERO,
        }),
    );

    // Nested-loop join with no condition = cross join; the limit gates the output.
    // Use a limit large enough to let the single match through.
    let payload = send_ok(
        &mut ctx.core,
        &mut ctx.tx,
        &mut ctx.rx,
        PhysicalPlan::Query(QueryOp::NestedLoopJoin {
            left_collection: "nlj_left".into(),
            right_collection: "nlj_right".into(),
            // Serialize a ScanFilter on the key column to restrict to the match.
            condition: zerompk::to_msgpack_vec(&vec![ScanFilter {
                field: "nlj_left.key".into(),
                op: FilterOp::EqColumn,
                value: nodedb_types::Value::String("nlj_right.key".into()),
                clauses: Vec::new(),
                expr: None,
            }])
            .unwrap(),
            join_type: "inner".into(),
            limit: 1_000_000,
        }),
    );

    let json = payload_value(&payload);
    let rows = json
        .as_array()
        .unwrap_or_else(|| panic!("expected JSON array, got {json}"));
    // The cross-product of 51k × 1 filtered to matching keys = 1 row.
    assert_eq!(
        rows.len(),
        1,
        "nested-loop join must find the match that sits beyond index 50k; got {} rows",
        rows.len()
    );
}

// ── 2. Deterministic error when a side exceeds the byte budget ────────────────

/// Hash join: left side (PROBE) over budget, both sides local → the join
/// STREAMS the probe side in bounded batches against the in-memory build index
/// and COMPLETES with the correct result — it does NOT surface
/// `ResourcesExhausted`.
///
/// Setup: large left side (500 rows `k0..k499`, whose byte total far exceeds
/// the 256-byte budget) joined against a single right row `k0`. The build
/// (right) side fits budget so the build buffers in RAM; the over-budget probe
/// side is streamed batch-by-batch against that index. The inner equi-join on
/// `key` matches exactly one pair (`k0`), so the streamed-and-completed result
/// must be exactly 1 row — proving the streamed probe produces the correct
/// join, not merely a non-error response.
#[test]
fn hash_join_left_side_over_budget_streams_and_completes() {
    let mut ctx = make_ctx();

    // Tiny budget: 256 bytes — easily exceeded by even a handful of rows.
    ctx.core
        .set_query_tuning(nodedb_types::config::tuning::QueryTuning {
            max_scan_result_bytes: 256,
            ..nodedb_types::config::tuning::QueryTuning::default()
        });

    // Large left (probe) side; small right (build) side that fits budget.
    batch_kv(&mut ctx, "bgt_left", 500);
    batch_kv(&mut ctx, "bgt_right", 1);

    let payload = send_ok(
        &mut ctx.core,
        &mut ctx.tx,
        &mut ctx.rx,
        PhysicalPlan::Query(QueryOp::HashJoin {
            left_collection: "bgt_left".into(),
            right_collection: "bgt_right".into(),
            left_alias: None,
            right_alias: None,
            on: vec![("key".into(), "key".into())],
            join_type: "inner".into(),
            limit: 1_000_000,
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

    let json = payload_value(&payload);
    let rows = json
        .as_array()
        .unwrap_or_else(|| panic!("expected JSON array, got {json}"));
    assert_eq!(
        rows.len(),
        1,
        "over-budget probe side must stream and complete with the one matching \
         row (k0); got {} rows",
        rows.len()
    );
}

/// Hash join: build (right) side over budget, both sides local → the join
/// streams the build side into a grace-hash partitioner, spills to disk,
/// streams the probe side through it, and COMPLETES with the correct result —
/// it does NOT surface `ResourcesExhausted`.
///
/// Setup: left side is a single row `k0`; right side is 500 rows `k0..k499`
/// whose byte total far exceeds the 256-byte budget, forcing the build side to
/// spill. The inner equi-join on `key` matches exactly one pair (`k0`), so the
/// spilled-and-completed result must be exactly 1 row — proving the spill path
/// produces the correct join, not merely a non-error response.
#[test]
fn hash_join_right_side_over_budget_spills_and_completes() {
    let mut ctx = make_ctx();

    ctx.core
        .set_query_tuning(nodedb_types::config::tuning::QueryTuning {
            max_scan_result_bytes: 256,
            ..nodedb_types::config::tuning::QueryTuning::default()
        });

    // Small left (single key k0), large right (k0..k499) — build side spills.
    batch_kv(&mut ctx, "bgtr_left", 1);
    batch_kv(&mut ctx, "bgtr_right", 500);

    let payload = send_ok(
        &mut ctx.core,
        &mut ctx.tx,
        &mut ctx.rx,
        PhysicalPlan::Query(QueryOp::HashJoin {
            left_collection: "bgtr_left".into(),
            right_collection: "bgtr_right".into(),
            left_alias: None,
            right_alias: None,
            on: vec![("key".into(), "key".into())],
            join_type: "inner".into(),
            limit: 1_000_000,
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

    let json = payload_value(&payload);
    let rows = json
        .as_array()
        .unwrap_or_else(|| panic!("expected JSON array, got {json}"));
    assert_eq!(
        rows.len(),
        1,
        "build-side spill must complete the join with the one matching row (k0); got {} rows",
        rows.len()
    );
}

/// Hash join build-side spill, MANY matches across partitions: a both-local
/// inner join whose build side far exceeds the byte budget must spill across
/// all 64 grace partitions and still return EVERY matching row — proving the
/// spilled, partition-by-partition probe is complete (no dropped partitions,
/// no truncated runs).
///
/// Both sides hold the same 2,000 keys `k0..k1999`; the inner equi-join on
/// `key` matches all 2,000. With a 256-byte budget the build side spills; the
/// probe side is streamed through the partitioner. The explicit large LIMIT
/// means the output is not budget-capped, so all 2,000 rows must come back.
#[test]
fn hash_join_build_side_spill_returns_all_matches_across_partitions() {
    let mut ctx = make_ctx();

    ctx.core
        .set_query_tuning(nodedb_types::config::tuning::QueryTuning {
            max_scan_result_bytes: 256,
            ..nodedb_types::config::tuning::QueryTuning::default()
        });

    let n = 2_000usize;
    batch_kv(&mut ctx, "spill_left", n);
    batch_kv(&mut ctx, "spill_right", n);

    let payload = send_ok(
        &mut ctx.core,
        &mut ctx.tx,
        &mut ctx.rx,
        PhysicalPlan::Query(QueryOp::HashJoin {
            left_collection: "spill_left".into(),
            right_collection: "spill_right".into(),
            left_alias: None,
            right_alias: None,
            on: vec![("key".into(), "key".into())],
            join_type: "inner".into(),
            limit: 1_000_000,
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

    let json = payload_value(&payload);
    let rows = json
        .as_array()
        .unwrap_or_else(|| panic!("expected JSON array, got {json}"));
    assert_eq!(
        rows.len(),
        n,
        "build-side spill across partitions must return all {n} matches; got {}",
        rows.len()
    );
}

/// Hash join PROBE-side streaming, MANY matches: a both-local inner join whose
/// BUILD (right) side fits the byte budget but whose PROBE (left) side far
/// exceeds it must stream the probe in bounded batches against the in-memory
/// build index and still return EVERY matching row — proving the batched probe
/// is complete (no dropped batches, no truncated runs) and that the global
/// output limit / match logic accumulate correctly across batches.
///
/// Sizing (documented to satisfy "build ≤ budget < probe bytes"):
/// - Budget = 4096 bytes.
/// - Build (right) = 50 keys `k0..k49`. Each KV row contributes its id
///   (`"kN"`, 2-3 bytes) plus its injected-`key` msgpack value (~12-15 bytes),
///   so the 50-row build total (~750-900 bytes) sits well UNDER 4096 → the
///   build side buffers in RAM (does not spill).
/// - Probe (left) = 4000 keys `k0..k3999`. Its byte total (~60 KiB) far EXCEEDS
///   4096 → the probe side is streamed in bounded ≤budget batches against the
///   in-memory build index.
///
/// The inner equi-join on `key` matches `k0..k49` = exactly 50 rows. The
/// explicit large LIMIT means the output is not budget-capped, so all 50 rows
/// must come back.
#[test]
fn hash_join_probe_side_spill_returns_all_matches() {
    let mut ctx = make_ctx();

    ctx.core
        .set_query_tuning(nodedb_types::config::tuning::QueryTuning {
            max_scan_result_bytes: 4096,
            ..nodedb_types::config::tuning::QueryTuning::default()
        });

    // Build (right) fits budget; probe (left) is far over budget.
    batch_kv(&mut ctx, "pspill_right", 50);
    batch_kv(&mut ctx, "pspill_left", 4000);

    let payload = send_ok(
        &mut ctx.core,
        &mut ctx.tx,
        &mut ctx.rx,
        PhysicalPlan::Query(QueryOp::HashJoin {
            left_collection: "pspill_left".into(),
            right_collection: "pspill_right".into(),
            left_alias: None,
            right_alias: None,
            on: vec![("key".into(), "key".into())],
            join_type: "inner".into(),
            limit: 1_000_000,
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

    let json = payload_value(&payload);
    let rows = json
        .as_array()
        .unwrap_or_else(|| panic!("expected JSON array, got {json}"));
    assert_eq!(
        rows.len(),
        50,
        "probe-side streaming must return all 50 matches (k0..k49) across \
         batches; got {}",
        rows.len()
    );
}

/// Sort-merge join: left side over budget → `ResourcesExhausted`.
#[test]
fn sort_merge_join_left_over_budget_surfaces_error() {
    let mut ctx = make_ctx();

    ctx.core
        .set_query_tuning(nodedb_types::config::tuning::QueryTuning {
            max_scan_result_bytes: 256,
            ..nodedb_types::config::tuning::QueryTuning::default()
        });

    batch_kv(&mut ctx, "smjb_left", 500);
    batch_kv(&mut ctx, "smjb_right", 1);

    let resp = send_raw(
        &mut ctx.core,
        &mut ctx.tx,
        &mut ctx.rx,
        PhysicalPlan::Query(QueryOp::SortMergeJoin {
            left_collection: "smjb_left".into(),
            right_collection: "smjb_right".into(),
            on: vec![("key".into(), "key".into())],
            join_type: "inner".into(),
            limit: 1_000_000,
            pre_sorted: false,
        }),
    );

    assert_eq!(resp.status, Status::Error);
    assert_eq!(
        resp.error_code.as_deref(),
        Some(&ErrorCode::ResourcesExhausted),
        "sort-merge left over-budget must surface ResourcesExhausted"
    );
}

/// Sort-merge join: right side over budget → `ResourcesExhausted`.
#[test]
fn sort_merge_join_right_over_budget_surfaces_error() {
    let mut ctx = make_ctx();

    ctx.core
        .set_query_tuning(nodedb_types::config::tuning::QueryTuning {
            max_scan_result_bytes: 256,
            ..nodedb_types::config::tuning::QueryTuning::default()
        });

    batch_kv(&mut ctx, "smjbr_left", 1);
    batch_kv(&mut ctx, "smjbr_right", 500);

    let resp = send_raw(
        &mut ctx.core,
        &mut ctx.tx,
        &mut ctx.rx,
        PhysicalPlan::Query(QueryOp::SortMergeJoin {
            left_collection: "smjbr_left".into(),
            right_collection: "smjbr_right".into(),
            on: vec![("key".into(), "key".into())],
            join_type: "inner".into(),
            limit: 1_000_000,
            pre_sorted: false,
        }),
    );

    assert_eq!(resp.status, Status::Error);
    assert_eq!(
        resp.error_code.as_deref(),
        Some(&ErrorCode::ResourcesExhausted),
        "sort-merge right over-budget must surface ResourcesExhausted"
    );
}

/// Nested-loop join: left side over budget → `ResourcesExhausted`.
#[test]
fn nested_loop_join_left_over_budget_surfaces_error() {
    let mut ctx = make_ctx();

    ctx.core
        .set_query_tuning(nodedb_types::config::tuning::QueryTuning {
            max_scan_result_bytes: 256,
            ..nodedb_types::config::tuning::QueryTuning::default()
        });

    batch_kv(&mut ctx, "nljb_left", 500);
    batch_kv(&mut ctx, "nljb_right", 1);

    let resp = send_raw(
        &mut ctx.core,
        &mut ctx.tx,
        &mut ctx.rx,
        PhysicalPlan::Query(QueryOp::NestedLoopJoin {
            left_collection: "nljb_left".into(),
            right_collection: "nljb_right".into(),
            condition: Vec::new(),
            join_type: "inner".into(),
            limit: 1_000_000,
        }),
    );

    assert_eq!(resp.status, Status::Error);
    assert_eq!(
        resp.error_code.as_deref(),
        Some(&ErrorCode::ResourcesExhausted),
        "nested-loop left over-budget must surface ResourcesExhausted"
    );
}

/// Nested-loop join: right side over budget → `ResourcesExhausted`.
#[test]
fn nested_loop_join_right_over_budget_surfaces_error() {
    let mut ctx = make_ctx();

    ctx.core
        .set_query_tuning(nodedb_types::config::tuning::QueryTuning {
            max_scan_result_bytes: 256,
            ..nodedb_types::config::tuning::QueryTuning::default()
        });

    batch_kv(&mut ctx, "nljbr_left", 1);
    batch_kv(&mut ctx, "nljbr_right", 500);

    let resp = send_raw(
        &mut ctx.core,
        &mut ctx.tx,
        &mut ctx.rx,
        PhysicalPlan::Query(QueryOp::NestedLoopJoin {
            left_collection: "nljbr_left".into(),
            right_collection: "nljbr_right".into(),
            condition: Vec::new(),
            join_type: "inner".into(),
            limit: 1_000_000,
        }),
    );

    assert_eq!(resp.status, Status::Error);
    assert_eq!(
        resp.error_code.as_deref(),
        Some(&ErrorCode::ResourcesExhausted),
        "nested-loop right over-budget must surface ResourcesExhausted"
    );
}

// ── 3. No-LIMIT join: output bounded by byte budget, never truncated ──────────

/// A no-LIMIT join (`limit == usize::MAX`) whose OUTPUT exceeds the byte budget
/// must surface `ResourcesExhausted` — NOT a silent 10,000-row (or any) cap.
///
/// Strategy: a cross (nested-loop, no condition) join of two small sides whose
/// individual byte totals fit the budget, but whose Cartesian product blows past
/// the budget-derived output ceiling (floored at 1000 rows). 40 × 40 = 1600
/// emitted rows ≥ 1000 → the post-emit budget check fires.
#[test]
fn no_limit_join_output_over_budget_surfaces_error() {
    let mut ctx = make_ctx();

    // Budget large enough that each 40-row side fits, but < 16 KiB so the
    // unbounded output ceiling stays at its 1000-row floor.
    ctx.core
        .set_query_tuning(nodedb_types::config::tuning::QueryTuning {
            max_scan_result_bytes: 4096,
            ..nodedb_types::config::tuning::QueryTuning::default()
        });

    batch_kv(&mut ctx, "nolim_left", 40);
    batch_kv(&mut ctx, "nolim_right", 40);

    let resp = send_raw(
        &mut ctx.core,
        &mut ctx.tx,
        &mut ctx.rx,
        PhysicalPlan::Query(QueryOp::NestedLoopJoin {
            left_collection: "nolim_left".into(),
            right_collection: "nolim_right".into(),
            // Cross join (no condition) → 40 × 40 = 1600 output rows.
            condition: Vec::new(),
            join_type: "inner".into(),
            // usize::MAX = no SQL LIMIT — output bounded by the byte budget.
            limit: usize::MAX,
        }),
    );

    assert_eq!(
        resp.status,
        Status::Error,
        "no-LIMIT join over output budget must error, not silently truncate"
    );
    assert_eq!(
        resp.error_code.as_deref(),
        Some(&ErrorCode::ResourcesExhausted),
        "expected ResourcesExhausted, got {:?}",
        resp.error_code
    );
}

/// A no-LIMIT join whose output comfortably fits the budget returns ALL rows —
/// and crucially returns MORE than the old hard-coded 10,000 cap, proving the
/// silent cap is gone. Budget 0 = unlimited.
///
/// Strategy: an inner equi-join of 12,000 left rows against 12,000 right rows
/// on matching keys → 12,000 matched rows (> 10,000). With `limit == usize::MAX`
/// and budget 0, every matched row must come back.
#[test]
fn no_limit_join_within_budget_returns_all_rows_past_10k() {
    let mut ctx = make_ctx();

    // Unlimited budget so the only thing that could cap output is the old bug.
    ctx.core
        .set_query_tuning(nodedb_types::config::tuning::QueryTuning {
            max_scan_result_bytes: 0,
            ..nodedb_types::config::tuning::QueryTuning::default()
        });

    let n = 12_000usize;
    batch_kv(&mut ctx, "allrows_left", n);
    batch_kv(&mut ctx, "allrows_right", n);

    let payload = send_ok(
        &mut ctx.core,
        &mut ctx.tx,
        &mut ctx.rx,
        PhysicalPlan::Query(QueryOp::HashJoin {
            left_collection: "allrows_left".into(),
            right_collection: "allrows_right".into(),
            left_alias: None,
            right_alias: None,
            on: vec![("key".into(), "key".into())],
            join_type: "inner".into(),
            // No SQL LIMIT.
            limit: usize::MAX,
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

    let json = payload_value(&payload);
    let rows = json
        .as_array()
        .unwrap_or_else(|| panic!("expected JSON array, got {json}"));
    assert_eq!(
        rows.len(),
        n,
        "no-LIMIT join with unlimited budget must return all {n} rows (old 10k cap is gone); got {}",
        rows.len()
    );
}

/// An explicit `LIMIT k` is honored exactly regardless of budget — at most `k`
/// rows, and the (much larger) byte-budget output check does NOT fire.
#[test]
fn explicit_limit_join_caps_at_k_regardless_of_budget() {
    let mut ctx = make_ctx();

    // Unlimited budget: only the explicit LIMIT should bound the output.
    ctx.core
        .set_query_tuning(nodedb_types::config::tuning::QueryTuning {
            max_scan_result_bytes: 0,
            ..nodedb_types::config::tuning::QueryTuning::default()
        });

    let n = 5_000usize;
    batch_kv(&mut ctx, "explim_left", n);
    batch_kv(&mut ctx, "explim_right", n);

    let k = 25usize;
    let payload = send_ok(
        &mut ctx.core,
        &mut ctx.tx,
        &mut ctx.rx,
        PhysicalPlan::Query(QueryOp::HashJoin {
            left_collection: "explim_left".into(),
            right_collection: "explim_right".into(),
            left_alias: None,
            right_alias: None,
            on: vec![("key".into(), "key".into())],
            join_type: "inner".into(),
            // Explicit LIMIT k — honored exactly.
            limit: k,
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

    let json = payload_value(&payload);
    let rows = json
        .as_array()
        .unwrap_or_else(|| panic!("expected JSON array, got {json}"));
    assert!(
        rows.len() <= k,
        "explicit LIMIT {k} join must return at most {k} rows; got {}",
        rows.len()
    );
}

/// Budget of 0 is unlimited — a large join must complete without error.
#[test]
fn join_budget_zero_is_unlimited() {
    let mut ctx = make_ctx();

    // Explicitly set budget to 0 (unlimited).
    ctx.core
        .set_query_tuning(nodedb_types::config::tuning::QueryTuning {
            max_scan_result_bytes: 0,
            ..nodedb_types::config::tuning::QueryTuning::default()
        });

    batch_kv(&mut ctx, "unlim_left", 500);
    batch_kv(&mut ctx, "unlim_right", 500);

    // Hash join: both sides large but budget = 0 → must succeed.
    send_ok(
        &mut ctx.core,
        &mut ctx.tx,
        &mut ctx.rx,
        PhysicalPlan::Query(QueryOp::HashJoin {
            left_collection: "unlim_left".into(),
            right_collection: "unlim_right".into(),
            left_alias: None,
            right_alias: None,
            on: vec![("key".into(), "key".into())],
            join_type: "inner".into(),
            limit: 1_000_000,
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
}
