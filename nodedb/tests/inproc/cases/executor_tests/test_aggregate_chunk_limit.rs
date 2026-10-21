// SPDX-License-Identifier: BUSL-1.1

//! Fast executor-level regression guard: LIMIT is honoured when groups span
//! multiple `aggregate_chunk_size` chunks.
//!
//! The slow wire-level test (`limit_on_group_by_honoured_above_page_cap` in
//! `sql_aggregate_functions.rs`) ingested 12 000 rows over the network (~46 s)
//! purely to exceed the hard-coded 10 000 chunk boundary.  By making the chunk
//! size a `QueryTuning` knob we can set it to 2 here and exercise the same
//! chunk-boundary property with a tiny dataset (30 rows, < 1 s).

use super::helpers::{make_ctx, payload_value, send_ok};
use nodedb_physical::physical_plan::{
    AggregateSpec, ColumnarInsertIntent, ColumnarOp, GroupKeySpec, PhysicalPlan, QueryOp,
};
use nodedb_types::config::tuning::QueryTuning;
use nodedb_types::{DatabaseId, QualifiedCollection};

/// Insert `rows` into a columnar collection in a single batch.
/// Each row carries `{ "id": "rN", "g": "gM", "v": N }` where `M = N % groups`.
fn insert_grouped_columnar(
    ctx: &mut super::helpers::TestCtx,
    collection: &str,
    total: usize,
    groups: usize,
) {
    let rows: Vec<serde_json::Value> = (0..total)
        .map(|i| {
            serde_json::json!({
                "id": format!("r{i}"),
                "g": format!("g{}", i % groups),
                "v": i,
            })
        })
        .collect();
    let payload = nodedb_types::json_to_msgpack(&serde_json::Value::Array(rows)).unwrap();
    send_ok(
        &mut ctx.core,
        &mut ctx.tx,
        &mut ctx.rx,
        PhysicalPlan::Columnar(ColumnarOp::Insert {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, collection),
            payload,
            format: "msgpack".into(),
            intent: ColumnarInsertIntent::Insert,
            on_conflict_updates: Vec::new(),
            surrogates: Vec::new(),
            schema_bytes: Vec::new(),
            provenance: None,
            wal_lsn: None,
            system_from: None,
            rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
            returning: None,
            rls_filters: Vec::new(),
        }),
    );
}

/// LIMIT is honoured even when the dataset spans many `aggregate_chunk_size`
/// chunks.
///
/// Setup: `aggregate_chunk_size = 2`, 10 distinct groups, 3 docs each (30 rows
/// total). The 30 rows form 15 chunks of 2.  We ask for `LIMIT 3` (fewer than
/// the 10 groups). The result must have exactly 3 rows regardless of how many
/// chunk iterations the aggregator ran.
#[test]
fn limit_honoured_when_groups_span_multiple_aggregate_chunks() {
    let mut ctx = make_ctx();

    // Shrink the chunk size to 2 so even a 30-row dataset spans many chunks.
    ctx.core.set_query_tuning(QueryTuning {
        aggregate_chunk_size: 2,
        ..QueryTuning::default()
    });

    // 10 distinct groups, 3 rows per group = 30 rows total.
    const GROUPS: usize = 10;
    const ROWS_PER_GROUP: usize = 3;
    const TOTAL: usize = GROUPS * ROWS_PER_GROUP;
    insert_grouped_columnar(&mut ctx, "chunk_limit_col", TOTAL, GROUPS);

    // Ask for LIMIT 3 — fewer than the 10 groups.
    const LIMIT: usize = 3;
    let payload = send_ok(
        &mut ctx.core,
        &mut ctx.tx,
        &mut ctx.rx,
        PhysicalPlan::Query(QueryOp::Aggregate {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "chunk_limit_col"),
            input: None,
            group_by: vec![GroupKeySpec::column("g")],
            aggregates: vec![AggregateSpec {
                function: "count".into(),
                alias: "count(*)".into(),
                user_alias: None,
                field: "*".into(),
                expr: None,
                param: None,
            }],
            filters: Vec::new(),
            having: Vec::new(),
            limit: LIMIT,
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

    assert!(
        rows.len() <= LIMIT,
        "LIMIT {LIMIT} must cap the GROUP BY result; got {} rows. \
         A result of {GROUPS} would indicate LIMIT was silently ignored \
         when groups span multiple aggregate_chunk_size chunks.",
        rows.len()
    );
    assert_eq!(
        rows.len(),
        LIMIT,
        "expected exactly {LIMIT} rows (LIMIT honoured); got {}",
        rows.len()
    );
}

/// Sanity check: without a LIMIT cap all groups are returned even with a tiny
/// chunk size.  This guards against a regression where setting a small chunk
/// size accidentally truncates results when no LIMIT is requested.
#[test]
fn no_limit_returns_all_groups_with_small_chunk_size() {
    let mut ctx = make_ctx();

    // Same tiny chunk size.
    ctx.core.set_query_tuning(QueryTuning {
        aggregate_chunk_size: 2,
        ..QueryTuning::default()
    });

    const GROUPS: usize = 10;
    insert_grouped_columnar(&mut ctx, "chunk_nolimit_col", 30, GROUPS);

    // usize::MAX = no LIMIT — all 10 groups must come back.
    let payload = send_ok(
        &mut ctx.core,
        &mut ctx.tx,
        &mut ctx.rx,
        PhysicalPlan::Query(QueryOp::Aggregate {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "chunk_nolimit_col"),
            input: None,
            group_by: vec![GroupKeySpec::column("g")],
            aggregates: vec![AggregateSpec {
                function: "count".into(),
                alias: "count(*)".into(),
                user_alias: None,
                field: "*".into(),
                expr: None,
                param: None,
            }],
            filters: Vec::new(),
            having: Vec::new(),
            limit: usize::MAX,
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

    assert_eq!(
        rows.len(),
        GROUPS,
        "no-LIMIT GROUP BY with small chunk_size must return all {GROUPS} groups; got {}",
        rows.len()
    );
}
