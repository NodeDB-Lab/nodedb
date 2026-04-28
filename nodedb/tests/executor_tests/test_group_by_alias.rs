//! Integration tests for GROUP BY alias and positional reference resolution.
//!
//! Verifies that GROUP BY clauses correctly resolve SELECT expression aliases
//! and positional (1-based) references. Tests the full path from SQL string →
//! nodedb_sql planner → sql_plan_convert → PhysicalPlan → Data Plane → response.

use nodedb::bridge::envelope::PhysicalPlan;
use nodedb::bridge::physical_plan::TimeseriesOp;
use nodedb::control::planner::sql_plan_convert::{ConvertContext, convert};
use nodedb_sql::types::{CollectionInfo, EngineType, SqlCatalog, SqlPlan};

use crate::helpers::*;

// ---------------------------------------------------------------------------
// Test catalog that knows about a timeseries collection
// ---------------------------------------------------------------------------

struct TimeseriesCatalog;

impl SqlCatalog for TimeseriesCatalog {
    fn get_collection(
        &self,
        name: &str,
    ) -> std::result::Result<Option<CollectionInfo>, nodedb_sql::SqlCatalogError> {
        let info = match name {
            "dns_bench" => Some(CollectionInfo {
                name: "dns_bench".into(),
                engine: EngineType::Timeseries,
                columns: Vec::new(),
                primary_key: None,
                has_auto_tier: false,
                indexes: Vec::new(),
                bitemporal: false,
                primary: nodedb_types::PrimaryEngine::Document,
                vector_primary: None,
            }),
            _ => None,
        };
        Ok(info)
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Plan SQL through the full nodedb_sql planner and return the SqlPlan.
fn plan_sql(sql: &str) -> SqlPlan {
    let plans = nodedb_sql::plan_sql(sql, &TimeseriesCatalog).unwrap();
    assert_eq!(plans.len(), 1, "expected exactly one plan");
    plans.into_iter().next().unwrap()
}

/// Plan SQL, convert to PhysicalPlan, and extract the plan from the first task.
fn sql_to_physical(sql: &str) -> PhysicalPlan {
    let plans = nodedb_sql::plan_sql(sql, &TimeseriesCatalog).unwrap();
    let ctx = ConvertContext {
        retention_registry: None,
        array_catalog: None,
        credentials: None,
        wal: None,
        surrogate_assigner: None,
        cluster_enabled: false,
        bitemporal_retention_registry: None,
    };
    let tenant_id = nodedb::types::TenantId::new(1);
    let tasks = convert(&plans, tenant_id, &ctx).unwrap();
    assert!(!tasks.is_empty(), "expected at least one physical task");
    tasks.into_iter().next().unwrap().plan
}

/// Build an ILP payload with `count` lines spanning multiple hours.
fn ilp_lines(collection: &str, count: usize, start_ts_ns: i64) -> String {
    let mut lines = String::new();
    for i in 0..count {
        // 1 hour apart so we get multiple buckets with 1h bucketing
        let ts_ns = start_ts_ns + i as i64 * 3_600_000_000_000;
        lines.push_str(&format!("{collection},qtype=A elapsed_ms=1.0 {ts_ns}\n",));
    }
    lines
}

fn ingest_ilp(ctx: &mut TestCtx, collection: &str, payload: &str) {
    send_ok(
        &mut ctx.core,
        &mut ctx.tx,
        &mut ctx.rx,
        PhysicalPlan::Timeseries(TimeseriesOp::Ingest {
            collection: collection.to_string(),
            payload: payload.as_bytes().to_vec(),
            format: "ilp".to_string(),
            wal_lsn: None,
            surrogates: Vec::new(),
        }),
    );
}

fn query_via_physical(ctx: &mut TestCtx, plan: PhysicalPlan) -> Vec<serde_json::Value> {
    let raw = send_ok(&mut ctx.core, &mut ctx.tx, &mut ctx.rx, plan);
    let json_str = nodedb::data::executor::response_codec::decode_payload_to_json(&raw);
    serde_json::from_str(&json_str).unwrap_or_default()
}

// ---------------------------------------------------------------------------
// SQL Planner tests: verify SqlPlan has correct bucket_interval_ms
// ---------------------------------------------------------------------------

/// GROUP BY full expression — baseline (this already works).
#[test]
fn plan_group_by_full_expression_has_bucket_interval() {
    let plan = plan_sql(
        "SELECT time_bucket('1 hour', timestamp) AS b, COUNT(*) FROM dns_bench \
         GROUP BY time_bucket('1 hour', timestamp)",
    );
    let SqlPlan::TimeseriesScan {
        bucket_interval_ms, ..
    } = plan
    else {
        panic!("expected TimeseriesScan, got {plan:?}");
    };
    assert_eq!(
        bucket_interval_ms, 3_600_000,
        "full expression GROUP BY should extract 1h interval"
    );
}

/// GROUP BY alias.
#[test]
fn plan_group_by_alias_has_bucket_interval() {
    let plan = plan_sql(
        "SELECT time_bucket('1 hour', timestamp) AS b, COUNT(*) FROM dns_bench GROUP BY b",
    );
    let SqlPlan::TimeseriesScan {
        bucket_interval_ms, ..
    } = plan
    else {
        panic!("expected TimeseriesScan, got {plan:?}");
    };
    assert_eq!(
        bucket_interval_ms, 3_600_000,
        "GROUP BY alias 'b' should resolve to time_bucket and extract 1h interval"
    );
}

/// GROUP BY positional (1-based).
#[test]
fn plan_group_by_positional_has_bucket_interval() {
    let plan = plan_sql(
        "SELECT time_bucket('1 hour', timestamp) AS b, COUNT(*) FROM dns_bench GROUP BY 1",
    );
    let SqlPlan::TimeseriesScan {
        bucket_interval_ms, ..
    } = plan
    else {
        panic!("expected TimeseriesScan, got {plan:?}");
    };
    assert_eq!(
        bucket_interval_ms, 3_600_000,
        "GROUP BY 1 should resolve to time_bucket and extract 1h interval"
    );
}

// ---------------------------------------------------------------------------
// End-to-end tests: SQL → planner → convert → Data Plane → response
// ---------------------------------------------------------------------------

/// GROUP BY full expression — end-to-end baseline.
#[test]
fn e2e_group_by_full_expression() {
    let mut ctx = make_ctx();
    let payload = ilp_lines("dns_bench", 5, 1_700_000_000_000_000_000);
    ingest_ilp(&mut ctx, "dns_bench", &payload);

    let plan = sql_to_physical(
        "SELECT time_bucket('1 hour', timestamp) AS b, COUNT(*) FROM dns_bench \
         GROUP BY time_bucket('1 hour', timestamp)",
    );
    let results = query_via_physical(&mut ctx, plan);
    assert!(
        !results.is_empty(),
        "full expression GROUP BY should return buckets"
    );
    let total: u64 = results
        .iter()
        .map(|r| r["count(*)"].as_u64().unwrap_or(0))
        .sum();
    assert_eq!(total, 5, "bucket counts should sum to ingested rows");
    // Each result must have the bucket column.
    for row in &results {
        assert!(
            row.get("bucket").is_some(),
            "each row should have 'bucket' column, got: {row}"
        );
    }
}

/// GROUP BY alias — end-to-end.
#[test]
fn e2e_group_by_alias() {
    let mut ctx = make_ctx();
    let payload = ilp_lines("dns_bench", 5, 1_700_000_000_000_000_000);
    ingest_ilp(&mut ctx, "dns_bench", &payload);

    let plan = sql_to_physical(
        "SELECT time_bucket('1 hour', timestamp) AS b, COUNT(*) FROM dns_bench GROUP BY b",
    );
    let results = query_via_physical(&mut ctx, plan);
    assert!(
        !results.is_empty(),
        "GROUP BY alias should return buckets, not empty"
    );
    let total: u64 = results
        .iter()
        .map(|r| r["count(*)"].as_u64().unwrap_or(0))
        .sum();
    assert_eq!(total, 5, "bucket counts should sum to ingested rows");
    for row in &results {
        assert!(
            row.get("bucket").is_some(),
            "each row should have 'bucket' column, got: {row}"
        );
    }
}

/// GROUP BY positional — end-to-end.
#[test]
fn e2e_group_by_positional() {
    let mut ctx = make_ctx();
    let payload = ilp_lines("dns_bench", 5, 1_700_000_000_000_000_000);
    ingest_ilp(&mut ctx, "dns_bench", &payload);

    let plan = sql_to_physical(
        "SELECT time_bucket('1 hour', timestamp) AS b, COUNT(*) FROM dns_bench GROUP BY 1",
    );
    let results = query_via_physical(&mut ctx, plan);
    assert!(
        !results.is_empty(),
        "GROUP BY 1 should return buckets, not empty"
    );
    let total: u64 = results
        .iter()
        .map(|r| r["count(*)"].as_u64().unwrap_or(0))
        .sum();
    assert_eq!(total, 5, "bucket counts should sum to ingested rows");
    for row in &results {
        assert!(
            row.get("bucket").is_some(),
            "each row should have 'bucket' column, got: {row}"
        );
    }
}

/// All three GROUP BY forms must produce identical results.
#[test]
fn e2e_all_three_forms_produce_same_results() {
    let mut ctx = make_ctx();
    let payload = ilp_lines("dns_bench", 10, 1_700_000_000_000_000_000);
    ingest_ilp(&mut ctx, "dns_bench", &payload);

    let plan_full = sql_to_physical(
        "SELECT time_bucket('1 hour', timestamp) AS b, COUNT(*) FROM dns_bench \
         GROUP BY time_bucket('1 hour', timestamp)",
    );
    let plan_alias = sql_to_physical(
        "SELECT time_bucket('1 hour', timestamp) AS b, COUNT(*) FROM dns_bench GROUP BY b",
    );
    let plan_pos = sql_to_physical(
        "SELECT time_bucket('1 hour', timestamp) AS b, COUNT(*) FROM dns_bench GROUP BY 1",
    );

    let results_full = query_via_physical(&mut ctx, plan_full);

    // Re-ingest into fresh contexts to get identical state.
    let mut ctx2 = make_ctx();
    let payload2 = ilp_lines("dns_bench", 10, 1_700_000_000_000_000_000);
    ingest_ilp(&mut ctx2, "dns_bench", &payload2);
    let results_alias = query_via_physical(&mut ctx2, plan_alias);

    let mut ctx3 = make_ctx();
    let payload3 = ilp_lines("dns_bench", 10, 1_700_000_000_000_000_000);
    ingest_ilp(&mut ctx3, "dns_bench", &payload3);
    let results_pos = query_via_physical(&mut ctx3, plan_pos);

    // Compare row counts.
    assert_eq!(
        results_full.len(),
        results_alias.len(),
        "alias form should produce same number of buckets as full expression"
    );
    assert_eq!(
        results_full.len(),
        results_pos.len(),
        "positional form should produce same number of buckets as full expression"
    );

    // Compare totals.
    let sum = |rows: &[serde_json::Value]| -> u64 {
        rows.iter()
            .map(|r| r["count(*)"].as_u64().unwrap_or(0))
            .sum()
    };
    assert_eq!(sum(&results_full), 10);
    assert_eq!(sum(&results_alias), 10);
    assert_eq!(sum(&results_pos), 10);
}
