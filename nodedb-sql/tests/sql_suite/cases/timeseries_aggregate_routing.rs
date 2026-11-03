// SPDX-License-Identifier: Apache-2.0

//! Which aggregate path a timeseries query takes.
//!
//! The native grouped scan (`SqlPlan::TimeseriesScan` with aggregates)
//! serves plain GROUP BY on columns and a time bucket with COUNT / SUM /
//! AVG / MIN / MAX / `ts_stddev` over numeric columns, and HAVING without a
//! time bucket. Every other shape becomes `SqlPlan::Aggregate` over a raw
//! timeseries scan: grouping sets, DISTINCT, FILTER, computed arguments,
//! text MIN / MAX, expression keys, and HAVING on a bucket.

use nodedb_sql::types::{CollectionInfo, ColumnInfo, EngineType, SqlDataType};
use nodedb_sql::{SqlCatalog, SqlCatalogError, SqlPlan, plan_sql};
use nodedb_types::DatabaseId;

struct Catalog;

fn column(name: &str, data_type: SqlDataType, raw: &str) -> ColumnInfo {
    ColumnInfo {
        name: name.into(),
        data_type,
        nullable: true,
        is_primary_key: false,
        default: None,
        raw_type: Some(raw.into()),
        int_width: None,
        float_width: None,
    }
}

impl SqlCatalog for Catalog {
    fn get_collection(
        &self,
        _: DatabaseId,
        name: &str,
    ) -> std::result::Result<Option<CollectionInfo>, SqlCatalogError> {
        if name != "m" {
            return Ok(None);
        }
        Ok(Some(CollectionInfo {
            name: name.into(),
            engine: EngineType::Timeseries,
            columns: vec![
                column("ts", SqlDataType::Int64, "BIGINT TIME_KEY"),
                column("host", SqlDataType::String, "TEXT"),
                column("v", SqlDataType::Float64, "FLOAT"),
            ],
            primary_key: None,
            has_auto_tier: false,
            indexes: Vec::new(),
            bitemporal: false,
            primary: nodedb_types::PrimaryEngine::Document,
            vector_primary: None,
            partition_strategy: nodedb_types::PartitionStrategy::CollectionHomed,
            open_schema: CollectionInfo::open_schema_for(EngineType::Timeseries),
        }))
    }

    fn lookup_array(&self, _name: &str) -> Option<nodedb_sql::types::ArrayCatalogView> {
        None
    }

    fn array_exists(&self, _name: &str) -> bool {
        false
    }
}

fn plan_one(sql: &str) -> SqlPlan {
    let mut plans = plan_sql(sql, &Catalog).unwrap_or_else(|e| panic!("{sql}: {e}"));
    assert_eq!(plans.len(), 1, "expected exactly one plan for: {sql}");
    plans.pop().expect("one plan")
}

fn is_native(plan: &SqlPlan) -> bool {
    matches!(plan, SqlPlan::TimeseriesScan(p) if !p.aggregates.is_empty() || p.bucket_interval_ms > 0)
}

fn is_generic_over_raw_scan(plan: &SqlPlan) -> bool {
    match plan {
        SqlPlan::Aggregate { input, .. } => {
            matches!(input.as_ref(), SqlPlan::TimeseriesScan(p) if p.aggregates.is_empty())
        }
        _ => false,
    }
}

#[test]
fn plain_shapes_take_the_native_scan() {
    for sql in [
        "SELECT host, COUNT(*), SUM(v), AVG(v), MIN(v), MAX(v) FROM m GROUP BY host",
        "SELECT time_bucket('1h', ts) AS b, COUNT(*) FROM m GROUP BY b",
        "SELECT ts_stddev(v) FROM m",
        "SELECT host, COUNT(*) FROM m GROUP BY host HAVING COUNT(*) > 1",
    ] {
        let plan = plan_one(sql);
        assert!(is_native(&plan), "{sql} must be native: {plan:?}");
    }
}

#[test]
fn native_having_is_carried_on_the_scan() {
    match plan_one("SELECT host, COUNT(*) FROM m GROUP BY host HAVING COUNT(*) > 1") {
        SqlPlan::TimeseriesScan(p) => assert_eq!(p.having.len(), 1, "HAVING on the scan"),
        other => panic!("expected a native scan, got {other:?}"),
    }
}

#[test]
fn other_shapes_take_the_generic_aggregate() {
    for sql in [
        "SELECT host, COUNT(*) FROM m GROUP BY ROLLUP (host)",
        "SELECT host, COUNT(*) FROM m GROUP BY CUBE (host)",
        "SELECT COUNT(DISTINCT host) FROM m",
        "SELECT COUNT(*) FILTER (WHERE v > 1) FROM m",
        "SELECT SUM(v * 2) FROM m",
        "SELECT MIN(host) FROM m",
        "SELECT UPPER(host), COUNT(*) FROM m GROUP BY UPPER(host)",
        "SELECT time_bucket('1h', ts) AS b, COUNT(*) FROM m GROUP BY b HAVING COUNT(*) > 1",
    ] {
        let plan = plan_one(sql);
        assert!(
            is_generic_over_raw_scan(&plan),
            "{sql} must be a generic aggregate over a raw scan: {plan:?}"
        );
    }
}

#[test]
fn generic_input_scan_is_unbounded() {
    match plan_one("SELECT host, COUNT(*) FROM m GROUP BY ROLLUP (host) LIMIT 1") {
        SqlPlan::Aggregate { input, .. } => match *input {
            SqlPlan::TimeseriesScan(p) => assert_eq!(p.limit, usize::MAX, "input scan has no cap"),
            other => panic!("expected a raw timeseries input, got {other:?}"),
        },
        other => panic!("expected a generic aggregate, got {other:?}"),
    }
}

/// A native scan sorts its encoded rows, which carry each aggregate under
/// its canonical key: an aggregate's alias and the call itself both sort by
/// that key.
#[test]
fn native_order_by_names_the_canonical_key() {
    for sql in [
        "SELECT host, SUM(v) AS s FROM m GROUP BY host ORDER BY s DESC LIMIT 2",
        "SELECT host, SUM(v) FROM m GROUP BY host ORDER BY SUM(v) DESC LIMIT 2",
    ] {
        match plan_one(sql) {
            SqlPlan::TimeseriesScan(p) => {
                assert_eq!(p.sort_keys.len(), 1, "{sql}");
                assert!(!p.sort_keys[0].ascending, "{sql}");
                assert!(
                    matches!(&p.sort_keys[0].expr, nodedb_sql::types::SqlExpr::Column { name, .. } if name == "sum(v)"),
                    "{sql}: {:?}",
                    p.sort_keys[0].expr
                );
            }
            other => panic!("{sql}: expected a native scan, got {other:?}"),
        }
    }
}

/// An aggregate named only in ORDER BY takes the generic aggregate, which
/// computes it.
#[test]
fn order_by_only_aggregate_takes_the_generic_aggregate() {
    let plan = plan_one("SELECT host FROM m GROUP BY host ORDER BY SUM(v) DESC");
    assert!(
        is_generic_over_raw_scan(&plan),
        "ORDER BY-only aggregate must be generic: {plan:?}"
    );
}

#[test]
fn group_by_position_out_of_range_is_refused() {
    let err = plan_sql("SELECT host, COUNT(*) FROM m GROUP BY 3", &Catalog)
        .expect_err("position 3 is past the select list");
    assert!(
        err.to_string()
            .contains("GROUP BY position 3 is not in select list"),
        "{err}"
    );
}
