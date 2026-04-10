//! Engine rules for timeseries collections.
//!
//! Timeseries is append-only. INSERT, UPDATE, and UPSERT are not supported.
//! Data enters via INGEST (mapped to `TimeseriesIngest`).
//! Scans route to `TimeseriesScan` for time-range-aware execution.

use crate::engine_rules::*;
use crate::error::{Result, SqlError};
use crate::types::*;

pub struct TimeseriesRules;

impl EngineRules for TimeseriesRules {
    fn plan_insert(&self, p: InsertParams) -> Result<Vec<SqlPlan>> {
        // Timeseries INSERT routes to TimeseriesIngest — append-only semantics.
        Ok(vec![SqlPlan::TimeseriesIngest {
            collection: p.collection,
            rows: p.rows,
        }])
    }

    fn plan_upsert(&self, _p: UpsertParams) -> Result<Vec<SqlPlan>> {
        Err(SqlError::Unsupported {
            detail: "UPSERT is not supported on timeseries collections (append-only)".into(),
        })
    }

    fn plan_scan(&self, p: ScanParams) -> Result<SqlPlan> {
        // Timeseries scans use TimeseriesScan for time-range-aware execution.
        let time_range = default_time_range();
        Ok(SqlPlan::TimeseriesScan {
            collection: p.collection,
            time_range,
            bucket_interval_ms: 0,
            group_by: Vec::new(),
            aggregates: Vec::new(),
            filters: p.filters,
            projection: p.projection,
            gap_fill: String::new(),
            limit: p.limit.unwrap_or(10000),
            tiered: false,
        })
    }

    fn plan_point_get(&self, _p: PointGetParams) -> Result<SqlPlan> {
        Err(SqlError::Unsupported {
            detail: "point lookups are not supported on timeseries collections; \
                     use SELECT with a time range filter instead"
                .into(),
        })
    }

    fn plan_update(&self, _p: UpdateParams) -> Result<Vec<SqlPlan>> {
        Err(SqlError::Unsupported {
            detail: "UPDATE is not supported on timeseries collections; \
                     timeseries data is append-only"
                .into(),
        })
    }

    fn plan_delete(&self, p: DeleteParams) -> Result<Vec<SqlPlan>> {
        // Timeseries supports range-based deletion (e.g. retention).
        Ok(vec![SqlPlan::Delete {
            collection: p.collection,
            engine: EngineType::Timeseries,
            filters: p.filters,
            target_keys: p.target_keys,
        }])
    }

    fn plan_aggregate(&self, p: AggregateParams) -> Result<SqlPlan> {
        Ok(SqlPlan::TimeseriesScan {
            collection: p.collection,
            time_range: default_time_range(),
            bucket_interval_ms: p.bucket_interval_ms.unwrap_or(0),
            group_by: p.group_columns,
            aggregates: p.aggregates,
            filters: p.filters,
            projection: Vec::new(),
            gap_fill: String::new(),
            limit: p.limit,
            tiered: p.has_auto_tier,
        })
    }
}

/// Default time range bounds for the SqlPlan IR.
///
/// Actual time range extraction from filter predicates happens during
/// PhysicalPlan conversion (Origin: `value::extract_time_range`).
/// At the SqlPlan level, we pass unbounded defaults.
fn default_time_range() -> (i64, i64) {
    (i64::MIN, i64::MAX)
}
