// SPDX-License-Identifier: Apache-2.0

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
            volatile_defaults: p.volatile_defaults,
        }])
    }

    fn plan_upsert(&self, _p: UpsertParams) -> Result<Vec<SqlPlan>> {
        Err(SqlError::Unsupported {
            detail: "UPSERT is not supported on timeseries collections (append-only)".into(),
        })
    }

    fn plan_scan(&self, p: ScanParams) -> Result<SqlPlan> {
        if p.temporal.is_temporal() && !p.bitemporal {
            return Err(SqlError::Unsupported {
                detail: format!(
                    "FOR SYSTEM_TIME / FOR VALID_TIME requires a bitemporal \
                     timeseries collection; '{}' was not created WITH bitemporal = true",
                    p.collection
                ),
            });
        }
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
            sort_keys: p.sort_keys,
            tiered: false,
            temporal: p.temporal,
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

    fn plan_update_from(&self, p: UpdateFromParams) -> Result<Vec<SqlPlan>> {
        Err(SqlError::Unsupported {
            detail: format!(
                "UPDATE ... FROM is not supported on timeseries collection '{}'; \
                 timeseries data is append-only",
                p.collection
            ),
        })
    }

    fn plan_delete(&self, _p: DeleteParams) -> Result<Vec<SqlPlan>> {
        // Timeseries has no row-level delete; only whole-partition drop via
        // retention policy. Routing to columnar deletes nothing.
        Err(SqlError::Unsupported {
            detail: "DELETE is not supported on timeseries collections; \
                     expire rows with CREATE RETENTION POLICY, which drops \
                     whole partitions older than the configured age"
                .into(),
        })
    }

    fn plan_truncate(&self, p: TruncateParams) -> Result<Vec<SqlPlan>> {
        Ok(vec![SqlPlan::Truncate {
            collection: p.collection,
            engine: EngineType::Timeseries,
            restart_identity: p.restart_identity,
        }])
    }

    fn plan_aggregate(&self, p: AggregateParams) -> Result<SqlPlan> {
        if p.temporal.is_temporal() && !p.bitemporal {
            return Err(SqlError::Unsupported {
                detail: format!(
                    "FOR SYSTEM_TIME / FOR VALID_TIME requires a bitemporal \
                     timeseries collection; '{}' was not created WITH bitemporal = true",
                    p.collection
                ),
            });
        }
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
            // ORDER BY is attached later by `apply_order_by`, which sees the
            // outer SELECT's sort list.
            sort_keys: Vec::new(),
            tiered: p.has_auto_tier,
            temporal: p.temporal,
        })
    }

    fn plan_merge(&self, p: MergeParams) -> Result<Vec<SqlPlan>> {
        Err(SqlError::Unsupported {
            detail: format!(
                "MERGE is not supported on timeseries collection '{}'",
                p.collection
            ),
        })
    }

    /// Every column is declared at creation and encoded per column.
    fn accepts_undeclared_columns(&self) -> bool {
        false
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
