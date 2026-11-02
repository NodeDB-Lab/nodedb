// SPDX-License-Identifier: Apache-2.0

//! Engine rules for timeseries collections.
//!
//! Timeseries is append-only. INSERT, UPDATE, and UPSERT are not supported.
//! Data enters via INGEST (mapped to `TimeseriesIngest`).
//! Scans route to `TimeseriesScan` for time-range-aware execution.

use crate::engine_rules::timeseries_native::native_shape;
use crate::engine_rules::*;
use crate::error::{Result, SqlError};
use crate::types::*;

pub struct TimeseriesRules;

impl EngineRules for TimeseriesRules {
    fn plan_insert(&self, p: InsertParams) -> Result<Vec<SqlPlan>> {
        // Timeseries INSERT routes to TimeseriesIngest — append-only semantics.
        Ok(vec![SqlPlan::TimeseriesIngest(TimeseriesIngestPlan {
            collection: p.collection,
            rows: p.rows,
            volatile_defaults: p.volatile_defaults,
        })])
    }

    fn plan_insert_select(&self, p: InsertSelectParams) -> Result<Vec<SqlPlan>> {
        Err(SqlError::Unsupported {
            detail: format!(
                "INSERT ... SELECT into timeseries collection '{}' is not supported; \
                 the target must be a document collection",
                p.collection
            ),
        })
    }

    fn plan_upsert(&self, _p: UpsertParams) -> Result<Vec<SqlPlan>> {
        Err(SqlError::Unsupported {
            detail: "UPSERT is not supported on timeseries collections (append-only)".into(),
        })
    }

    fn plan_scan(&self, p: ScanParams) -> Result<SqlPlan> {
        p.temporal.require_bitemporal(&p.collection, p.bitemporal)?;
        // Timeseries scans use TimeseriesScan for time-range-aware execution.
        let time_range = default_time_range();
        Ok(SqlPlan::TimeseriesScan(TimeseriesScanPlan {
            collection: p.collection,
            time_range,
            bucket_interval_ms: 0,
            group_by: Vec::new(),
            aggregates: Vec::new(),
            filters: p.filters,
            having: Vec::new(),
            projection: p.projection,
            gap_fill: String::new(),
            // No LIMIT reads every row. The Data Plane bounds that read by
            // the per-query byte budget and fails it loudly past the budget.
            limit: p.limit.unwrap_or(usize::MAX),
            sort_keys: p.sort_keys,
            tiered: p.has_auto_tier,
            temporal: p.temporal,
            output: Vec::new(),
            group_keys: Vec::new(),
        }))
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

    fn plan_bulk_load(&self, p: BulkLoadParams) -> Result<BulkLoadPlan> {
        Err(SqlError::Unsupported {
            detail: format!(
                "COPY: collection '{}' uses the timeseries engine; \
                 use ILP or INSERT with explicit time column instead",
                p.collection
            ),
        })
    }

    fn plan_aggregate(&self, p: AggregateParams) -> Result<SqlPlan> {
        p.temporal.require_bitemporal(&p.collection, p.bitemporal)?;
        let Some(shape) = native_shape(&p) else {
            // The generic aggregate runs over the raw row source: memtable,
            // flushed partitions, staged rows, read policy and bitemporal
            // scope. On an auto-tier collection the scan refuses a range that
            // only downsampled tiers hold, since those carry no raw rows.
            let input = SqlPlan::TimeseriesScan(TimeseriesScanPlan {
                collection: p.collection,
                time_range: default_time_range(),
                bucket_interval_ms: 0,
                group_by: Vec::new(),
                aggregates: Vec::new(),
                filters: p.filters,
                having: Vec::new(),
                projection: Vec::new(),
                gap_fill: String::new(),
                limit: usize::MAX,
                sort_keys: Vec::new(),
                tiered: p.has_auto_tier,
                temporal: p.temporal,
                output: Vec::new(),
                group_keys: Vec::new(),
            });
            return Ok(SqlPlan::Aggregate {
                input: Box::new(input),
                group_by: p.group_by,
                group_by_aliases: Vec::new(),
                output_order: Vec::new(),
                aggregates: p.aggregates,
                having: p.having,
                limit: p.limit,
                grouping_sets: p.grouping_sets,
                sort_keys: Vec::new(),
            });
        };
        Ok(SqlPlan::TimeseriesScan(TimeseriesScanPlan {
            collection: p.collection,
            time_range: default_time_range(),
            bucket_interval_ms: shape.bucket_interval_ms,
            group_by: shape.group_columns,
            aggregates: p.aggregates,
            filters: p.filters,
            having: p.having,
            projection: Vec::new(),
            gap_fill: String::new(),
            limit: p.limit,
            // ORDER BY is attached later by `apply_order_by`, which sees the
            // outer SELECT's sort list.
            sort_keys: Vec::new(),
            tiered: p.has_auto_tier,
            temporal: p.temporal,
            // The SELECT-list order and aliases are attached by the caller,
            // which holds the projection.
            output: Vec::new(),
            group_keys: p.group_by,
        }))
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

#[cfg(test)]
mod tests {
    use super::*;

    fn scan(limit: Option<usize>) -> SqlPlan {
        TimeseriesRules
            .plan_scan(ScanParams {
                collection: "metrics".into(),
                alias: None,
                filters: Vec::new(),
                projection: Vec::new(),
                sort_keys: Vec::new(),
                limit,
                offset: 0,
                distinct: false,
                window_functions: Vec::new(),
                indexes: Vec::new(),
                temporal: crate::temporal::TemporalScope::default(),
                bitemporal: false,
                has_auto_tier: false,
            })
            .expect("plans")
    }

    /// No LIMIT reads every row. A 10,000-row default cut the read silently.
    #[test]
    fn plan_scan_without_limit_is_unbounded() {
        let SqlPlan::TimeseriesScan(plan) = scan(None) else {
            panic!("expected a timeseries scan");
        };
        assert_eq!(plan.limit, usize::MAX);
        let SqlPlan::TimeseriesScan(plan) = scan(Some(5)) else {
            panic!("expected a timeseries scan");
        };
        assert_eq!(plan.limit, 5);
    }
}
