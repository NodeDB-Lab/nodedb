// SPDX-License-Identifier: Apache-2.0

//! Timeseries plan payloads: scan and ingest.

use crate::temporal::TemporalScope;
use crate::types::filter::Filter;
use crate::types::query::{AggregateExpr, Projection, SortKey};
use crate::types_expr::{SqlExpr, SqlValue};

/// Payload of [`SqlPlan::TimeseriesScan`](crate::types::SqlPlan::TimeseriesScan).
#[derive(Debug, Clone)]
pub struct TimeseriesScanPlan {
    pub collection: String,
    pub time_range: (i64, i64),
    pub bucket_interval_ms: i64,
    pub group_by: Vec<String>,
    pub aggregates: Vec<AggregateExpr>,
    pub filters: Vec<Filter>,
    /// HAVING predicates over the finalized group rows. Empty on a raw scan.
    pub having: Vec<Filter>,
    pub projection: Vec<Projection>,
    pub gap_fill: String,
    pub limit: usize,
    /// ORDER BY applied to the scan result. Empty = the engine's natural
    /// order (ascending by the collection's time key).
    pub sort_keys: Vec<SortKey>,
    pub tiered: bool,
    /// Bitemporal system-time / valid-time scope. Only non-default
    /// on collections created `WITH BITEMPORAL`; `TimeseriesRules::plan_scan`
    /// rejects temporal scopes otherwise.
    pub temporal: TemporalScope,
    /// The SELECT-list output columns of an aggregating scan, in SELECT-list
    /// order. Empty on a raw scan, and on a scan planned without a SELECT list.
    pub output: Vec<TimeseriesAggOutput>,
    /// The GROUP BY keys as planned, the `time_bucket` key among them. A tail
    /// that evaluates an item over the group rows matches its sub-expressions
    /// against these keys. Empty on a raw scan.
    pub group_keys: Vec<SqlExpr>,
}

/// One SELECT-list output column of an aggregating timeseries scan.
///
/// The scan encodes each group row under fixed keys: `bucket` for the time
/// bucket, the bare column name for a GROUP BY column, and the canonical key
/// for an aggregate. This entry names which of those keys a column reads and
/// the name the client sees.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TimeseriesAggOutput {
    /// The `time_bucket` boundary.
    Bucket {
        /// The SELECT-list alias, when the item has one.
        alias: Option<String>,
        /// The column the `time_bucket` call buckets, when it names one.
        source: Option<String>,
    },
    /// A plain GROUP BY column.
    GroupColumn {
        /// The bare column name the scan emits the key under.
        name: String,
        /// The SELECT-list alias, when the item has one.
        alias: Option<String>,
    },
    /// `aggregates[index]`, shown under its `AggregateExpr::alias`.
    Aggregate(usize),
}

/// The key a native grouped timeseries scan encodes the time bucket under.
pub const TIMESERIES_BUCKET_KEY: &str = "bucket";

impl TimeseriesAggOutput {
    /// The `(encoded key, output name)` of this column over `aggregates`.
    ///
    /// `None` for an `Aggregate` index past `aggregates`.
    pub fn key_and_name(&self, aggregates: &[AggregateExpr]) -> Option<(String, String)> {
        match self {
            Self::Bucket { alias, .. } => Some((
                TIMESERIES_BUCKET_KEY.to_string(),
                alias
                    .clone()
                    .unwrap_or_else(|| TIMESERIES_BUCKET_KEY.to_string()),
            )),
            Self::GroupColumn { name, alias } => {
                Some((name.clone(), alias.clone().unwrap_or_else(|| name.clone())))
            }
            Self::Aggregate(index) => aggregates.get(*index).map(|agg| {
                (
                    crate::planner::agg_naming::aggregate_output_key(agg),
                    agg.alias.clone(),
                )
            }),
        }
    }
}

/// Payload of [`SqlPlan::TimeseriesIngest`](crate::types::SqlPlan::TimeseriesIngest).
#[derive(Debug, Clone)]
pub struct TimeseriesIngestPlan {
    pub returning: Option<crate::types::ResolvedReturning>,
    pub collection: String,
    /// Defaults materialized and literals coerced, as in `Insert::rows`.
    /// A row that omits the `TIME_KEY` column carries its declared
    /// default here when one exists; only a row with no time value at
    /// all takes the ingest clock.
    pub rows: Vec<Vec<(String, SqlValue)>>,
    /// Mirrors `Insert::volatile_defaults`.
    pub volatile_defaults: bool,
}
