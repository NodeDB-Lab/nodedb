// SPDX-License-Identifier: Apache-2.0

//! Timeseries plan payloads: scan and ingest.

use crate::temporal::TemporalScope;
use crate::types::filter::Filter;
use crate::types::query::{AggregateExpr, Projection, SortKey};
use crate::types_expr::SqlValue;

/// Payload of [`SqlPlan::TimeseriesScan`](crate::types::SqlPlan::TimeseriesScan).
#[derive(Debug, Clone)]
pub struct TimeseriesScanPlan {
    pub collection: String,
    pub time_range: (i64, i64),
    pub bucket_interval_ms: i64,
    pub group_by: Vec<String>,
    pub aggregates: Vec<AggregateExpr>,
    pub filters: Vec<Filter>,
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
}

/// Payload of [`SqlPlan::TimeseriesIngest`](crate::types::SqlPlan::TimeseriesIngest).
#[derive(Debug, Clone)]
pub struct TimeseriesIngestPlan {
    pub collection: String,
    /// Defaults materialized and literals coerced, as in `Insert::rows`.
    /// A row that omits the `TIME_KEY` column carries its declared
    /// default here when one exists; only a row with no time value at
    /// all takes the ingest clock.
    pub rows: Vec<Vec<(String, SqlValue)>>,
    /// Mirrors `Insert::volatile_defaults`.
    pub volatile_defaults: bool,
}
