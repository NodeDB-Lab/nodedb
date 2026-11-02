// SPDX-License-Identifier: Apache-2.0

//! Continuous aggregate definition types.
//!
//! Shared between Origin and Lite. Origin uses these for SQL DDL parsing
//! and the continuous aggregate manager. Lite uses them for the embedded
//! continuous aggregate engine and its DDL handler.

use serde::{Deserialize, Serialize};

/// Definition of a continuous aggregate.
///
/// `Default` gives an empty definition whose `incarnation` is `Hlc::ZERO`.
/// No CREATE stamps that incarnation, and Origin never installs persisted
/// state into, or from, a definition that carries it.
#[derive(
    Debug,
    Clone,
    Default,
    PartialEq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
#[msgpack(map)]
pub struct ContinuousAggregateDef {
    /// Owning database. Scopes the Data-Plane manager's per-name maps and
    /// catalog keys so an aggregate named identically in two databases
    /// never collides or materializes against the wrong database's storage.
    pub database_id: u64,
    /// Name of this aggregate (e.g., "metrics_1m").
    pub name: String,
    /// Source collection or aggregate to read from.
    pub source: String,
    /// Time bucket interval string (e.g., "1m", "1h", "1d").
    pub bucket_interval: String,
    /// Bucket interval in milliseconds (computed from bucket_interval).
    pub bucket_interval_ms: i64,
    /// Columns to GROUP BY (tag/symbol columns).
    pub group_by: Vec<String>,
    /// Aggregate expressions to compute.
    pub aggregates: Vec<AggregateExpr>,
    /// When to refresh.
    pub refresh_policy: RefreshPolicy,
    /// Retention period in milliseconds (0 = infinite, independent of source).
    pub retention_period_ms: u64,
    /// Whether this aggregate is currently stale (schema change invalidation).
    pub stale: bool,
    /// Identity of the CREATE that made this definition. A re-created
    /// aggregate of the same name never loads the persisted buckets of a
    /// dropped one.
    pub incarnation: crate::Hlc,
}

/// An aggregate expression: function + source column → result column.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct AggregateExpr {
    /// Aggregate function.
    pub function: AggFunction,
    /// Source column name (e.g., "cpu"). "*" for COUNT.
    pub source_column: String,
    /// Output column name (e.g., "cpu_avg"). Auto-generated if empty.
    pub output_column: String,
}

/// Supported aggregate functions.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum AggFunction {
    #[serde(rename = "sum")]
    Sum,
    #[serde(rename = "count")]
    Count,
    #[serde(rename = "min")]
    Min,
    #[serde(rename = "max")]
    Max,
    #[serde(rename = "avg")]
    Avg,
    #[serde(rename = "first")]
    First,
    #[serde(rename = "last")]
    Last,
    /// Approximate count distinct via HyperLogLog.
    #[serde(rename = "count_distinct")]
    CountDistinct,
    /// Approximate percentile via TDigest. Inner value is the quantile (0.0–1.0).
    #[serde(rename = "percentile")]
    Percentile(f64),
    /// Approximate top-K heavy hitters via SpaceSaving. Inner value is K.
    #[serde(rename = "topk")]
    TopK(usize),
}

impl AggFunction {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Sum => "sum",
            Self::Count => "count",
            Self::Min => "min",
            Self::Max => "max",
            Self::Avg => "avg",
            Self::First => "first",
            Self::Last => "last",
            Self::CountDistinct => "count_distinct",
            Self::Percentile(_) => "percentile",
            Self::TopK(_) => "topk",
        }
    }

    /// Whether this function requires sketch state in PartialAggregate.
    pub fn uses_sketch(&self) -> bool {
        matches!(
            self,
            Self::CountDistinct | Self::Percentile(_) | Self::TopK(_)
        )
    }
}

/// When to refresh the aggregate.
#[derive(
    Debug,
    Clone,
    Default,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum RefreshPolicy {
    /// Refresh on every memtable flush. Lowest latency.
    #[default]
    #[serde(rename = "on_flush")]
    OnFlush,
    /// Refresh when a partition is sealed. Lower CPU cost.
    #[serde(rename = "on_seal")]
    OnSeal,
    /// Refresh every N milliseconds.
    #[serde(rename = "periodic")]
    Periodic(u64),
    /// Only refresh via explicit REFRESH command.
    #[serde(rename = "manual")]
    Manual,
}
