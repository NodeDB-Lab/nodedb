//! Continuous aggregate definition types.

use serde::{Deserialize, Serialize};

/// Definition of a continuous aggregate.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContinuousAggregateDef {
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
}

/// An aggregate expression: function + source column → result column.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AggregateExpr {
    /// Aggregate function.
    pub function: AggFunction,
    /// Source column name (e.g., "cpu"). "*" for COUNT.
    pub source_column: String,
    /// Output column name (e.g., "cpu_avg"). Auto-generated if empty.
    pub output_column: String,
}

/// Supported aggregate functions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AggFunction {
    Sum,
    Count,
    Min,
    Max,
    Avg,
    First,
    Last,
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
        }
    }
}

/// When to refresh the aggregate.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub enum RefreshPolicy {
    /// Refresh on every memtable flush. Lowest latency.
    #[default]
    OnFlush,
    /// Refresh when a partition is sealed. Lower CPU cost.
    OnSeal,
    /// Refresh every N milliseconds.
    Periodic(u64),
    /// Only refresh via explicit REFRESH command.
    Manual,
}
