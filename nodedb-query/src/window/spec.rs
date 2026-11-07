// SPDX-License-Identifier: Apache-2.0

//! Window function spec and frame types serialized over the SPSC bridge.

use crate::expr::types::SqlExpr;

/// Every function the window evaluators compute. The planner refuses any
/// other name in `<func>() OVER (...)`.
pub const WINDOW_FUNCTION_NAMES: [&str; 16] = [
    "row_number",
    "rank",
    "dense_rank",
    "ntile",
    "percent_rank",
    "cume_dist",
    "lag",
    "lead",
    "nth_value",
    "sum",
    "count",
    "avg",
    "min",
    "max",
    "first_value",
    "last_value",
];

/// A window function specification.
#[derive(
    Debug,
    Clone,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct WindowFuncSpec {
    /// Output column name (e.g., "row_num", "running_sum").
    pub alias: String,
    /// Function name: row_number, rank, dense_rank, ntile, percent_rank,
    /// cume_dist, lag, lead, nth_value, sum, count, avg, min, max,
    /// first_value, last_value.
    pub func_name: String,
    /// Function arguments (e.g., `salary` for SUM(salary)). Empty for ROW_NUMBER.
    pub args: Vec<SqlExpr>,
    /// PARTITION BY expressions. Empty = single partition (entire result set).
    pub partition_by: Vec<SqlExpr>,
    /// ORDER BY within each partition: [(expr, ascending)].
    pub order_by: Vec<(SqlExpr, bool)>,
    /// Window frame specification.
    pub frame: WindowFrame,
}

/// Window frame: defines which rows within the partition are visible to the function.
#[derive(
    Debug,
    Clone,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct WindowFrame {
    /// Frame mode: "rows" or "range".
    pub mode: String,
    /// Start bound.
    pub start: FrameBound,
    /// End bound.
    pub end: FrameBound,
}

impl Default for WindowFrame {
    fn default() -> Self {
        Self {
            mode: "range".into(),
            start: FrameBound::UnboundedPreceding,
            end: FrameBound::CurrentRow,
        }
    }
}

/// Window frame boundary.
#[derive(
    Debug,
    Clone,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub enum FrameBound {
    UnboundedPreceding,
    Preceding(u64),
    CurrentRow,
    Following(u64),
    UnboundedFollowing,
}
