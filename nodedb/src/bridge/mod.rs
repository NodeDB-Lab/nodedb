// SPDX-License-Identifier: BUSL-1.1

pub mod admission_chokepoint;
pub mod dispatch;
pub mod envelope;
pub mod quiesce;
pub mod runtime_health;
pub mod slab;

// Re-export shared query engine from nodedb-query crate.
// Origin's internal code continues to use `crate::bridge::expr_eval`,
// `crate::bridge::json_ops`, `crate::bridge::scan_filter`, and
// `crate::bridge::window_func` — they now resolve to nodedb-query.
pub mod expr_eval {
    pub use nodedb_query::expr::{BinaryOp, CastType, ComputedColumn, SqlExpr};
}
pub mod scan_filter {
    pub use nodedb_query::scan_filter::*;
}
pub mod window_func {
    pub use nodedb_query::window::*;
}

pub use admission_chokepoint::writes_bypassed_admission_gate;
pub use dispatch::Dispatcher;
pub use envelope::{Request, Response, Status};
