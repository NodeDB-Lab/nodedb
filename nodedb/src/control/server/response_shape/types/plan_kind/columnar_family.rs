// SPDX-License-Identifier: BUSL-1.1

//! `ColumnarOp`, `TimeseriesOp` and `SpatialOp` classification — the three
//! peer engines on the compressed-column storage core.

use nodedb_physical::physical_plan::{ColumnarOp, SpatialOp, TimeseriesOp};

use super::kind::PlanKind;

pub(super) fn describe_columnar(op: &ColumnarOp) -> PlanKind {
    match op {
        ColumnarOp::Scan { .. } => PlanKind::MultiRow,

        ColumnarOp::Insert {
            returning: Some(_), ..
        } => PlanKind::ReturningRows,

        // Reports `{"accepted": n}`.
        ColumnarOp::Insert { .. } => PlanKind::DmlResult("INSERT"),

        // Reports `{"affected": n}`.
        ColumnarOp::Update { .. } => PlanKind::DmlResult("UPDATE"),
        ColumnarOp::Delete { .. } => PlanKind::DmlResult("DELETE"),

        // Never reach this classifier: write-resolve proposes them and returns
        // the response itself, shaped from the intercepted `Update` / `Delete`.
        ColumnarOp::ResolvedUpdate { .. }
        | ColumnarOp::ResolvedDelete { .. }
        // Read-only resolve: payload is the internal row set, never a client row.
        | ColumnarOp::ResolveDml { .. }
        // Clone materializer payload (`[cursor, entries]`), decoded by its caller.
        | ColumnarOp::MaterializeScan { .. } => PlanKind::Execution,
    }
}

pub(super) fn describe_timeseries(op: &TimeseriesOp) -> PlanKind {
    match op {
        TimeseriesOp::Scan { .. } => PlanKind::MultiRow,

        TimeseriesOp::Ingest {
            returning: Some(_), ..
        } => PlanKind::ReturningRows,

        // Reports `{"accepted": n}`.
        TimeseriesOp::Ingest { .. } => PlanKind::DmlResult("INSERT"),

        // Read-only resolve: payload is the internal admission verdict, never a client row.
        TimeseriesOp::ResolveIngest(_) => PlanKind::Execution,
    }
}

pub(super) fn describe_spatial(op: &SpatialOp) -> PlanKind {
    match op {
        SpatialOp::Scan { .. } => PlanKind::MultiRow,

        // Replication-apply plans (sync inbound, WAL dispatch, Raft apply).
        // A client statement never lowers to them, so they answer no client.
        SpatialOp::Insert { .. } | SpatialOp::Delete { .. } => PlanKind::Execution,
    }
}
