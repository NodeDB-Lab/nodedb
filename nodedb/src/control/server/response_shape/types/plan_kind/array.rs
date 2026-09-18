// SPDX-License-Identifier: BUSL-1.1

//! `ArrayOp` classification.

use nodedb_physical::physical_plan::ArrayOp;

use super::kind::PlanKind;

pub(super) fn describe_array(op: &ArrayOp) -> PlanKind {
    match op {
        ArrayOp::Slice { .. } => PlanKind::ArraySlice,

        // JSON-array payloads: each row streams as its own pgwire field.
        ArrayOp::Project { .. } | ArrayOp::Aggregate { .. } | ArrayOp::Elementwise { .. } => {
            PlanKind::MultiRow
        }

        // Reports `{"inserted": n}` / `{"deleted": n}`.
        ArrayOp::Put { .. } => PlanKind::DmlResult("INSERT"),
        ArrayOp::Delete { .. } => PlanKind::DmlResult("DELETE"),

        // Flush/Compact return status JSON — route SingleDocument.
        ArrayOp::Flush { .. } | ArrayOp::Compact { .. } => PlanKind::SingleDocument,

        // Array DDL: `{"opened": 1}` / `{"dropped": 1}` status, not a row count.
        ArrayOp::OpenArray { .. }
        | ArrayOp::DropArray { .. }
        | ArrayOp::RestoreArrayDrop { .. }
        | ArrayOp::PurgeArrayDrop { .. }
        // Internal roaring bitmap for cross-engine prefilter, never a client row.
        | ArrayOp::SurrogateBitmapScan { .. } => PlanKind::Execution,
    }
}
