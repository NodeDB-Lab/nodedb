// SPDX-License-Identifier: BUSL-1.1

//! `DocumentOp` classification.

use nodedb_physical::physical_plan::DocumentOp;

use super::kind::PlanKind;

pub(super) fn describe_document(op: &DocumentOp) -> PlanKind {
    match op {
        DocumentOp::PointGet { .. } => PlanKind::SingleDocument,

        DocumentOp::RangeScan { .. }
        | DocumentOp::Scan { .. }
        | DocumentOp::IndexedFetch { .. } => PlanKind::MultiRow,

        // A write with a projection returns real stored rows and must be
        // decoded and redacted, never passed through unshaped.
        DocumentOp::PointPut {
            returning: Some(_), ..
        }
        | DocumentOp::PointInsert {
            returning: Some(_), ..
        }
        | DocumentOp::BatchInsert {
            returning: Some(_), ..
        }
        | DocumentOp::PointUpdate {
            returning: Some(_), ..
        }
        | DocumentOp::BulkUpdate {
            returning: Some(_), ..
        }
        | DocumentOp::PointDelete {
            returning: Some(_), ..
        }
        | DocumentOp::BulkDelete {
            returning: Some(_), ..
        }
        | DocumentOp::UpdateFromJoin {
            returning: Some(_), ..
        }
        | DocumentOp::Merge {
            returning: Some(_), ..
        }
        | DocumentOp::Upsert {
            returning: Some(_), ..
        } => PlanKind::ReturningRows,

        // `PointInsert`: `ON CONFLICT DO NOTHING` makes it no-op-capable, so
        // the count must come from the write's response.
        DocumentOp::PointPut { .. }
        | DocumentOp::PointInsert { .. }
        | DocumentOp::BatchInsert { .. }
        | DocumentOp::InsertSelect { .. } => PlanKind::DmlResult("INSERT"),

        DocumentOp::PointUpdate { .. }
        | DocumentOp::BulkUpdate { .. }
        | DocumentOp::UpdateFromJoin { .. } => PlanKind::DmlResult("UPDATE"),

        DocumentOp::PointDelete { .. } | DocumentOp::BulkDelete { .. } => {
            PlanKind::DmlResult("DELETE")
        }

        // Postgres tags a plain MERGE `MERGE <rows-affected>`, matching the staged path.
        DocumentOp::Merge { .. } => PlanKind::DmlResult("MERGE"),

        DocumentOp::Truncate { .. } => PlanKind::DmlResult("TRUNCATE"),

        DocumentOp::Upsert { .. } => PlanKind::DmlResult("UPSERT"),

        // Index DDL and catalog maintenance: no row payload, no row count.
        DocumentOp::Register { .. }
        | DocumentOp::IndexLookup { .. }
        | DocumentOp::DropIndex { .. }
        | DocumentOp::BackfillIndex { .. }
        | DocumentOp::EstimateCount { .. }
        // Clone materializer payload (`[cursor, entries]`), decoded by its caller.
        | DocumentOp::MaterializeScan { .. }
        // Read-only resolve: payload is the internal classification tuple, never a client row.
        | DocumentOp::ResolveWrite(_)
        // A derived balance write answers no client — reports an affected count only.
        | DocumentOp::ApplyBalanceDelta { .. }
        // Never reaches this classifier: write-resolve returns the response itself,
        // shaped from the intercepted plan whose `returning` slot decides.
        | DocumentOp::ResolvedWrite { .. } => PlanKind::Execution,
    }
}
