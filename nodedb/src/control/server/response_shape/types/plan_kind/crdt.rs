// SPDX-License-Identifier: BUSL-1.1

//! `CrdtOp` classification.

use nodedb_physical::physical_plan::CrdtOp;

use super::kind::PlanKind;

pub(super) fn describe_crdt(op: &CrdtOp) -> PlanKind {
    match op {
        CrdtOp::DocUpsert {
            returning: Some(_), ..
        }
        | CrdtOp::DocDelete {
            returning: Some(_), ..
        } => PlanKind::ReturningRows,

        // INSERT, UPSERT and UPDATE all lower to `DocUpsert`; the verb the
        // statement used decides the tag.
        CrdtOp::DocUpsert { verb, .. } => PlanKind::DmlResult(verb.command_tag()),

        // A CRDT delete can legitimately remove nothing, so its count must render
        // as a DML count from the write's own response, not a document-shaped read.
        CrdtOp::DocDelete { .. } => PlanKind::DmlResult("DELETE"),

        // One document body, or one policy object.
        CrdtOp::Read { .. } | CrdtOp::ReadAtVersion { .. } | CrdtOp::GetPolicy { .. } => {
            PlanKind::SingleDocument
        }

        // Delta application and snapshot import: sync/replication writes with
        // no row count.
        CrdtOp::Apply { .. }
        | CrdtOp::ApplyAuthenticated { .. }
        | CrdtOp::ImportSnapshot { .. }
        // Constraint and policy DDL.
        | CrdtOp::SetConstraints { .. }
        | CrdtOp::DropConstraints { .. }
        | CrdtOp::SetPolicy { .. }
        // History maintenance.
        | CrdtOp::RestoreToVersion { .. }
        | CrdtOp::CompactAtVersion { .. }
        // Block-list edits: the handler reports no count.
        | CrdtOp::ListInsert { .. }
        | CrdtOp::ListDelete { .. }
        | CrdtOp::ListMove { .. }
        // Internal typed zerompk payloads, decoded by their own dispatcher:
        // the installed constraint set, a version vector, a Loro delta, and
        // the admission caller's `CrdtPreviewResult`.
        | CrdtOp::ReadConstraints { .. }
        | CrdtOp::GetVersionVector { .. }
        | CrdtOp::ExportDelta { .. }
        | CrdtOp::PreviewApply { .. } => PlanKind::Execution,
    }
}
