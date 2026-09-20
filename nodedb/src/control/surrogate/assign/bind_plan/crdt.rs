// SPDX-License-Identifier: BUSL-1.1

//! CRDT ops: every document-level op keys its row by `document_id.as_bytes()`.

use nodedb_physical::physical_plan::CrdtOp;

use super::binder::IdentityBinder;

pub(super) fn bind(binder: &IdentityBinder<'_>, op: &mut CrdtOp) -> crate::Result<()> {
    match op {
        // A delta apply creates the document on first sight, so it is the one
        // op that may still allocate for a pre-surrogate entry.
        CrdtOp::Apply {
            collection,
            document_id,
            surrogate,
            ..
        }
        | CrdtOp::ApplyAuthenticated {
            collection,
            document_id,
            surrogate,
            ..
        } => binder.resolve_or_assign_in_place(collection.as_str(), document_id, surrogate),
        CrdtOp::DocUpsert {
            collection,
            document_id,
            surrogate,
            ..
        }
        | CrdtOp::DocDelete {
            collection,
            document_id,
            surrogate,
            ..
        }
        | CrdtOp::RestoreToVersion {
            collection,
            document_id,
            surrogate,
            ..
        }
        | CrdtOp::ListInsert {
            collection,
            document_id,
            surrogate,
            ..
        }
        | CrdtOp::ListDelete {
            collection,
            document_id,
            surrogate,
            ..
        }
        | CrdtOp::ListMove {
            collection,
            document_id,
            surrogate,
            ..
        } => binder.resolve_in_place(collection.as_str(), document_id.as_bytes(), surrogate),
        // Reads, snapshots, constraints, policies and previews create no row.
        CrdtOp::Read { .. }
        | CrdtOp::ImportSnapshot { .. }
        | CrdtOp::SetConstraints { .. }
        | CrdtOp::DropConstraints { .. }
        | CrdtOp::ReadConstraints { .. }
        | CrdtOp::SetPolicy { .. }
        | CrdtOp::GetPolicy { .. }
        | CrdtOp::ReadAtVersion { .. }
        | CrdtOp::GetVersionVector { .. }
        | CrdtOp::ExportDelta { .. }
        | CrdtOp::CompactAtVersion { .. }
        | CrdtOp::PreviewApply { .. } => Ok(()),
    }
}
