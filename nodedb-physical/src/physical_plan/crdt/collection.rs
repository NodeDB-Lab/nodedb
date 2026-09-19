// SPDX-License-Identifier: Apache-2.0

//! Which user collection a CRDT operation targets.
//!
//! Kept beside the operation enum rather than inside it so `op.rs` stays the
//! single declaration of the wire shape and nothing else.

use nodedb_types::QualifiedCollection;

use super::op::CrdtOp;

impl CrdtOp {
    /// The user collection this op targets.
    ///
    /// Infallible by construction: every `CrdtOp` variant carries exactly one
    /// `collection`, because a CRDT op is always scoped to one collection's
    /// Loro document — history reads, constraint installs, snapshot imports
    /// and block-list edits included. The match is deliberately exhaustive
    /// with no wildcard arm, so a new variant is a compile error here rather
    /// than a silently unscoped op: callers use this to decide RLS injection,
    /// redaction, clone-read/write interception, read-set tracking and
    /// metering, all of which are skipped when a collection cannot be named.
    pub fn collection(&self) -> &QualifiedCollection {
        match self {
            CrdtOp::Read { collection, .. }
            | CrdtOp::Apply { collection, .. }
            | CrdtOp::ApplyAuthenticated { collection, .. }
            | CrdtOp::ImportSnapshot { collection, .. }
            | CrdtOp::SetConstraints { collection, .. }
            | CrdtOp::DropConstraints { collection, .. }
            | CrdtOp::ReadConstraints { collection }
            | CrdtOp::SetPolicy { collection, .. }
            | CrdtOp::GetPolicy { collection }
            | CrdtOp::ReadAtVersion { collection, .. }
            | CrdtOp::GetVersionVector { collection }
            | CrdtOp::ExportDelta { collection, .. }
            | CrdtOp::RestoreToVersion { collection, .. }
            | CrdtOp::CompactAtVersion { collection, .. }
            | CrdtOp::ListInsert { collection, .. }
            | CrdtOp::ListDelete { collection, .. }
            | CrdtOp::ListMove { collection, .. }
            | CrdtOp::DocUpsert { collection, .. }
            | CrdtOp::DocDelete { collection, .. }
            | CrdtOp::PreviewApply { collection, .. } => collection,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_types::{DatabaseId, Surrogate, sync::wire::SyncProvenance};

    fn coll(name: &str) -> QualifiedCollection {
        QualifiedCollection::new(DatabaseId::DEFAULT, name)
    }

    fn provenance() -> SyncProvenance {
        SyncProvenance {
            producer_id: 1,
            epoch: 1,
            stream_id: 1,
            seq: 1,
        }
    }

    /// Every variant reports the collection it was built with.
    ///
    /// The point of this test is that it stops compiling when a 21st
    /// `CrdtOp` variant is added: the author must build it here and thereby
    /// decide what `collection()` returns for it, instead of the op silently
    /// losing RLS, redaction, clone and metering scoping at the Control Plane.
    #[test]
    fn collection_is_total_over_every_variant() {
        let ops = vec![
            CrdtOp::Read {
                collection: coll("read"),
                document_id: "d".to_string(),
            },
            CrdtOp::Apply {
                collection: coll("apply"),
                document_id: "d".to_string(),
                delta: Vec::new(),
                peer_id: 1,
                mutation_id: 1,
                surrogate: Surrogate::ZERO,
                provenance: None,
                constraint_version_required: 0,
                expected_frontier_digest: None,
            },
            CrdtOp::ApplyAuthenticated {
                collection: coll("apply_authenticated"),
                document_id: "d".to_string(),
                delta: Vec::new(),
                peer_id: 1,
                mutation_id: 1,
                surrogate: Surrogate::ZERO,
                provenance: provenance(),
                constraint_version_required: 0,
                expected_frontier_digest: None,
                auth_user_id: 1,
                auth_device_id: 1,
                auth_seq_no: 1,
                delta_signature: [0u8; 32],
                signing_required: false,
            },
            CrdtOp::ImportSnapshot {
                tenant_id: 1,
                collection: coll("import_snapshot"),
                bytes: Vec::new(),
            },
            CrdtOp::SetConstraints {
                collection: coll("set_constraints"),
                constraint_version: 1,
                constraints: Vec::new(),
            },
            CrdtOp::DropConstraints {
                collection: coll("drop_constraints"),
                constraint_version: 1,
            },
            CrdtOp::ReadConstraints {
                collection: coll("read_constraints"),
            },
            CrdtOp::SetPolicy {
                collection: coll("set_policy"),
                policy_json: "{}".to_string(),
            },
            CrdtOp::GetPolicy {
                collection: coll("get_policy"),
            },
            CrdtOp::ReadAtVersion {
                collection: coll("read_at_version"),
                document_id: "d".to_string(),
                version_vector_json: "{}".to_string(),
            },
            CrdtOp::GetVersionVector {
                collection: coll("get_version_vector"),
            },
            CrdtOp::ExportDelta {
                collection: coll("export_delta"),
                from_version_json: "{}".to_string(),
            },
            CrdtOp::RestoreToVersion {
                collection: coll("restore_to_version"),
                document_id: "d".to_string(),
                target_version_json: "{}".to_string(),
                surrogate: Surrogate::ZERO,
            },
            CrdtOp::CompactAtVersion {
                collection: coll("compact_at_version"),
                target_version_json: "{}".to_string(),
            },
            CrdtOp::ListInsert {
                collection: coll("list_insert"),
                document_id: "d".to_string(),
                list_path: "blocks".to_string(),
                index: 0,
                fields_json: "{}".to_string(),
                surrogate: Surrogate::ZERO,
            },
            CrdtOp::ListDelete {
                collection: coll("list_delete"),
                document_id: "d".to_string(),
                list_path: "blocks".to_string(),
                index: 0,
                surrogate: Surrogate::ZERO,
            },
            CrdtOp::ListMove {
                collection: coll("list_move"),
                document_id: "d".to_string(),
                list_path: "blocks".to_string(),
                from_index: 0,
                to_index: 1,
                surrogate: Surrogate::ZERO,
            },
            CrdtOp::DocUpsert {
                collection: coll("doc_upsert"),
                document_id: "d".to_string(),
                fields_json: "{}".to_string(),
                surrogate: Surrogate::ZERO,
                partial: false,
                verb: crate::physical_plan::CrdtWriteVerb::Insert,
                returning: None,
                rls_filters: Vec::new(),
            },
            CrdtOp::DocDelete {
                collection: coll("doc_delete"),
                document_id: "d".to_string(),
                surrogate: Surrogate::ZERO,
                returning: None,
                rls_filters: Vec::new(),
            },
            CrdtOp::PreviewApply {
                collection: coll("preview_apply"),
                document_id: "d".to_string(),
                delta: Vec::new(),
            },
        ];

        assert_eq!(ops.len(), 20, "every CrdtOp variant must be covered here");

        let expected = [
            "read",
            "apply",
            "apply_authenticated",
            "import_snapshot",
            "set_constraints",
            "drop_constraints",
            "read_constraints",
            "set_policy",
            "get_policy",
            "read_at_version",
            "get_version_vector",
            "export_delta",
            "restore_to_version",
            "compact_at_version",
            "list_insert",
            "list_delete",
            "list_move",
            "doc_upsert",
            "doc_delete",
            "preview_apply",
        ];

        for (op, name) in ops.iter().zip(expected) {
            assert_eq!(op.collection().as_str(), name);
        }
    }
}
