// SPDX-License-Identifier: BUSL-1.1

//! Which CRDT operations move a document's Loro frontier.

use nodedb_physical::physical_plan::CrdtOp;

/// Whether an operation changes the Loro frontier and must serialize with an
/// admission preview when it is dispatched to this node's cores without a
/// proposal.
pub fn changes_crdt_frontier(op: &CrdtOp) -> bool {
    match op {
        CrdtOp::Apply { .. }
        | CrdtOp::ApplyAuthenticated { .. }
        | CrdtOp::ImportSnapshot { .. }
        | CrdtOp::ListInsert { .. }
        | CrdtOp::ListDelete { .. }
        | CrdtOp::ListMove { .. }
        | CrdtOp::DocUpsert { .. }
        | CrdtOp::DocDelete { .. }
        | CrdtOp::RestoreToVersion { .. } => true,
        CrdtOp::Read { .. }
        | CrdtOp::PreviewApply { .. }
        | CrdtOp::GetVersionVector { .. }
        | CrdtOp::ExportDelta { .. }
        | CrdtOp::CompactAtVersion { .. }
        | CrdtOp::SetConstraints { .. }
        | CrdtOp::DropConstraints { .. }
        | CrdtOp::ReadConstraints { .. }
        | CrdtOp::SetPolicy { .. }
        | CrdtOp::GetPolicy { .. }
        | CrdtOp::ReadAtVersion { .. } => false,
    }
}

#[cfg(test)]
mod tests {
    use nodedb_types::{QualifiedCollection, Surrogate};

    use super::*;
    use crate::types::DatabaseId;

    fn collection() -> String {
        "docs".to_owned()
    }

    fn assert_frontier_mutation(op: CrdtOp) {
        assert!(changes_crdt_frontier(&op), "{op:?} must serialize");
    }

    fn assert_frontier_read(op: CrdtOp) {
        assert!(!changes_crdt_frontier(&op), "{op:?} must not serialize");
    }

    #[test]
    fn frontier_classifier_covers_every_crdt_operation_category() {
        let surrogate = Surrogate::new(1);
        assert_frontier_mutation(CrdtOp::Apply {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, &collection()),
            document_id: "id".into(),
            delta: Vec::new(),
            peer_id: 1,
            mutation_id: 1,
            surrogate,
            provenance: None,
            constraint_version_required: 0,
            expected_frontier_digest: None,
        });
        assert_frontier_mutation(CrdtOp::ImportSnapshot {
            tenant_id: 1,
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, &collection()),
            bytes: Vec::new(),
        });
        assert_frontier_mutation(CrdtOp::RestoreToVersion {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, &collection()),
            document_id: "id".into(),
            target_version_json: "{}".into(),
            surrogate,
        });
        assert_frontier_mutation(CrdtOp::ListInsert {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, &collection()),
            document_id: "id".into(),
            list_path: "blocks".into(),
            index: 0,
            fields: Vec::new(),
            surrogate,
        });
        assert_frontier_mutation(CrdtOp::ListDelete {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, &collection()),
            document_id: "id".into(),
            list_path: "blocks".into(),
            index: 0,
            surrogate,
        });
        assert_frontier_mutation(CrdtOp::ListMove {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, &collection()),
            document_id: "id".into(),
            list_path: "blocks".into(),
            from_index: 0,
            to_index: 1,
            surrogate,
        });
        assert_frontier_mutation(CrdtOp::DocUpsert {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, &collection()),
            document_id: "id".into(),
            fields: Vec::new(),
            surrogate,
            partial: false,
            verb: nodedb_physical::physical_plan::CrdtWriteVerb::Insert,
            returning: None,
            rls_filters: Vec::new(),
        });
        assert_frontier_mutation(CrdtOp::DocDelete {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, &collection()),
            document_id: "id".into(),
            surrogate: Some(surrogate),
            returning: None,
            rls_filters: Vec::new(),
        });

        assert_frontier_read(CrdtOp::Read {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, &collection()),
            document_id: "id".into(),
        });
        assert_frontier_read(CrdtOp::PreviewApply {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, &collection()),
            document_id: "id".into(),
            delta: Vec::new(),
        });
        assert_frontier_read(CrdtOp::GetVersionVector {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, &collection()),
        });
        assert_frontier_read(CrdtOp::ExportDelta {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, &collection()),
            from_version_json: "{}".into(),
        });
        assert_frontier_read(CrdtOp::CompactAtVersion {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, &collection()),
            target_version_json: "{}".into(),
        });
        assert_frontier_read(CrdtOp::SetConstraints {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, &collection()),
            constraint_version: 1,
            constraints: Vec::new(),
        });
        assert_frontier_read(CrdtOp::DropConstraints {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, &collection()),
            constraint_version: 1,
        });
        assert_frontier_read(CrdtOp::ReadConstraints {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, &collection()),
        });
        assert_frontier_read(CrdtOp::SetPolicy {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, &collection()),
            policy_json: "{}".into(),
        });
        assert_frontier_read(CrdtOp::GetPolicy {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, &collection()),
        });
        assert_frontier_read(CrdtOp::ReadAtVersion {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, &collection()),
            document_id: "id".into(),
            version_vector_json: "{}".into(),
        });
    }
}
