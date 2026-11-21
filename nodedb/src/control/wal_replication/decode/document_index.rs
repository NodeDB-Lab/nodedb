// SPDX-License-Identifier: BUSL-1.1

//! Decode the document secondary-index variants back to their plans.
//!
//! `DocIndexBuild` becomes `DocumentOp::BackfillIndex` and `DocIndexRetire`
//! becomes `DocumentOp::DropIndex`. Each carries every field of its plan, so
//! a replica runs the build or purge the proposer planned.

use crate::bridge::envelope::PhysicalPlan;
use crate::types::DatabaseId;
use nodedb_physical::physical_plan::{DocIndexFence, DocIndexOwner, DocumentOp};
use nodedb_types::QualifiedCollection;

/// The fields of a `DocIndexBuild` record.
pub(super) struct IndexBuildFields<'a> {
    pub collection: &'a str,
    pub path: &'a str,
    pub is_array: bool,
    pub unique: bool,
    pub case_insensitive: bool,
    pub predicate: Option<&'a str>,
    pub owner: &'a DocIndexOwner,
    pub fence: DocIndexFence,
}

/// Rebuild a `BackfillIndex` plan. The replica scans the rows it holds at
/// the entry's log position.
pub(super) fn index_build(
    database_id: DatabaseId,
    fields: IndexBuildFields<'_>,
) -> crate::Result<PhysicalPlan> {
    let IndexBuildFields {
        collection,
        path,
        is_array,
        unique,
        case_insensitive,
        predicate,
        owner,
        fence,
    } = fields;
    Ok(PhysicalPlan::Document(DocumentOp::BackfillIndex {
        collection: QualifiedCollection::from_stored(database_id, collection.to_owned())?,
        path: path.to_owned(),
        is_array,
        unique,
        case_insensitive,
        predicate: predicate.map(str::to_owned),
        owner: owner.clone(),
        fence,
    }))
}

/// Rebuild a `DropIndex` plan. The replica removes `owner` from `field`.
pub(super) fn index_retire(
    database_id: DatabaseId,
    collection: &str,
    field: &str,
    owner: &DocIndexOwner,
    fence: DocIndexFence,
) -> crate::Result<PhysicalPlan> {
    Ok(PhysicalPlan::Document(DocumentOp::DropIndex {
        collection: QualifiedCollection::from_stored(database_id, collection.to_owned())?,
        field: field.to_owned(),
        owner: owner.clone(),
        fence,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::wal_replication::decode;
    use crate::control::wal_replication::types::{ReplicatedEntry, ReplicatedWrite};
    use crate::types::{TenantId, VShardId};

    /// Decide and encode in one call, so each test names only its plan.
    fn encode(plan: &PhysicalPlan) -> ReplicatedEntry {
        let write = crate::control::wal_replication::ReplicableWrite::decide_for_replication(plan)
            .expect("decide");
        crate::control::wal_replication::encode::to_replicated_entry(
            TenantId::new(1),
            DatabaseId::DEFAULT,
            VShardId::new(0),
            &write,
        )
        .expect("encode must not error")
        .expect("index plan must produce a ReplicatedEntry")
    }

    /// The fence every test plan carries.
    fn fence() -> DocIndexFence {
        DocIndexFence {
            incarnation: nodedb_types::Hlc::new(7, 1),
            descriptor_version: 9,
        }
    }

    fn decode_plan(entry: &ReplicatedEntry) -> PhysicalPlan {
        let (_, _, plan, _) = decode::decode_replicated_entry(&entry.to_bytes())
            .expect("decode_replicated_entry error")
            .expect("decode_replicated_entry returned None");
        plan
    }

    /// Every field of the build plan survives the wire, since a replica
    /// cannot infer the UNIQUE check, the case fold or the predicate.
    #[test]
    fn index_build_roundtrip() {
        let plan = PhysicalPlan::Document(DocumentOp::BackfillIndex {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "users"),
            path: "$.tags".into(),
            is_array: true,
            unique: true,
            case_insensitive: true,
            predicate: Some("active = true".into()),
            owner: DocIndexOwner::Named("by_tags".into()),
            fence: fence(),
        });
        let entry = encode(&plan);
        assert!(
            matches!(entry.write, ReplicatedWrite::DocIndexBuild { .. }),
            "BackfillIndex encodes to DocIndexBuild, got {:?}",
            entry.write
        );
        match decode_plan(&entry) {
            PhysicalPlan::Document(DocumentOp::BackfillIndex {
                collection,
                path,
                is_array,
                unique,
                case_insensitive,
                predicate,
                owner,
                fence: decoded,
            }) => {
                assert_eq!(decoded, fence());
                assert_eq!(collection.as_str(), "users");
                assert_eq!(path, "$.tags");
                assert!(is_array);
                assert!(unique);
                assert!(case_insensitive);
                assert_eq!(predicate.as_deref(), Some("active = true"));
                assert_eq!(owner, DocIndexOwner::Named("by_tags".into()));
            }
            other => panic!("expected Document(BackfillIndex), got {other:?}"),
        }
    }

    /// A full index carries no predicate, and none appears on decode.
    #[test]
    fn index_build_without_predicate_roundtrip() {
        let plan = PhysicalPlan::Document(DocumentOp::BackfillIndex {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "users"),
            path: "$.email".into(),
            is_array: false,
            unique: false,
            case_insensitive: false,
            predicate: None,
            owner: DocIndexOwner::Declared,
            fence: fence(),
        });
        match decode_plan(&encode(&plan)) {
            PhysicalPlan::Document(DocumentOp::BackfillIndex {
                path,
                is_array,
                unique,
                case_insensitive,
                predicate,
                ..
            }) => {
                assert_eq!(path, "$.email");
                assert!(!is_array && !unique && !case_insensitive);
                assert!(predicate.is_none());
            }
            other => panic!("expected Document(BackfillIndex), got {other:?}"),
        }
    }

    #[test]
    fn index_retire_roundtrip() {
        let plan = PhysicalPlan::Document(DocumentOp::DropIndex {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "users"),
            field: "$.email".into(),
            owner: DocIndexOwner::Named("by_email".into()),
            fence: fence(),
        });
        let entry = encode(&plan);
        assert!(
            matches!(entry.write, ReplicatedWrite::DocIndexRetire { .. }),
            "DropIndex encodes to DocIndexRetire, got {:?}",
            entry.write
        );
        match decode_plan(&entry) {
            PhysicalPlan::Document(DocumentOp::DropIndex {
                collection,
                field,
                owner,
                fence: decoded,
            }) => {
                assert_eq!(decoded, fence());
                assert_eq!(collection.as_str(), "users");
                assert_eq!(field, "$.email");
                assert_eq!(owner, DocIndexOwner::Named("by_email".into()));
            }
            other => panic!("expected Document(DropIndex), got {other:?}"),
        }
    }
}
