// SPDX-License-Identifier: BUSL-1.1

//! CRDT write keys.
//!
//! A CRDT document is keyed by its document id, bound or not: a delete
//! planned before a concurrent upsert bound the document locks the key the
//! upsert locks. A write of the whole collection locks the collection key.
//!
//! A constraint install or drop changes no row. It replaces the validator
//! rules a CRDT delta passes at its log apply, and each delta carries the
//! constraint version it was admitted against: the apply fences a delta whose
//! version the replica has not installed yet. So the install needs no
//! exclusion from row writers, and it takes its collection `Intent` like one.
//! It still orders against a writer of the whole collection.
//!
//! A sync delta (`Apply`, `ApplyAuthenticated`) is never sequenced: a Calvin
//! resolve refuses it. At the write-admission gate it takes its document's
//! id key (`unsequenced_keys`) and waits while a Calvin CRDT write holds it.
//! A Calvin CRDT write holds its row key `Exclusive` from its grant to its
//! install, so no delta merges into the row between the stage that predicts
//! the merged row and the install that stores it. The `RETURNING` image the
//! stage predicts is the row the install stores.

#![deny(clippy::wildcard_enum_match_arm)]

use nodedb_physical::physical_plan::CrdtOp;

use super::plan::{not_a_write, unsequenced};
use super::set::WriteKeys;

/// Add the write keys of CRDT op `op` to `keys`.
pub(super) fn add_keys(keys: &mut WriteKeys, op: &CrdtOp) -> crate::Result<()> {
    match op {
        CrdtOp::Apply { .. } | CrdtOp::ApplyAuthenticated { .. } => {
            return Err(unsequenced(
                "a CRDT sync delta applies through its own admission and answers its own \
                 verdict, outside any transaction",
            ));
        }
        CrdtOp::RestoreToVersion {
            collection,
            document_id,
            ..
        }
        | CrdtOp::ListInsert {
            collection,
            document_id,
            ..
        }
        | CrdtOp::ListDelete {
            collection,
            document_id,
            ..
        }
        | CrdtOp::ListMove {
            collection,
            document_id,
            ..
        }
        | CrdtOp::DocUpsert {
            collection,
            document_id,
            ..
        }
        | CrdtOp::DocDelete {
            collection,
            document_id,
            ..
        } => keys.row_id(collection.as_str(), document_id),
        CrdtOp::ImportSnapshot { collection, .. } => keys.whole_collection(collection.as_str()),
        CrdtOp::SetConstraints { collection, .. } | CrdtOp::DropConstraints { collection, .. } => {
            keys.rows(collection.as_str(), [])
        }
        CrdtOp::Read { .. }
        | CrdtOp::ReadConstraints { .. }
        | CrdtOp::SetPolicy { .. }
        | CrdtOp::GetPolicy { .. }
        | CrdtOp::ReadAtVersion { .. }
        | CrdtOp::GetVersionVector { .. }
        | CrdtOp::ExportDelta { .. }
        | CrdtOp::CompactAtVersion { .. }
        | CrdtOp::PreviewApply { .. } => {
            return Err(not_a_write("a CRDT read, policy or compaction op"));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use nodedb_types::{DatabaseId, QualifiedCollection};

    use super::*;
    use crate::control::cluster::calvin::scheduler::driver::helpers::expand_write_key_sets;
    use crate::control::cluster::calvin::scheduler::lock::{LockKey, LockMode};

    fn locks(op: &CrdtOp) -> Vec<(LockKey, LockMode)> {
        let mut keys = WriteKeys::default();
        add_keys(&mut keys, op).expect("a write");
        expand_write_key_sets(&keys.into_key_sets())
            .into_iter()
            .collect()
    }

    /// A sync delta is never sequenced: a Calvin resolve refuses it. The
    /// extraction refuses it by name, so no builder sequences one and a
    /// contended delta waits at the write gate instead of routing.
    #[test]
    fn a_sync_delta_is_refused_as_unsequenced() {
        let delta = CrdtOp::Apply {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "docs"),
            document_id: "t1".into(),
            delta: Vec::new(),
            peer_id: 1,
            mutation_id: 1,
            surrogate: nodedb_types::Surrogate::new(7),
            provenance: None,
            constraint_version_required: 0,
            expected_frontier_digest: None,
        };
        let mut keys = WriteKeys::default();
        assert!(matches!(
            add_keys(&mut keys, &delta),
            Err(crate::Error::BadRequest { .. })
        ));
        assert!(keys.into_key_sets().is_empty());
    }

    fn collection_key() -> LockKey {
        LockKey::Collection {
            collection: std::sync::Arc::from("docs"),
        }
    }

    /// A constraint install takes its collection `Intent` alone, so it runs
    /// beside a row writer of the collection.
    #[test]
    fn a_constraint_install_takes_its_collection_intent() {
        let collection = QualifiedCollection::new(DatabaseId::DEFAULT, "docs");
        let set = CrdtOp::SetConstraints {
            collection: collection.clone(),
            constraint_version: 1,
            constraints: Vec::new(),
        };
        let drop = CrdtOp::DropConstraints {
            collection,
            constraint_version: 2,
        };
        for op in [set, drop] {
            assert_eq!(locks(&op), vec![(collection_key(), LockMode::Intent)]);
        }
    }
}
