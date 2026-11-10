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
//! A sync delta (`Apply`, `ApplyAuthenticated`) passes the write-admission
//! gate with these same keys. A Calvin CRDT write holds its row key
//! `Exclusive` from its grant to its install, so no delta merges into the
//! row between the stage that predicts the merged row and the install that
//! stores it. The `RETURNING` image the stage predicts is the row the
//! install stores.

#![deny(clippy::wildcard_enum_match_arm)]

use nodedb_physical::physical_plan::CrdtOp;

use super::plan::not_a_write;
use super::set::WriteKeys;

/// Add the write keys of CRDT op `op` to `keys`.
pub(super) fn add_keys(keys: &mut WriteKeys, op: &CrdtOp) -> crate::Result<()> {
    match op {
        CrdtOp::Apply {
            collection,
            document_id,
            ..
        }
        | CrdtOp::ApplyAuthenticated {
            collection,
            document_id,
            ..
        }
        | CrdtOp::RestoreToVersion {
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

    /// A sync delta and a Calvin CRDT upsert of one document lock the same
    /// row key `Exclusive`. While the upsert holds its keys, the delta
    /// cannot take them.
    #[test]
    fn a_sync_delta_waits_for_a_calvin_write_of_its_row() {
        use crate::control::cluster::calvin::scheduler::lock_manager::{LockManager, TxnId};
        let collection = QualifiedCollection::new(DatabaseId::DEFAULT, "docs");
        let upsert = CrdtOp::DocUpsert {
            collection: collection.clone(),
            document_id: "t1".into(),
            fields: Vec::new(),
            surrogate: nodedb_types::Surrogate::new(7),
            partial: true,
            verb: nodedb_physical::physical_plan::CrdtWriteVerb::Insert,
            returning: None,
            rls_filters: Vec::new(),
        };
        let delta = CrdtOp::Apply {
            collection,
            document_id: "t1".into(),
            delta: Vec::new(),
            peer_id: 1,
            mutation_id: 1,
            surrogate: nodedb_types::Surrogate::new(7),
            provenance: None,
            constraint_version_required: 0,
            expected_frontier_digest: None,
        };
        let held: std::collections::BTreeMap<_, _> = locks(&upsert).into_iter().collect();
        let requested: std::collections::BTreeMap<_, _> = locks(&delta).into_iter().collect();
        let row = held
            .iter()
            .find(|(key, _)| **key != collection_key())
            .map(|(key, mode)| (key.clone(), *mode))
            .expect("a row key");
        assert_eq!(row.1, LockMode::Exclusive);
        assert_eq!(requested.get(&row.0), Some(&LockMode::Exclusive));

        let mut table = LockManager::new();
        assert!(table.try_acquire(TxnId::new(1, 0), held));
        assert!(
            !table.try_acquire(TxnId::new(TxnId::AUTOCOMMIT_EPOCH, 0), requested),
            "the delta must not merge into the row while the upsert holds it"
        );
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
