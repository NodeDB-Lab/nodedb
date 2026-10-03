// SPDX-License-Identifier: BUSL-1.1

//! Snapshot export and import for a tenant's per-collection CRDT state.

use super::core::TenantCrdtEngine;

impl TenantCrdtEngine {
    /// Export one collection's CRDT state as binary snapshot bytes.
    ///
    /// Returns `None` when the collection has no local state.
    pub fn export_snapshot_bytes(&self, collection: &str) -> crate::Result<Option<Vec<u8>>> {
        match self.collections.get(collection) {
            Some(state) => state
                .export_snapshot()
                .map(Some)
                .map_err(crate::Error::Crdt),
            None => Ok(None),
        }
    }

    /// Export every collection's snapshot as `(collection, bytes)` pairs.
    pub fn export_all_snapshots(&self) -> crate::Result<Vec<(String, Vec<u8>)>> {
        let mut out = Vec::with_capacity(self.collections.len());
        for (collection, state) in &self.collections {
            let bytes = state.export_snapshot().map_err(crate::Error::Crdt)?;
            out.push((collection.clone(), bytes));
        }
        Ok(out)
    }

    /// Read a document's CRDT state, returning the raw snapshot bytes for the
    /// document's collection. `None` when the collection or row is absent.
    pub fn read_snapshot(&self, collection: &str, row_id: &str) -> crate::Result<Option<Vec<u8>>> {
        match self.collections.get(collection) {
            Some(state) if state.row_exists(collection, row_id) => {
                Ok(Some(state.export_snapshot().map_err(crate::Error::Crdt)?))
            }
            _ => Ok(None),
        }
    }

    /// Import a full CRDT snapshot for a single collection (snapshot restore).
    ///
    /// Routes through the validated apply path so a restored snapshot is held
    /// to the same constraints as any peer delta: a violating row is rejected
    /// rather than silently trusted. Fails when the blob's operations cannot be
    /// fully applied — a restore that left operations causally pending has NOT
    /// restored the collection, and reporting success would leave the caller
    /// unable to tell a complete restore from a partial one.
    pub fn import_snapshot_bytes(&mut self, collection: &str, bytes: &[u8]) -> crate::Result<()> {
        match self.apply_committed_delta_validated(
            collection,
            bytes,
            super::ApplyTarget::Collection,
            0,
        ) {
            super::ValidatedApplyOutcome::Clean { .. } => Ok(()),
            super::ValidatedApplyOutcome::Rejected(reason) => Err(crate::Error::Crdt(
                nodedb_crdt::CrdtError::ConstraintViolation {
                    constraint: "snapshot import".into(),
                    collection: collection.into(),
                    detail: reason.to_string(),
                },
            )),
            // The DLQ refusal is the error: nothing applied and nothing
            // records the snapshot.
            super::ValidatedApplyOutcome::DeadLetterRefused { error, .. } => {
                Err(crate::Error::Crdt(error))
            }
            super::ValidatedApplyOutcome::Malformed => Err(crate::Error::Crdt(
                nodedb_crdt::CrdtError::DeltaApplyFailed("malformed snapshot".into()),
            )),
            super::ValidatedApplyOutcome::PendingDependencies => Err(crate::Error::Crdt(
                nodedb_crdt::CrdtError::DeltaApplyFailed(
                    "snapshot import left operations causally pending".into(),
                ),
            )),
            super::ValidatedApplyOutcome::CandidateUnavailable => Err(crate::Error::Crdt(
                nodedb_crdt::CrdtError::DeltaApplyFailed(format!(
                    "no apply candidate for collection {collection}; nothing was imported"
                )),
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use loro::LoroValue;
    use nodedb_crdt::constraint::ConstraintSet;

    use crate::types::TenantId;

    use super::*;

    /// Build a peer document spanning two collections and export one incremental
    /// delta per write, mirroring how an embedded client that keeps a single Loro
    /// document for the whole database produces its deltas.
    ///
    /// Returns `(first_delta_for_target, later_delta_for_target)` where the later
    /// delta causally depends on an intervening write to the *other* collection.
    fn interleaved_collection_deltas(peer: u64, target: &str, other: &str) -> (Vec<u8>, Vec<u8>) {
        let state = nodedb_crdt::state::CrdtState::new(peer).unwrap();

        let v0 = state.oplog_version_vector();
        state
            .upsert(target, "first", &[("v", LoroValue::I64(1))])
            .unwrap();
        let first = state.export_updates_since(&v0).unwrap();

        state
            .upsert(other, "aside", &[("v", LoroValue::I64(2))])
            .unwrap();

        let v2 = state.oplog_version_vector();
        state
            .upsert(target, "later", &[("v", LoroValue::I64(3))])
            .unwrap();
        let later = state.export_updates_since(&v2).unwrap();

        (first, later)
    }

    /// Snapshot restore must not report a completed restore when the blob's
    /// operations could not be applied: the collection would come back partially
    /// populated and be indistinguishable from a correct restore.
    #[test]
    fn snapshot_import_does_not_report_success_without_applying() {
        let mut engine = TenantCrdtEngine::new(TenantId::new(1), 0, ConstraintSet::new()).unwrap();
        let (first, later) = interleaved_collection_deltas(32, "users", "signals");

        engine.import_snapshot_bytes("users", &first).unwrap();
        assert!(engine.row_exists("users", "first"));

        let result = engine.import_snapshot_bytes("users", &later);

        assert!(
            result.is_err() || engine.row_exists("users", "later"),
            "snapshot import reported a completed restore while its operations \
             stayed causally pending"
        );
    }

    /// Every collection's document is constructed with the same peer id, so Loro
    /// operation ids are unique only *within* a collection. Two collections'
    /// snapshots therefore carry colliding `(peer, counter)` identities, and a
    /// consumer that merges them into one document — which is exactly how an
    /// embedded client stores them — silently loses one side of the collision.
    #[test]
    fn collection_snapshots_carry_distinct_operation_identities() {
        let mut engine = TenantCrdtEngine::new(TenantId::new(1), 0, ConstraintSet::new()).unwrap();

        engine
            .doc_upsert(
                "users",
                "u1",
                &[("name", LoroValue::String("Alice".into()))],
            )
            .unwrap();
        engine
            .doc_upsert(
                "orders",
                "o1",
                &[("item", LoroValue::String("book".into()))],
            )
            .unwrap();

        let users = engine
            .export_snapshot_bytes("users")
            .unwrap()
            .expect("users snapshot");
        let orders = engine
            .export_snapshot_bytes("orders")
            .unwrap()
            .expect("orders snapshot");

        // A single document holding both collections — the embedded-client shape.
        let merged = nodedb_crdt::state::CrdtState::new(99).unwrap();
        merged.import(&users).expect("import users snapshot");
        merged.import(&orders).expect("import orders snapshot");

        assert!(
            merged.row_exists("users", "u1"),
            "the users row was lost when both collections merged into one document"
        );
        assert!(
            merged.row_exists("orders", "o1"),
            "the orders row was lost when both collections merged into one document"
        );
    }
}
