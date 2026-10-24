// SPDX-License-Identifier: BUSL-1.1

//! Row reads, bitemporal history access, and collection purge.

use loro::LoroValue;

use super::core::TenantCrdtEngine;

impl TenantCrdtEngine {
    /// Read a single row's fields as a `LoroValue`, or `None` if absent.
    pub fn read_row(&self, collection: &str, row_id: &str) -> Option<LoroValue> {
        self.collections
            .get(collection)
            .and_then(|state| state.read_row(collection, row_id))
    }

    /// Check if a row exists in a collection's document store.
    pub fn row_exists(&self, collection: &str, row_id: &str) -> bool {
        self.collections
            .get(collection)
            .is_some_and(|state| state.row_exists(collection, row_id))
    }

    /// Read the row as it was at `asof_ms` (system-time). Returns `None` when
    /// the collection is absent or no version existed at or before that time.
    pub fn read_row_as_of(
        &self,
        collection: &str,
        row_id: &str,
        asof_ms: i64,
    ) -> Option<LoroValue> {
        self.collections
            .get(collection)
            .and_then(|state| state.read_row_as_of(collection, row_id, asof_ms))
    }

    /// Count archived (superseded) bitemporal versions for a row.
    /// Returns `0` when the collection has no local state.
    pub fn archive_version_count(&self, collection: &str, row_id: &str) -> usize {
        self.collections
            .get(collection)
            .map(|state| state.archive_version_count(collection, row_id))
            .unwrap_or(0)
    }

    /// Drop archived bitemporal versions older than `cutoff_system_ms`
    /// for the given collection. The live row is never touched. Called
    /// from the Data Plane purge handler. A collection with no local state
    /// purges nothing.
    pub fn purge_history_before(
        &self,
        collection: &str,
        cutoff_system_ms: i64,
    ) -> crate::Result<usize> {
        match self.collections.get(collection) {
            Some(state) => state
                .purge_history_before(collection, cutoff_system_ms)
                .map_err(crate::Error::Crdt),
            None => Ok(0),
        }
    }

    /// Number of entries in the dead-letter queue.
    pub fn dlq_len(&self) -> usize {
        self.validator.dlq().len()
    }

    /// Purge all CRDT state for a single collection.
    ///
    /// Four things happen:
    /// 1. The collection's Loro doc and its retained validation candidate
    ///    are dropped. A Loro doc keeps every value its rows ever held in its
    ///    op history, and a snapshot export carries that history, so a doc
    ///    with only its rows deleted still holds the purged data.
    /// 2. The collection's conflict-resolution policy is removed from
    ///    the policy registry.
    /// 3. The collection's installed constraints and their version fence
    ///    are cleared — otherwise a re-created collection of the same name
    ///    would be validated against the dropped collection's constraints,
    ///    and (because its constraint version restarts at 1) its fresh
    ///    constraint install would be rejected as stale by the fence.
    /// 4. Any dead-letter entries (rejected deltas) scoped to this
    ///    collection are dropped — otherwise a re-created collection
    ///    of the same name would inherit unrelated rejected deltas.
    ///
    /// Returns the number of CRDT rows removed. Idempotent.
    pub fn purge_collection(&mut self, collection: &str) -> crate::Result<usize> {
        let removed = self
            .collections
            .remove(collection)
            .map_or(0, |state| state.row_ids(collection).len());
        self.apply_candidates.remove(collection);
        self.validator.policies_mut().remove(collection);
        self.validator.clear_collection_constraints(collection);
        self.constraint_versions.remove(collection);
        let dlq_dropped = self
            .validator
            .dlq_mut()
            .purge_collection(self.tenant_id.as_u64(), collection);
        if dlq_dropped > 0 {
            tracing::debug!(
                tenant = self.tenant_id.as_u64(),
                collection,
                dlq_dropped,
                "crdt: dropped DLQ entries scoped to purged collection"
            );
        }
        Ok(removed)
    }
}

#[cfg(test)]
mod tests {
    use nodedb_crdt::constraint::ConstraintSet;
    use nodedb_crdt::validator::ProposedChange;

    use crate::types::TenantId;

    use super::*;

    #[test]
    fn separate_collections_have_isolated_docs() {
        let mut engine = TenantCrdtEngine::new(TenantId::new(1), 0, ConstraintSet::new()).unwrap();

        let change = ProposedChange {
            collection: "users".into(),
            row_id: "u1".into(),
            surrogate: nodedb_types::Surrogate::new(1),
            fields: vec![("name".into(), LoroValue::String("Alice".into()))],
        };
        engine
            .validate_and_apply(
                1,
                nodedb_crdt::CrdtAuthContext::default(),
                &change,
                b"d".to_vec(),
                &nodedb_crs::CrsContext::builtin_only(),
            )
            .unwrap();

        assert!(engine.row_exists("users", "u1"));
        assert!(!engine.row_exists("orders", "u1"));
        assert!(engine.read_row("users", "u1").is_some());
        assert!(engine.read_row("orders", "u1").is_none());
    }

    /// A purge drops the collection's Loro doc, so no snapshot export carries
    /// the history of its rows. Other collections keep theirs.
    #[test]
    fn a_purge_drops_the_collection_doc_and_its_history() {
        let mut engine = TenantCrdtEngine::new(TenantId::new(1), 0, ConstraintSet::new()).unwrap();
        for (collection, surrogate) in [("users", 1), ("orders", 2)] {
            let change = ProposedChange {
                collection: collection.into(),
                row_id: "r1".into(),
                surrogate: nodedb_types::Surrogate::new(surrogate),
                fields: vec![("secret".into(), LoroValue::String("s3cr3t".into()))],
            };
            engine
                .validate_and_apply(
                    1,
                    nodedb_crdt::CrdtAuthContext::default(),
                    &change,
                    b"d".to_vec(),
                    &nodedb_crs::CrsContext::builtin_only(),
                )
                .unwrap();
        }

        assert_eq!(engine.purge_collection("users").unwrap(), 1);

        let exported: Vec<String> = engine
            .export_all_snapshots()
            .unwrap()
            .into_iter()
            .map(|(collection, _)| collection)
            .collect();
        assert_eq!(exported, vec!["orders".to_string()]);
        assert!(engine.export_snapshot_bytes("users").unwrap().is_none());
        assert!(!engine.collection_names().contains(&"users".to_string()));
        assert_eq!(engine.purge_collection("users").unwrap(), 0, "idempotent");
    }

    fn write(engine: &mut TenantCrdtEngine, row_id: &str, surrogate: u32, value: &str) {
        let change = ProposedChange {
            collection: "notes".into(),
            row_id: row_id.into(),
            surrogate: nodedb_types::Surrogate::new(surrogate),
            fields: vec![("body".into(), LoroValue::String(value.into()))],
        };
        engine
            .validate_and_apply(
                1,
                nodedb_crdt::CrdtAuthContext::default(),
                &change,
                b"d".to_vec(),
                &nodedb_crs::CrsContext::builtin_only(),
            )
            .unwrap();
    }

    /// A drop and a same-name create start a new incarnation, and the new
    /// document mints its operations under a new peer id. A peer that still
    /// holds the dropped document's history takes the new document's
    /// operations as new ones, never as operations it already holds.
    #[test]
    fn a_recreated_collection_never_reuses_an_operation_id() {
        let first = nodedb_types::Hlc::new(1_000, 0);
        let second = nodedb_types::Hlc::new(2_000, 0);
        let mut engine = TenantCrdtEngine::new(TenantId::new(1), 0, ConstraintSet::new()).unwrap();
        engine.bind_incarnation("notes", first).unwrap();
        write(&mut engine, "r1", 1, "dropped");
        let dropped_peer = engine.peer_id_of("notes");
        // A client that synced the collection before the drop.
        let dropped_history = engine.export_snapshot_bytes("notes").unwrap().unwrap();

        engine.purge_collection("notes").unwrap();
        engine.bind_incarnation("notes", second).unwrap();
        write(&mut engine, "r2", 2, "fresh");
        let fresh_peer = engine.peer_id_of("notes");
        assert_ne!(
            fresh_peer, dropped_peer,
            "the new incarnation mints new ids"
        );

        let client =
            nodedb_crdt::state::CrdtState::from_local_snapshot(99, &dropped_history).unwrap();
        let fresh = engine.export_snapshot_bytes("notes").unwrap().unwrap();
        client.import(&fresh).unwrap();
        assert!(
            client.read_row("notes", "r2").is_some(),
            "the new row reaches a peer holding the dropped history"
        );
    }

    /// A document loaded before its collection's incarnation is bound mints
    /// its later operations under the derived peer id. A binding to a
    /// different incarnation drops a document of the earlier one.
    #[test]
    fn binding_an_incarnation_rebinds_or_drops_the_document() {
        let first = nodedb_types::Hlc::new(1_000, 0);
        let second = nodedb_types::Hlc::new(2_000, 0);
        let mut engine = TenantCrdtEngine::new(TenantId::new(1), 0, ConstraintSet::new()).unwrap();
        write(&mut engine, "r1", 1, "loaded");
        engine.bind_incarnation("notes", first).unwrap();
        assert_eq!(
            engine.collection_peer_id_for_test("notes"),
            Some(TenantCrdtEngine::collection_peer_id(0, "notes", first)),
        );
        assert!(engine.row_exists("notes", "r1"), "the loaded rows stay");

        engine.bind_incarnation("notes", second).unwrap();
        assert!(
            !engine.row_exists("notes", "r1"),
            "a document of a dropped incarnation goes"
        );
    }
}
