// SPDX-License-Identifier: BUSL-1.1

//! Soft-delete a document's prior vector nodes, per field or whole-document.

use crate::data::executor::core_loop::CoreLoop;
use crate::engine::document::store::StorageKey;

use super::types::{VectorIndexDelta, VectorRemoval};

impl CoreLoop {
    /// End, at system-time ordinal `ordinal`, the HNSW vector node a document
    /// holds for one `field`, and drop the paired `vector_doc_map` reverse
    /// entry. The node is the one bound to the row's surrogate, else the one
    /// the map records. The binding survives a restart and a snapshot
    /// install, and the map does not, so the binding decides. Returns the
    /// removed delta, or `None` when the row holds no node in `field` (a
    /// genuine first insert). This is the per-field unit the whole-doc
    /// `remove_document_vector_indexes` loops over, and the put path calls it
    /// for the current field only so a sibling field's just-inserted node is
    /// never clobbered.
    pub(in crate::data::executor) fn remove_document_vector_index_field(
        &mut self,
        database_id: u64,
        tid: u64,
        collection: &str,
        field: &str,
        storage_key: StorageKey,
        ordinal: i64,
    ) -> Option<VectorIndexDelta> {
        let doc_key = (
            nodedb_types::DatabaseId::new(database_id),
            crate::types::TenantId::new(tid),
            collection.to_string(),
            field.to_string(),
            storage_key,
        );
        let recorded = self.vector_doc_map.remove(&doc_key);
        let index_key = Self::vector_index_key(database_id, tid, collection, field);
        let coll = self.vector_collections.get_mut(&index_key);
        // A later insert under the same surrogate (a vector write over this
        // row) replaces the recorded node, so the recorded id can name a node
        // that is already soft-deleted while the bound one keeps scoring.
        let bound = coll
            .as_ref()
            .and_then(|coll| coll.local_for_surrogate(storage_key.surrogate()));
        let vector_id = bound.or(recorded)?;
        // The node ends at the write's system time: a read at an earlier cut
        // still finds it.
        let node_deleted = coll.is_some_and(|coll| coll.end_at(vector_id, ordinal));
        let removal = match recorded {
            Some(_) => VectorRemoval::Recorded { node_deleted },
            // A bound node with no map entry is live, so the end above took.
            None => VectorRemoval::Bound,
        };
        Some(VectorIndexDelta {
            index_key,
            vector_id,
            collection: collection.to_string(),
            field: field.to_string(),
            doc_id: storage_key,
            removal,
        })
    }

    /// End, at system-time ordinal `ordinal`, every HNSW vector entry a
    /// document produced, keyed by its storage key, and drop the paired
    /// `vector_doc_map` reverse entries. A multi-vector document bound to the
    /// row's surrogate in any vector index of `collection` ends too.
    /// Shared by the PointDelete cascade and the other row removals, which
    /// end the vectors of a removed row at the removal's ordinal.
    ///
    /// Candidate fields come from the same strict-schema / `vector_params` /
    /// declared-column enumeration the put path uses, so each `vector_doc_map` entry is looked
    /// up by its exact key (via `remove_document_vector_index_field`) instead
    /// of scanning the whole map. Returns the removal deltas so a
    /// transactional caller can push their reversals.
    pub(in crate::data::executor) fn remove_document_vector_indexes(
        &mut self,
        database_id: u64,
        tid: u64,
        collection: &str,
        storage_key: StorageKey,
        ordinal: i64,
    ) -> Vec<VectorIndexDelta> {
        let candidate_fields = self.document_vector_field_names(database_id, tid, collection);
        let mut vector_deletes = Vec::with_capacity(candidate_fields.len());
        for field in candidate_fields {
            if let Some(delta) = self.remove_document_vector_index_field(
                database_id,
                tid,
                collection,
                &field,
                storage_key,
                ordinal,
            ) {
                vector_deletes.push(delta);
            }
        }
        vector_deletes.extend(self.end_document_multi_vectors(
            database_id,
            tid,
            collection,
            storage_key,
            ordinal,
        ));
        vector_deletes
    }

    /// End, at system-time ordinal `ordinal`, the multi-vector document of the
    /// row `storage_key` in every vector index of `collection`. A read at an
    /// earlier cut still scores it. Returns one delta per index whose
    /// document ended.
    pub(in crate::data::executor) fn end_document_multi_vectors(
        &mut self,
        database_id: u64,
        tid: u64,
        collection: &str,
        storage_key: StorageKey,
        ordinal: i64,
    ) -> Vec<VectorIndexDelta> {
        let surrogate = storage_key.surrogate();
        let db = nodedb_types::DatabaseId::new(database_id);
        let tenant = crate::types::TenantId::new(tid);
        let prefix = format!("{collection}:");
        let keys: Vec<_> = self
            .vector_collections
            .iter()
            .filter(|((d, t, key), coll)| {
                *d == db
                    && *t == tenant
                    && (key == collection || key.starts_with(&prefix))
                    && coll.multi_doc_map.contains_key(&surrogate)
            })
            .map(|(key, _)| key.clone())
            .collect();
        let mut deltas = Vec::with_capacity(keys.len());
        for index_key in keys {
            let Some(coll) = self.vector_collections.get_mut(&index_key) else {
                continue;
            };
            let members = coll
                .multi_doc_map
                .get(&surrogate)
                .cloned()
                .unwrap_or_default();
            let ended = coll.end_multi_vector_ids_at(surrogate, ordinal);
            let Some(&vector_id) = ended.first() else {
                // Every member was dead already: nothing ends, and the
                // membership stays as it was.
                coll.multi_doc_map.insert(surrogate, members);
                continue;
            };
            self.checkpoint_coordinator
                .mark_dirty("vector", ended.len());
            let field = index_key
                .2
                .strip_prefix(&prefix)
                .unwrap_or_default()
                .to_string();
            deltas.push(VectorIndexDelta {
                index_key,
                vector_id,
                collection: collection.to_string(),
                field,
                doc_id: storage_key,
                removal: VectorRemoval::MultiVector { members, ended },
            });
        }
        deltas
    }

    /// The vector fields a document of `collection` can carry: the strict
    /// schema's vector columns, else the schemaless vector fields.
    pub(in crate::data::executor) fn document_vector_field_names(
        &self,
        database_id: u64,
        tid: u64,
        collection: &str,
    ) -> Vec<String> {
        let strict_fields = self.strict_vector_fields(database_id, tid, collection);
        if strict_fields.is_empty() {
            self.schemaless_vector_field_names(database_id, tid, collection)
        } else {
            strict_fields.into_iter().map(|(name, _dim)| name).collect()
        }
    }
}

#[cfg(test)]
mod tests {
    use nodedb_types::Surrogate;

    use super::*;
    use crate::data::executor::core_loop::tests::make_core_with_dir;
    use crate::engine::vector::collection::VectorCollection;
    use crate::engine::vector::distance::DistanceMetric;
    use crate::engine::vector::hnsw::HnswParams;

    const TID: u64 = 1;

    /// A row whose node no `vector_doc_map` entry records, as after a
    /// restart or a snapshot install, still loses its node on delete: the
    /// surrogate binding names it. Undo binds it again.
    #[test]
    fn a_delete_ends_the_bound_node_the_map_does_not_record() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _req, _resp) = make_core_with_dir(dir.path());
        let key = CoreLoop::vector_index_key(0, TID, "docs", "emb");
        core.vector_params
            .insert(key.clone(), HnswParams::default());
        let row = Surrogate::new(4);
        let mut coll = VectorCollection::new(2, HnswParams::default());
        coll.insert_at(vec![1.0, 0.0], row, 10).expect("insert");
        core.vector_collections.insert(key.clone(), coll);
        assert!(core.vector_doc_map.is_empty(), "no recorded entry");

        let deltas =
            core.remove_document_vector_indexes(0, TID, "docs", StorageKey::for_surrogate(row), 20);
        assert_eq!(deltas.len(), 1);
        assert_eq!(deltas[0].removal, VectorRemoval::Bound);
        let live = |core: &CoreLoop| core.vector_collections.get(&key).map(|c| c.live_count());
        assert_eq!(live(&core), Some(0), "the delete ends the node");
        assert_eq!(
            core.vector_collections
                .get(&key)
                .and_then(|c| c.vector_for_surrogate_at(row, 15)),
            Some(vec![1.0, 0.0]),
            "a read below the delete still finds it"
        );

        let undo = deltas
            .into_iter()
            .map(VectorIndexDelta::into_delete_undo)
            .collect();
        core.undo_memory_effects(0, TID, undo).expect("undo");
        assert_eq!(live(&core), Some(1), "undo revives the node");
        assert!(core.vector_doc_map.is_empty(), "undo records no map entry");
    }

    /// A row's multi-vector document ends with the row in an index the
    /// collection declares no field for. A read below the removal scores it,
    /// and undo revives it with its membership.
    #[test]
    fn a_delete_ends_the_rows_multi_vector_document() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _req, _resp) = make_core_with_dir(dir.path());
        let key = CoreLoop::vector_index_key(0, TID, "docs", "tokens");
        let row = Surrogate::new(9);
        let mut coll = VectorCollection::new(2, HnswParams::default());
        let ids = coll
            .insert_multi_vector_at(&[&[1.0, 0.0], &[0.0, 1.0]], row, 10)
            .expect("insert");
        core.vector_collections.insert(key.clone(), coll);

        let deltas =
            core.remove_document_vector_indexes(0, TID, "docs", StorageKey::for_surrogate(row), 20);
        assert_eq!(deltas.len(), 1);
        assert_eq!(
            deltas[0].removal,
            VectorRemoval::MultiVector {
                members: ids.clone(),
                ended: ids.clone(),
            }
        );
        let coll = core.vector_collections.get(&key).expect("index");
        assert_eq!(coll.live_count(), 0);
        assert!(coll.multi_vector_documents().is_empty());
        let at_cut = coll
            .search_at(&[1.0, 0.0], 10, 32, DistanceMetric::L2, None, 15)
            .expect("search");
        assert_eq!(
            at_cut.len(),
            2,
            "a read below the delete scores the document"
        );

        let undo = deltas
            .into_iter()
            .map(VectorIndexDelta::into_delete_undo)
            .collect();
        core.undo_memory_effects(0, TID, undo).expect("undo");
        let coll = core.vector_collections.get(&key).expect("index");
        assert_eq!(coll.live_count(), 2);
        assert_eq!(coll.multi_vector_documents(), vec![row]);
    }
}
