// SPDX-License-Identifier: BUSL-1.1

//! WAL replay for sparse-vector inserts and deletes.
//!
//! Both upsert or remove by `doc_id`. A record the restored sparse-vector
//! checkpoint's stamp names is skipped, and every other record replays in LSN
//! order on top of the checkpoint, the order the live core applied it in.

use nodedb_physical::physical_plan::VectorOp;

use crate::bridge::envelope::{PhysicalPlan, Status};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::transaction::undo::UndoEntry;
use crate::types::DatabaseId;

impl CoreLoop {
    /// In the install pass of a committed-redo apply, record the prior state
    /// of `doc_id` in the sparse index of `field` before a write replaces it.
    fn record_sparse_doc_undo(
        &mut self,
        database_id: u64,
        tenant_id: u64,
        (collection, field): (&str, &str),
        doc_id: &str,
    ) {
        if !self.recording_redo_undo() {
            return;
        }
        let key = Self::sparse_index_key(database_id, tenant_id, collection, field);
        let (prior, next_id) = match self.sparse_vector_indexes.get(&key) {
            Some(index) => (index.doc_image(doc_id), Some(index.next_internal_id())),
            None => (None, None),
        };
        self.record_redo_undo([UndoEntry::SparseDoc {
            key,
            doc_id: doc_id.to_string(),
            prior,
            next_id,
        }]);
    }

    /// Replay one `SparseVectorPut` record, unless the restored checkpoint
    /// holds it.
    pub(in crate::data::executor) fn replay_sparse_put(
        &mut self,
        payload: &[u8],
        tenant_id: u64,
        database_id: u64,
        record_lsn: u64,
        tombstones: &nodedb_wal::TombstoneSet,
    ) -> bool {
        let tombstones = tombstones.for_database(database_id);
        let Ok((collection, field_name, doc_id, entries)) =
            zerompk::from_msgpack::<(String, String, String, Vec<(u32, f32)>)>(payload)
        else {
            self.replay_record_unapplied(
                "vector",
                "sparse_put_decode",
                record_lsn,
                "SparseVectorPut payload does not decode",
            );
            return false;
        };
        if tombstones.is_tombstoned(tenant_id, &collection, record_lsn) {
            return false;
        }
        if self.sparse_vector_replay_skips(record_lsn) {
            return false;
        }
        if self.applying_committed_redo()
            && let Err(e) = nodedb_types::SparseVector::from_entries(entries.clone())
        {
            self.replay_record_unapplied(
                "vector",
                "sparse_entries",
                record_lsn,
                &format!("sparse vector for '{collection}' is invalid: {e}"),
            );
            return false;
        }
        if self.claim_for_validation() {
            return false;
        }
        self.record_sparse_doc_undo(database_id, tenant_id, (&collection, &field_name), &doc_id);
        let vshard = crate::types::VShardId::from_collection_in_database(
            DatabaseId::new(database_id),
            &collection,
        );
        let task = Self::replay_vector_task(
            nodedb_types::TenantId::new(tenant_id),
            DatabaseId::new(database_id),
            vshard,
            PhysicalPlan::Vector(VectorOp::SparseInsert {
                collection: nodedb_types::QualifiedCollection::from_stored(collection.clone()),
                field_name: field_name.clone(),
                doc_id: doc_id.clone(),
                entries: entries.clone(),
            }),
        );
        let response = self.execute_sparse_insert(
            &task,
            tenant_id,
            &collection,
            &field_name,
            &doc_id,
            &entries,
        );
        if response.status != Status::Ok {
            self.replay_record_rejected(
                "vector",
                record_lsn,
                response.error_code,
                &format!("sparse vector insert into '{collection}' failed"),
            );
            return false;
        }
        true
    }

    /// Replay one `SparseVectorDelete` record, unless the restored checkpoint
    /// holds it.
    pub(in crate::data::executor) fn replay_sparse_delete(
        &mut self,
        payload: &[u8],
        tenant_id: u64,
        database_id: u64,
        record_lsn: u64,
        tombstones: &nodedb_wal::TombstoneSet,
    ) -> bool {
        let tombstones = tombstones.for_database(database_id);
        let Ok((collection, field_name, doc_id)) =
            zerompk::from_msgpack::<(String, String, String)>(payload)
        else {
            self.replay_record_unapplied(
                "vector",
                "sparse_delete_decode",
                record_lsn,
                "SparseVectorDelete payload does not decode",
            );
            return false;
        };
        if tombstones.is_tombstoned(tenant_id, &collection, record_lsn) {
            return false;
        }
        if self.sparse_vector_replay_skips(record_lsn) {
            return false;
        }
        if self.claim_for_validation() {
            return false;
        }
        self.record_sparse_doc_undo(database_id, tenant_id, (&collection, &field_name), &doc_id);
        let vshard = crate::types::VShardId::from_collection_in_database(
            DatabaseId::new(database_id),
            &collection,
        );
        let task = Self::replay_vector_task(
            nodedb_types::TenantId::new(tenant_id),
            DatabaseId::new(database_id),
            vshard,
            PhysicalPlan::Vector(VectorOp::SparseDelete {
                collection: nodedb_types::QualifiedCollection::from_stored(collection.clone()),
                field_name: field_name.clone(),
                doc_id: doc_id.clone(),
            }),
        );
        // An absent document yields NotFound; that is an expected idempotent
        // no-op on replay, not a failure.
        let _ = self.execute_sparse_delete(&task, tenant_id, &collection, &field_name, &doc_id);
        true
    }
}
