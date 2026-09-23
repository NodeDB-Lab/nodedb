// SPDX-License-Identifier: BUSL-1.1

//! WAL replay of one `VectorDelete` record.
//!
//! Decode order, longest shape first:
//!
//! * `(collection, surrogate, field_name, Option<SyncProvenance>)`: the
//!   delete-by-surrogate shape. It routes through the live handler so the
//!   sync idempotency gate runs on replay exactly as on the live path.
//! * `(collection, vector_id, Option<SyncProvenance>)`: a delete by local node
//!   id. The provenance is not used.
//! * `(collection, vector_id)`: the same delete without provenance.

use crate::bridge::envelope::PhysicalPlan;
use crate::types::DatabaseId;

use super::core_loop::CoreLoop;
use super::wal_replay_vector_redo::RedoVectorWrite;

impl CoreLoop {
    /// Replay one `VectorDelete` record. Returns whether a vector was
    /// deleted. A record no shape decodes is reported through
    /// `replay_policy` as unapplied.
    pub(in crate::data::executor) fn replay_vector_delete_record(
        &mut self,
        payload: &[u8],
        tenant_id: u64,
        database_id: u64,
        record_lsn: u64,
        tombstones: &nodedb_wal::DatabaseTombstones<'_>,
    ) -> bool {
        if let Ok((collection, surrogate_u32, field_name, provenance)) = zerompk::from_msgpack::<(
            String,
            u32,
            String,
            Option<nodedb_types::sync::wire::SyncProvenance>,
        )>(payload)
        {
            if tombstones.is_tombstoned(tenant_id, &collection, record_lsn) {
                return false;
            }
            let surrogate = nodedb_types::Surrogate::new(surrogate_u32);
            // The handler resolves the field-suffixed index first, then the
            // plain one; the undo covers the index it will write.
            let field_key =
                CoreLoop::vector_index_key(database_id, tenant_id, &collection, &field_name);
            let index_key = if self.vector_collections.contains_key(&field_key) {
                field_key
            } else {
                CoreLoop::vector_index_key(database_id, tenant_id, &collection, "")
            };
            if !self.redo_vector_prelude(
                RedoVectorWrite {
                    index_key: &index_key,
                    tid: tenant_id,
                    collection: &collection,
                    dim: 0,
                    surrogates: &[surrogate],
                    ids: &[],
                    sidecars: false,
                },
                provenance.as_ref(),
                record_lsn,
            ) {
                return false;
            }
            let vshard = crate::types::VShardId::from_collection_in_database(
                DatabaseId::new(database_id),
                &collection,
            );
            let task = Self::replay_vector_task(
                nodedb_types::TenantId::new(tenant_id),
                DatabaseId::new(database_id),
                vshard,
                PhysicalPlan::Vector(
                    nodedb_physical::physical_plan::VectorOp::DeleteBySurrogate {
                        collection: nodedb_types::QualifiedCollection::from_stored(
                            collection.clone(),
                        ),
                        surrogate,
                        field_name: field_name.clone(),
                        provenance: provenance.clone(),
                    },
                ),
            );
            let response = self.execute_vector_delete_by_surrogate(
                &task,
                tenant_id,
                &collection,
                surrogate,
                &field_name,
                provenance.as_ref(),
            );
            if response.status != crate::bridge::envelope::Status::Ok {
                self.replay_record_rejected(
                    "vector",
                    record_lsn,
                    response.error_code,
                    &format!("vector delete-by-surrogate on '{collection}' failed"),
                );
                return false;
            }
            return true;
        }

        let decoded = zerompk::from_msgpack::<(
            String,
            u32,
            Option<nodedb_types::sync::wire::SyncProvenance>,
        )>(payload)
        .map(|(collection, vector_id, _prov)| (collection, vector_id))
        .or_else(|_| zerompk::from_msgpack::<(String, u32)>(payload));
        let Ok((collection, vector_id)) = decoded else {
            self.replay_record_unapplied(
                "vector",
                "delete_decode",
                record_lsn,
                "VectorDelete payload matched none of its record shapes",
            );
            return false;
        };
        if tombstones.is_tombstoned(tenant_id, &collection, record_lsn) {
            return false;
        }
        let index_key = CoreLoop::vector_index_key(database_id, tenant_id, &collection, "");
        if !self.redo_vector_prelude(
            RedoVectorWrite {
                index_key: &index_key,
                tid: tenant_id,
                collection: &collection,
                dim: 0,
                surrogates: &[],
                ids: &[vector_id],
                sidecars: false,
            },
            None,
            record_lsn,
        ) {
            return false;
        }
        match self.vector_collections.get_mut(&index_key) {
            Some(index) => {
                index.delete(vector_id);
                true
            }
            None => false,
        }
    }
}
