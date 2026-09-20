// SPDX-License-Identifier: BUSL-1.1

//! WAL replay for the `VectorResolvedDirectWrite` record.
//!
//! The record carries every row mutation a governed vector-primary write
//! resolved to, each with its full stored image, so replay applies the rows
//! through the same primitives the live apply uses and recomputes nothing.
//! Gated by the per-collection checkpoint watermark like the other direct
//! records: a restored checkpoint already holds every write at or below it.
//! No pre-image is checked — a replayed record is committed history, not a
//! proposal that could have drifted.

use nodedb_physical::physical_plan::VectorOp;

use crate::bridge::envelope::PhysicalPlan;
use crate::control::server::wal_dispatch::VectorResolvedDirectWriteRecord;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::vector_direct_resolve::VectorResolvedIndexSpec;
use crate::types::DatabaseId;

impl CoreLoop {
    /// Replay one `VectorResolvedDirectWrite` record.
    pub(in crate::data::executor) fn replay_vector_resolved_direct_write(
        &mut self,
        payload: &[u8],
        tenant_id: u64,
        database_id: u64,
        record_lsn: u64,
        tombstones: &nodedb_wal::TombstoneSet,
    ) -> bool {
        let tombstones = tombstones.for_database(database_id);
        let Ok((collection, field, quantization, storage_dtype, payload_indexes, mutations)) =
            zerompk::from_msgpack::<VectorResolvedDirectWriteRecord>(payload)
        else {
            return false;
        };
        if tombstones.is_tombstoned(tenant_id, &collection, record_lsn) {
            return false;
        }
        let index_key = CoreLoop::vector_index_key(database_id, tenant_id, &collection, &field);
        if let Some(existing) = self.vector_collections.get(&index_key)
            && record_lsn <= existing.checkpoint_wal_lsn()
        {
            return false;
        }
        let vshard = crate::types::VShardId::from_collection_in_database(
            DatabaseId::new(database_id),
            &collection,
        );
        let rls_write_check = nodedb_types::RlsWriteCheck::already_decided_elsewhere();
        let task = Self::replay_vector_task(
            nodedb_types::TenantId::new(tenant_id),
            DatabaseId::new(database_id),
            vshard,
            PhysicalPlan::Vector(VectorOp::ResolvedDirectWrite {
                collection: nodedb_types::QualifiedCollection::from_stored(collection.clone()),
                field: field.clone(),
                quantization,
                storage_dtype,
                payload_indexes: payload_indexes.clone(),
                mutations: mutations.clone(),
                response_payload: Vec::new(),
                rls_write_check: rls_write_check.clone(),
            }),
        );
        let index = VectorResolvedIndexSpec {
            collection: &collection,
            field: &field,
            quantization,
            storage_dtype,
            payload_indexes: &payload_indexes,
        };
        let touched = match self.apply_vector_resolved_mutations(
            &task,
            tenant_id,
            index,
            &mutations,
            &rls_write_check,
        ) {
            Ok(touched) => touched,
            Err(e) => {
                tracing::warn!(
                    core = self.core_id,
                    %collection,
                    lsn = record_lsn,
                    error = ?e,
                    "WAL replay: resolved direct write apply returned error; skipping"
                );
                return false;
            }
        };
        if !touched.is_empty() {
            self.finish_vector_direct_write(&task, &index_key, tenant_id, &collection, &touched);
        }
        if let Some(coll) = self.vector_collections.get_mut(&index_key) {
            coll.note_checkpoint_lsn(record_lsn);
        }
        true
    }
}
