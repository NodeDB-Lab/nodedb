// SPDX-License-Identifier: BUSL-1.1

//! WAL replay for the vector-primary `DELETE` / `UPDATE` / `TRUNCATE`
//! records.
//!
//! Every record routes through the live handler, so the rebuilt node,
//! bitmap entries, and sidecar match what the live write produced. Each is
//! gated by the per-collection checkpoint watermark like the direct upsert:
//! a restored checkpoint already holds every write at or below it.

use nodedb_physical::physical_plan::VectorOp;

use crate::bridge::envelope::{PhysicalPlan, Status};
use crate::control::server::wal_dispatch::{
    VectorDirectDeleteRecord, VectorDirectTruncateRecord, VectorDirectUpdateRecord,
};
use crate::data::executor::core_loop::CoreLoop;
use crate::types::DatabaseId;

impl CoreLoop {
    /// Replay one `VectorDirectDelete` record. A surrogate whose node is
    /// already gone is skipped by the handler, so re-applying over a restored
    /// checkpoint is idempotent.
    pub(in crate::data::executor) fn replay_direct_delete(
        &mut self,
        payload: &[u8],
        tenant_id: u64,
        database_id: u64,
        record_lsn: u64,
        tombstones: &nodedb_wal::TombstoneSet,
    ) -> bool {
        let tombstones = tombstones.for_database(database_id);
        let Ok((collection, field, targets)) =
            zerompk::from_msgpack::<VectorDirectDeleteRecord>(payload)
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
        let task = Self::replay_vector_task(
            nodedb_types::TenantId::new(tenant_id),
            DatabaseId::new(database_id),
            vshard,
            PhysicalPlan::Vector(VectorOp::DirectDelete {
                collection: nodedb_types::QualifiedCollection::from_stored(collection.clone()),
                field: field.clone(),
                targets: targets.clone(),
                returning: None,
                rls_filters: Vec::new(),
                rls_write_check: nodedb_types::RlsWriteCheck::already_decided_elsewhere(),
            }),
        );
        let response = self.execute_vector_direct_delete(
            crate::data::executor::handlers::vector_direct_delete::VectorDirectDeleteParams {
                task: &task,
                tid: tenant_id,
                collection: &collection,
                field: &field,
                targets: &targets,
                returning: None,
                rls_filters: &[],
                rls_write_check: &nodedb_types::RlsWriteCheck::already_decided_elsewhere(),
            },
        );
        if response.status != Status::Ok {
            tracing::warn!(
                core = self.core_id,
                %collection,
                lsn = record_lsn,
                "WAL replay: direct-delete handler returned error; skipping"
            );
            return false;
        }
        if let Some(coll) = self.vector_collections.get_mut(&index_key) {
            coll.note_checkpoint_lsn(record_lsn);
        }
        true
    }

    /// Replay one `VectorDirectTruncate` record through the live handler. A
    /// collection with no index or no rows truncates to zero rows, so
    /// re-applying over a restored checkpoint is idempotent.
    pub(in crate::data::executor) fn replay_direct_truncate(
        &mut self,
        payload: &[u8],
        tenant_id: u64,
        database_id: u64,
        record_lsn: u64,
        tombstones: &nodedb_wal::TombstoneSet,
    ) -> bool {
        let tombstones = tombstones.for_database(database_id);
        let Ok((collection, field)) = zerompk::from_msgpack::<VectorDirectTruncateRecord>(payload)
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
        let task = Self::replay_vector_task(
            nodedb_types::TenantId::new(tenant_id),
            DatabaseId::new(database_id),
            vshard,
            PhysicalPlan::Vector(VectorOp::DirectTruncate {
                collection: nodedb_types::QualifiedCollection::from_stored(collection.clone()),
                field: field.clone(),
                restart_identity: false,
            }),
        );
        let response = self.execute_vector_direct_truncate(&task, tenant_id, &collection, &field);
        if response.status != Status::Ok {
            tracing::warn!(
                core = self.core_id,
                %collection,
                lsn = record_lsn,
                "WAL replay: direct-truncate handler returned error; skipping"
            );
            return false;
        }
        if let Some(coll) = self.vector_collections.get_mut(&index_key) {
            coll.note_checkpoint_lsn(record_lsn);
        }
        true
    }

    /// Replay one `VectorDirectUpdate` record through the live handler, so
    /// the rebuilt node, bitmap entries, and sidecar match the live write.
    pub(in crate::data::executor) fn replay_direct_update(
        &mut self,
        payload: &[u8],
        tenant_id: u64,
        database_id: u64,
        record_lsn: u64,
        tombstones: &nodedb_wal::TombstoneSet,
    ) -> bool {
        let tombstones = tombstones.for_database(database_id);
        let Ok((
            collection,
            field,
            targets,
            new_vector,
            payload_patch,
            quantization,
            storage_dtype,
            payload_indexes,
        )) = zerompk::from_msgpack::<VectorDirectUpdateRecord>(payload)
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
        let task = Self::replay_vector_task(
            nodedb_types::TenantId::new(tenant_id),
            DatabaseId::new(database_id),
            vshard,
            PhysicalPlan::Vector(VectorOp::DirectUpdate {
                collection: nodedb_types::QualifiedCollection::from_stored(collection.clone()),
                field: field.clone(),
                targets: targets.clone(),
                new_vector: new_vector.clone(),
                payload_patch: payload_patch.clone(),
                quantization,
                storage_dtype,
                payload_indexes: payload_indexes.clone(),
                returning: None,
                rls_filters: Vec::new(),
                rls_write_check: nodedb_types::RlsWriteCheck::already_decided_elsewhere(),
            }),
        );
        let response = self.execute_vector_direct_update(
            crate::data::executor::handlers::vector_direct_update::VectorDirectUpdateParams {
                task: &task,
                tid: tenant_id,
                collection: &collection,
                field: &field,
                targets: &targets,
                new_vector: new_vector.as_deref(),
                payload_patch: &payload_patch,
                quantization,
                storage_dtype,
                payload_indexes: &payload_indexes,
                returning: None,
                rls_filters: &[],
                rls_write_check: &nodedb_types::RlsWriteCheck::already_decided_elsewhere(),
            },
        );
        if response.status != Status::Ok {
            tracing::warn!(
                core = self.core_id,
                %collection,
                lsn = record_lsn,
                "WAL replay: direct-update handler returned error; skipping"
            );
            return false;
        }
        if let Some(coll) = self.vector_collections.get_mut(&index_key) {
            coll.note_checkpoint_lsn(record_lsn);
        }
        true
    }
}
