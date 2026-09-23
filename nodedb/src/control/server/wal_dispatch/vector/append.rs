// SPDX-License-Identifier: BUSL-1.1

//! Append the WAL record for a vector-engine physical op.
//!
//! The sync helpers ([`wal_append_vector_put`],
//! [`wal_append_vector_delete_by_surrogate`]) and the autocommit dispatcher
//! ([`wal_append_vector_op`]) all encode through [`super::encode`], so every
//! path writes the same record shapes.

use nodedb_physical::physical_plan::VectorOp;

use crate::types::{DatabaseId, Lsn, TenantId, VShardId};
use crate::wal::manager::WalAppender;

use super::encode::{
    VectorDirectUpdatePayload, VectorDirectUpsertPayload, VectorResolvedDirectWritePayload,
    encode_multi_vector_delete_payload, encode_multi_vector_put_payload,
    encode_sparse_vector_delete_payload, encode_sparse_vector_put_payload,
    encode_vector_batch_put_payload, encode_vector_delete_by_surrogate_payload,
    encode_vector_delete_payload, encode_vector_direct_delete_payload,
    encode_vector_direct_truncate_payload, encode_vector_direct_update_payload,
    encode_vector_direct_upsert_payload, encode_vector_index_drop_payload,
    encode_vector_put_payload, encode_vector_resolved_direct_write_payload,
};
use nodedb_physical::physical_plan::VectorDirectWriteIntent;

/// Operation fields for a vector put WAL record.
///
/// Groups the vector-identity and provenance fields that together describe a
/// single vector insert, reducing the call-site argument count.
pub struct VectorPutWalArgs<'a> {
    pub collection: &'a str,
    pub vector: &'a [f32],
    pub dim: usize,
    pub field_name: &'a str,
    pub surrogate: nodedb_types::Surrogate,
    pub provenance: Option<&'a nodedb_types::sync::wire::SyncProvenance>,
}

/// Operation fields for a vector delete-by-surrogate WAL record.
///
/// Groups the collection, surrogate, field, and provenance fields that
/// together identify a single vector deletion.
pub struct VectorDeleteWalArgs<'a> {
    pub collection: &'a str,
    pub surrogate: nodedb_types::Surrogate,
    pub field_name: &'a str,
    pub provenance: Option<&'a nodedb_types::sync::wire::SyncProvenance>,
}

/// Append a vector put (insert) to the WAL and return the assigned LSN.
///
/// Encodes `(collection, vector, dim, field_name, doc_id_compat, surrogate_u32, provenance)`
/// exactly as the non-sync `VectorOp::Insert` arm in `wal_append_if_write_with_creds` does,
/// so replay decodes both paths with the same 7-element shape.
pub fn wal_append_vector_put(
    wal: WalAppender<'_>,
    tenant_id: TenantId,
    vshard_id: VShardId,
    database_id: DatabaseId,
    args: VectorPutWalArgs<'_>,
) -> crate::Result<nodedb_types::Lsn> {
    let VectorPutWalArgs {
        collection,
        vector,
        dim,
        field_name,
        surrogate,
        provenance,
    } = args;
    let entry =
        encode_vector_put_payload(collection, vector, dim, field_name, surrogate, provenance)?;
    let lsn = wal.append_vector_put(tenant_id, vshard_id, database_id, &entry)?;
    Ok(lsn)
}

/// Append the WAL record for a single vector-engine physical op, returning the
/// allocated LSN for writes (`Some`) or `None` for reads / index-maintenance
/// ops that carry no durable per-write effect.
///
/// The match over [`VectorOp`] is **exhaustive**: a new variant fails to
/// compile until its durability is decided here, so a future write can never
/// silently become non-durable (the class of bug this function was hardened
/// against). Read and maintenance ops map to `None` explicitly, by name.
pub(crate) fn wal_append_vector_op(
    wal: WalAppender<'_>,
    tenant_id: TenantId,
    vshard_id: VShardId,
    database_id: DatabaseId,
    op: &VectorOp,
) -> crate::Result<Option<Lsn>> {
    let appended = match op {
        VectorOp::Insert {
            collection,
            vector,
            dim,
            field_name,
            surrogate,
            pk_bytes: _,
            provenance,
        } => {
            // The local-WAL record carries the surrogate as a u32 so recovery
            // can rebind without consulting the catalog. See
            // `encode_vector_put_payload` for the compatibility slot.
            let entry = encode_vector_put_payload(
                collection.as_str(),
                vector,
                *dim,
                field_name,
                *surrogate,
                provenance.as_ref(),
            )?;
            Some(wal.append_vector_put(tenant_id, vshard_id, database_id, &entry)?)
        }
        VectorOp::BatchInsert {
            collection,
            vectors,
            dim,
            surrogates: _,
        } => {
            let entry = encode_vector_batch_put_payload(collection.as_str(), vectors, *dim)?;
            Some(wal.append_vector_put(tenant_id, vshard_id, database_id, &entry)?)
        }
        VectorOp::Delete {
            collection,
            vector_id,
        } => {
            let entry = encode_vector_delete_payload(collection.as_str(), *vector_id)?;
            Some(wal.append_vector_delete(tenant_id, vshard_id, database_id, &entry)?)
        }
        VectorOp::DeleteBySurrogate {
            collection,
            surrogate,
            field_name,
            provenance,
        } => {
            // Durable by node-independent surrogate. The sync-inbound path logs
            // this via `wal_append_vector_delete_by_surrogate` before dispatch;
            // logging it here too keeps every path that reaches this function
            // durable without double-logging (the sync path bypasses it).
            let entry = encode_vector_delete_by_surrogate_payload(
                collection.as_str(),
                *surrogate,
                field_name,
                provenance.as_ref(),
            )?;
            Some(wal.append_vector_delete(tenant_id, vshard_id, database_id, &entry)?)
        }
        VectorOp::SetParams {
            collection,
            field_name,
            dim,
            m,
            ef_construction,
            metric,
            index_type,
            pq_m,
            ivf_cells,
            ivf_nprobe,
        } => {
            // Fields are appended, never reordered, so older 4-/8-/9-element
            // WAL records still decode (replay reads the leading positions
            // first and falls back on the shorter shapes).
            let entry = zerompk::to_msgpack_vec(&(
                collection,
                m,
                ef_construction,
                metric,
                index_type,
                pq_m,
                ivf_cells,
                ivf_nprobe,
                field_name,
                dim,
            ))
            .map_err(|e| crate::Error::Serialization {
                format: "msgpack".into(),
                detail: format!("wal set vector params: {e}"),
            })?;
            Some(wal.append_vector_params(tenant_id, vshard_id, database_id, &entry)?)
        }
        VectorOp::DropIndex {
            collection,
            field_name,
        } => {
            // Durable, and required to be: the `VectorParams` record that
            // created this index is still in the log, so a replay that does
            // not see the drop rebuilds an index the user dropped.
            let entry = encode_vector_index_drop_payload(collection.as_str(), field_name)?;
            Some(wal.append_vector_index_drop(tenant_id, vshard_id, database_id, &entry)?)
        }
        // A projection is a client-session concern and must not enter the
        // durable record: replay re-applies the write, it does not answer the
        // statement that asked for rows. The write check was decided against
        // a live identity; replay admits what the leader committed.
        VectorOp::DirectUpsert {
            collection,
            field,
            surrogate,
            pk_bytes,
            vector,
            payload,
            quantization,
            storage_dtype,
            payload_indexes,
            returning: _,
            rls_filters: _,
            on_conflict_updates,
            rls_write_check: _,
        } => {
            let entry = encode_vector_direct_upsert_payload(VectorDirectUpsertPayload {
                collection: collection.as_str(),
                field,
                surrogate: *surrogate,
                pk_bytes,
                vector,
                payload,
                quantization: *quantization,
                storage_dtype: *storage_dtype,
                payload_indexes,
                intent: VectorDirectWriteIntent::Upsert,
                on_conflict_updates,
            })?;
            Some(wal.append_vector_direct_upsert(tenant_id, vshard_id, database_id, &entry)?)
        }
        VectorOp::DirectInsert {
            collection,
            field,
            surrogate,
            pk_bytes,
            vector,
            payload,
            quantization,
            storage_dtype,
            payload_indexes,
            returning: _,
            rls_filters: _,
        } => {
            let entry = encode_vector_direct_upsert_payload(VectorDirectUpsertPayload {
                collection: collection.as_str(),
                field,
                surrogate: *surrogate,
                pk_bytes,
                vector,
                payload,
                quantization: *quantization,
                storage_dtype: *storage_dtype,
                payload_indexes,
                intent: VectorDirectWriteIntent::Insert,
                on_conflict_updates: &[],
            })?;
            Some(wal.append_vector_direct_upsert(tenant_id, vshard_id, database_id, &entry)?)
        }
        VectorOp::DirectInsertIfAbsent {
            collection,
            field,
            surrogate,
            pk_bytes,
            vector,
            payload,
            quantization,
            storage_dtype,
            payload_indexes,
            returning: _,
            rls_filters: _,
        } => {
            let entry = encode_vector_direct_upsert_payload(VectorDirectUpsertPayload {
                collection: collection.as_str(),
                field,
                surrogate: *surrogate,
                pk_bytes,
                vector,
                payload,
                quantization: *quantization,
                storage_dtype: *storage_dtype,
                payload_indexes,
                intent: VectorDirectWriteIntent::InsertIfAbsent,
                on_conflict_updates: &[],
            })?;
            Some(wal.append_vector_direct_upsert(tenant_id, vshard_id, database_id, &entry)?)
        }
        VectorOp::DirectDelete {
            collection,
            field,
            targets,
            returning: _,
            rls_filters: _,
            rls_write_check: _,
        } => {
            let entry = encode_vector_direct_delete_payload(collection.as_str(), field, targets)?;
            Some(wal.append_vector_direct_delete(tenant_id, vshard_id, database_id, &entry)?)
        }
        // `restart_identity` is applied by the Control Plane after commit,
        // against the sequence store; the Data-Plane record carries only what
        // replay re-applies.
        VectorOp::DirectTruncate {
            collection,
            field,
            restart_identity: _,
        } => {
            let entry = encode_vector_direct_truncate_payload(collection.as_str(), field)?;
            Some(wal.append_vector_direct_truncate(tenant_id, vshard_id, database_id, &entry)?)
        }
        VectorOp::DirectUpdate {
            collection,
            field,
            targets,
            new_vector,
            payload_patch,
            quantization,
            storage_dtype,
            payload_indexes,
            returning: _,
            rls_filters: _,
            rls_write_check: _,
        } => {
            let entry = encode_vector_direct_update_payload(VectorDirectUpdatePayload {
                collection: collection.as_str(),
                field,
                targets,
                new_vector: new_vector.as_deref(),
                payload_patch,
                quantization: *quantization,
                storage_dtype: *storage_dtype,
                payload_indexes,
            })?;
            Some(wal.append_vector_direct_update(tenant_id, vshard_id, database_id, &entry)?)
        }
        // The reply and the verdict are per request; replay re-applies the
        // rows, it answers nobody and re-decides nothing.
        VectorOp::ResolvedDirectWrite {
            collection,
            field,
            quantization,
            storage_dtype,
            payload_indexes,
            mutations,
            response_payload: _,
            rls_write_check: _,
        } => {
            let entry =
                encode_vector_resolved_direct_write_payload(VectorResolvedDirectWritePayload {
                    collection: collection.as_str(),
                    field,
                    quantization: *quantization,
                    storage_dtype: *storage_dtype,
                    payload_indexes,
                    mutations,
                })?;
            Some(wal.append_vector_resolved_direct_write(
                tenant_id,
                vshard_id,
                database_id,
                &entry,
            )?)
        }
        // Read-only: it reports what the wrapped write would do and mutates
        // nothing.
        VectorOp::ResolveDirectWrite(_) => None,
        VectorOp::SparseInsert {
            collection,
            field_name,
            doc_id,
            entries,
        } => {
            let entry =
                encode_sparse_vector_put_payload(collection.as_str(), field_name, doc_id, entries)?;
            Some(wal.append_sparse_vector_put(tenant_id, vshard_id, database_id, &entry)?)
        }
        VectorOp::SparseDelete {
            collection,
            field_name,
            doc_id,
        } => {
            let entry =
                encode_sparse_vector_delete_payload(collection.as_str(), field_name, doc_id)?;
            Some(wal.append_sparse_vector_delete(tenant_id, vshard_id, database_id, &entry)?)
        }
        VectorOp::MultiVectorInsert {
            collection,
            field_name,
            document_surrogate,
            vectors,
            count,
            dim,
        } => {
            let entry = encode_multi_vector_put_payload(
                collection.as_str(),
                field_name,
                *document_surrogate,
                vectors,
                *count,
                *dim,
            )?;
            Some(wal.append_multi_vector_put(tenant_id, vshard_id, database_id, &entry)?)
        }
        VectorOp::MultiVectorDelete {
            collection,
            field_name,
            document_surrogate,
        } => {
            let entry = encode_multi_vector_delete_payload(
                collection.as_str(),
                field_name,
                *document_surrogate,
            )?;
            Some(wal.append_multi_vector_delete(tenant_id, vshard_id, database_id, &entry)?)
        }
        // Reads: no durable effect.
        VectorOp::Search { .. }
        | VectorOp::MultiSearch { .. }
        | VectorOp::SparseSearch { .. }
        | VectorOp::MultiVectorScoreSearch { .. }
        | VectorOp::QueryStats { .. } => None,
        // Index maintenance: reorganizes an index that is itself rebuilt from
        // the replayed writes plus checkpoints. No logical row is created or
        // destroyed, so no durable record is needed.
        VectorOp::Seal { .. } | VectorOp::CompactIndex { .. } | VectorOp::Rebuild { .. } => None,
    };
    Ok(appended)
}

/// Append a vector delete-by-surrogate to the WAL and return the assigned LSN.
///
/// Encodes `(collection, surrogate_u32, field_name, provenance)` as a `VectorDelete`
/// record. The replay decoder uses a surrogate-aware arm (4-element shape) that maps
/// back to `execute_vector_delete_by_surrogate`; the legacy 2-element and 3-element
/// delete arms fall through to direct node-id deletion and remain backward-compatible.
pub fn wal_append_vector_delete_by_surrogate(
    wal: WalAppender<'_>,
    tenant_id: TenantId,
    vshard_id: VShardId,
    database_id: DatabaseId,
    args: VectorDeleteWalArgs<'_>,
) -> crate::Result<nodedb_types::Lsn> {
    let VectorDeleteWalArgs {
        collection,
        surrogate,
        field_name,
        provenance,
    } = args;
    let entry =
        encode_vector_delete_by_surrogate_payload(collection, surrogate, field_name, provenance)?;
    let lsn = wal.append_vector_delete(tenant_id, vshard_id, database_id, &entry)?;
    Ok(lsn)
}
