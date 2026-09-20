// SPDX-License-Identifier: BUSL-1.1

//! Vector serializer for transaction resolve.
//!
//! Unlike the KV / document / graph serializers, the vector serializer is
//! **plan-driven**, not overlay-driven. A vector-primary direct write does
//! stage a `StagedVectorRow` (`stage_write/stage_vector.rs`), but only so the
//! transaction's own reads see it; the redo record still comes from the plan.
//! A vector post-image is inexpressible — the HNSW graph mutation has no
//! compact absolute form — so the redo record logs the INSERT itself and
//! replay rebuilds the index
//! (`replay_vector_wal`, dispatched from the redo reconstitute path). This
//! module therefore reads the [`VectorOp`] plan node directly and emits the
//! SAME engine-native WAL sub-record shape the autocommit vector path produces,
//! reusing its encoders (`control::server::wal_dispatch::vector`) so producer
//! and replay never drift:
//!
//! * `Insert` → `RecordType::VectorPut`, the 7-element
//!   `(collection, vector, dim, field_name, doc_id_compat, surrogate, provenance)`
//!   shape carrying the row's cross-engine surrogate identity.
//! * `BatchInsert` → `RecordType::VectorPut`, the 3-element
//!   `(collection, vectors, dim)` headless-batch shape.
//! * `Delete` → `RecordType::VectorDelete`, `(collection, vector_id, None)`.
//! * `DeleteBySurrogate` → `RecordType::VectorDelete`,
//!   `(collection, surrogate, field_name, provenance)`.
//! * `DirectInsert` / `DirectInsertIfAbsent` / `DirectUpsert` →
//!   `RecordType::VectorDirectUpsert`, the 11-element vector-primary
//!   post-image with its intent (`replay_direct_upsert`).
//! * `DirectDelete` → `RecordType::VectorDirectDelete`,
//!   `(collection, field, targets)` (`replay_direct_delete`).
//! * `DirectTruncate` → `RecordType::VectorDirectTruncate`,
//!   `(collection, field)` (`replay_direct_truncate`).
//! * `DirectUpdate` → `RecordType::VectorDirectUpdate`, the 8-element
//!   vector-primary patch (`replay_direct_update`).
//! * `MultiVectorInsert` → `RecordType::MultiVectorPut`, the 6-element
//!   flattened multi-vector shape (`replay_multi_vector_put`).
//! * `MultiVectorDelete` → `RecordType::MultiVectorDelete`,
//!   `(collection, field_name, document_surrogate)` (`replay_multi_vector_delete`).
//! * `SparseInsert` → `RecordType::SparseVectorPut`,
//!   `(collection, field_name, doc_id, entries)` (`replay_sparse_put`).
//! * `SparseDelete` → `RecordType::SparseVectorDelete`,
//!   `(collection, field_name, doc_id)` (`replay_sparse_delete`).
//!
//! These five share the autocommit WAL shapes emitted by `wal_append_vector_op`
//! and decoded by `replay_vector_extended_wal`, which the redo replay path
//! invokes after `replay_vector_wal`, so producer and replay never drift.
//!
//! ## Ops that raise a typed error
//!
//! `SetParams` (vector-index DDL) raises a typed error, matching how the KV /
//! document serializers reject index / DDL ops: a `CREATE VECTOR INDEX` rides
//! its own autocommit `VectorParams` record, not a transaction redo.
//!
//! ## Ops that emit nothing
//!
//! Read and index-maintenance ops carry no persisted logical post-image: the
//! logical vectors survive via their `VectorPut` records and the index is
//! rebuilt from them on replay, so `Seal` / `CompactIndex` / `Rebuild` are
//! naturally reconstructed and need no redo sub-record.
//!
//! ## Determinism
//!
//! Emission is in plan order, which is already deterministic (the plan set is a
//! fixed `&[PhysicalPlan]`). A `VectorParams` record would have to precede its
//! puts on replay, but `SetParams` is rejected here, so ordering reduces to the
//! given plan order.

use nodedb_physical::physical_plan::VectorOp;
use nodedb_wal::record::RecordType;

use crate::control::server::wal_dispatch::{
    VectorDirectUpdatePayload, VectorDirectUpsertPayload, VectorResolvedDirectWritePayload,
    encode_multi_vector_delete_payload, encode_multi_vector_put_payload,
    encode_sparse_vector_delete_payload, encode_sparse_vector_put_payload,
    encode_vector_batch_put_payload, encode_vector_delete_by_surrogate_payload,
    encode_vector_delete_payload, encode_vector_direct_delete_payload,
    encode_vector_direct_truncate_payload, encode_vector_direct_update_payload,
    encode_vector_direct_upsert_payload, encode_vector_put_payload,
    encode_vector_resolved_direct_write_payload,
};
use crate::wal::RedoSubRecord;
use nodedb_physical::physical_plan::VectorDirectWriteIntent;

/// Append the redo sub-record(s) for a single vector plan op to `ops`.
///
/// Writes serialize to their engine-native record shape (`VectorPut` /
/// `VectorDelete` / `VectorDirectUpsert` / `MultiVectorPut` /
/// `MultiVectorDelete` / `SparseVectorPut` / `SparseVectorDelete`); read and
/// index-maintenance ops emit nothing; vector-index DDL (`SetParams`) raises a
/// typed error (see module docs).
pub(super) fn serialize_vector_op(
    op: &VectorOp,
    ops: &mut Vec<RedoSubRecord>,
) -> crate::Result<()> {
    match op {
        VectorOp::Insert {
            collection,
            vector,
            dim,
            field_name,
            surrogate,
            pk_bytes: _,
            provenance,
        } => {
            let payload = encode_vector_put_payload(
                collection.as_str(),
                vector,
                *dim,
                field_name,
                *surrogate,
                provenance.as_ref(),
            )?;
            ops.push(RedoSubRecord {
                record_type: RecordType::VectorPut as u32,
                payload,
            });
            Ok(())
        }
        VectorOp::BatchInsert {
            collection,
            vectors,
            dim,
            surrogates: _,
        } => {
            let payload = encode_vector_batch_put_payload(collection.as_str(), vectors, *dim)?;
            ops.push(RedoSubRecord {
                record_type: RecordType::VectorPut as u32,
                payload,
            });
            Ok(())
        }
        VectorOp::Delete {
            collection,
            vector_id,
        } => {
            let payload = encode_vector_delete_payload(collection.as_str(), *vector_id)?;
            ops.push(RedoSubRecord {
                record_type: RecordType::VectorDelete as u32,
                payload,
            });
            Ok(())
        }
        VectorOp::DeleteBySurrogate {
            collection,
            surrogate,
            field_name,
            provenance,
        } => {
            let payload = encode_vector_delete_by_surrogate_payload(
                collection.as_str(),
                *surrogate,
                field_name,
                provenance.as_ref(),
            )?;
            ops.push(RedoSubRecord {
                record_type: RecordType::VectorDelete as u32,
                payload,
            });
            Ok(())
        }

        // Read families: no persisted post-image.
        VectorOp::Search { .. }
        | VectorOp::MultiSearch { .. }
        | VectorOp::MultiVectorScoreSearch { .. }
        | VectorOp::SparseSearch { .. }
        | VectorOp::QueryStats { .. } => Ok(()),

        // Index maintenance: the logical vectors survive via their `VectorPut`
        // records and the index is rebuilt from them on replay, so seal /
        // compact / rebuild are reconstructed without a redo sub-record.
        VectorOp::Seal { .. } | VectorOp::CompactIndex { .. } | VectorOp::Rebuild { .. } => Ok(()),

        // Vector-index configuration DDL: rejected like the KV / document
        // index-DDL ops. No row-level post-image; a CREATE VECTOR INDEX rides
        // its own autocommit `VectorParams` record, not a transaction redo.
        VectorOp::SetParams { .. } => Err(crate::Error::PlanError {
            detail: "vector SetParams (index DDL) is not supported in transaction resolve"
                .to_string(),
        }),

        // Same contract on the teardown side: a DROP INDEX rides its own
        // autocommit `VectorIndexDrop` record, never a transaction redo.
        VectorOp::DropIndex { .. } => Err(crate::Error::PlanError {
            detail: "vector DropIndex (index DDL) is not supported in transaction resolve"
                .to_string(),
        }),

        // Vector-primary direct writes: full post-image plus the row's
        // existence intent, replayed via `replay_direct_upsert`. A redo record
        // replays a write, and a replayed write answers nobody — no client
        // session is behind it to receive rows, so the projection, its read
        // gate, and the already-decided write check are not carried.
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
            let payload = encode_vector_direct_upsert_payload(VectorDirectUpsertPayload {
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
            ops.push(RedoSubRecord {
                record_type: RecordType::VectorDirectUpsert as u32,
                payload,
            });
            Ok(())
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
            let payload = encode_vector_direct_upsert_payload(VectorDirectUpsertPayload {
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
            ops.push(RedoSubRecord {
                record_type: RecordType::VectorDirectUpsert as u32,
                payload,
            });
            Ok(())
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
            let payload = encode_vector_direct_upsert_payload(VectorDirectUpsertPayload {
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
            ops.push(RedoSubRecord {
                record_type: RecordType::VectorDirectUpsert as u32,
                payload,
            });
            Ok(())
        }
        // Vector-primary delete, replayed via `replay_direct_delete`.
        VectorOp::DirectDelete {
            collection,
            field,
            targets,
            returning: _,
            rls_filters: _,
            rls_write_check: _,
        } => {
            let payload = encode_vector_direct_delete_payload(collection.as_str(), field, targets)?;
            ops.push(RedoSubRecord {
                record_type: RecordType::VectorDirectDelete as u32,
                payload,
            });
            Ok(())
        }
        // Vector-primary truncate, replayed via `replay_direct_truncate`.
        // `restart_identity` is a Control-Plane sequence concern and never
        // enters the redo record.
        VectorOp::DirectTruncate {
            collection,
            field,
            restart_identity: _,
        } => {
            let payload = encode_vector_direct_truncate_payload(collection.as_str(), field)?;
            ops.push(RedoSubRecord {
                record_type: RecordType::VectorDirectTruncate as u32,
                payload,
            });
            Ok(())
        }
        // Vector-primary update, replayed via `replay_direct_update`.
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
            let payload = encode_vector_direct_update_payload(VectorDirectUpdatePayload {
                collection: collection.as_str(),
                field,
                targets,
                new_vector: new_vector.as_deref(),
                payload_patch,
                quantization: *quantization,
                storage_dtype: *storage_dtype,
                payload_indexes,
            })?;
            ops.push(RedoSubRecord {
                record_type: RecordType::VectorDirectUpdate as u32,
                payload,
            });
            Ok(())
        }
        // Resolved vector-primary write, replayed via
        // `replay_vector_resolved_direct_write`: the same record the
        // autocommit path appends, carrying every row's stored image.
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
            let payload =
                encode_vector_resolved_direct_write_payload(VectorResolvedDirectWritePayload {
                    collection: collection.as_str(),
                    field,
                    quantization: *quantization,
                    storage_dtype: *storage_dtype,
                    payload_indexes,
                    mutations,
                })?;
            ops.push(RedoSubRecord {
                record_type: RecordType::VectorResolvedDirectWrite as u32,
                payload,
            });
            Ok(())
        }
        // The resolve pass writes nothing; the mutations it decides are
        // proposed separately by the write-resolve orchestrator.
        VectorOp::ResolveDirectWrite(_) => Ok(()),
        // Multi-vector (ColBERT-style) insert, replayed via
        // `replay_multi_vector_put`.
        VectorOp::MultiVectorInsert {
            collection,
            field_name,
            document_surrogate,
            vectors,
            count,
            dim,
        } => {
            let payload = encode_multi_vector_put_payload(
                collection.as_str(),
                field_name,
                *document_surrogate,
                vectors,
                *count,
                *dim,
            )?;
            ops.push(RedoSubRecord {
                record_type: RecordType::MultiVectorPut as u32,
                payload,
            });
            Ok(())
        }
        // Multi-vector delete, replayed via `replay_multi_vector_delete`.
        VectorOp::MultiVectorDelete {
            collection,
            field_name,
            document_surrogate,
        } => {
            let payload = encode_multi_vector_delete_payload(
                collection.as_str(),
                field_name,
                *document_surrogate,
            )?;
            ops.push(RedoSubRecord {
                record_type: RecordType::MultiVectorDelete as u32,
                payload,
            });
            Ok(())
        }
        // Sparse-vector insert, replayed via `replay_sparse_put`.
        VectorOp::SparseInsert {
            collection,
            field_name,
            doc_id,
            entries,
        } => {
            let payload =
                encode_sparse_vector_put_payload(collection.as_str(), field_name, doc_id, entries)?;
            ops.push(RedoSubRecord {
                record_type: RecordType::SparseVectorPut as u32,
                payload,
            });
            Ok(())
        }
        // Sparse-vector delete, replayed via `replay_sparse_delete`.
        VectorOp::SparseDelete {
            collection,
            field_name,
            doc_id,
        } => {
            let payload =
                encode_sparse_vector_delete_payload(collection.as_str(), field_name, doc_id)?;
            ops.push(RedoSubRecord {
                record_type: RecordType::SparseVectorDelete as u32,
                payload,
            });
            Ok(())
        }
    }
}
