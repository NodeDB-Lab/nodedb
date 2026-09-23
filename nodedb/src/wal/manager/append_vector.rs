// SPDX-License-Identifier: BUSL-1.1

//! WAL appends for the vector engine (dense, sparse, and multi-vector).

use nodedb_wal::record::RecordType;

use super::appender::WalAppender;
use crate::types::{DatabaseId, Lsn, TenantId, VShardId};

impl WalAppender<'_> {
    pub fn append_vector_put(
        &self,
        tid: TenantId,
        vs: VShardId,
        db: DatabaseId,
        p: &[u8],
    ) -> crate::Result<Lsn> {
        self.append_record(RecordType::VectorPut, tid, vs, db, p)
    }

    pub fn append_vector_delete(
        &self,
        tid: TenantId,
        vs: VShardId,
        db: DatabaseId,
        p: &[u8],
    ) -> crate::Result<Lsn> {
        self.append_record(RecordType::VectorDelete, tid, vs, db, p)
    }

    pub fn append_vector_params(
        &self,
        tid: TenantId,
        vs: VShardId,
        db: DatabaseId,
        p: &[u8],
    ) -> crate::Result<Lsn> {
        self.append_record(RecordType::VectorParams, tid, vs, db, p)
    }

    /// Append a `VectorIndexDrop` record. Payload is the
    /// `(collection, field_name)` tuple the drop targets.
    pub fn append_vector_index_drop(
        &self,
        tid: TenantId,
        vs: VShardId,
        db: DatabaseId,
        p: &[u8],
    ) -> crate::Result<Lsn> {
        self.append_record(RecordType::VectorIndexDrop, tid, vs, db, p)
    }

    /// Append a `VectorDirectUpsert` record for a vector-primary insert.
    /// Payload is produced by `encode_vector_direct_upsert_payload`.
    pub fn append_vector_direct_upsert(
        &self,
        tid: TenantId,
        vs: VShardId,
        db: DatabaseId,
        p: &[u8],
    ) -> crate::Result<Lsn> {
        self.append_record(RecordType::VectorDirectUpsert, tid, vs, db, p)
    }

    /// Append a `VectorDirectDelete` record for a vector-primary delete.
    /// Payload is produced by `encode_vector_direct_delete_payload`.
    pub fn append_vector_direct_delete(
        &self,
        tid: TenantId,
        vs: VShardId,
        db: DatabaseId,
        p: &[u8],
    ) -> crate::Result<Lsn> {
        self.append_record(RecordType::VectorDirectDelete, tid, vs, db, p)
    }

    /// Append a `VectorDirectTruncate` record for a vector-primary truncate.
    /// Payload is produced by `encode_vector_direct_truncate_payload`.
    pub fn append_vector_direct_truncate(
        &self,
        tid: TenantId,
        vs: VShardId,
        db: DatabaseId,
        p: &[u8],
    ) -> crate::Result<Lsn> {
        self.append_record(RecordType::VectorDirectTruncate, tid, vs, db, p)
    }

    /// Append a `VectorDirectUpdate` record for a vector-primary update.
    /// Payload is produced by `encode_vector_direct_update_payload`.
    pub fn append_vector_direct_update(
        &self,
        tid: TenantId,
        vs: VShardId,
        db: DatabaseId,
        p: &[u8],
    ) -> crate::Result<Lsn> {
        self.append_record(RecordType::VectorDirectUpdate, tid, vs, db, p)
    }

    /// Append a `VectorResolvedDirectWrite` record for a resolved
    /// vector-primary write. Payload is produced by
    /// `encode_vector_resolved_direct_write_payload`.
    pub fn append_vector_resolved_direct_write(
        &self,
        tid: TenantId,
        vs: VShardId,
        db: DatabaseId,
        p: &[u8],
    ) -> crate::Result<Lsn> {
        self.append_record(RecordType::VectorResolvedDirectWrite, tid, vs, db, p)
    }

    /// Append a `SparseVectorPut` record. Payload is produced by
    /// `encode_sparse_vector_put_payload`.
    pub fn append_sparse_vector_put(
        &self,
        tid: TenantId,
        vs: VShardId,
        db: DatabaseId,
        p: &[u8],
    ) -> crate::Result<Lsn> {
        self.append_record(RecordType::SparseVectorPut, tid, vs, db, p)
    }

    /// Append a `SparseVectorDelete` record. Payload is produced by
    /// `encode_sparse_vector_delete_payload`.
    pub fn append_sparse_vector_delete(
        &self,
        tid: TenantId,
        vs: VShardId,
        db: DatabaseId,
        p: &[u8],
    ) -> crate::Result<Lsn> {
        self.append_record(RecordType::SparseVectorDelete, tid, vs, db, p)
    }

    /// Append a `MultiVectorPut` record. Payload is produced by
    /// `encode_multi_vector_put_payload`.
    pub fn append_multi_vector_put(
        &self,
        tid: TenantId,
        vs: VShardId,
        db: DatabaseId,
        p: &[u8],
    ) -> crate::Result<Lsn> {
        self.append_record(RecordType::MultiVectorPut, tid, vs, db, p)
    }

    /// Append a `MultiVectorDelete` record. Payload is produced by
    /// `encode_multi_vector_delete_payload`.
    pub fn append_multi_vector_delete(
        &self,
        tid: TenantId,
        vs: VShardId,
        db: DatabaseId,
        p: &[u8],
    ) -> crate::Result<Lsn> {
        self.append_record(RecordType::MultiVectorDelete, tid, vs, db, p)
    }
}
