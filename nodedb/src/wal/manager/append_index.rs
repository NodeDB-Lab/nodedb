// SPDX-License-Identifier: BUSL-1.1

//! WAL appends for FTS, spatial, and graph index records, plus write-abort
//! and collection-tombstone markers.

use nodedb_wal::record::RecordType;

use super::appender::WalAppender;
use crate::types::{DatabaseId, Lsn, TenantId, VShardId};

impl WalAppender<'_> {
    /// Append an `FtsIndex` record. Payload is a length-prefixed `FtsIndexPayload`
    /// produced by `nodedb_wal::record::FtsIndexPayload::to_bytes()`.
    pub fn append_fts_index(
        &self,
        tid: TenantId,
        vs: VShardId,
        db: DatabaseId,
        p: &[u8],
    ) -> crate::Result<Lsn> {
        self.append_record(RecordType::FtsIndex, tid, vs, db, p)
    }

    /// Append an `FtsDelete` record. Payload is a length-prefixed `FtsDeletePayload`
    /// produced by `nodedb_wal::record::FtsDeletePayload::to_bytes()`.
    pub fn append_fts_delete(
        &self,
        tid: TenantId,
        vs: VShardId,
        db: DatabaseId,
        p: &[u8],
    ) -> crate::Result<Lsn> {
        self.append_record(RecordType::FtsDelete, tid, vs, db, p)
    }

    /// Append a `SpatialPut` record. Payload is a length-prefixed `SpatialPutPayload`
    /// produced by `nodedb_wal::record::SpatialPutPayload::to_bytes()`.
    pub fn append_spatial_put(
        &self,
        tid: TenantId,
        vs: VShardId,
        db: DatabaseId,
        p: &[u8],
    ) -> crate::Result<Lsn> {
        self.append_record(RecordType::SpatialPut, tid, vs, db, p)
    }

    /// Append a `SpatialDelete` record. Payload is a length-prefixed `SpatialDeletePayload`
    /// produced by `nodedb_wal::record::SpatialDeletePayload::to_bytes()`.
    pub fn append_spatial_delete(
        &self,
        tid: TenantId,
        vs: VShardId,
        db: DatabaseId,
        p: &[u8],
    ) -> crate::Result<Lsn> {
        self.append_record(RecordType::SpatialDelete, tid, vs, db, p)
    }

    /// Append a `GraphNodeLabelSet` record. Payload is produced by
    /// `encode_graph_node_label_payload`.
    pub fn append_graph_node_label_set(
        &self,
        tid: TenantId,
        vs: VShardId,
        db: DatabaseId,
        p: &[u8],
    ) -> crate::Result<Lsn> {
        self.append_record(RecordType::GraphNodeLabelSet, tid, vs, db, p)
    }

    /// Append a `GraphNodeLabelRemove` record. Payload is produced by
    /// `encode_graph_node_label_payload`.
    pub fn append_graph_node_label_remove(
        &self,
        tid: TenantId,
        vs: VShardId,
        db: DatabaseId,
        p: &[u8],
    ) -> crate::Result<Lsn> {
        self.append_record(RecordType::GraphNodeLabelRemove, tid, vs, db, p)
    }

    /// Append a `WriteAborted` record naming `aborted_lsn`, the LSN of a
    /// forward write record the executing engine refused after it was already
    /// appended.
    ///
    /// The returned LSN must be made durable before the refusal is
    /// acknowledged: the forward record may already have been fsynced by a
    /// concurrent writer's group commit, so an abort that is still only
    /// buffered leaves the refused write recoverable.
    pub fn append_write_aborted(
        &self,
        tid: TenantId,
        vs: VShardId,
        db: DatabaseId,
        aborted_lsn: Lsn,
    ) -> crate::Result<Lsn> {
        crate::fail_point_err!("wal::append_write_aborted", |detail: String| {
            crate::Error::Internal {
                detail: format!("write-aborted append failed (failpoint): {detail}"),
            }
        });
        let payload = nodedb_wal::WriteAbortedPayload::new(aborted_lsn.as_u64()).to_bytes();
        self.append_record(RecordType::WriteAborted, tid, vs, db, &payload)
    }

    /// Append a `CollectionTombstoned` record. Any subsequent replay
    /// that extracts this record will filter prior writes for
    /// `(tid, collection)` whose LSN is less than `purge_lsn`.
    ///
    /// `vshard_id` of `0` is conventional — tombstones are tenant-level
    /// metadata, not sharded user data. Replay filters on
    /// `(tenant_id, collection)` pair alone.
    pub fn append_collection_tombstone(
        &self,
        tid: TenantId,
        database_id: DatabaseId,
        collection: &str,
        purge_lsn: u64,
    ) -> crate::Result<Lsn> {
        let payload = nodedb_wal::CollectionTombstonePayload::new(collection, purge_lsn)
            .to_bytes()
            .map_err(crate::Error::Wal)?;
        self.append_record(
            RecordType::CollectionTombstoned,
            tid,
            VShardId::new(0),
            database_id,
            &payload,
        )
    }
}
