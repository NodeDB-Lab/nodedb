// SPDX-License-Identifier: BUSL-1.1

//! WAL appends for checkpoints, batched timeseries/log writes, and array records.

use nodedb_wal::record::RecordType;

use super::appender::WalAppender;
use crate::types::{DatabaseId, Lsn, TenantId, VShardId};

impl WalAppender<'_> {
    /// Append a checkpoint marker. Serializes the LSN before writing.
    pub fn append_checkpoint(
        &self,
        tid: TenantId,
        vs: VShardId,
        db: DatabaseId,
        checkpoint_lsn: u64,
    ) -> crate::Result<Lsn> {
        let payload =
            zerompk::to_msgpack_vec(&checkpoint_lsn).map_err(|e| crate::Error::Serialization {
                format: "msgpack".into(),
                detail: format!("checkpoint: {e}"),
            })?;
        self.append_record(RecordType::Checkpoint, tid, vs, db, &payload)
    }

    /// Append a timeseries or columnar batch. It is a row-write record: WAL
    /// replay rebuilds a timeseries ingest's events from it, so it carries
    /// the event source its write ran with.
    pub fn append_timeseries_batch(
        &self,
        tid: TenantId,
        vs: VShardId,
        db: DatabaseId,
        p: &[u8],
    ) -> crate::Result<Lsn> {
        self.append_row_record(RecordType::TimeseriesBatch, tid, vs, db, p)
    }

    pub fn append_log_batch(
        &self,
        tid: TenantId,
        vs: VShardId,
        db: DatabaseId,
        p: &[u8],
    ) -> crate::Result<Lsn> {
        self.append_record(RecordType::LogBatch, tid, vs, db, p)
    }

    /// Append an array cell put. It is a row-write record: replay keeps the
    /// stamps of a restored write's cells, so it carries the event source
    /// its write ran with.
    pub fn append_array_put(
        &self,
        tid: TenantId,
        vs: VShardId,
        db: DatabaseId,
        p: &[u8],
    ) -> crate::Result<Lsn> {
        self.append_row_record(RecordType::ArrayPut, tid, vs, db, p)
    }

    /// Append an array cell delete. It carries its write's event source, as
    /// [`Self::append_array_put`] does.
    pub fn append_array_delete(
        &self,
        tid: TenantId,
        vs: VShardId,
        db: DatabaseId,
        p: &[u8],
    ) -> crate::Result<Lsn> {
        self.append_row_record(RecordType::ArrayDelete, tid, vs, db, p)
    }
}
