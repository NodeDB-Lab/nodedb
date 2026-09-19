// SPDX-License-Identifier: BUSL-1.1

//! WAL appends for checkpoints, batched timeseries/log writes, and array records.

use nodedb_wal::record::RecordType;

use super::core::WalManager;
use crate::types::{DatabaseId, Lsn, TenantId, VShardId};

impl WalManager {
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

    pub fn append_timeseries_batch(
        &self,
        tid: TenantId,
        vs: VShardId,
        db: DatabaseId,
        p: &[u8],
    ) -> crate::Result<Lsn> {
        self.append_record(RecordType::TimeseriesBatch, tid, vs, db, p)
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

    pub fn append_array_put(
        &self,
        tid: TenantId,
        vs: VShardId,
        db: DatabaseId,
        p: &[u8],
    ) -> crate::Result<Lsn> {
        self.append_record(RecordType::ArrayPut, tid, vs, db, p)
    }

    pub fn append_array_delete(
        &self,
        tid: TenantId,
        vs: VShardId,
        db: DatabaseId,
        p: &[u8],
    ) -> crate::Result<Lsn> {
        self.append_record(RecordType::ArrayDelete, tid, vs, db, p)
    }
}
