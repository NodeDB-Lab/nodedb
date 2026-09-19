// SPDX-License-Identifier: BUSL-1.1

//! WAL appends for the columnar-family `TRUNCATE` records.

use nodedb_wal::record::RecordType;

use super::core::WalManager;
use crate::types::{DatabaseId, Lsn, TenantId, VShardId};

impl WalManager {
    /// Append a `ColumnarTruncate` record for a columnar or spatial truncate.
    /// Payload is produced by `encode_columnar_truncate_payload`.
    pub fn append_columnar_truncate(
        &self,
        tid: TenantId,
        vs: VShardId,
        db: DatabaseId,
        p: &[u8],
    ) -> crate::Result<Lsn> {
        self.append_record(RecordType::ColumnarTruncate, tid, vs, db, p)
    }

    /// Append a `TimeseriesTruncate` record for a timeseries truncate.
    /// Payload is produced by `encode_columnar_truncate_payload`.
    pub fn append_timeseries_truncate(
        &self,
        tid: TenantId,
        vs: VShardId,
        db: DatabaseId,
        p: &[u8],
    ) -> crate::Result<Lsn> {
        self.append_record(RecordType::TimeseriesTruncate, tid, vs, db, p)
    }
}
