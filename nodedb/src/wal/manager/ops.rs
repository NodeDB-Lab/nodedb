// SPDX-License-Identifier: BUSL-1.1

use tracing::info;

use super::core::WalManager;
use crate::types::Lsn;

impl WalManager {
    /// Truncate old WAL segments that are fully below the checkpoint LSN.
    ///
    /// Deletes sealed segment files whose records are all below `checkpoint_lsn`.
    /// The active segment is never deleted. Safe to call only after a checkpoint
    /// has been confirmed — all engines have flushed their dirty pages.
    pub fn truncate_before(
        &self,
        checkpoint_lsn: Lsn,
    ) -> crate::Result<nodedb_wal::segment::TruncateResult> {
        let wal = self.wal.lock().unwrap_or_else(|p| p.into_inner());
        let result = wal
            .truncate_before(checkpoint_lsn.as_u64())
            .map_err(crate::Error::Wal)?;

        if result.segments_deleted > 0 {
            info!(
                checkpoint_lsn = checkpoint_lsn.as_u64(),
                segments_deleted = result.segments_deleted,
                bytes_reclaimed = result.bytes_reclaimed,
                "WAL truncated"
            );
        }

        Ok(result)
    }

    /// Flush all buffered records to disk (group commit / fsync).
    pub fn sync(&self) -> crate::Result<()> {
        let mut wal = self.wal.lock().unwrap_or_else(|p| p.into_inner());
        wal.sync().map_err(crate::Error::Wal)
    }

    /// Next LSN that will be assigned.
    pub fn next_lsn(&self) -> Lsn {
        let wal = self.wal.lock().unwrap_or_else(|p| p.into_inner());
        Lsn::new(wal.next_lsn())
    }

    /// Total WAL size on disk across all segments.
    pub fn total_size_bytes(&self) -> crate::Result<u64> {
        let wal = self.wal.lock().unwrap_or_else(|p| p.into_inner());
        wal.total_size_bytes().map_err(crate::Error::Wal)
    }

    /// List all WAL segment metadata (for monitoring).
    pub fn list_segments(&self) -> crate::Result<Vec<nodedb_wal::segment::SegmentMeta>> {
        let wal = self.wal.lock().unwrap_or_else(|p| p.into_inner());
        wal.list_segments().map_err(crate::Error::Wal)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{DatabaseId, TenantId, VShardId};
    use crate::wal::manager::NO_APPLY_KEY;

    #[test]
    fn next_lsn_continues_after_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal_dir");

        {
            let wal = WalManager::open_for_testing(&path).unwrap();
            wal.appender(NO_APPLY_KEY)
                .append_put(
                    TenantId::new(1),
                    VShardId::new(0),
                    DatabaseId::DEFAULT,
                    b"a",
                )
                .unwrap();
            wal.appender(NO_APPLY_KEY)
                .append_put(
                    TenantId::new(1),
                    VShardId::new(0),
                    DatabaseId::DEFAULT,
                    b"b",
                )
                .unwrap();
            wal.sync().unwrap();
        }

        let wal = WalManager::open_for_testing(&path).unwrap();
        assert_eq!(wal.next_lsn(), Lsn::new(3));

        let lsn = wal
            .appender(NO_APPLY_KEY)
            .append_put(
                TenantId::new(1),
                VShardId::new(0),
                DatabaseId::DEFAULT,
                b"c",
            )
            .unwrap();
        assert_eq!(lsn, Lsn::new(3));
    }

    #[test]
    fn truncate_reclaims_space() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal_dir");

        let wal = WalManager::open_for_testing(&path).unwrap();

        let t = TenantId::new(1);
        let v = VShardId::new(0);
        let db = DatabaseId::DEFAULT;

        for i in 0..10u32 {
            wal.appender(NO_APPLY_KEY)
                .append_put(t, v, db, format!("val-{i}").as_bytes())
                .unwrap();
        }
        wal.sync().unwrap();

        let result = wal.truncate_before(Lsn::new(5)).unwrap();
        assert_eq!(result.segments_deleted, 0);

        let records = wal.replay().unwrap();
        assert_eq!(records.len(), 10);
    }

    #[test]
    fn total_size_and_list_segments() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal_dir");

        let wal = WalManager::open_for_testing(&path).unwrap();
        wal.appender(NO_APPLY_KEY)
            .append_put(
                TenantId::new(1),
                VShardId::new(0),
                DatabaseId::DEFAULT,
                b"data",
            )
            .unwrap();
        wal.sync().unwrap();

        let size = wal.total_size_bytes().unwrap();
        assert!(size > 0);

        let segments = wal.list_segments().unwrap();
        assert_eq!(segments.len(), 1);
    }
}
