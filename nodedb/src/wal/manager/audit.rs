// SPDX-License-Identifier: BUSL-1.1

use super::core::WalManager;

impl WalManager {
    /// Write an audit entry durably to the dedicated audit WAL.
    ///
    /// `data_lsn` is the data WAL LSN this audit entry corresponds to.
    /// The audit entry is serialized, appended, and fsynced before returning.
    /// Returns the audit WAL LSN of the record, or 0 when no audit WAL is open.
    pub fn append_audit_durable(&self, audit_bytes: &[u8], data_lsn: u64) -> crate::Result<u64> {
        match self.audit_wal {
            Some(ref audit_wal) => audit_wal.append_durable(audit_bytes, data_lsn),
            None => Ok(0),
        }
    }

    /// Write a batch of audit entries durably with one fsync.
    ///
    /// Each item is `(audit_bytes, data_lsn)`. Returns the audit WAL LSN of each, in order.
    /// With no audit WAL open, every LSN is 0.
    pub fn append_audit_batch_durable(
        &self,
        records: &[(Vec<u8>, u64)],
    ) -> crate::Result<Vec<u64>> {
        match self.audit_wal {
            Some(ref audit_wal) => audit_wal.append_batch_durable(records),
            None => Ok(vec![0; records.len()]),
        }
    }

    /// Append a batch of audit entries with no sync. Returns the audit WAL LSN of each, in order.
    ///
    /// The entries are durable only after [`Self::sync_audit_through`] covers the last LSN.
    /// With no audit WAL open, every LSN is 0.
    pub fn append_audit_batch(&self, records: &[(Vec<u8>, u64)]) -> crate::Result<Vec<u64>> {
        match self.audit_wal {
            Some(ref audit_wal) => audit_wal.append_batch(records),
            None => Ok(vec![0; records.len()]),
        }
    }

    /// Wait until every audit WAL record at or below `audit_lsn` is durable.
    ///
    /// Concurrent callers share one fsync. A failed fsync reaches every caller it covered.
    pub fn sync_audit_through(&self, audit_lsn: u64) -> crate::Result<()> {
        match self.audit_wal {
            Some(ref audit_wal) => audit_wal.sync_through(audit_lsn),
            None => Ok(()),
        }
    }

    /// Audit WAL syncs issued since open, or 0 when no audit WAL is open.
    pub fn audit_sync_count(&self) -> u64 {
        self.audit_wal
            .as_ref()
            .map_or(0, |audit_wal| audit_wal.sync_count())
    }

    /// Recover all audit WAL entries for crash recovery.
    ///
    /// Returns `(data_lsn, audit_entry_bytes)` pairs in LSN order.
    pub fn recover_audit_entries(&self) -> crate::Result<Vec<(u64, Vec<u8>)>> {
        if let Some(ref audit_wal) = self.audit_wal {
            audit_wal.recover()
        } else {
            Ok(Vec::new())
        }
    }

    /// Visit every audit WAL record in LSN order, one at a time, until the visitor breaks.
    ///
    /// The visitor gets `(data_lsn, audit_entry_bytes)`. It can see a record twice.
    pub fn scan_audit_entries(
        &self,
        visit: impl FnMut(u64, &[u8]) -> crate::Result<std::ops::ControlFlow<()>>,
    ) -> crate::Result<()> {
        match self.audit_wal {
            Some(ref audit_wal) => audit_wal.scan(visit),
            None => Ok(()),
        }
    }

    /// Drop every audit WAL record at or below `audit_lsn`.
    ///
    /// The caller must pass only an LSN whose record, and every earlier one, the catalog has committed.
    pub fn truncate_audit_through(&self, audit_lsn: u64) -> crate::Result<()> {
        match self.audit_wal {
            Some(ref audit_wal) => audit_wal.truncate_through(audit_lsn),
            None => Ok(()),
        }
    }

    /// The highest audit WAL LSN written, or 0 when none is.
    pub fn audit_last_lsn(&self) -> crate::Result<u64> {
        match self.audit_wal {
            Some(ref audit_wal) => audit_wal.last_lsn(),
            None => Ok(0),
        }
    }

    /// Whether the durable audit WAL is available.
    pub fn has_audit_wal(&self) -> bool {
        self.audit_wal.is_some()
    }
}
