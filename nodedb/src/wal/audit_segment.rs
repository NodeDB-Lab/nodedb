// SPDX-License-Identifier: BUSL-1.1

//! Dedicated audit WAL segment.
//!
//! Provides a separate, append-only WAL for audit entries with the same
//! O_DIRECT + fsync guarantees as the data WAL. The audit WAL is:
//! - Append-only between truncations
//! - Truncated only through a record the system catalog has committed
//! - Durable (fsync on every commit batch)
//!
//! Audit entries are hash-chain linked at the application layer (see `audit.rs`).
//! This WAL stores the serialized entries and carries the data WAL LSN each
//! entry corresponds to, enabling cross-reference verification on crash recovery.

use std::ops::ControlFlow;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use super::group_commit::GroupCommit;

use nodedb_wal::record::RecordType;
use nodedb_wal::segmented::{SegmentedWal, SegmentedWalConfig};

/// Scans one call makes when truncations keep removing segments under it.
const MAX_SCAN_ATTEMPTS: u32 = 5;

fn poisoned_lock() -> crate::Error {
    crate::Error::Internal {
        detail: "audit WAL lock poisoned".into(),
    }
}

/// Dedicated audit WAL segment manager.
///
/// Wraps a `SegmentedWal` in its own directory (`audit.wal/`).
/// Thread-safe via internal `Mutex`.
/// Appends and syncs are separate steps. Concurrent callers share syncs through [`GroupCommit`].
pub struct AuditWalSegment {
    wal: Mutex<SegmentedWal>,
    group: GroupCommit,
    /// Syncs issued since open, so a test can count them.
    syncs: AtomicU64,
}

impl AuditWalSegment {
    /// Open or create the audit WAL in the given directory.
    ///
    /// The directory is created if it doesn't exist. Uses the same O_DIRECT
    /// configuration as the data WAL.
    pub fn open(audit_dir: &Path, use_direct_io: bool) -> crate::Result<Self> {
        std::fs::create_dir_all(audit_dir).map_err(|e| crate::Error::Storage {
            engine: "audit_wal".into(),
            detail: format!("failed to create audit WAL directory: {e}"),
        })?;

        let mut config = SegmentedWalConfig::new(PathBuf::from(audit_dir));
        config.segment_target_size = 256 * 1024 * 1024;
        config.writer_config.use_direct_io = use_direct_io;

        let wal = SegmentedWal::open(config).map_err(crate::Error::Wal)?;
        // Every record that survived reopen is on disk.
        let durable_through = wal.next_lsn().saturating_sub(1);

        Ok(Self {
            wal: Mutex::new(wal),
            group: GroupCommit::new("audit_wal", durable_through),
            syncs: AtomicU64::new(0),
        })
    }

    /// Append a serialized audit entry to the audit WAL.
    ///
    /// `data_lsn` is the data WAL LSN this audit entry corresponds to.
    /// Returns the audit WAL's own LSN for the appended record.
    ///
    /// Uses `RecordType::Put` (0x01) — the record_type is not semantically
    /// meaningful for the audit WAL; it uses the same WAL format for
    /// compatibility with the existing reader/writer infrastructure.
    pub fn append(&self, audit_bytes: &[u8], data_lsn: u64) -> crate::Result<u64> {
        let mut wal = self.wal.lock().map_err(|_| crate::Error::Internal {
            detail: "audit WAL lock poisoned".into(),
        })?;

        // Prefix the payload with the data_lsn for cross-reference.
        let mut payload = Vec::with_capacity(8 + audit_bytes.len());
        payload.extend_from_slice(&data_lsn.to_le_bytes());
        payload.extend_from_slice(audit_bytes);

        let lsn = wal
            .append(RecordType::Put as u32, 0, 0, 0, &payload)
            .map_err(crate::Error::Wal)?;
        // Noted before the WAL mutex releases, so no sync can fail and prune before this LSN counts.
        self.group.note_appended(lsn);

        Ok(lsn)
    }

    /// Wait until every record at or below `lsn` is durable, sharing the sync with concurrent callers.
    ///
    /// The leader holds the `wal` mutex to write the buffer, and to write the double-write copy
    /// when that is on. It runs the WAL fsync with no mutex held, so an append runs during the fsync.
    /// That append waits for a later sync. A sync claims only LSNs its file held when it started.
    /// A roll during the fsync is safe. The roll fsyncs the old segment before it seals it.
    /// The caller must hold no lock the leader needs to reach its sync.
    /// A failed sync returns an error to every caller whose LSN it covered.
    pub fn sync_through(&self, lsn: u64) -> crate::Result<()> {
        self.sync_through_with(lsn, || {})
    }

    /// [`Self::sync_through`] that runs `before_fsync` in the fsync window, with no mutex held.
    fn sync_through_with(&self, lsn: u64, before_fsync: impl Fn()) -> crate::Result<()> {
        self.group
            .sync_through(lsn, || self.lead_sync(lsn, &before_fsync))
    }

    /// One leader's sync. Returns the highest LSN it covers, and its outcome.
    fn lead_sync(&self, lsn: u64, before_fsync: &impl Fn()) -> (u64, crate::Result<()>) {
        let Ok(mut wal) = self.wal.lock() else {
            return (lsn, Err(poisoned_lock()));
        };
        self.syncs.fetch_add(1, Ordering::Relaxed);
        let appended = wal.next_lsn().saturating_sub(1);
        let pending = match wal.begin_sync() {
            Ok(Some(pending)) => pending,
            Ok(None) => return (appended, Ok(())),
            Err(e) => return (appended, Err(crate::Error::Wal(e))),
        };
        let through = pending.through_lsn();
        drop(wal);

        before_fsync();
        let fsynced = pending.fsync();

        let Ok(mut wal) = self.wal.lock() else {
            return (through, Err(poisoned_lock()));
        };
        (
            through,
            wal.finish_sync(pending, fsynced).map_err(crate::Error::Wal),
        )
    }

    /// Visit every audit record in LSN order, one at a time, until the visitor breaks.
    ///
    /// The visitor gets `(data_lsn, audit_entry_bytes)`. A malformed record is skipped with a warning.
    /// The scan holds no mutex, so appends and syncs run during it.
    /// A truncation can remove a segment mid-scan, and the scan then starts over.
    /// So a visitor can see a record twice, and it must tolerate that.
    pub fn scan(
        &self,
        mut visit: impl FnMut(u64, &[u8]) -> crate::Result<ControlFlow<()>>,
    ) -> crate::Result<()> {
        let scanner = self.wal.lock().map_err(|_| poisoned_lock())?.scanner();
        let mut attempts = 0;
        loop {
            attempts += 1;
            let outcome = scanner.visit::<crate::Error>(|record| {
                let Some((data_lsn, audit_bytes)) = record.payload.split_first_chunk::<8>() else {
                    tracing::warn!(
                        payload_len = record.payload.len(),
                        "skipping malformed audit WAL record (payload < 8 bytes)"
                    );
                    return Ok(ControlFlow::Continue(()));
                };
                visit(u64::from_le_bytes(*data_lsn), audit_bytes)
            });
            match outcome {
                Err(crate::Error::Wal(nodedb_wal::WalError::Io(e)))
                    if e.kind() == std::io::ErrorKind::NotFound && attempts < MAX_SCAN_ATTEMPTS => {
                }
                other => return other,
            }
        }
    }

    /// Syncs issued since open.
    pub fn sync_count(&self) -> u64 {
        self.syncs.load(Ordering::Relaxed)
    }

    /// Append and wait until the record is durable. Returns its LSN.
    pub fn append_durable(&self, audit_bytes: &[u8], data_lsn: u64) -> crate::Result<u64> {
        let lsn = self.append(audit_bytes, data_lsn)?;
        self.sync_through(lsn)?;
        Ok(lsn)
    }

    /// Append every record of `records` in order, with no sync.
    ///
    /// Each item is `(audit_bytes, data_lsn)`. Returns the audit WAL LSN of each record, in order.
    /// The records are durable only after [`Self::sync_through`] covers the last LSN.
    pub fn append_batch(&self, records: &[(Vec<u8>, u64)]) -> crate::Result<Vec<u64>> {
        let mut wal = self.wal.lock().map_err(|_| crate::Error::Internal {
            detail: "audit WAL lock poisoned".into(),
        })?;
        let mut lsns = Vec::with_capacity(records.len());
        for (audit_bytes, data_lsn) in records {
            let mut payload = Vec::with_capacity(8 + audit_bytes.len());
            payload.extend_from_slice(&data_lsn.to_le_bytes());
            payload.extend_from_slice(audit_bytes);
            lsns.push(
                wal.append(RecordType::Put as u32, 0, 0, 0, &payload)
                    .map_err(crate::Error::Wal)?,
            );
        }
        // Noted before the WAL mutex releases, so no sync can fail and prune before this LSN counts.
        // The caller syncs through the last LSN only. A sync that covers it covers the earlier ones.
        if let Some(&last) = lsns.last() {
            self.group.note_appended(last);
        }
        Ok(lsns)
    }

    /// Append every record of `records` in order, then wait until they are durable.
    ///
    /// The batch shares one sync, and concurrent callers can share it too.
    pub fn append_batch_durable(&self, records: &[(Vec<u8>, u64)]) -> crate::Result<Vec<u64>> {
        let lsns = self.append_batch(records)?;
        if let Some(&last) = lsns.last() {
            self.sync_through(last)?;
        }
        Ok(lsns)
    }

    /// Drop every record at or below `lsn`.
    ///
    /// The segmented WAL deletes whole sealed segments only.
    /// So the active segment is sealed first when it holds a record at or below `lsn`.
    /// A sealed segment that also holds a record above `lsn` survives whole.
    /// The caller must pass only an LSN the system catalog has committed through.
    pub fn truncate_through(&self, lsn: u64) -> crate::Result<()> {
        let mut wal = self.wal.lock().map_err(|_| crate::Error::Internal {
            detail: "audit WAL lock poisoned".into(),
        })?;
        if lsn == 0 {
            return Ok(());
        }
        if wal.active_segment_first_lsn() <= lsn {
            wal.seal_active_segment().map_err(crate::Error::Wal)?;
        }
        wal.truncate_before(lsn.saturating_add(1))
            .map_err(crate::Error::Wal)?;
        Ok(())
    }

    /// The highest LSN written, or 0 when none is.
    pub fn last_lsn(&self) -> crate::Result<u64> {
        let wal = self.wal.lock().map_err(|_| crate::Error::Internal {
            detail: "audit WAL lock poisoned".into(),
        })?;
        Ok(wal.next_lsn().saturating_sub(1))
    }

    /// Replay all audit WAL entries for crash recovery.
    ///
    /// Returns `(data_lsn, audit_entry_bytes)` pairs in LSN order.
    /// The caller deserializes the audit entry bytes and rebuilds the in-memory cache.
    pub fn recover(&self) -> crate::Result<Vec<(u64, Vec<u8>)>> {
        let wal = self.wal.lock().map_err(|_| crate::Error::Internal {
            detail: "audit WAL lock poisoned".into(),
        })?;

        let records = wal.replay().map_err(crate::Error::Wal)?;

        let mut entries = Vec::with_capacity(records.len());
        for record in records {
            let Some((data_lsn, audit_bytes)) = record.payload.split_first_chunk::<8>() else {
                tracing::warn!(
                    payload_len = record.payload.len(),
                    "skipping malformed audit WAL record (payload < 8 bytes)"
                );
                continue;
            };
            entries.push((u64::from_le_bytes(*data_lsn), audit_bytes.to_vec()));
        }

        Ok(entries)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn append_and_recover() {
        let dir = tempfile::tempdir().expect("create temp dir");
        let audit_dir = dir.path().join("audit.wal");

        let segment = AuditWalSegment::open(&audit_dir, false).expect("open audit WAL");

        // Append two entries.
        let lsn1 = segment
            .append_durable(b"audit-entry-1", 100)
            .expect("append entry 1");
        let lsn2 = segment
            .append_durable(b"audit-entry-2", 200)
            .expect("append entry 2");
        assert!(lsn2 > lsn1);

        // Recover.
        let recovered = segment.recover().expect("recover audit WAL");
        assert_eq!(recovered.len(), 2);
        assert_eq!(recovered[0].0, 100); // data_lsn
        assert_eq!(recovered[0].1, b"audit-entry-1");
        assert_eq!(recovered[1].0, 200);
        assert_eq!(recovered[1].1, b"audit-entry-2");
    }

    /// An append runs inside the fsync window of a sync, with the lock free.
    #[test]
    fn an_append_completes_during_a_sync() {
        let dir = tempfile::tempdir().expect("create temp dir");
        let segment = AuditWalSegment::open(&dir.path().join("audit.wal"), false).expect("open");
        let first = segment.append(b"first", 1).expect("append first");
        let late = std::sync::Mutex::new(None);

        segment
            .sync_through_with(first, || {
                // A held mutex would deadlock this call on this thread.
                let lsn = segment.append(b"late", 2).expect("append during the sync");
                *late.lock().expect("late") = Some(lsn);
            })
            .expect("sync");

        let late = late.lock().expect("late").expect("the hook ran");
        assert!(late > first);
        assert_eq!(
            segment.group.synced(),
            first,
            "the sync claims only its own LSNs"
        );
        segment.sync_through(late).expect("sync the late record");
        assert_eq!(segment.group.synced(), late);
        assert_eq!(segment.recover().expect("recover").len(), 2);
    }

    /// A segment sealed between the flush and the fsync still leaves every record durable.
    #[test]
    fn a_roll_during_a_sync_keeps_its_records() {
        let dir = tempfile::tempdir().expect("create temp dir");
        let segment = AuditWalSegment::open(&dir.path().join("audit.wal"), false).expect("open");
        let first = segment.append(b"first", 1).expect("append first");

        segment
            .sync_through_with(first, || {
                segment.append(b"late", 2).expect("append during the sync");
                segment
                    .wal
                    .lock()
                    .expect("wal lock")
                    .seal_active_segment()
                    .expect("roll during the sync");
            })
            .expect("sync");
        let after = segment.append(b"after", 3).expect("append after the roll");
        segment.sync_through(after).expect("sync after the roll");

        let segments = segment.wal.lock().expect("wal lock").list_segments();
        assert_eq!(segments.expect("segments").len(), 2);
        let datas: Vec<u64> = segment
            .recover()
            .expect("recover")
            .iter()
            .map(|(data_lsn, _)| *data_lsn)
            .collect();
        assert_eq!(datas, vec![1, 2, 3]);
    }

    #[test]
    fn a_scan_visits_records_in_order_and_can_stop() {
        let dir = tempfile::tempdir().expect("create temp dir");
        let segment = AuditWalSegment::open(&dir.path().join("audit.wal"), false).expect("open");
        for i in 1..=3u64 {
            segment.append_durable(b"entry", i).expect("append");
        }
        let mut seen = Vec::new();
        segment
            .scan(|data_lsn, bytes| {
                assert_eq!(bytes, b"entry");
                seen.push(data_lsn);
                Ok(if seen.len() == 2 {
                    ControlFlow::Break(())
                } else {
                    ControlFlow::Continue(())
                })
            })
            .expect("scan");
        assert_eq!(seen, vec![1, 2]);
    }

    #[test]
    fn empty_recovery() {
        let dir = tempfile::tempdir().expect("create temp dir");
        let audit_dir = dir.path().join("audit.wal");

        let segment = AuditWalSegment::open(&audit_dir, false).expect("open audit WAL");
        let recovered = segment.recover().expect("recover empty audit WAL");
        assert!(recovered.is_empty());
    }
}
