//! Dedicated audit WAL segment.
//!
//! Provides a separate, append-only WAL for audit entries with the same
//! O_DIRECT + fsync guarantees as the data WAL. The audit WAL is:
//! - Append-only (never compacted, never truncated)
//! - Durable (fsync on every commit batch)
//!
//! Audit entries are hash-chain linked at the application layer (see `audit.rs`).
//! This WAL stores the serialized entries and carries the data WAL LSN each
//! entry corresponds to, enabling cross-reference verification on crash recovery.

use std::path::{Path, PathBuf};
use std::sync::Mutex;

use nodedb_wal::record::RecordType;
use nodedb_wal::segmented::{SegmentedWal, SegmentedWalConfig};

/// Dedicated audit WAL segment manager.
///
/// Wraps a `SegmentedWal` in its own directory (`audit.wal/`).
/// Thread-safe via internal `Mutex`.
pub struct AuditWalSegment {
    wal: Mutex<SegmentedWal>,
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

        Ok(Self {
            wal: Mutex::new(wal),
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
            .append(RecordType::Put as u16, 0, 0, &payload)
            .map_err(crate::Error::Wal)?;

        Ok(lsn)
    }

    /// Sync the audit WAL to disk (fsync).
    pub fn sync(&self) -> crate::Result<()> {
        let mut wal = self.wal.lock().map_err(|_| crate::Error::Internal {
            detail: "audit WAL lock poisoned".into(),
        })?;
        wal.sync().map_err(crate::Error::Wal)
    }

    /// Append AND sync atomically. This is the primary durable write path.
    pub fn append_durable(&self, audit_bytes: &[u8], data_lsn: u64) -> crate::Result<u64> {
        let mut wal = self.wal.lock().map_err(|_| crate::Error::Internal {
            detail: "audit WAL lock poisoned".into(),
        })?;

        let mut payload = Vec::with_capacity(8 + audit_bytes.len());
        payload.extend_from_slice(&data_lsn.to_le_bytes());
        payload.extend_from_slice(audit_bytes);

        let lsn = wal
            .append(RecordType::Put as u16, 0, 0, &payload)
            .map_err(crate::Error::Wal)?;
        wal.sync().map_err(crate::Error::Wal)?;

        Ok(lsn)
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
            if record.payload.len() < 8 {
                tracing::warn!(
                    payload_len = record.payload.len(),
                    "skipping malformed audit WAL record (payload < 8 bytes)"
                );
                continue;
            }
            // Safe: length checked above guarantees exactly 8 bytes.
            let data_lsn = u64::from_le_bytes(
                record.payload[..8]
                    .try_into()
                    .expect("length checked above"),
            );
            let audit_bytes = record.payload[8..].to_vec();
            entries.push((data_lsn, audit_bytes));
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

    #[test]
    fn empty_recovery() {
        let dir = tempfile::tempdir().expect("create temp dir");
        let audit_dir = dir.path().join("audit.wal");

        let segment = AuditWalSegment::open(&audit_dir, false).expect("open audit WAL");
        let recovered = segment.recover().expect("recover empty audit WAL");
        assert!(recovered.is_empty());
    }
}
