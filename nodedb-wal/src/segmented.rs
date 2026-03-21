//! Segmented WAL: manages a directory of segment files with automatic rollover
//! and truncation.
//!
//! This is the primary WAL interface for production use. It wraps `WalWriter`
//! and `WalReader` to provide:
//!
//! - Automatic segment rollover when the active segment exceeds a target size.
//! - Truncation of old segments after checkpoint confirmation.
//! - Transparent multi-segment replay for crash recovery.
//! - Legacy single-file WAL migration.
//!
//! ## Thread safety
//!
//! `SegmentedWal` is NOT `Send + Sync` by itself. The `WalManager` in the main
//! crate wraps it in a `Mutex` for thread-safe access from the Control Plane.

use std::fs;
use std::path::{Path, PathBuf};

use tracing::info;

use crate::error::{Result, WalError};
use crate::record::WalRecord;
use crate::segment::{
    DEFAULT_SEGMENT_TARGET_SIZE, SegmentMeta, TruncateResult, discover_segments, segment_path,
    truncate_segments,
};
use crate::writer::{WalWriter, WalWriterConfig};

/// Configuration for the segmented WAL.
#[derive(Debug, Clone)]
pub struct SegmentedWalConfig {
    /// WAL directory path.
    pub wal_dir: PathBuf,

    /// Target segment size in bytes. When the active segment exceeds this,
    /// a new segment is created. This is a soft limit — the current record
    /// is always completed before rolling.
    pub segment_target_size: u64,

    /// Writer configuration (alignment, buffer size, O_DIRECT).
    pub writer_config: WalWriterConfig,
}

impl SegmentedWalConfig {
    /// Create a config with defaults for the given directory.
    pub fn new(wal_dir: PathBuf) -> Self {
        Self {
            wal_dir,
            segment_target_size: DEFAULT_SEGMENT_TARGET_SIZE,
            writer_config: WalWriterConfig::default(),
        }
    }

    /// Create a config for testing (no O_DIRECT).
    pub fn for_testing(wal_dir: PathBuf) -> Self {
        Self {
            wal_dir,
            segment_target_size: DEFAULT_SEGMENT_TARGET_SIZE,
            writer_config: WalWriterConfig {
                use_direct_io: false,
                ..Default::default()
            },
        }
    }
}

/// A segmented write-ahead log.
///
/// Manages a directory of WAL segment files. Records are appended to the active
/// segment. When the active segment exceeds `segment_target_size`, a new segment
/// is created automatically on the next append.
pub struct SegmentedWal {
    /// WAL directory.
    wal_dir: PathBuf,

    /// The currently active writer (appending to the latest segment).
    writer: WalWriter,

    /// First LSN of the active segment (used for truncation safety).
    active_first_lsn: u64,

    /// Target size for segment rollover.
    segment_target_size: u64,

    /// Writer config (for creating new segments).
    writer_config: WalWriterConfig,

    /// Optional encryption key ring.
    encryption_ring: Option<crate::crypto::KeyRing>,
}

impl SegmentedWal {
    /// Open or create a segmented WAL in the given directory.
    ///
    /// On first startup, creates the directory and the first segment.
    /// On subsequent startups, discovers existing segments and opens the
    /// last one for continued appending.
    pub fn open(config: SegmentedWalConfig) -> Result<Self> {
        fs::create_dir_all(&config.wal_dir).map_err(WalError::Io)?;

        let segments = discover_segments(&config.wal_dir)?;

        let (writer, active_first_lsn) = if segments.is_empty() {
            // Fresh WAL — create the first segment starting at LSN 1.
            let path = segment_path(&config.wal_dir, 1);
            let writer = WalWriter::open(&path, config.writer_config.clone())?;
            (writer, 1u64)
        } else {
            // Resume from the last segment.
            let last = &segments[segments.len() - 1];
            let writer = WalWriter::open(&last.path, config.writer_config.clone())?;
            (writer, last.first_lsn)
        };

        info!(
            wal_dir = %config.wal_dir.display(),
            segments = segments.len().max(1),
            active_first_lsn,
            next_lsn = writer.next_lsn(),
            "segmented WAL opened"
        );

        Ok(Self {
            wal_dir: config.wal_dir,
            writer,
            active_first_lsn,
            segment_target_size: config.segment_target_size,
            writer_config: config.writer_config,
            encryption_ring: None,
        })
    }

    /// Set the encryption key ring. All subsequent records will be encrypted.
    pub fn set_encryption_ring(&mut self, ring: crate::crypto::KeyRing) {
        self.writer.set_encryption_ring(ring.clone());
        self.encryption_ring = Some(ring);
    }

    /// Get the encryption key ring (for replay decryption).
    pub fn encryption_ring(&self) -> Option<&crate::crypto::KeyRing> {
        self.encryption_ring.as_ref()
    }

    /// Append a record to the WAL. Returns the assigned LSN.
    ///
    /// If the active segment has exceeded the target size, a new segment is
    /// created before appending. The rollover is transparent to the caller.
    pub fn append(
        &mut self,
        record_type: u16,
        tenant_id: u32,
        vshard_id: u16,
        payload: &[u8],
    ) -> Result<u64> {
        // Check if we need to roll to a new segment.
        if self.writer.file_offset() >= self.segment_target_size {
            self.roll_segment()?;
        }

        self.writer.append(record_type, tenant_id, vshard_id, payload)
    }

    /// Flush all buffered records and fsync the active segment.
    pub fn sync(&mut self) -> Result<()> {
        self.writer.sync()
    }

    /// The next LSN that will be assigned.
    pub fn next_lsn(&self) -> u64 {
        self.writer.next_lsn()
    }

    /// First LSN of the active (currently written) segment.
    pub fn active_segment_first_lsn(&self) -> u64 {
        self.active_first_lsn
    }

    /// The WAL directory path.
    pub fn wal_dir(&self) -> &Path {
        &self.wal_dir
    }

    /// Truncate old WAL segments that are fully below the checkpoint LSN.
    ///
    /// Only sealed (non-active) segments are eligible for deletion.
    /// The active segment is never deleted.
    ///
    /// Returns the number of segments deleted and bytes reclaimed.
    pub fn truncate_before(&self, checkpoint_lsn: u64) -> Result<TruncateResult> {
        truncate_segments(&self.wal_dir, checkpoint_lsn, self.active_first_lsn)
    }

    /// Replay all committed records across all segments, in LSN order.
    ///
    /// Used for crash recovery on startup.
    pub fn replay(&self) -> Result<Vec<WalRecord>> {
        replay_all_segments(&self.wal_dir)
    }

    /// Replay only records with LSN >= `from_lsn`.
    ///
    /// Used when recovering from a checkpoint — records before the checkpoint
    /// LSN have already been applied from the snapshot.
    pub fn replay_from(&self, from_lsn: u64) -> Result<Vec<WalRecord>> {
        let all = self.replay()?;
        Ok(all
            .into_iter()
            .filter(|r| r.header.lsn >= from_lsn)
            .collect())
    }

    /// List all segment metadata (for monitoring / operational tooling).
    pub fn list_segments(&self) -> Result<Vec<SegmentMeta>> {
        discover_segments(&self.wal_dir)
    }

    /// Total WAL size on disk across all segments.
    pub fn total_size_bytes(&self) -> Result<u64> {
        let segments = discover_segments(&self.wal_dir)?;
        Ok(segments.iter().map(|s| s.file_size).sum())
    }

    /// Roll to a new segment: seal the current writer and create a new one.
    fn roll_segment(&mut self) -> Result<()> {
        // Flush and seal the current segment.
        self.writer.seal()?;

        // The new segment starts at the next LSN.
        let new_first_lsn = self.writer.next_lsn();
        let new_path = segment_path(&self.wal_dir, new_first_lsn);

        let mut new_writer =
            WalWriter::open_with_start_lsn(&new_path, self.writer_config.clone(), new_first_lsn)?;

        // Propagate encryption ring to the new writer.
        if let Some(ref ring) = self.encryption_ring {
            new_writer.set_encryption_ring(ring.clone());
        }

        self.writer = new_writer;
        self.active_first_lsn = new_first_lsn;

        info!(
            segment = %new_path.display(),
            first_lsn = new_first_lsn,
            "rolled to new WAL segment"
        );

        Ok(())
    }
}

/// Replay all records from all segments in a WAL directory, in LSN order.
///
/// Segments are read in order of their first_lsn. Within each segment,
/// records are read sequentially. This produces a globally ordered stream.
pub fn replay_all_segments(wal_dir: &Path) -> Result<Vec<WalRecord>> {
    let segments = discover_segments(wal_dir)?;
    let mut all_records = Vec::new();

    for seg in &segments {
        let reader = crate::reader::WalReader::open(&seg.path)?;
        for record_result in reader.records() {
            all_records.push(record_result?);
        }
    }

    Ok(all_records)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::record::RecordType;

    fn test_config(dir: &Path) -> SegmentedWalConfig {
        SegmentedWalConfig::for_testing(dir.to_path_buf())
    }

    #[test]
    fn create_and_append() {
        let dir = tempfile::tempdir().unwrap();
        let wal_dir = dir.path().join("wal");

        let mut wal = SegmentedWal::open(test_config(&wal_dir)).unwrap();
        let lsn1 = wal.append(RecordType::Put as u16, 1, 0, b"hello").unwrap();
        let lsn2 = wal.append(RecordType::Put as u16, 1, 0, b"world").unwrap();
        wal.sync().unwrap();

        assert_eq!(lsn1, 1);
        assert_eq!(lsn2, 2);
        assert_eq!(wal.next_lsn(), 3);
    }

    #[test]
    fn replay_after_close() {
        let dir = tempfile::tempdir().unwrap();
        let wal_dir = dir.path().join("wal");

        // Write records.
        {
            let mut wal = SegmentedWal::open(test_config(&wal_dir)).unwrap();
            wal.append(RecordType::Put as u16, 1, 0, b"first").unwrap();
            wal.append(RecordType::Delete as u16, 2, 1, b"second").unwrap();
            wal.append(RecordType::Put as u16, 1, 0, b"third").unwrap();
            wal.sync().unwrap();
        }

        // Reopen and replay.
        let wal = SegmentedWal::open(test_config(&wal_dir)).unwrap();
        let records = wal.replay().unwrap();
        assert_eq!(records.len(), 3);
        assert_eq!(records[0].payload, b"first");
        assert_eq!(records[1].payload, b"second");
        assert_eq!(records[2].payload, b"third");
        assert_eq!(wal.next_lsn(), 4);
    }

    #[test]
    fn automatic_segment_rollover() {
        let dir = tempfile::tempdir().unwrap();
        let wal_dir = dir.path().join("wal");

        // Use a tiny segment target to force rollover.
        let config = SegmentedWalConfig {
            wal_dir: wal_dir.clone(),
            segment_target_size: 100, // 100 bytes — will roll after ~2 records.
            writer_config: WalWriterConfig {
                use_direct_io: false,
                ..Default::default()
            },
        };

        let mut wal = SegmentedWal::open(config).unwrap();

        // Write enough records to trigger multiple rollovers.
        for i in 0..20u32 {
            let payload = format!("record-{i:04}");
            wal.append(RecordType::Put as u16, 1, 0, payload.as_bytes())
                .unwrap();
            wal.sync().unwrap();
        }

        // Should have created multiple segments.
        let segments = wal.list_segments().unwrap();
        assert!(
            segments.len() > 1,
            "expected multiple segments, got {}",
            segments.len()
        );

        // Replay should return all 20 records in order.
        let records = wal.replay().unwrap();
        assert_eq!(records.len(), 20);
        for (i, record) in records.iter().enumerate() {
            assert_eq!(record.header.lsn, (i + 1) as u64);
            let expected = format!("record-{i:04}");
            assert_eq!(record.payload, expected.as_bytes());
        }
    }

    #[test]
    fn truncation_removes_old_segments() {
        let dir = tempfile::tempdir().unwrap();
        let wal_dir = dir.path().join("wal");

        let config = SegmentedWalConfig {
            wal_dir: wal_dir.clone(),
            segment_target_size: 100,
            writer_config: WalWriterConfig {
                use_direct_io: false,
                ..Default::default()
            },
        };

        let mut wal = SegmentedWal::open(config).unwrap();

        // Write records to create multiple segments.
        for i in 0..20u32 {
            let payload = format!("record-{i:04}");
            wal.append(RecordType::Put as u16, 1, 0, payload.as_bytes())
                .unwrap();
            wal.sync().unwrap();
        }

        let segments_before = wal.list_segments().unwrap();
        assert!(segments_before.len() > 1);

        // Truncate with a checkpoint at LSN 15.
        let result = wal.truncate_before(15).unwrap();
        assert!(result.segments_deleted > 0);
        assert!(result.bytes_reclaimed > 0);

        let segments_after = wal.list_segments().unwrap();
        assert!(segments_after.len() < segments_before.len());

        // Remaining records should still be replayable.
        let records = wal.replay().unwrap();
        // At minimum, records from LSN 15+ should be present.
        assert!(records.iter().any(|r| r.header.lsn >= 15));
    }

    #[test]
    fn replay_from_checkpoint_lsn() {
        let dir = tempfile::tempdir().unwrap();
        let wal_dir = dir.path().join("wal");

        let mut wal = SegmentedWal::open(test_config(&wal_dir)).unwrap();
        for i in 0..10u32 {
            wal.append(RecordType::Put as u16, 1, 0, format!("r{i}").as_bytes())
                .unwrap();
        }
        wal.sync().unwrap();

        // Replay from LSN 6 — should return LSNs 6..=10.
        let records = wal.replay_from(6).unwrap();
        assert_eq!(records.len(), 5);
        assert_eq!(records[0].header.lsn, 6);
        assert_eq!(records[4].header.lsn, 10);
    }

    #[test]
    fn total_size_bytes() {
        let dir = tempfile::tempdir().unwrap();
        let wal_dir = dir.path().join("wal");

        let mut wal = SegmentedWal::open(test_config(&wal_dir)).unwrap();
        wal.append(RecordType::Put as u16, 1, 0, b"data").unwrap();
        wal.sync().unwrap();

        let size = wal.total_size_bytes().unwrap();
        assert!(size > 0);
    }

    #[test]
    fn reopen_continues_lsn() {
        let dir = tempfile::tempdir().unwrap();
        let wal_dir = dir.path().join("wal");

        {
            let mut wal = SegmentedWal::open(test_config(&wal_dir)).unwrap();
            wal.append(RecordType::Put as u16, 1, 0, b"a").unwrap();
            wal.append(RecordType::Put as u16, 1, 0, b"b").unwrap();
            wal.sync().unwrap();
        }

        {
            let mut wal = SegmentedWal::open(test_config(&wal_dir)).unwrap();
            assert_eq!(wal.next_lsn(), 3);
            let lsn = wal.append(RecordType::Put as u16, 1, 0, b"c").unwrap();
            assert_eq!(lsn, 3);
            wal.sync().unwrap();
        }

        let wal = SegmentedWal::open(test_config(&wal_dir)).unwrap();
        let records = wal.replay().unwrap();
        assert_eq!(records.len(), 3);
    }

    #[test]
    fn encryption_persists_across_segments() {
        let dir = tempfile::tempdir().unwrap();
        let wal_dir = dir.path().join("wal");

        let key = crate::crypto::WalEncryptionKey::from_bytes(&[42u8; 32]);
        let ring = crate::crypto::KeyRing::new(key);

        let config = SegmentedWalConfig {
            wal_dir: wal_dir.clone(),
            segment_target_size: 100, // Tiny to force rollover.
            writer_config: WalWriterConfig {
                use_direct_io: false,
                ..Default::default()
            },
        };

        let mut wal = SegmentedWal::open(config).unwrap();
        wal.set_encryption_ring(ring);

        // Write enough to trigger rollover.
        for i in 0..10u32 {
            wal.append(RecordType::Put as u16, 1, 0, format!("enc-{i}").as_bytes())
                .unwrap();
            wal.sync().unwrap();
        }

        // Verify multiple segments were created.
        assert!(wal.list_segments().unwrap().len() > 1);

        // Replay should work (reader handles encrypted records via DWB/checksum).
        // Note: encrypted records are readable because the reader doesn't decrypt —
        // decryption happens at the consumer level via decrypt_payload().
        let records = wal.replay().unwrap();
        assert_eq!(records.len(), 10);
        // All records should be marked as encrypted.
        assert!(records.iter().all(|r| r.is_encrypted()));
    }
}
