// SPDX-License-Identifier: Apache-2.0

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

use crate::crypto::KeyRing;
use crate::error::{Result, WalError};
use crate::record::{RecordTarget, WalRecord};
use crate::segment::{
    DEFAULT_SEGMENT_TARGET_SIZE, SegmentContinuity, SegmentMeta, TruncateResult,
    check_retained_floor, discover_segments, segment_path, truncate_segments,
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
            // Creating the file makes its data durable on the next fsync, but
            // POSIX promises nothing about the name pointing at that inode.
            // Without this, a crash can leave the very first segment
            // unreachable and every record acknowledged into it lost.
            crate::segment::fsync_directory(&config.wal_dir)?;
            (writer, 1u64)
        } else {
            // Resume from the last segment. The filename's first LSN is the
            // floor for the resumed sequence — a segment that was rolled but
            // never written to (or whose records were all torn away) holds no
            // LSN to recover from, and restarting at 1 would re-issue LSNs the
            // earlier segments already own.
            let last = &segments[segments.len() - 1];
            let writer =
                WalWriter::open_resuming(&last.path, config.writer_config.clone(), last.first_lsn)?;
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
    ///
    /// Must be called before the first `append`. Returns an error if records
    /// have already been written to the active segment.
    pub fn set_encryption_ring(&mut self, ring: crate::crypto::KeyRing) -> Result<()> {
        self.writer.set_encryption_ring(ring.clone())?;
        self.encryption_ring = Some(ring);
        Ok(())
    }

    /// Configure encryption for a newly created WAL or roll an already-open
    /// active segment before switching keys on restart.
    pub fn configure_encryption_ring(&mut self, ring: crate::crypto::KeyRing) -> Result<()> {
        if self.writer.can_set_encryption_ring() {
            self.set_encryption_ring(ring)
        } else {
            self.rotate_encryption_ring(ring)
        }
    }

    /// Get the encryption key ring (for replay decryption).
    pub fn encryption_ring(&self) -> Option<&crate::crypto::KeyRing> {
        self.encryption_ring.as_ref()
    }

    /// Seal the active segment and start a new segment under `ring`.
    /// This is the only safe runtime key-rotation boundary because a segment's
    /// encryption epoch and key are fixed by its preamble.
    pub fn rotate_encryption_ring(&mut self, ring: crate::crypto::KeyRing) -> Result<()> {
        self.roll_segment_with_ring(Some(ring))
    }

    /// Append a record to the WAL. Returns the assigned LSN.
    ///
    /// If the active segment has exceeded the target size, a new segment is
    /// created before appending. The rollover is transparent to the caller.
    ///
    /// `database_id` is stored in header bytes 34-41. Pass `0` for the
    /// default database (backward-compatible with pre-existing records).
    pub fn append(
        &mut self,
        record_type: u32,
        tenant_id: u64,
        vshard_id: u32,
        database_id: u64,
        payload: &[u8],
    ) -> Result<u64> {
        self.append_keyed(
            RecordTarget {
                record_type,
                tenant_id,
                vshard_id,
                database_id,
                event_source: crate::record::NO_EVENT_SOURCE,
            },
            payload,
            0,
        )
    }

    /// [`Self::append`] for a record appended by the apply of the replicated
    /// proposal `apply_key`, carrying the event source in `target` (see
    /// [`crate::WalRecord::new_stamped`]).
    pub fn append_keyed(
        &mut self,
        target: RecordTarget,
        payload: &[u8],
        apply_key: u64,
    ) -> Result<u64> {
        // Check if we need to roll to a new segment.
        if self.writer.file_offset() >= self.segment_target_size {
            self.roll_segment()?;
        }

        self.writer.append_keyed(target, payload, apply_key)
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
    /// Used for crash recovery on startup. Payloads are returned as plaintext:
    /// this WAL's own key ring is applied to every encrypted record.
    pub fn replay(&self) -> Result<Vec<WalRecord>> {
        replay_all_segments(&self.wal_dir, self.encryption_ring.as_ref())
    }

    /// Replay only records with LSN >= `from_lsn`.
    ///
    /// Used when recovering from a checkpoint — records before the checkpoint
    /// LSN have already been applied from the snapshot.
    ///
    /// Fails with [`WalError::ReplayBelowRetainedFloor`] when `from_lsn` names
    /// an LSN truncation has already deleted, rather than returning the shorter
    /// suffix that survives. Delegating to `replay_from_limit_dir` keeps that
    /// check — and the per-segment continuity and torn-tail checks — in one
    /// place instead of replaying everything and filtering afterwards.
    pub fn replay_from(&self, from_lsn: u64) -> Result<Vec<WalRecord>> {
        Ok(replay_from_limit_dir(
            &self.wal_dir,
            from_lsn,
            usize::MAX,
            self.encryption_ring.as_ref(),
        )?
        .0)
    }

    /// Paginated replay (delegates to standalone `replay_from_limit_dir`).
    pub fn replay_from_limit(
        &self,
        from_lsn: u64,
        max_records: usize,
    ) -> Result<(Vec<WalRecord>, bool)> {
        replay_from_limit_dir(
            &self.wal_dir,
            from_lsn,
            max_records,
            self.encryption_ring.as_ref(),
        )
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
        let next_ring = self
            .encryption_ring
            .as_ref()
            .map(|ring| {
                ring.current()
                    .with_fresh_epoch()
                    .map(crate::crypto::KeyRing::new)
            })
            .transpose()?;
        self.roll_segment_with_ring(next_ring)
    }

    /// The new segment is built and validated first, and the old writer is
    /// sealed only once nothing fallible remains. Sealing earlier would leave
    /// a writer that rejects every append still installed whenever a later
    /// step failed — a roll that fails once would shut the WAL down for the
    /// life of the process. Here a failure returns with the old writer intact
    /// and still accepting appends; the next append simply retries the roll.
    ///
    /// The two ordering constraints the log depends on still hold: the seal
    /// precedes the install, so no record can land in the old segment past the
    /// new segment's declared `first_lsn`; and the directory fsync precedes
    /// the install, so no record is acknowledged into a segment whose name
    /// might not survive a crash.
    fn roll_segment_with_ring(&mut self, next_ring: Option<crate::crypto::KeyRing>) -> Result<()> {
        // Reading the next LSN does not consume it, so this is the same value
        // the seal below would leave behind — nothing appends in between.
        let new_first_lsn = self.writer.next_lsn();
        let new_path = segment_path(&self.wal_dir, new_first_lsn);
        let mut new_writer =
            WalWriter::open_with_start_lsn(&new_path, self.writer_config.clone(), new_first_lsn)?;

        crate::segment::fsync_directory(&self.wal_dir)?;

        if let Some(ref ring) = next_ring {
            new_writer.set_encryption_ring(ring.clone())?;
        }

        // Last fallible step. A seal is a durability barrier over the old
        // segment's buffered records; if it fails those records were never
        // made durable, and the writer reports that on every later call — a
        // real data-loss error, not a bookkeeping state this roll created.
        self.writer.seal()?;

        // Everything past here is infallible, so no failure can strand a
        // sealed writer that was never replaced.
        self.encryption_ring = next_ring;
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
///
/// This is `replay_from_limit_dir` with no LSN floor and no record cap, so the
/// per-segment continuity/torn-tail checks live in exactly one place.
///
/// `keys` must be the key ring the directory was written under, or `None` for a
/// WAL that was never encrypted. It is a required argument rather than an
/// opt-in so that no caller can reach ciphertext by omission: an encrypted
/// record met with `keys == None` fails with
/// [`WalError::EncryptedRecordWithoutKey`].
pub fn replay_all_segments(wal_dir: &Path, keys: Option<&KeyRing>) -> Result<Vec<WalRecord>> {
    Ok(replay_from_limit_dir(wal_dir, 0, usize::MAX, keys)?.0)
}

/// Paginated replay from a WAL directory: reads at most `max_records` from `from_lsn`.
///
/// Uses sequential I/O (not mmap) and does NOT require the WAL mutex — safe
/// to call concurrently with writes. Sealed segments are immutable; the active
/// segment is read via buffered I/O which sees data after the writer's fsync.
///
/// Returns `(records, has_more)` where `has_more` is `true` if the limit was hit.
///
/// Records are returned as plaintext. See [`replay_all_segments`] for why
/// `keys` is mandatory.
///
/// A `from_lsn` below the earliest LSN the directory still retains fails with
/// [`WalError::ReplayBelowRetainedFloor`] instead of returning the surviving
/// (shorter) suffix.
pub fn replay_from_limit_dir(
    wal_dir: &Path,
    from_lsn: u64,
    max_records: usize,
    keys: Option<&KeyRing>,
) -> Result<(Vec<WalRecord>, bool)> {
    let segments = discover_segments(wal_dir)?;
    // Before any record is read: the answer to "is the requested suffix still
    // here?" is in the segment set, and once records start flowing back a short
    // result is indistinguishable from a complete one.
    check_retained_floor(&segments, from_lsn)?;
    let mut records = Vec::with_capacity(max_records.min(4096));
    let mut continuity = SegmentContinuity::new();

    for seg in &segments {
        continuity.check(seg)?;
        // The reader decrypts: records reach the caller as plaintext or not at
        // all.
        let mut reader = crate::reader::WalReader::open(&seg.path, keys)?;
        // Tracked over everything the segment contains, not over what survives
        // the `from_lsn` filter — continuity is a property of the log, not of
        // the caller's window into it.
        let mut last_lsn = 0u64;
        while let Some(record) = reader.next_record()? {
            last_lsn = record.header.lsn;
            if record.header.lsn >= from_lsn {
                records.push(record);
                if records.len() >= max_records {
                    // Stopping short leaves the rest of this segment unread, so
                    // its end LSN is unknown and no boundary can be judged.
                    return Ok((records, true));
                }
            }
        }
        // A segment that stops early may be an interrupted final write, or it
        // may be a hole with committed records behind it. Only the first is a
        // legal end of the log.
        crate::torn_tail::verify_committed_prefix(&seg.path, reader.stop_reason(), last_lsn)?;
        continuity.completed(seg, last_lsn);
    }

    Ok((records, false))
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
        let lsn1 = wal
            .append(RecordType::Put as u32, 1, 0, 0, b"hello")
            .unwrap();
        let lsn2 = wal
            .append(RecordType::Put as u32, 1, 0, 0, b"world")
            .unwrap();
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
            wal.append(RecordType::Put as u32, 1, 0, 0, b"first")
                .unwrap();
            wal.append(RecordType::Delete as u32, 2, 1, 0, b"second")
                .unwrap();
            wal.append(RecordType::Put as u32, 1, 0, 0, b"third")
                .unwrap();
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
            wal.append(RecordType::Put as u32, 1, 0, 0, payload.as_bytes())
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
            wal.append(RecordType::Put as u32, 1, 0, 0, payload.as_bytes())
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
            wal.append(RecordType::Put as u32, 1, 0, 0, format!("r{i}").as_bytes())
                .unwrap();
        }
        wal.sync().unwrap();

        // Replay from LSN 6 — should return LSNs 6..=10.
        let records = wal.replay_from(6).unwrap();
        assert_eq!(records.len(), 5);
        assert_eq!(records[0].header.lsn, 6);
        assert_eq!(records[4].header.lsn, 10);
    }

    /// Build a WAL with several segments, truncate its prefix away, and report
    /// the earliest LSN that survives.
    fn truncated_wal(wal_dir: &Path) -> (SegmentedWal, u64) {
        let config = SegmentedWalConfig {
            wal_dir: wal_dir.to_path_buf(),
            segment_target_size: 100,
            writer_config: WalWriterConfig {
                use_direct_io: false,
                ..Default::default()
            },
        };

        let mut wal = SegmentedWal::open(config).expect("open");
        for i in 0..40u32 {
            wal.append(
                RecordType::Put as u32,
                1,
                0,
                0,
                format!("record-{i:04}").as_bytes(),
            )
            .expect("append");
            wal.sync().expect("sync");
        }

        let result = wal.truncate_before(20).expect("truncate");
        assert!(
            result.segments_deleted > 0,
            "the test needs a truncated prefix to have a floor above LSN 1"
        );

        let floor = wal.list_segments().expect("list")[0].first_lsn;
        assert!(floor > 1, "expected the retained floor to have moved up");
        (wal, floor)
    }

    #[test]
    fn replay_from_the_retained_floor_returns_the_whole_suffix() {
        let dir = tempfile::tempdir().unwrap();
        let (wal, floor) = truncated_wal(&dir.path().join("wal"));

        let records = wal.replay_from(floor).expect("floor is fully retained");
        assert_eq!(records[0].header.lsn, floor);
        assert_eq!(records.last().unwrap().header.lsn, 40);
        assert_eq!(records.len() as u64, 40 - floor + 1);
    }

    /// The off-by-one guard: the floor itself is the first retained LSN, so a
    /// request starting exactly there is complete, not short.
    #[test]
    fn replay_from_exactly_the_floor_is_not_an_error() {
        let dir = tempfile::tempdir().unwrap();
        let (wal, floor) = truncated_wal(&dir.path().join("wal"));

        assert!(wal.replay_from(floor).is_ok());
        // One below is the first request that cannot be served.
        assert!(wal.replay_from(floor - 1).is_err());
    }

    #[test]
    fn replay_below_the_retained_floor_is_a_typed_error_naming_both_lsns() {
        let dir = tempfile::tempdir().unwrap();
        let (wal, floor) = truncated_wal(&dir.path().join("wal"));

        match wal.replay_from(1) {
            Err(WalError::ReplayBelowRetainedFloor {
                from_lsn,
                retained_floor_lsn,
                ..
            }) => {
                assert_eq!(from_lsn, 1);
                assert_eq!(retained_floor_lsn, floor);
            }
            other => panic!("expected a retained-floor violation, got {other:?}"),
        }

        // The paginated variant answers the same way: a bounded read must not
        // be the way a caller gets a silently short suffix either.
        assert!(matches!(
            wal.replay_from_limit(1, 5),
            Err(WalError::ReplayBelowRetainedFloor { .. })
        ));

        // Full replay names no particular record and is unaffected.
        assert!(wal.replay().is_ok());
    }

    #[test]
    fn total_size_bytes() {
        let dir = tempfile::tempdir().unwrap();
        let wal_dir = dir.path().join("wal");

        let mut wal = SegmentedWal::open(test_config(&wal_dir)).unwrap();
        wal.append(RecordType::Put as u32, 1, 0, 0, b"data")
            .unwrap();
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
            wal.append(RecordType::Put as u32, 1, 0, 0, b"a").unwrap();
            wal.append(RecordType::Put as u32, 1, 0, 0, b"b").unwrap();
            wal.sync().unwrap();
        }

        {
            let mut wal = SegmentedWal::open(test_config(&wal_dir)).unwrap();
            assert_eq!(wal.next_lsn(), 3);
            let lsn = wal.append(RecordType::Put as u32, 1, 0, 0, b"c").unwrap();
            assert_eq!(lsn, 3);
            wal.sync().unwrap();
        }

        let wal = SegmentedWal::open(test_config(&wal_dir)).unwrap();
        let records = wal.replay().unwrap();
        assert_eq!(records.len(), 3);
    }

    #[test]
    fn encryption_persists_across_segments() {
        // Rewritten: actually decrypts across a simulated restart.
        //
        // Lifecycle:
        //  1. Write 10 records with encryption, forcing segment rollover.
        //  2. Drop the WAL (simulating process exit).
        //  3. Reopen a new reader and replay — records are marked encrypted.
        //  4. For each segment, open the reader, read its preamble epoch, and
        //     decrypt each record using that epoch. Verify payloads match.
        let dir = tempfile::tempdir().unwrap();
        let wal_dir = dir.path().join("wal");
        let key_bytes = [42u8; 32];

        let config = SegmentedWalConfig {
            wal_dir: wal_dir.clone(),
            segment_target_size: 100,
            writer_config: WalWriterConfig {
                use_direct_io: false,
                ..Default::default()
            },
        };

        // Step 1 & 2: write and drop.
        {
            let key = crate::crypto::WalEncryptionKey::from_bytes(&key_bytes).unwrap();
            let ring = crate::crypto::KeyRing::new(key);
            let mut wal = SegmentedWal::open(config.clone()).unwrap();
            wal.set_encryption_ring(ring).unwrap();

            for i in 0..10u32 {
                wal.append(
                    RecordType::Put as u32,
                    1,
                    0,
                    0,
                    format!("enc-{i}").as_bytes(),
                )
                .unwrap();
                wal.sync().unwrap();
            }
            assert!(wal.list_segments().unwrap().len() > 1);
        }

        // Step 3: reopen WAL (new in-memory epoch, simulates restart).
        let segments = crate::segment::discover_segments(&wal_dir).unwrap();
        assert!(
            segments.len() > 1,
            "expected multiple segments after rollover"
        );

        let key_for_read = crate::crypto::WalEncryptionKey::from_bytes(&key_bytes).unwrap();
        let ring_for_read = crate::crypto::KeyRing::new(key_for_read);

        let mut all_payloads = Vec::new();

        // Step 4: per-segment read + decrypt. Raw mode on purpose — this test
        // asserts what is actually on disk, then decrypts by hand.
        for seg in &segments {
            let reader = crate::reader::WalReader::open_raw(&seg.path).unwrap();
            // Read the epoch from the preamble written at segment open time.
            let epoch = *reader
                .segment_preamble()
                .expect("encrypted segment must have a preamble")
                .epoch();
            let preamble_bytes = reader.segment_preamble().unwrap().to_bytes();

            for record_result in reader.records() {
                let record = record_result.unwrap();
                assert!(record.is_encrypted(), "all records should be encrypted");
                let plaintext = record
                    .decrypt_payload_ring(&epoch, Some(&preamble_bytes), Some(&ring_for_read))
                    .unwrap();
                all_payloads.push(plaintext);
            }
        }

        assert_eq!(all_payloads.len(), 10);
        for (i, payload) in all_payloads.iter().enumerate() {
            assert_eq!(payload, format!("enc-{i}").as_bytes());
        }
    }

    /// WAL restart roundtrip with encryption.
    ///
    /// Writes encrypted records, simulates a process restart (different in-memory
    /// epoch), replays, and verifies that decryption succeeds because the epoch
    /// is read from the on-disk preamble rather than the current key's epoch.
    #[test]
    fn wal_encrypted_restart_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let wal_dir = dir.path().join("wal");
        let key_bytes = [0xABu8; 32];

        let config = SegmentedWalConfig::for_testing(wal_dir.clone());

        // Write with key lifetime 1.
        {
            let key = crate::crypto::WalEncryptionKey::from_bytes(&key_bytes).unwrap();
            let ring = crate::crypto::KeyRing::new(key);
            let mut wal = SegmentedWal::open(config.clone()).unwrap();
            wal.set_encryption_ring(ring).unwrap();

            for i in 0..5u32 {
                wal.append(
                    RecordType::Put as u32,
                    1,
                    0,
                    0,
                    format!("restart-{i}").as_bytes(),
                )
                .unwrap();
            }
            wal.sync().unwrap();
        }

        // Simulate restart: new WalEncryptionKey with same bytes (fresh random epoch).
        let key_restart = crate::crypto::WalEncryptionKey::from_bytes(&key_bytes).unwrap();
        let ring_restart = crate::crypto::KeyRing::new(key_restart);

        // Replay all segments and decrypt.
        let segments = crate::segment::discover_segments(&wal_dir).unwrap();
        let mut payloads = Vec::new();

        for seg in &segments {
            let reader = crate::reader::WalReader::open_raw(&seg.path).unwrap();
            let epoch = *reader
                .segment_preamble()
                .expect("segment must have preamble after encrypted write")
                .epoch();
            let preamble_bytes = reader.segment_preamble().unwrap().to_bytes();

            for record_result in reader.records() {
                let record = record_result.unwrap();
                let pt = record
                    .decrypt_payload_ring(&epoch, Some(&preamble_bytes), Some(&ring_restart))
                    .unwrap();
                payloads.push(pt);
            }
        }

        assert_eq!(payloads.len(), 5);
        for (i, pt) in payloads.iter().enumerate() {
            assert_eq!(pt, format!("restart-{i}").as_bytes());
        }
    }

    /// Epoch tamper rejection.
    ///
    /// After writing an encrypted segment, corrupt the preamble bytes on disk.
    /// Decryption must fail because the preamble is part of the AAD.
    #[test]
    fn epoch_tamper_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let wal_dir = dir.path().join("wal");
        let key_bytes = [0x55u8; 32];

        let config = SegmentedWalConfig::for_testing(wal_dir.clone());

        {
            let key = crate::crypto::WalEncryptionKey::from_bytes(&key_bytes).unwrap();
            let ring = crate::crypto::KeyRing::new(key);
            let mut wal = SegmentedWal::open(config).unwrap();
            wal.set_encryption_ring(ring).unwrap();
            wal.append(RecordType::Put as u32, 1, 0, 0, b"sensitive payload")
                .unwrap();
            wal.sync().unwrap();
        }

        // Find the segment file and corrupt byte 9 of the preamble (epoch field).
        let segments = crate::segment::discover_segments(&wal_dir).unwrap();
        assert_eq!(segments.len(), 1);
        let seg_path = &segments[0].path;

        let mut raw = std::fs::read(seg_path).unwrap();
        // Preamble is at offset 0; epoch is bytes 8..12. Flip byte 9.
        raw[9] ^= 0xFF;
        std::fs::write(seg_path, &raw).unwrap();

        // Reading the preamble will succeed (bytes are valid), but the epoch
        // in the preamble will be wrong, so the AAD during decryption won't
        // match what was used at encryption time — decryption must fail.
        let key_read = crate::crypto::WalEncryptionKey::from_bytes(&key_bytes).unwrap();
        let ring_read = crate::crypto::KeyRing::new(key_read);

        let reader = crate::reader::WalReader::open_raw(seg_path).unwrap();
        let epoch = *reader.segment_preamble().unwrap().epoch();
        let preamble_bytes = reader.segment_preamble().unwrap().to_bytes();

        let record = reader.records().next().unwrap().unwrap();
        let result = record.decrypt_payload_ring(&epoch, Some(&preamble_bytes), Some(&ring_read));
        assert!(
            result.is_err(),
            "decryption with tampered preamble epoch must fail"
        );
    }

    #[test]
    fn replay_from_limit_basic() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let mut wal = SegmentedWal::open(config).unwrap();

        // Append 10 records.
        for i in 0..10u8 {
            wal.append(RecordType::Put as u32, 1, 0, 0, &[i]).unwrap();
        }
        wal.sync().unwrap();

        // Read all with limit > count.
        let (records, has_more) = wal.replay_from_limit(1, 100).unwrap();
        assert_eq!(records.len(), 10);
        assert!(!has_more);

        // Read with limit = 3.
        let (records, has_more) = wal.replay_from_limit(1, 3).unwrap();
        assert_eq!(records.len(), 3);
        assert!(has_more);
        assert_eq!(records[0].header.lsn, 1);
        assert_eq!(records[2].header.lsn, 3);
    }

    #[test]
    fn replay_from_limit_with_lsn_filter() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let mut wal = SegmentedWal::open(config).unwrap();

        for i in 0..10u8 {
            wal.append(RecordType::Put as u32, 1, 0, 0, &[i]).unwrap();
        }
        wal.sync().unwrap();

        // Start from LSN 6 with limit 100 — should get 5 records (LSNs 6-10).
        let (records, has_more) = wal.replay_from_limit(6, 100).unwrap();
        assert_eq!(records.len(), 5);
        assert!(!has_more);
        assert_eq!(records[0].header.lsn, 6);

        // Start from LSN 6 with limit 2 — should get 2 records.
        let (records, has_more) = wal.replay_from_limit(6, 2).unwrap();
        assert_eq!(records.len(), 2);
        assert!(has_more);
    }

    #[test]
    fn replay_from_limit_empty() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let mut wal = SegmentedWal::open(config).unwrap();

        wal.append(RecordType::Put as u32, 1, 0, 0, b"a").unwrap();
        wal.sync().unwrap();

        // Start from beyond all records.
        let (records, has_more) = wal.replay_from_limit(999, 100).unwrap();
        assert!(records.is_empty());
        assert!(!has_more);
    }

    #[test]
    fn replay_from_limit_across_segments() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let mut wal = SegmentedWal::open(config).unwrap();

        // Write 10 records to first segment.
        for i in 0..10u8 {
            wal.append(RecordType::Put as u32, 1, 0, 0, &[i]).unwrap();
        }
        wal.sync().unwrap();
        // Force a segment rollover.
        wal.roll_segment().unwrap();

        // Write 10 more records to second segment.
        for i in 10..20u8 {
            wal.append(RecordType::Put as u32, 1, 0, 0, &[i]).unwrap();
        }
        wal.sync().unwrap();

        let seg_count = wal.list_segments().unwrap().len();
        assert!(
            seg_count >= 2,
            "expected multiple segments, got {seg_count}"
        );

        // Paginated read should span segments correctly.
        let (records, has_more) = wal.replay_from_limit(1, 5).unwrap();
        assert_eq!(records.len(), 5);
        assert!(has_more);

        // Continue from where we left off.
        let next_lsn = records.last().unwrap().header.lsn + 1;
        let (records2, _) = wal.replay_from_limit(next_lsn, 200).unwrap();
        assert_eq!(records2.len(), 15); // 20 - 5 = 15 remaining
    }
}
