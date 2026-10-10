// SPDX-License-Identifier: BUSL-1.1

use nodedb_wal::WalRecord;
use tracing::info;

use super::core::WalManager;
use crate::types::Lsn;

impl WalManager {
    /// Validate each WAL segment for startup integrity.
    ///
    /// Returns `Err` if any non-empty segment contains no valid WAL records —
    /// a reliable signal that the segment was corrupted (wrong magic, truncated
    /// header, etc.) rather than simply rolled over empty. The newest segment is
    /// the exception: a crash can leave it with no parseable record, and the
    /// next boot resumes that same file, so it passes.
    ///
    /// This check is intentionally strict: a segment file with content that
    /// does not parse as WAL records is treated as fatal corruption, not as an
    /// empty WAL. The WAL replay path is lenient (stops at the first invalid
    /// record) — this method is the complementary hard check run at startup.
    ///
    /// A segment whose records are in a WAL format this build cannot read is
    /// reported as a version gap rather than as corruption. Its answer is to
    /// start the store with the build that wrote the segment, or to re-create
    /// the data directory; corruption calls for repair.
    pub fn validate_for_startup(&self) -> crate::Result<()> {
        let segments =
            nodedb_wal::segment::discover_segments(&self.wal_dir).map_err(crate::Error::Wal)?;

        for (idx, seg) in segments.iter().enumerate() {
            let file_len = std::fs::metadata(&seg.path).map(|m| m.len()).unwrap_or(0);

            if file_len == 0 {
                continue;
            }
            if file_len == nodedb_wal::preamble::PREAMBLE_SIZE as u64 {
                use std::io::Read as _;
                let mut bytes = [0u8; nodedb_wal::preamble::PREAMBLE_SIZE];
                let mut file = std::fs::File::open(&seg.path).map_err(|error| {
                    crate::Error::SegmentCorrupted {
                        detail: format!("read WAL preamble '{}': {error}", seg.path.display()),
                    }
                })?;
                file.read_exact(&mut bytes)
                    .map_err(|error| crate::Error::SegmentCorrupted {
                        detail: format!("read WAL preamble '{}': {error}", seg.path.display()),
                    })?;
                nodedb_wal::preamble::SegmentPreamble::from_bytes(
                    &bytes,
                    &nodedb_wal::preamble::WAL_PREAMBLE_MAGIC,
                )
                .map_err(crate::Error::Wal)?;
                continue;
            }

            let info = nodedb_wal::recovery::recover(&seg.path).map_err(|error| match error {
                nodedb_wal::error::WalError::SegmentFormatVersion {
                    path,
                    version,
                    supported,
                } => crate::Error::VersionCompat {
                    detail: version_gap_detail(&path, version, supported),
                },
                other => crate::Error::Wal(other),
            })?;

            if info.end_offset == 0 {
                // This check covers only segments without a preamble. A
                // segment that opens with one reports its end at the end of the
                // preamble, never at 0, even when no record follows it.
                //
                // The newest segment is the one exception. A crash can leave
                // zero-filled pages in it, and the next boot resumes that same
                // file. Refusing it here makes the store permanently
                // unbootable, because every later boot reads the same bytes and
                // refuses them again. The version check runs before this one,
                // so a segment written by another build is still refused above.
                //
                // Any other segment without a preamble and without a record is
                // refused: nothing resumes it, and replay would silently skip
                // whatever it held.
                if idx + 1 == segments.len() {
                    continue;
                }
                return Err(crate::Error::SegmentCorrupted {
                    detail: format!(
                        "WAL segment '{}' is non-empty ({file_len} bytes) but contains no valid \
                         WAL records — the segment appears to be corrupted",
                        seg.path.display()
                    ),
                });
            }
        }

        Ok(())
    }

    /// Drop every record named by a `WriteAborted` marker in the same stream.
    ///
    /// A forward write record is appended before the Data Plane decides whether
    /// to accept the write, so a refusal always arrives with the record already
    /// in the log. Every replay stream this manager hands out is filtered here,
    /// at its source, rather than re-checked inside each engine's replay arm:
    /// the predicate is the record header's LSN and nothing else, so a per-arm
    /// gate would be dozens of identical checks and the first one forgotten
    /// silently resurrects that engine's refused writes.
    ///
    /// Requires the whole stream in hand — the abort marker is always at a
    /// HIGHER LSN than the record it names, so a streaming filter could not see
    /// it in time. The paginated `replay_*_limit` readers below therefore
    /// cannot use this and gate at their own call site.
    fn without_aborted_writes(records: Vec<WalRecord>) -> crate::Result<Vec<WalRecord>> {
        let filters = nodedb_wal::extract_replay_filters(&records).map_err(crate::Error::Wal)?;
        if filters.aborted.is_empty() {
            return Ok(records);
        }
        let before = records.len();
        let kept = nodedb_wal::drop_aborted_records(records, &filters.aborted);
        info!(
            dropped = before - kept.len(),
            "WAL replay excluded records for writes the engine refused"
        );
        Ok(kept)
    }

    /// Record every time anchor in `records` and drop the anchors from the
    /// stream. Anchors are commit-time metadata, and no engine replays them.
    ///
    /// The paginated readers keep anchors in their pages: a page of only
    /// anchors otherwise reads as empty while `has_more` is set.
    fn without_time_anchors(&self, mut records: Vec<WalRecord>) -> crate::Result<Vec<WalRecord>> {
        self.time_anchors
            .absorb_replayed(&records)
            .map_err(crate::Error::Wal)?;
        records.retain(|record| {
            nodedb_wal::record::RecordType::from_raw(record.logical_record_type())
                != Some(nodedb_wal::record::RecordType::TimeAnchor)
        });
        Ok(records)
    }

    /// Anchor every record recovered at open that no persisted anchor covers.
    ///
    /// Boot calls this once after [`Self::replay`] and before any append, so
    /// the WAL frontier is the last recovered LSN.
    pub fn anchor_recovered_tail(&self) {
        let last_recovered = self.next_lsn().as_u64().saturating_sub(1);
        self.time_anchors.cover_recovered(last_recovered);
    }

    /// Replay all committed records from the WAL.
    ///
    /// Payloads come back as plaintext: the manager's key ring is handed to the
    /// replay driver, which decrypts each record inside the WAL layer. Every
    /// record type is encrypted when a key is configured, so replaying without
    /// the ring would hand ciphertext to every engine's decoder.
    ///
    /// Records naming a refused write are excluded — see
    /// [`Self::without_aborted_writes`].
    pub fn replay(&self) -> crate::Result<Vec<WalRecord>> {
        let records = nodedb_wal::segmented::replay_all_segments(
            &self.wal_dir,
            self.encryption_ring.as_ref(),
        )
        .map_err(crate::Error::Wal)?;
        let records = self.without_time_anchors(records)?;
        let records = Self::without_aborted_writes(records)?;
        info!(records = records.len(), "WAL replay complete");
        Ok(records)
    }

    /// Replay committed records from the WAL starting at `from_lsn`.
    ///
    /// A `from_lsn` below the earliest LSN the WAL still retains fails with
    /// [`nodedb_wal::WalError::ReplayBelowRetainedFloor`] rather than returning
    /// the shorter suffix that survived truncation. A caller recovering from a
    /// persisted position must treat that as unrecoverable: the records it is
    /// asking for are gone, and a short answer is indistinguishable from a
    /// complete one. The same applies to [`Self::replay_mmap_from`],
    /// [`Self::replay_from_limit`], and [`Self::replay_mmap_from_limit`].
    pub fn replay_from(&self, from_lsn: Lsn) -> crate::Result<Vec<WalRecord>> {
        let records = {
            let wal = self.wal.lock().unwrap_or_else(|p| p.into_inner());
            wal.replay_from(from_lsn.as_u64())
                .map_err(crate::Error::Wal)?
        };
        let records = self.without_time_anchors(records)?;
        Self::without_aborted_writes(records)
    }

    /// Replay WAL records from `from_lsn` using mmap (tier-2 catchup).
    pub fn replay_mmap_from(&self, from_lsn: Lsn) -> crate::Result<Vec<WalRecord>> {
        let records = nodedb_wal::mmap_reader::replay_segments_mmap(
            self.wal_dir(),
            from_lsn.as_u64(),
            self.encryption_ring.as_ref(),
        )
        .map_err(crate::Error::Wal)?;
        let records = self.without_time_anchors(records)?;
        Self::without_aborted_writes(records)
    }

    /// Paginated mmap replay: reads at most `max_records` from `from_lsn`.
    ///
    /// **Note:** Uses mmap, which cannot see data written via O_DIRECT to the
    /// active segment. Use `replay_from_limit` for the catch-up task instead.
    pub fn replay_mmap_from_limit(
        &self,
        from_lsn: Lsn,
        max_records: usize,
    ) -> crate::Result<(Vec<WalRecord>, bool)> {
        nodedb_wal::mmap_reader::replay_segments_mmap_limit(
            self.wal_dir(),
            from_lsn.as_u64(),
            max_records,
            self.encryption_ring.as_ref(),
        )
        .map_err(crate::Error::Wal)
    }

    /// Paginated sequential replay: reads at most `max_records` from `from_lsn`.
    ///
    /// Unlike [`Self::replay`] / [`Self::replay_from`], the page is NOT filtered
    /// for refused writes: a page can end between a forward record and the
    /// abort marker that names it, so the filter has to run where the caller
    /// decides what to do with the page.
    pub fn replay_from_limit(
        &self,
        from_lsn: Lsn,
        max_records: usize,
    ) -> crate::Result<(Vec<WalRecord>, bool)> {
        nodedb_wal::segmented::replay_from_limit_dir(
            self.wal_dir(),
            from_lsn.as_u64(),
            max_records,
            self.encryption_ring.as_ref(),
        )
        .map_err(crate::Error::Wal)
    }
}

/// The operator-facing text for a store whose records are in a WAL format this
/// build cannot read.
///
/// One text, because the same store is refused from two places: the open fails
/// on the newest segment, and startup validation finds the gap in any segment
/// behind it. It names the segment, both versions, and the actions that exist.
/// No WAL format migration tool exists, so naming one would send an operator
/// looking for something they cannot find.
pub(crate) fn version_gap_detail(path: &str, version: u16, supported: u16) -> String {
    format!(
        "WAL segment '{path}' holds records in WAL format version {version}; this build reads \
         version {supported}. Start the store with the build that wrote that segment, or re-create \
         the data directory and load the data again"
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{DatabaseId, TenantId, VShardId};
    use crate::wal::manager::NO_APPLY_KEY;
    use nodedb_wal::record::{RecordType, WAL_FORMAT_VERSION, WAL_MAGIC, WalRecordArgs};

    fn put(wal: &WalManager, payload: &[u8]) -> Lsn {
        wal.appender(NO_APPLY_KEY)
            .with_event_source(crate::event::EventSource::User)
            .append_put(
                TenantId::new(1),
                VShardId::new(0),
                DatabaseId::DEFAULT,
                payload,
            )
            .expect("append")
    }

    #[test]
    fn time_anchors_survive_a_restart() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("wal");

        let live = {
            let wal = WalManager::open_for_testing(&path).expect("open wal");
            put(&wal, b"a");
            put(&wal, b"b");
            wal.sync().expect("sync");
            put(&wal, b"c");
            wal.sync().expect("sync");
            wal.time_anchors().anchors()
        };
        // The empty log's open anchor is in memory only.
        let persisted: Vec<_> = live.into_iter().filter(|a| a.lsn > 0).collect();
        assert_eq!(
            persisted.iter().map(|a| a.lsn).collect::<Vec<_>>(),
            vec![3, 5]
        );

        let wal = WalManager::open_for_testing(&path).expect("reopen wal");
        let records = wal.replay().expect("replay");
        assert_eq!(records.len(), 3, "anchors are not handed to engine replay");
        wal.anchor_recovered_tail();
        assert_eq!(wal.time_anchors().anchors(), persisted);

        let first_ns = persisted[0].hlc_wall_ns;
        assert_eq!(
            wal.time_anchors()
                .lsn_at_or_before(first_ns)
                .expect("lookup"),
            3
        );
        assert!(wal.time_anchors().lsn_at_or_before(first_ns - 1).is_err());
    }

    /// One record that declares `version`, framed the way this build frames
    /// one: header, then payload.
    ///
    /// Only the version field is under test. The framing is this build's, which
    /// a version-1 writer never produced, because its record type field was 16
    /// bits wide and this one is 32, so the file is a store this build cannot
    /// read rather than a faithful copy of one it once wrote. The reader decides
    /// on the version field before it reaches the checksum, so no assertion here
    /// rests on the rest of the header.
    fn record_with_format_version(lsn: u64, version: u16) -> Vec<u8> {
        let mut record = WalRecord::new(WalRecordArgs {
            record_type: RecordType::Put as u32,
            lsn,
            tenant_id: 1,
            vshard_id: 0,
            database_id: 0,
            payload: vec![7u8; 16],
            encryption_key: None,
            preamble_bytes: None,
        })
        .expect("build a record");
        record.header.format_version = version;
        let mut bytes = record.header.to_bytes().to_vec();
        bytes.extend_from_slice(&record.payload);
        bytes
    }

    /// A segment whose first record declares `version`.
    fn write_segment_with_format_version(
        path: &std::path::Path,
        first_lsn: u64,
        version: u16,
    ) -> std::path::PathBuf {
        let bytes = record_with_format_version(first_lsn, version);
        let segment = nodedb_wal::segment::segment_path(path, first_lsn);
        std::fs::write(&segment, &bytes).expect("write the segment");
        segment
    }

    /// A valid store, then a newest segment holding no parseable record: the
    /// zero-filled pages a crash can leave behind, which the next boot resumes.
    fn wal_with_a_recordless_newest_segment(path: &std::path::Path) -> WalManager {
        {
            let wal = WalManager::open_for_testing(path).expect("open wal");
            put(&wal, b"a");
            wal.sync().expect("sync");
        }

        let wal = WalManager::open_for_testing(path).expect("reopen wal");
        let newest = nodedb_wal::segment::discover_segments(path)
            .expect("discover segments")
            .last()
            .expect("the boot's own segment")
            .first_lsn;
        // Written after open on purpose: open resumes the newest segment and may
        // rewrite its preamble, and the state under test is the one the gate
        // then reads back.
        std::fs::write(
            nodedb_wal::segment::segment_path(path, newest + 1_000),
            vec![0u8; 64 * 1024],
        )
        .expect("write a zero-filled segment");
        wal
    }

    #[test]
    fn a_recordless_newest_segment_does_not_block_startup() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("wal");
        let wal = wal_with_a_recordless_newest_segment(&path);

        assert!(
            wal.validate_for_startup().is_ok(),
            "the newest segment holds no committed record, which torn_tail already warns \
             about, and the next boot resumes it. Refusing it here makes the store \
             permanently unbootable"
        );
    }

    #[test]
    fn a_segment_with_no_records_behind_the_newest_still_blocks_startup() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("wal");
        let wal = wal_with_a_recordless_newest_segment(&path);
        let segments = nodedb_wal::segment::discover_segments(&path).expect("discover segments");
        let recordless = segments.last().expect("the zero-filled segment");
        // A newer zero-filled segment becomes the newest, so the first one sits
        // behind it: nothing resumes that file, and the gate refuses it.
        std::fs::write(
            nodedb_wal::segment::segment_path(&path, recordless.first_lsn + 2_000),
            vec![0u8; 64 * 1024],
        )
        .expect("write a newer zero-filled segment");

        let error = wal
            .validate_for_startup()
            .expect_err("a record-less segment behind the newest must be refused");
        assert!(
            matches!(error, crate::Error::SegmentCorrupted { .. }),
            "a record-less segment behind the newest is corruption: {error}"
        );
        let text = format!("{error}");
        assert!(
            text.contains(&recordless.path.display().to_string()),
            "the refusal must name the record-less segment: {text}"
        );
    }

    /// An encrypted store keeps its first record behind the preamble, so the
    /// version has to be read from behind it too.
    #[test]
    fn a_store_with_a_preamble_says_which_version() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("wal");
        {
            let wal = WalManager::open_for_testing(&path).expect("open wal");
            put(&wal, b"a");
            wal.sync().expect("sync");
        }
        let wal = WalManager::open_for_testing(&path).expect("reopen wal");
        let newest = nodedb_wal::segment::discover_segments(&path)
            .expect("discover segments")
            .last()
            .expect("the boot's own segment")
            .first_lsn;

        // A real preamble, not a zeroed one: the reader validates its own
        // version field and would reject a malformed preamble for the wrong
        // reason.
        let mut bytes = nodedb_wal::preamble::SegmentPreamble::new_wal([9, 9, 9, 9])
            .to_bytes()
            .to_vec();
        bytes.extend_from_slice(&record_with_format_version(newest + 1_000, 1));
        std::fs::write(
            nodedb_wal::segment::segment_path(&path, newest + 1_000),
            &bytes,
        )
        .expect("write an encrypted-format segment");
        // A newer segment, so the one above is not the tail the gate tolerates.
        std::fs::write(
            nodedb_wal::segment::segment_path(&path, newest + 2_000),
            vec![0u8; 64 * 1024],
        )
        .expect("write a newer segment");

        let error = wal
            .validate_for_startup()
            .expect_err("an older format behind a preamble must be refused");
        let text = format!("{error}");
        assert!(
            text.contains("version 1"),
            "the version sits behind the preamble and the message must still name it: {text}"
        );
        assert!(
            text.contains(&format!("reads version {WAL_FORMAT_VERSION}")),
            "the message must name the version this build reads: {text}"
        );
    }

    /// A torn write that persists the magic and leaves the version zeroed is
    /// not a format gap, and must not turn a bootable store into a refused one.
    #[test]
    fn a_zeroed_version_is_not_a_format_gap() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("wal");
        {
            let wal = WalManager::open_for_testing(&path).expect("open wal");
            put(&wal, b"a");
            wal.sync().expect("sync");
        }
        let wal = WalManager::open_for_testing(&path).expect("reopen wal");
        let newest = nodedb_wal::segment::discover_segments(&path)
            .expect("discover segments")
            .last()
            .expect("the boot's own segment")
            .first_lsn;

        // The magic of a real record, the version bytes never written.
        let mut bytes = vec![0u8; 64 * 1024];
        bytes[0..4].copy_from_slice(&WAL_MAGIC.to_le_bytes());
        std::fs::write(
            nodedb_wal::segment::segment_path(&path, newest + 1_000),
            &bytes,
        )
        .expect("write a torn newest segment");

        assert!(
            wal.validate_for_startup().is_ok(),
            "a zeroed version is uninitialised bytes, not a format this build cannot read"
        );
    }

    /// A valid store, then a newer segment written in an older format.
    fn store_with_an_older_newest_segment(
        path: &std::path::Path,
        version: u16,
    ) -> (WalManager, std::path::PathBuf) {
        {
            let wal = WalManager::open_for_testing(path).expect("open wal");
            put(&wal, b"a");
            wal.sync().expect("sync");
        }
        let wal = WalManager::open_for_testing(path).expect("reopen wal");
        let newest = nodedb_wal::segment::discover_segments(path)
            .expect("discover segments")
            .last()
            .expect("the boot's own segment")
            .first_lsn;
        let segment = write_segment_with_format_version(path, newest + 1_000, version);
        (wal, segment)
    }

    /// The newest segment is the one the open recovers, so a store in another
    /// format fails there, before validation runs. The refusal names the
    /// segment and both versions: that is what tells a store written by another
    /// build apart from a damaged one.
    #[test]
    fn the_open_refuses_a_newest_segment_in_another_format() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("wal");
        let (_wal, segment) = store_with_an_older_newest_segment(&path, 1);

        let error = match WalManager::open_for_testing(&path) {
            Ok(_) => panic!("a store written in another format must not open"),
            Err(error) => error,
        };
        let text = format!("{error}");

        assert!(
            text.contains(&segment.display().to_string()),
            "the refusal must name the segment: {text}"
        );
        assert!(
            text.contains("version 1"),
            "the refusal must name the version it found: {text}"
        );
        assert!(
            text.contains(&format!("reads version {WAL_FORMAT_VERSION}")),
            "the refusal must name the version this build reads: {text}"
        );
        assert!(
            !text.contains("corrupted"),
            "a format gap is not corruption: {text}"
        );
    }

    /// The same gap behind the newest segment is out of the open's reach, so
    /// startup validation is what finds it. That message also names the actions
    /// that exist, which the WAL layer cannot know.
    #[test]
    fn validation_names_a_gap_behind_the_newest_segment() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("wal");
        let (wal, segment) = store_with_an_older_newest_segment(&path, 1);
        let newest = nodedb_wal::segment::discover_segments(&path)
            .expect("discover segments")
            .last()
            .expect("a segment")
            .first_lsn;
        std::fs::write(
            nodedb_wal::segment::segment_path(&path, newest + 1_000),
            vec![0u8; 64 * 1024],
        )
        .expect("write a newer segment");

        let error = wal
            .validate_for_startup()
            .expect_err("a gap behind the tail is still a gap");
        let text = format!("{error}");

        assert!(
            text.contains(&segment.display().to_string()),
            "the message must name the segment: {text}"
        );
        assert!(
            text.contains("version 1"),
            "the message must name the version it found, and it named none: {text}"
        );
        assert!(
            text.contains(&format!("reads version {WAL_FORMAT_VERSION}")),
            "the message must name the version this build reads: {text}"
        );
        assert!(
            !text.contains("corrupted"),
            "a format gap is reported as a version gap, never as corruption: {text}"
        );
        assert!(
            text.contains("re-create the data directory"),
            "the message must name an action that exists: {text}"
        );
    }

    /// The other side of the same branch: a segment that is not a WAL at all is
    /// still corruption, and still says so.
    #[test]
    fn a_segment_that_is_not_a_wal_still_says_corrupted() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("wal");
        {
            let wal = WalManager::open_for_testing(&path).expect("open wal");
            put(&wal, b"a");
            wal.sync().expect("sync");
        }
        let wal = WalManager::open_for_testing(&path).expect("reopen wal");
        let newest = nodedb_wal::segment::discover_segments(&path)
            .expect("discover segments")
            .last()
            .expect("a segment")
            .first_lsn;
        // No magic at all, which is what a truncated or overwritten file looks
        // like, as opposed to a file another version wrote. A newer segment
        // follows it so this one is not the tail, because the tail is the one
        // case the gate tolerates.
        std::fs::write(
            nodedb_wal::segment::segment_path(&path, newest + 1_000),
            vec![0u8; 64 * 1024],
        )
        .expect("write a segment with no framing");
        std::fs::write(
            nodedb_wal::segment::segment_path(&path, newest + 2_000),
            vec![0u8; 64 * 1024],
        )
        .expect("write a newer zero-filled segment");

        let error = wal
            .validate_for_startup()
            .expect_err("a segment with no framing must be refused");
        let text = format!("{error}");
        assert!(
            text.contains("corrupted"),
            "a segment that is not a WAL is corruption: {text}"
        );
    }
}
