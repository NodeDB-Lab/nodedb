// SPDX-License-Identifier: Apache-2.0

//! Reading the records of a segmented WAL directory, with no lock on its writer.
//!
//! [`visit_records_dir`] holds one record at a time. Every replay here is that scan.

use std::ops::ControlFlow;
use std::path::{Path, PathBuf};

use crate::crypto::KeyRing;
use crate::error::{Result, WalError};
use crate::record::WalRecord;
use crate::segment::{SegmentContinuity, check_retained_floor, discover_segments};

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
    let mut records = Vec::with_capacity(max_records.min(4096));
    let stopped = visit_records_dir::<WalError>(wal_dir, from_lsn, keys, |record| {
        records.push(record);
        Ok(if records.len() >= max_records {
            ControlFlow::Break(())
        } else {
            ControlFlow::Continue(())
        })
    })?;
    Ok((records, stopped))
}

/// A reader of one WAL directory that needs no lock on the WAL's writer.
///
/// It reads as [`replay_from_limit_dir`] does, so it is safe during writes.
/// It holds one record at a time.
pub struct SegmentScanner {
    wal_dir: PathBuf,
    keys: Option<KeyRing>,
}

impl SegmentScanner {
    /// A scanner of `wal_dir`, whose records `keys` opens.
    pub fn new(wal_dir: PathBuf, keys: Option<KeyRing>) -> Self {
        Self { wal_dir, keys }
    }

    /// Visit every record in LSN order, one at a time, until the visitor breaks.
    ///
    /// A truncation during the scan can remove a segment, and the scan then fails with a
    /// `NotFound` I/O error. The caller can scan again.
    pub fn visit<E: From<WalError>>(
        &self,
        visit: impl FnMut(WalRecord) -> std::result::Result<ControlFlow<()>, E>,
    ) -> std::result::Result<(), E> {
        visit_records_dir(&self.wal_dir, 0, self.keys.as_ref(), visit).map(drop)
    }
}

/// Visit the records of `wal_dir` at or above `from_lsn`, in LSN order, one at a time.
///
/// Returns `true` when the visitor stopped the scan.
/// The checks are those of [`replay_from_limit_dir`], which is this scan with a collecting visitor.
pub fn visit_records_dir<E: From<WalError>>(
    wal_dir: &Path,
    from_lsn: u64,
    keys: Option<&KeyRing>,
    mut visit: impl FnMut(WalRecord) -> std::result::Result<ControlFlow<()>, E>,
) -> std::result::Result<bool, E> {
    let segments = discover_segments(wal_dir)?;
    // Before any record is read: the answer to "is the requested suffix still
    // here?" is in the segment set, and once records start flowing back a short
    // result is indistinguishable from a complete one.
    check_retained_floor(&segments, from_lsn)?;
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
        while let Some(record) = reader
            .next_record()
            .map_err(|error| error.with_segment_path(&seg.path))?
        {
            last_lsn = record.header.lsn;
            if record.header.lsn >= from_lsn && visit(record)?.is_break() {
                // Stopping short leaves the rest of this segment unread, so
                // its end LSN is unknown and no boundary can be judged.
                return Ok(true);
            }
        }
        // A segment that stops early may be an interrupted final write, or it
        // may be a hole with committed records behind it. Only the first is a
        // legal end of the log.
        crate::torn_tail::verify_committed_prefix(&seg.path, reader.stop_reason(), last_lsn)?;
        continuity.completed(seg, last_lsn);
    }

    Ok(false)
}
