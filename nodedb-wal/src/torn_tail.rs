// SPDX-License-Identifier: Apache-2.0

//! Telling a torn tail apart from mid-file corruption.
//!
//! A reader stops at the first byte it cannot parse as a record. That stop
//! point has two very different meanings:
//!
//! - **Torn tail.** The process died partway through the last write. The bytes
//!   after the stop point were never fsynced, so they were never acknowledged
//!   to any client. Treating the stop point as the end of the log is correct
//!   and is the normal outcome of a crash.
//!
//! - **Mid-file corruption.** A block went bad (or was overwritten) somewhere
//!   in the middle of a segment while committed records continued past it.
//!   Treating *this* stop point as the end of the log silently discards every
//!   committed record behind the hole, with no error anywhere.
//!
//! The two are indistinguishable at the stop point itself, so this module
//! looks past it: WAL segments are written strictly in LSN order, so a record
//! that is intact (magic, version, CRC) and carries an LSN *above* everything
//! read so far cannot be on the far side of a torn tail — the writer had
//! already moved past it. Finding one proves the damage is a hole, and
//! recovery fails loudly instead of truncating.

use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;

use crate::error::{Result, WalError};
use crate::reader::StopReason;
use crate::record::{HEADER_SIZE, RecordHeader, WAL_MAGIC, WalRecord};

/// Bytes scanned per read while searching for a resync point.
const SCAN_CHUNK: usize = 1 << 20;

/// What the bytes after a reader's stop point turned out to be.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TailVerdict {
    /// Nothing parseable follows — the unfsynced tail of an interrupted write.
    TornTail,

    /// An intact record with a higher LSN follows the damaged region.
    MidFileCorruption { resync_offset: u64, resync_lsn: u64 },
}

/// Turn a reader's stop reason into an error when it hides committed records.
///
/// `last_lsn` is the highest LSN the caller read from this segment before it
/// stopped. A clean EOF, or damage with nothing valid behind it, returns
/// `Ok(())`.
pub fn verify_committed_prefix(path: &Path, stop: Option<StopReason>, last_lsn: u64) -> Result<()> {
    let Some(StopReason::Corruption { offset }) = stop else {
        return Ok(());
    };

    match classify(path, offset, last_lsn)? {
        TailVerdict::TornTail => {
            tracing::warn!(
                path = %path.display(),
                offset,
                last_lsn,
                "WAL segment ends in a torn write — records past this point were never fsynced"
            );
            Ok(())
        }
        TailVerdict::MidFileCorruption {
            resync_offset,
            resync_lsn,
        } => {
            let err = WalError::MidFileCorruption {
                path: path.display().to_string(),
                offset,
                resync_offset,
                resync_lsn,
            };
            // Reported here and only here. This is the layer that knows the
            // stop point is a hole rather than a torn tail, and it is the last
            // moment the damaged segment is guaranteed to still be on disk
            // exactly as recovery found it. Callers propagate the error.
            crate::diag::mid_file_corruption(
                &err,
                path,
                offset,
                resync_offset,
                resync_lsn,
                last_lsn,
            );
            Err(err)
        }
    }
}

/// Scan the bytes after `corruption_offset` for an intact record whose LSN is
/// above `last_lsn`.
pub fn classify(path: &Path, corruption_offset: u64, last_lsn: u64) -> Result<TailVerdict> {
    let mut file = File::open(path)?;
    let file_len = file.metadata()?.len();

    // The damaged bytes themselves cannot be the resync point.
    let start = corruption_offset.saturating_add(1);
    if start >= file_len {
        return Ok(TailVerdict::TornTail);
    }

    let mut window = Vec::with_capacity(SCAN_CHUNK);
    let magic = WAL_MAGIC.to_le_bytes();
    let mut base = start;

    while base < file_len {
        let want = SCAN_CHUNK.min(usize::try_from(file_len - base).unwrap_or(SCAN_CHUNK));
        file.seek(SeekFrom::Start(base))?;
        let filled = read_up_to(&mut file, want, &mut window)?;
        if filled < magic.len() {
            break;
        }

        for (i, candidate_bytes) in window[..filled].windows(magic.len()).enumerate() {
            if candidate_bytes != magic {
                continue;
            }
            let candidate = base.saturating_add(i as u64);
            if let Some(resync_lsn) = intact_record_lsn(&mut file, candidate, file_len)?
                && resync_lsn > last_lsn
            {
                return Ok(TailVerdict::MidFileCorruption {
                    resync_offset: candidate,
                    resync_lsn,
                });
            }
        }

        // Overlap by `magic.len() - 1` so a magic straddling the window edge
        // is still found.
        base = base.saturating_add((filled - (magic.len() - 1)) as u64);
    }

    Ok(TailVerdict::TornTail)
}

/// Read a full record at `offset` and return its LSN if header and CRC both
/// check out. Anything else is a coincidental magic match, not a record.
///
/// A format version this build cannot read is not a record either. The scan
/// starts past a reader's stop point, which is never before the segment's
/// first record, and a version mismatch after the first record is damage.
fn intact_record_lsn(file: &mut File, offset: u64, file_len: u64) -> Result<Option<u64>> {
    let header_end = match offset.checked_add(HEADER_SIZE as u64) {
        Some(end) if end <= file_len => end,
        _ => return Ok(None),
    };

    file.seek(SeekFrom::Start(offset))?;
    let mut header_buf = Vec::with_capacity(HEADER_SIZE);
    read_up_to(file, HEADER_SIZE, &mut header_buf)?;
    let Ok(header_bytes) = <&[u8; HEADER_SIZE]>::try_from(header_buf.as_slice()) else {
        return Ok(None);
    };

    let header = RecordHeader::from_bytes(header_bytes);
    if header.validate(offset).is_err() {
        return Ok(None);
    }

    // `header.payload_len` is a `u32`; the widening conversion to `usize`
    // (>= 32 bits on every supported target) always succeeds.
    let payload_len = header.payload_len as usize;
    match header_end.checked_add(payload_len as u64) {
        Some(end) if end <= file_len => {}
        _ => return Ok(None),
    }

    let mut payload = Vec::with_capacity(payload_len);
    if read_up_to(file, payload_len, &mut payload)? != payload_len {
        return Ok(None);
    }

    let record = WalRecord { header, payload };
    if record.verify_checksum().is_err() {
        return Ok(None);
    }

    Ok(Some(header.lsn))
}

/// Replace `buf` with up to `len` bytes from the file's current position,
/// returning the number of bytes read. Fewer than `len` means EOF.
///
/// `buf` holds only bytes read from the file, never a placeholder fill.
fn read_up_to(file: &mut File, len: usize, buf: &mut Vec<u8>) -> Result<usize> {
    buf.clear();
    file.by_ref()
        .take(len as u64)
        .read_to_end(buf)
        .map_err(WalError::Io)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::record::RecordType;
    use crate::writer::WalWriter;
    use std::io::Write;

    /// Write `count` records, each in its own fsynced batch.
    fn write_segment(path: &Path, count: u64) -> Vec<u64> {
        let mut writer = WalWriter::open_without_direct_io(path).unwrap();
        let mut lsns = Vec::new();
        for i in 0..count {
            lsns.push(
                writer
                    .append(
                        RecordType::Put as u32,
                        1,
                        0,
                        0,
                        format!("row-{i}").as_bytes(),
                    )
                    .unwrap(),
            );
            writer.sync().unwrap();
        }
        lsns
    }

    /// Overwrite `len` bytes at `offset` with a byte pattern that is not the
    /// WAL magic, simulating a bad block.
    fn smash(path: &Path, offset: u64, len: usize) {
        let mut file = std::fs::OpenOptions::new().write(true).open(path).unwrap();
        file.seek(SeekFrom::Start(offset)).unwrap();
        file.write_all(&vec![0xA5u8; len]).unwrap();
        file.sync_all().unwrap();
    }

    #[test]
    fn truncated_tail_is_a_torn_write() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("torn.wal");
        write_segment(&path, 4);

        // Chop the file mid-record: nothing parseable follows.
        let len = std::fs::metadata(&path).unwrap().len();
        let file = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
        file.set_len(len - 3).unwrap();
        file.sync_all().unwrap();

        let verdict = classify(&path, len - 3, 4).unwrap();
        assert_eq!(verdict, TailVerdict::TornTail);
    }

    #[test]
    fn garbage_at_the_end_is_a_torn_write() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("garbage_tail.wal");
        write_segment(&path, 3);

        let len = std::fs::metadata(&path).unwrap().len();
        smash(&path, len - 10, 10);

        assert_eq!(classify(&path, len - 10, 3).unwrap(), TailVerdict::TornTail);
    }

    #[test]
    fn damage_with_committed_records_behind_it_is_mid_file_corruption() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("hole.wal");
        write_segment(&path, 6);

        // Smash the second record's header; records 3..6 stay intact.
        let hole = (HEADER_SIZE + 5) as u64;
        smash(&path, hole, HEADER_SIZE);

        match classify(&path, hole, 1).unwrap() {
            TailVerdict::MidFileCorruption { resync_lsn, .. } => {
                assert!(
                    resync_lsn > 1,
                    "resync LSN {resync_lsn} must be past the hole"
                );
            }
            TailVerdict::TornTail => panic!("committed records behind the hole were not detected"),
        }
    }

    #[test]
    fn verify_rejects_a_hole_and_accepts_a_torn_tail() {
        let dir = tempfile::tempdir().unwrap();

        let hole_path = dir.path().join("verify_hole.wal");
        write_segment(&hole_path, 6);
        let hole = (HEADER_SIZE + 5) as u64;
        smash(&hole_path, hole, HEADER_SIZE);
        assert!(matches!(
            verify_committed_prefix(&hole_path, Some(StopReason::Corruption { offset: hole }), 1),
            Err(WalError::MidFileCorruption { .. })
        ));

        let torn_path = dir.path().join("verify_torn.wal");
        write_segment(&torn_path, 3);
        let len = std::fs::metadata(&torn_path).unwrap().len();
        smash(&torn_path, len - 8, 8);
        assert!(
            verify_committed_prefix(
                &torn_path,
                Some(StopReason::Corruption { offset: len - 8 }),
                3
            )
            .is_ok()
        );
    }

    /// A record whose format version this build cannot read is damage, not a
    /// resync point: the scan passes over it to the intact record behind it.
    #[test]
    fn a_version_mismatch_past_the_stop_is_not_a_record() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("version_mismatch.wal");
        write_segment(&path, 4);

        // Where each record starts: the reader's committed end after the one
        // before it.
        let mut starts = Vec::new();
        let mut reader = crate::reader::WalReader::open_raw(&path).unwrap();
        loop {
            let start = reader.committed_end();
            if reader.next_record().unwrap().is_none() {
                break;
            }
            starts.push(start);
        }

        // Damage the second record, and give the third a version this build
        // cannot read. The checksum covers the version, so it is recomputed:
        // the third record is intact in every way except its version. The
        // fourth stays intact.
        smash(&path, starts[1], HEADER_SIZE);
        let mut bytes = std::fs::read(&path).unwrap();
        let third = usize::try_from(starts[2]).unwrap();
        let head: &[u8; HEADER_SIZE] = bytes[third..third + HEADER_SIZE].try_into().unwrap();
        let mut header = RecordHeader::from_bytes(head);
        let payload_end = third + HEADER_SIZE + header.payload_len as usize;
        header.format_version = 1;
        header.crc32c = header.compute_checksum(&bytes[third + HEADER_SIZE..payload_end]);
        bytes[third..third + HEADER_SIZE].copy_from_slice(&header.to_bytes());
        std::fs::write(&path, &bytes).unwrap();

        assert_eq!(
            classify(&path, starts[1], 1).unwrap(),
            TailVerdict::MidFileCorruption {
                resync_offset: starts[3],
                resync_lsn: 4,
            }
        );
    }

    #[test]
    fn clean_eof_needs_no_scan() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("clean.wal");
        write_segment(&path, 2);
        assert!(verify_committed_prefix(&path, Some(StopReason::Eof), 2).is_ok());
        assert!(verify_committed_prefix(&path, None, 2).is_ok());
    }
}
