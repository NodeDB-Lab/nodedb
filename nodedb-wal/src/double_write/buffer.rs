// SPDX-License-Identifier: Apache-2.0

//! The double-write buffer file and its write path.

use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

#[cfg(target_os = "linux")]
use std::os::unix::fs::OpenOptionsExt as _;

use crate::align::{AlignedBuf, DEFAULT_ALIGNMENT, is_aligned};
use crate::error::{Result, WalError};
use crate::record::{HEADER_SIZE, WalRecord};

use super::layout::{
    DWB_CAPACITY, DWB_HEADER_FIELDS, DWB_HEADER_STRIDE, DWB_MAGIC, DWB_SLOT_RECORD_MAX,
    DWB_SLOT_STRIDE, SlotPrefix, slot_offset,
};
use super::metrics;
use super::mode::DwbMode;
use super::raw_io::{full_capacity_slice, pwrite_all, zero_tail};
use super::status::{DwbMirror, DwbSkipReason};

/// Double-write buffer file.
pub struct DoubleWriteBuffer {
    pub(super) file: File,
    path: PathBuf,
    pub(super) mode: DwbMode,
    /// Current write position (circular, wraps at DWB_CAPACITY).
    write_pos: u32,
    /// Number of valid records in the buffer.
    count: u32,
    /// Sequence number the next slot write will carry. Resolved lazily on the
    /// first write so that read-only openers (recovery) never pay for the
    /// ring scan it needs.
    next_seq: Option<u64>,
    /// Whether there are deferred writes that haven't been fsynced.
    pub(super) dirty: bool,
    /// Single-slot aligned staging buffer (Direct mode only). One slot is
    /// serialized here, then pwrite'd at the slot offset.
    slot_buf: Option<AlignedBuf>,
    /// Aligned header block (Direct mode only). Written on `flush()`.
    header_buf: Option<AlignedBuf>,
}

impl std::fmt::Debug for DoubleWriteBuffer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DoubleWriteBuffer")
            .field("path", &self.path)
            .field("mode", &self.mode)
            .field("write_pos", &self.write_pos)
            .field("count", &self.count)
            .finish()
    }
}

impl DoubleWriteBuffer {
    /// Open or create the double-write buffer file in the requested I/O mode.
    ///
    /// Callers that want the DWB disabled must not call this at all;
    /// `DwbMode::Off` returns [`WalError::DwbOffNotOpenable`].
    pub fn open(path: &Path, mode: DwbMode) -> Result<Self> {
        if mode == DwbMode::Off {
            return Err(WalError::DwbOffNotOpenable);
        }

        let mut opts = OpenOptions::new();
        opts.read(true).write(true).create(true).truncate(false);
        #[cfg(target_os = "linux")]
        if mode == DwbMode::Direct {
            opts.custom_flags(libc::O_DIRECT);
        }

        let file = opts.open(path).map_err(|e| {
            tracing::warn!(path = %path.display(), error = %e, mode = ?mode, "failed to open double-write buffer");
            WalError::Io(e)
        })?;

        let (slot_buf, header_buf) = if mode == DwbMode::Direct {
            (
                Some(AlignedBuf::new(DWB_SLOT_STRIDE, DEFAULT_ALIGNMENT)?),
                Some(AlignedBuf::new(DWB_HEADER_STRIDE, DEFAULT_ALIGNMENT)?),
            )
        } else {
            (None, None)
        };

        let mut dwb = Self {
            file,
            path: path.to_path_buf(),
            mode,
            write_pos: 0,
            count: 0,
            next_seq: None,
            dirty: false,
            slot_buf,
            header_buf,
        };

        // Try to read existing header (first DWB_HEADER_FIELDS bytes of block 0).
        let file_len = dwb.file.metadata().map(|m| m.len()).unwrap_or(0);
        if file_len >= DWB_HEADER_STRIDE as u64 {
            let mut block = vec![0u8; DWB_HEADER_STRIDE];
            dwb.file.seek(SeekFrom::Start(0)).map_err(WalError::Io)?;
            if dwb.file.read_exact(&mut block).is_ok() {
                let mut arr4 = [0u8; 4];
                arr4.copy_from_slice(&block[0..4]);
                let magic = u32::from_le_bytes(arr4);
                if magic == DWB_MAGIC {
                    arr4.copy_from_slice(&block[4..8]);
                    dwb.count = u32::from_le_bytes(arr4);
                    arr4.copy_from_slice(&block[8..12]);
                    dwb.write_pos = u32::from_le_bytes(arr4);
                }
            }
        }

        Ok(dwb)
    }

    /// I/O mode this buffer was opened with.
    pub fn mode(&self) -> DwbMode {
        self.mode
    }

    /// Path to the double-write buffer file.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Write a WAL record to the double-write buffer before WAL append.
    ///
    /// The record is written at the current circular position and the file
    /// is fsynced immediately. Use `write_record_deferred` + `flush` for
    /// batch mode (multiple records per fsync).
    pub fn write_record(&mut self, record: &WalRecord) -> Result<DwbMirror> {
        let mirrored = self.write_record_deferred(record)?;
        self.flush()?;
        Ok(mirrored)
    }

    /// Write a WAL record to the DWB without fsyncing.
    ///
    /// The data is written to the OS page cache (Buffered mode) or directly
    /// to the block device (Direct mode) but not guaranteed durable until
    /// `flush()` is called. Use this in batch mode: write all records in a
    /// group commit batch, then call `flush()` once — reducing fsync calls
    /// from N-per-batch to 1-per-batch.
    ///
    /// Returns [`DwbMirror::Skipped`] for a record that cannot be mirrored, so
    /// the caller can account for the lost protection instead of assuming the
    /// record is covered.
    pub fn write_record_deferred(&mut self, record: &WalRecord) -> Result<DwbMirror> {
        // Crash injection: a DWB slot write that fails on a healthy WAL. The
        // append it belongs to must still succeed, degraded but observable.
        nodedb_types::fail_point_err!("wal::dwb_write_failure", |detail: String| WalError::Io(
            std::io::Error::other(format!("failpoint wal::dwb_write_failure: {detail}"))
        ));

        let total_size = HEADER_SIZE + record.payload.len();

        // A record spanning several slots could not be validated by a single
        // slot's CRC, which is what recovery relies on. Report the gap rather
        // than leaving the caller believing the record is protected.
        if total_size > DWB_SLOT_RECORD_MAX {
            return Ok(DwbMirror::Skipped(DwbSkipReason::RecordTooLarge {
                size: total_size,
                max: DWB_SLOT_RECORD_MAX,
            }));
        }

        let seq = self.take_slot_seq()?;
        let prefix = SlotPrefix { seq, total_size }.encode();
        let header_bytes = record.header.to_bytes();
        let offset = slot_offset(self.write_pos);

        match self.mode {
            DwbMode::Off => return Err(WalError::DwbOffNotOpenable),
            DwbMode::Buffered => {
                self.file
                    .seek(SeekFrom::Start(offset))
                    .map_err(WalError::Io)?;
                self.file.write_all(&prefix).map_err(WalError::Io)?;
                self.file.write_all(&header_bytes).map_err(WalError::Io)?;
                self.file.write_all(&record.payload).map_err(WalError::Io)?;
                metrics::add_bytes_written(
                    (prefix.len() + header_bytes.len() + record.payload.len()) as u64,
                );
            }
            DwbMode::Direct => {
                let Some(buf) = self.slot_buf.as_mut() else {
                    return Err(WalError::AlignmentViolation {
                        context: "DWB Direct mode without an aligned slot buffer",
                        required: DWB_SLOT_STRIDE,
                        actual: 0,
                    });
                };
                buf.clear();
                buf.write(&prefix);
                buf.write(&header_bytes);
                buf.write(&record.payload);
                // Zero the tail so the full aligned slot can be written
                // without leaking prior contents.
                zero_tail(buf);
                let slice = full_capacity_slice(buf);
                debug_assert_eq!(slice.len(), DWB_SLOT_STRIDE);
                debug_assert!(is_aligned(offset as usize, DEFAULT_ALIGNMENT));
                pwrite_all(&self.file, slice, offset)?;
                metrics::add_bytes_written(slice.len() as u64);
            }
        }

        self.write_pos = self.write_pos.wrapping_add(1);
        self.count = self.count.saturating_add(1).min(DWB_CAPACITY as u32);
        self.dirty = true;

        Ok(DwbMirror::Mirrored)
    }

    /// Flush the DWB header and fsync the file.
    ///
    /// Must be called after one or more `write_record_deferred` calls to make
    /// the records durable. The single fsync covers all deferred writes since
    /// the last flush — amortizing the cost across the group commit batch.
    pub fn flush(&mut self) -> Result<()> {
        if !self.dirty {
            return Ok(());
        }

        let mut header = [0u8; DWB_HEADER_FIELDS];
        header[0..4].copy_from_slice(&DWB_MAGIC.to_le_bytes());
        header[4..8].copy_from_slice(&self.count.to_le_bytes());
        header[8..12].copy_from_slice(&self.write_pos.to_le_bytes());

        match self.mode {
            DwbMode::Off => return Err(WalError::DwbOffNotOpenable),
            DwbMode::Buffered => {
                self.file.seek(SeekFrom::Start(0)).map_err(WalError::Io)?;
                self.file.write_all(&header).map_err(WalError::Io)?;
                metrics::add_bytes_written(header.len() as u64);
            }
            DwbMode::Direct => {
                let Some(buf) = self.header_buf.as_mut() else {
                    return Err(WalError::AlignmentViolation {
                        context: "DWB Direct mode without an aligned header buffer",
                        required: DWB_HEADER_STRIDE,
                        actual: 0,
                    });
                };
                buf.clear();
                buf.write(&header);
                zero_tail(buf);
                let slice = full_capacity_slice(buf);
                debug_assert_eq!(slice.len(), DWB_HEADER_STRIDE);
                pwrite_all(&self.file, slice, 0)?;
                metrics::add_bytes_written(slice.len() as u64);
            }
        }

        self.file.sync_all().map_err(WalError::Io)?;
        self.dirty = false;

        Ok(())
    }

    /// Sequence number for the slot about to be written.
    ///
    /// The first call outranks every sequence number still readable in the
    /// ring. A crash can leave the file header behind the slots it describes,
    /// so the slots themselves — not the header — decide where the sequence
    /// resumes; otherwise a reused number would let a stale copy tie with the
    /// record that replaced it.
    fn take_slot_seq(&mut self) -> Result<u64> {
        let seq = match self.next_seq {
            Some(seq) => seq,
            None => super::recover::scan_max_seq(self)?.saturating_add(1),
        };
        self.next_seq = Some(seq.saturating_add(1));
        Ok(seq)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::double_write::metrics::wal_dwb_bytes_written_total;
    use crate::record::{RecordType, WalRecordArgs};

    fn open_buffered(path: &Path) -> DoubleWriteBuffer {
        DoubleWriteBuffer::open(path, DwbMode::Buffered).unwrap()
    }

    fn record(lsn: u64, payload: &[u8]) -> WalRecord {
        WalRecord::new(WalRecordArgs {
            record_type: RecordType::Put as u32,
            lsn,
            tenant_id: 1,
            vshard_id: 0,
            database_id: 0,
            payload: payload.to_vec(),
            encryption_key: None,
            preamble_bytes: None,
        })
        .unwrap()
    }

    fn mirror(dwb: &mut DoubleWriteBuffer, rec: &WalRecord) {
        assert_eq!(dwb.write_record(rec).unwrap(), DwbMirror::Mirrored);
    }

    fn mirror_deferred(dwb: &mut DoubleWriteBuffer, rec: &WalRecord) {
        assert_eq!(dwb.write_record_deferred(rec).unwrap(), DwbMirror::Mirrored);
    }

    #[test]
    fn write_and_recover() {
        let dir = crate::test_tempdir().unwrap();
        let mut dwb = open_buffered(&dir.path().join("test.dwb"));

        mirror(&mut dwb, &record(42, b"hello double-write"));

        let rec = dwb.recover_record(42).unwrap().expect("recoverable");
        assert_eq!(rec.header.lsn, 42);
        assert_eq!(rec.payload, b"hello double-write");
    }

    #[test]
    fn recover_nonexistent_returns_none() {
        let dir = crate::test_tempdir().unwrap();
        let mut dwb = open_buffered(&dir.path().join("test2.dwb"));
        assert!(dwb.recover_record(999).unwrap().is_none());
    }

    #[test]
    fn survives_reopen() {
        let dir = crate::test_tempdir().unwrap();
        let path = dir.path().join("reopen.dwb");

        {
            let mut dwb = open_buffered(&path);
            mirror(&mut dwb, &record(7, b"durable"));
        }

        let mut dwb = open_buffered(&path);
        let recovered = dwb.recover_record(7).unwrap().expect("recoverable");
        assert_eq!(recovered.payload, b"durable");
    }

    #[test]
    fn batch_deferred_writes_and_flush() {
        let dir = crate::test_tempdir().unwrap();
        let mut dwb = open_buffered(&dir.path().join("batch.dwb"));

        for lsn in 1..=5u64 {
            mirror_deferred(&mut dwb, &record(lsn, format!("batch-{lsn}").as_bytes()));
        }

        assert!(dwb.dirty);
        dwb.flush().unwrap();
        assert!(!dwb.dirty);

        for lsn in 1..=5u64 {
            let recovered = dwb.recover_record(lsn).unwrap().expect("recoverable");
            assert_eq!(recovered.payload, format!("batch-{lsn}").into_bytes());
        }
    }

    #[test]
    fn flush_is_idempotent() {
        let dir = crate::test_tempdir().unwrap();
        let mut dwb = open_buffered(&dir.path().join("idem.dwb"));

        dwb.flush().unwrap();
        assert!(!dwb.dirty);

        mirror_deferred(&mut dwb, &record(1, b"data"));
        dwb.flush().unwrap();
        dwb.flush().unwrap();
        assert!(!dwb.dirty);
    }

    #[test]
    fn oversized_record_is_reported_as_skipped() {
        let dir = crate::test_tempdir().unwrap();
        let mut dwb = open_buffered(&dir.path().join("oversized.dwb"));

        let payload = vec![0xabu8; DWB_SLOT_RECORD_MAX];
        let outcome = dwb.write_record_deferred(&record(11, &payload)).unwrap();

        assert_eq!(
            outcome,
            DwbMirror::Skipped(DwbSkipReason::RecordTooLarge {
                size: HEADER_SIZE + payload.len(),
                max: DWB_SLOT_RECORD_MAX,
            })
        );
        assert!(dwb.recover_record(11).unwrap().is_none());
    }

    #[test]
    fn bytes_written_counter_increments() {
        let dir = crate::test_tempdir().unwrap();
        let before = wal_dwb_bytes_written_total();

        let mut dwb = open_buffered(&dir.path().join("counter.dwb"));
        mirror(&mut dwb, &record(1, b"counted"));

        assert!(wal_dwb_bytes_written_total() > before);
    }
}
