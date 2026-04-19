//! io_uring-based WAL writer for Thread-per-Core data plane.
//!
//! This replaces `pwrite` + `fsync` with io_uring submission, enabling:
//!
//! - **Async I/O without threads**: the TPC core submits write + fsync SQEs
//!   and continues processing other work until the CQE arrives.
//! - **Batched submissions**: multiple records are written to the aligned buffer,
//!   then a single `IORING_OP_WRITE` + `IORING_OP_FSYNC` pair is submitted.
//! - **O_DIRECT compatible**: the aligned buffer satisfies O_DIRECT constraints.
//!
//! ## Integration
//!
//! In the real Data Plane, the io_uring instance is shared with the TPC event
//! loop. This module provides the WAL-specific SQE preparation; the caller
//! owns the ring and calls `submit()` + `wait()`.

use std::fs::{File, OpenOptions};
use std::os::unix::fs::OpenOptionsExt;
use std::os::unix::io::AsRawFd;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

use io_uring::{IoUring, opcode, types};

use crate::align::{AlignedBuf, DEFAULT_ALIGNMENT};
use crate::error::{Result, WalError};
use crate::record::{HEADER_SIZE, WalRecord};

/// io_uring WAL writer configuration.
#[derive(Debug, Clone)]
pub struct UringWriterConfig {
    /// Size of the aligned write buffer.
    pub write_buffer_size: usize,
    /// O_DIRECT alignment.
    pub alignment: usize,
    /// io_uring queue depth.
    pub ring_depth: u32,
    /// Use O_DIRECT.
    pub use_direct_io: bool,
}

impl Default for UringWriterConfig {
    fn default() -> Self {
        Self {
            write_buffer_size: crate::writer::DEFAULT_WRITE_BUFFER_SIZE,
            alignment: DEFAULT_ALIGNMENT,
            ring_depth: 64,
            use_direct_io: true,
        }
    }
}

/// WAL writer using io_uring for O_DIRECT writes.
pub struct UringWriter {
    /// The WAL file handle.
    file: File,
    /// Aligned write buffer.
    buffer: AlignedBuf,
    /// Current file offset.
    file_offset: u64,
    /// Next LSN to assign.
    next_lsn: AtomicU64,
    /// io_uring instance.
    ring: IoUring,
    /// Sealed flag.
    sealed: bool,
    /// Config.
    config: UringWriterConfig,
    /// Optional encryption key for WAL-at-rest encryption.
    encryption_key: Option<crate::crypto::WalEncryptionKey>,
}

impl UringWriter {
    /// Open or create a WAL file with io_uring support.
    pub fn open(path: &Path, config: UringWriterConfig) -> Result<Self> {
        let mut opts = OpenOptions::new();
        opts.create(true).write(true).read(true);

        if config.use_direct_io {
            opts.custom_flags(libc::O_DIRECT);
        }

        let file = opts.open(path)?;
        let buffer = AlignedBuf::new(config.write_buffer_size, config.alignment)?;

        // Recovery: scan existing WAL for last LSN.
        let (file_offset, next_lsn) = if path.exists() && std::fs::metadata(path)?.len() > 0 {
            let info = crate::recovery::recover(path)?;
            (info.end_offset, info.next_lsn())
        } else {
            (0, 1)
        };

        let ring = IoUring::new(config.ring_depth).map_err(WalError::Io)?;

        Ok(Self {
            file,
            buffer,
            file_offset,
            next_lsn: AtomicU64::new(next_lsn),
            ring,
            sealed: false,
            config,
            encryption_key: None,
        })
    }

    /// Open without O_DIRECT (for testing on tmpfs).
    pub fn open_without_direct_io(path: &Path) -> Result<Self> {
        Self::open(
            path,
            UringWriterConfig {
                use_direct_io: false,
                ..Default::default()
            },
        )
    }

    /// Set the encryption key for WAL-at-rest encryption.
    pub fn set_encryption_key(&mut self, key: crate::crypto::WalEncryptionKey) {
        self.encryption_key = Some(key);
    }

    /// Append a record to the in-memory buffer. Returns the assigned LSN.
    ///
    /// The record is NOT durable until `submit_and_sync()` is called.
    pub fn append(
        &mut self,
        record_type: u16,
        tenant_id: u32,
        vshard_id: u16,
        payload: &[u8],
    ) -> Result<u64> {
        if self.sealed {
            return Err(WalError::Sealed);
        }

        let lsn = self.next_lsn.fetch_add(1, Ordering::Relaxed);
        let record = WalRecord::new(
            record_type,
            lsn,
            tenant_id,
            vshard_id,
            payload.to_vec(),
            self.encryption_key.as_ref(),
        )?;

        let header_bytes = record.header.to_bytes();
        let total_size = HEADER_SIZE + record.payload.len();

        if self.buffer.remaining() < total_size {
            self.submit_and_wait_write()?;
        }

        if total_size > self.buffer.capacity() {
            return Err(WalError::PayloadTooLarge {
                size: record.payload.len(),
                max: self.buffer.capacity() - HEADER_SIZE,
            });
        }

        self.buffer.write(&header_bytes);
        self.buffer.write(&record.payload);

        Ok(lsn)
    }

    /// Submit the buffered data via io_uring write + fsync, and wait for completion.
    ///
    /// This is the group commit point: all records appended since the last
    /// `submit_and_sync()` become durable after this returns.
    pub fn submit_and_sync(&mut self) -> Result<()> {
        if self.buffer.is_empty() {
            return Ok(());
        }
        self.submit_and_wait_write()?;
        self.submit_and_wait_fsync()?;
        Ok(())
    }

    /// Submit a write SQE and wait for the CQE.
    ///
    /// ## O_DIRECT alignment invariant
    ///
    /// When `use_direct_io` is true, the submission slice (`data`) is
    /// zero-padded up to the alignment boundary via `as_aligned_slice`.
    /// The kernel writes exactly `data.len()` bytes, so `file_offset` MUST
    /// advance by `data.len()` — the padded length, not the unpadded
    /// buffer content length. Advancing by the unpadded length leaves the
    /// next submission's offset unaligned, and the kernel rejects the
    /// write with `-EINVAL`. Mirrors the precedent set by
    /// `WalWriter::flush_buffer`.
    fn submit_and_wait_write(&mut self) -> Result<()> {
        if self.buffer.is_empty() {
            return Ok(());
        }

        let data = if self.config.use_direct_io {
            self.buffer.as_aligned_slice()
        } else {
            self.buffer.as_slice()
        };
        let write_len = data.len() as u64;

        let fd = types::Fd(self.file.as_raw_fd());
        let write_op = opcode::Write::new(fd, data.as_ptr(), data.len() as u32)
            .offset(self.file_offset)
            .build()
            .user_data(0x01);

        // SAFETY: write_op holds a raw pointer to `data` (which borrows self.buffer).
        // The buffer remains valid and unmodified until submit_and_wait(1) returns
        // with the CQE, after which self.buffer.clear() is called. Do NOT pipeline
        // submissions without ensuring the buffer outlives the SQE.
        unsafe {
            self.ring
                .submission()
                .push(&write_op)
                .map_err(|_| WalError::Io(std::io::Error::other("io_uring SQ full")))?;
        }

        self.ring.submit_and_wait(1).map_err(WalError::Io)?;

        // Check completion.
        let cqe =
            self.ring.completion().next().ok_or_else(|| {
                WalError::Io(std::io::Error::other("io_uring: no CQE after write"))
            })?;

        if cqe.result() < 0 {
            return Err(WalError::Io(std::io::Error::from_raw_os_error(
                -cqe.result(),
            )));
        }

        // See the O_DIRECT alignment invariant on this function's doc comment.
        self.file_offset += write_len;
        self.buffer.clear();
        Ok(())
    }

    /// Submit an fsync SQE and wait for the CQE.
    fn submit_and_wait_fsync(&mut self) -> Result<()> {
        let fd = types::Fd(self.file.as_raw_fd());
        let fsync_op = opcode::Fsync::new(fd).build().user_data(0x02);

        unsafe {
            self.ring
                .submission()
                .push(&fsync_op)
                .map_err(|_| WalError::Io(std::io::Error::other("io_uring SQ full")))?;
        }

        self.ring.submit_and_wait(1).map_err(WalError::Io)?;

        let cqe =
            self.ring.completion().next().ok_or_else(|| {
                WalError::Io(std::io::Error::other("io_uring: no CQE after fsync"))
            })?;

        if cqe.result() < 0 {
            return Err(WalError::Io(std::io::Error::from_raw_os_error(
                -cqe.result(),
            )));
        }

        Ok(())
    }

    /// Seal the WAL.
    pub fn seal(&mut self) -> Result<()> {
        self.submit_and_sync()?;
        self.sealed = true;
        Ok(())
    }

    /// Next LSN that will be assigned.
    pub fn next_lsn(&self) -> u64 {
        self.next_lsn.load(Ordering::Relaxed)
    }

    /// Current file offset.
    pub fn file_offset(&self) -> u64 {
        self.file_offset
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::reader::WalReader;
    use crate::record::RecordType;

    #[test]
    fn uring_write_and_read_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("uring.wal");

        {
            let mut writer = UringWriter::open_without_direct_io(&path).unwrap();
            writer
                .append(RecordType::Put as u16, 1, 0, b"hello-uring")
                .unwrap();
            writer
                .append(RecordType::Put as u16, 2, 1, b"world-uring")
                .unwrap();
            writer.submit_and_sync().unwrap();
        }

        let reader = WalReader::open(&path).unwrap();
        let records: Vec<_> = reader
            .records()
            .collect::<crate::error::Result<_>>()
            .unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].payload, b"hello-uring");
        assert_eq!(records[1].payload, b"world-uring");
        assert_eq!(records[0].header.lsn, 1);
        assert_eq!(records[1].header.lsn, 2);
    }

    #[test]
    fn uring_group_commit_many_records() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("uring_batch.wal");

        {
            let mut writer = UringWriter::open_without_direct_io(&path).unwrap();
            for i in 0..1000u32 {
                let payload = format!("record-{i}");
                writer
                    .append(RecordType::Put as u16, 1, 0, payload.as_bytes())
                    .unwrap();
            }
            writer.submit_and_sync().unwrap();
        }

        let reader = WalReader::open(&path).unwrap();
        let records: Vec<_> = reader
            .records()
            .collect::<crate::error::Result<_>>()
            .unwrap();
        assert_eq!(records.len(), 1000);
        assert_eq!(records[999].header.lsn, 1000);
    }

    #[test]
    fn uring_reopen_continues_lsn() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("uring_reopen.wal");

        {
            let mut writer = UringWriter::open_without_direct_io(&path).unwrap();
            writer
                .append(RecordType::Put as u16, 1, 0, b"first")
                .unwrap();
            writer
                .append(RecordType::Put as u16, 1, 0, b"second")
                .unwrap();
            writer.submit_and_sync().unwrap();
        }

        {
            let mut writer = UringWriter::open_without_direct_io(&path).unwrap();
            assert_eq!(writer.next_lsn(), 3);
            let lsn = writer
                .append(RecordType::Put as u16, 1, 0, b"third")
                .unwrap();
            assert_eq!(lsn, 3);
            writer.submit_and_sync().unwrap();
        }

        let reader = WalReader::open(&path).unwrap();
        let records: Vec<_> = reader
            .records()
            .collect::<crate::error::Result<_>>()
            .unwrap();
        assert_eq!(records.len(), 3);
    }
}
