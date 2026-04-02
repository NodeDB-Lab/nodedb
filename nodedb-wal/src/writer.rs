//! WAL writer with O_DIRECT and group commit.
//!
//! The writer accumulates records into an aligned buffer and flushes to disk
//! when the buffer is full or when an explicit sync is requested.
//!
//! ## I/O path
//!
//! 1. Caller creates a `WalRecord` and submits it to the writer.
//! 2. Writer serializes the record into the aligned write buffer.
//! 3. When the buffer is full or `sync()` is called, the buffer is written
//!    to the WAL file via `O_DIRECT` + `fsync`.
//! 4. Group commit: multiple concurrent writers can submit records between
//!    syncs, and they all share a single `fsync` call.
//!
//! ## Future: io_uring
//!
//! The current implementation uses standard `pwrite` + `fsync` with O_DIRECT.
//! Phase 1 validates correctness and determinism. io_uring submission will be
//! added once the bridge crate provides the TPC event loop integration.

use std::fs::{File, OpenOptions};
use std::os::unix::fs::OpenOptionsExt;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::align::{AlignedBuf, DEFAULT_ALIGNMENT};
use crate::error::{Result, WalError};
use crate::record::{HEADER_SIZE, WalRecord};

/// Default write buffer size: 2 MiB.
///
/// This is the batch size for group commit. Records accumulate here until
/// the buffer is full or `sync()` is called.
///
/// Matches `WalTuning::write_buffer_size` default. Override via
/// `WalWriterConfig::write_buffer_size` at construction time.
pub const DEFAULT_WRITE_BUFFER_SIZE: usize = 2 * 1024 * 1024;

/// Configuration for the WAL writer.
#[derive(Debug, Clone)]
pub struct WalWriterConfig {
    /// Size of the aligned write buffer (rounded up to alignment).
    pub write_buffer_size: usize,

    /// O_DIRECT alignment (typically 4096 for NVMe).
    pub alignment: usize,

    /// Whether to use O_DIRECT. Set to `false` for testing on filesystems
    /// that don't support it (e.g., tmpfs).
    pub use_direct_io: bool,
}

impl Default for WalWriterConfig {
    fn default() -> Self {
        Self {
            write_buffer_size: DEFAULT_WRITE_BUFFER_SIZE,
            alignment: DEFAULT_ALIGNMENT,
            use_direct_io: true,
        }
    }
}

/// Append-only WAL writer.
pub struct WalWriter {
    /// The WAL file handle (opened with O_DIRECT if configured).
    file: File,

    /// Aligned write buffer for batching records before flush.
    buffer: AlignedBuf,

    /// Current file write offset (always aligned).
    file_offset: u64,

    /// Next LSN to assign.
    next_lsn: AtomicU64,

    /// Whether the writer has been sealed (no more writes accepted).
    sealed: bool,

    /// Configuration.
    config: WalWriterConfig,

    /// Optional key ring for payload encryption (supports key rotation).
    encryption_ring: Option<crate::crypto::KeyRing>,

    /// Optional double-write buffer for torn write protection.
    /// Records are written here before the WAL for crash recovery.
    double_write: Option<crate::double_write::DoubleWriteBuffer>,
}

impl WalWriter {
    /// Open or create a WAL file at the given path.
    pub fn open(path: &Path, config: WalWriterConfig) -> Result<Self> {
        let mut opts = OpenOptions::new();
        opts.create(true).write(true).append(false);

        if config.use_direct_io {
            // O_DIRECT: bypass page cache.
            opts.custom_flags(libc::O_DIRECT);
        }

        let file = opts.open(path)?;

        let buffer = AlignedBuf::new(config.write_buffer_size, config.alignment)?;

        // Scan existing WAL for recovery if the file has data.
        let (file_offset, next_lsn) = if path.exists() && std::fs::metadata(path)?.len() > 0 {
            let info = crate::recovery::recover(path)?;
            (info.end_offset, info.next_lsn())
        } else {
            (0, 1)
        };

        // Open double-write buffer alongside the WAL file.
        let dwb_path = path.with_extension("dwb");
        let double_write = crate::double_write::DoubleWriteBuffer::open(&dwb_path).ok();

        Ok(Self {
            file,
            buffer,
            file_offset,
            next_lsn: AtomicU64::new(next_lsn),
            sealed: false,
            config,
            encryption_ring: None,
            double_write,
        })
    }

    /// Set the encryption key. When set, all subsequent records will have
    /// their payloads encrypted with AES-256-GCM.
    pub fn set_encryption_key(&mut self, key: crate::crypto::WalEncryptionKey) {
        self.encryption_ring = Some(crate::crypto::KeyRing::new(key));
    }

    /// Set the key ring directly (for key rotation with dual-key reads).
    pub fn set_encryption_ring(&mut self, ring: crate::crypto::KeyRing) {
        self.encryption_ring = Some(ring);
    }

    /// Access the key ring (for decryption during replay).
    pub fn encryption_ring(&self) -> Option<&crate::crypto::KeyRing> {
        self.encryption_ring.as_ref()
    }

    /// Open a new WAL segment file with a specific starting LSN.
    ///
    /// Used by `SegmentedWal` when rolling to a new segment. The file must
    /// not already exist (or be empty). The writer will assign LSNs starting
    /// from `start_lsn`.
    pub fn open_with_start_lsn(
        path: &Path,
        config: WalWriterConfig,
        start_lsn: u64,
    ) -> Result<Self> {
        let mut opts = OpenOptions::new();
        opts.create(true).write(true).append(false);

        if config.use_direct_io {
            opts.custom_flags(libc::O_DIRECT);
        }

        let file = opts.open(path)?;
        let buffer = AlignedBuf::new(config.write_buffer_size, config.alignment)?;

        let dwb_path = path.with_extension("dwb");
        let double_write = crate::double_write::DoubleWriteBuffer::open(&dwb_path).ok();

        Ok(Self {
            file,
            buffer,
            file_offset: 0,
            next_lsn: AtomicU64::new(start_lsn),
            sealed: false,
            config,
            encryption_ring: None,
            double_write,
        })
    }

    /// Open a WAL writer with O_DIRECT disabled (for testing on tmpfs, etc.).
    pub fn open_without_direct_io(path: &Path) -> Result<Self> {
        Self::open(
            path,
            WalWriterConfig {
                use_direct_io: false,
                ..Default::default()
            },
        )
    }

    /// Append a record to the WAL. Returns the assigned LSN.
    ///
    /// The record is written to the in-memory buffer. Call `sync()` to
    /// flush to disk and make the write durable.
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
            self.encryption_ring.as_ref().map(|r| r.current()),
        )?;

        // Write to double-write buffer (deferred — no fsync yet).
        // The DWB is fsynced in batch during `sync()`, before the WAL fsync.
        // This amortizes DWB fsync cost across the entire group commit batch.
        //
        // DWB failure is non-fatal for the write itself (the WAL is the
        // authoritative store), but we log a warning because it means
        // torn-write recovery is degraded. If the DWB is persistently
        // broken, we detach it to avoid repeated error noise.
        if let Some(dwb) = &mut self.double_write
            && let Err(e) = dwb.write_record_deferred(&record)
        {
            tracing::warn!(
                lsn = lsn,
                error = %e,
                "DWB write failed — torn-write protection degraded, detaching DWB"
            );
            self.double_write = None;
        }

        let header_bytes = record.header.to_bytes();
        let total_size = HEADER_SIZE + record.payload.len();

        // If this record doesn't fit in the remaining buffer, flush first.
        if self.buffer.remaining() < total_size {
            self.flush_buffer()?;
        }

        // If the record is larger than the entire buffer, we have a problem.
        // This shouldn't happen with MAX_WAL_PAYLOAD_SIZE checks, but guard anyway.
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

    /// Flush the write buffer to disk (group commit).
    ///
    /// This issues a single write + fsync for all records accumulated
    /// since the last flush. The DWB is also fsynced (one fsync for all
    /// deferred DWB writes in this batch).
    pub fn sync(&mut self) -> Result<()> {
        if self.buffer.is_empty() {
            return Ok(());
        }
        // Flush DWB first — records must be durable in DWB before WAL.
        // If DWB flush fails, torn-write protection is lost for this batch.
        // We log a warning and detach the DWB rather than silently continuing
        // as if torn-write protection is active.
        if let Some(dwb) = &mut self.double_write
            && let Err(e) = dwb.flush()
        {
            tracing::warn!(
                error = %e,
                "DWB flush failed — torn-write protection lost for this batch, detaching DWB"
            );
            self.double_write = None;
        }
        self.flush_buffer()?;
        self.file.sync_all()?;
        Ok(())
    }

    /// Seal the WAL — no more writes will be accepted.
    ///
    /// Flushes any buffered data before sealing.
    pub fn seal(&mut self) -> Result<()> {
        self.sync()?;
        self.sealed = true;
        Ok(())
    }

    /// The next LSN that will be assigned.
    pub fn next_lsn(&self) -> u64 {
        self.next_lsn.load(Ordering::Relaxed)
    }

    /// Current file size (bytes written to disk).
    pub fn file_offset(&self) -> u64 {
        self.file_offset
    }

    /// Flush the aligned buffer to the file.
    fn flush_buffer(&mut self) -> Result<()> {
        if self.buffer.is_empty() {
            return Ok(());
        }

        let data = if self.config.use_direct_io {
            // O_DIRECT requires aligned I/O size.
            self.buffer.as_aligned_slice()
        } else {
            // Without O_DIRECT, write only the actual data.
            self.buffer.as_slice()
        };

        // Use pwrite to write at the exact offset.
        #[cfg(unix)]
        {
            use std::os::unix::io::AsRawFd;
            let fd = self.file.as_raw_fd();
            let written = unsafe {
                libc::pwrite(
                    fd,
                    data.as_ptr() as *const libc::c_void,
                    data.len(),
                    self.file_offset as libc::off_t,
                )
            };
            if written < 0 {
                return Err(WalError::Io(std::io::Error::last_os_error()));
            }
        }

        self.file_offset += self.buffer.len() as u64;
        self.buffer.clear();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::record::RecordType;

    #[test]
    fn write_and_sync_single_record() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.wal");

        let mut writer = WalWriter::open_without_direct_io(&path).unwrap();
        let lsn = writer
            .append(RecordType::Put as u16, 1, 0, b"hello")
            .unwrap();
        assert_eq!(lsn, 1);

        writer.sync().unwrap();
        assert!(writer.file_offset() > 0);
    }

    #[test]
    fn lsn_increments() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.wal");

        let mut writer = WalWriter::open_without_direct_io(&path).unwrap();

        let lsn1 = writer
            .append(RecordType::Put as u16, 1, 0, b"first")
            .unwrap();
        let lsn2 = writer
            .append(RecordType::Put as u16, 1, 0, b"second")
            .unwrap();
        let lsn3 = writer
            .append(RecordType::Put as u16, 1, 0, b"third")
            .unwrap();

        assert_eq!(lsn1, 1);
        assert_eq!(lsn2, 2);
        assert_eq!(lsn3, 3);
    }

    #[test]
    fn sealed_writer_rejects_writes() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.wal");

        let mut writer = WalWriter::open_without_direct_io(&path).unwrap();
        writer.seal().unwrap();

        assert!(matches!(
            writer.append(RecordType::Put as u16, 1, 0, b"rejected"),
            Err(WalError::Sealed)
        ));
    }
}
