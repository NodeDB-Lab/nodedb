//! Double-write buffer for torn write protection.
//!
//! NVMe drives guarantee atomic 4 KiB sector writes but NOT atomic writes
//! for larger pages (e.g., 16 KiB). If power fails mid-write on a 16 KiB
//! page, the WAL page can be partially written (torn).
//!
//! CRC32C detects torn writes during replay, but without the double-write
//! buffer, the record is lost — even though it was acknowledged to the client.
//!
//! The double-write buffer solves this:
//! 1. Before writing to WAL, write the record to the double-write file.
//! 2. `fsync` the double-write file.
//! 3. Write to the WAL file.
//! 4. `fsync` the WAL file.
//!
//! On recovery, if a WAL record's CRC fails:
//! - Check the double-write buffer for an intact copy (verify CRC).
//! - If found, use the double-write copy to reconstruct the WAL page.
//! - If not found, the record is truly lost (pre-fsync crash).
//!
//! The double-write file is a fixed-size circular buffer. Only the most
//! recent N records are kept — older ones are overwritten. This is fine
//! because torn writes can only happen on the most recent write.
//!
//! ## O_DIRECT mode
//!
//! When the parent WAL uses `O_DIRECT`, the DWB can also be opened with
//! `O_DIRECT` (`DwbMode::Direct`). This:
//! - Keeps the page cache free of DWB bytes — the O_DIRECT WAL was
//!   specifically designed not to warm the cache, and a buffered DWB
//!   undoes that by writing the exact same payload through the cache.
//! - Surfaces DWB bytes in block-layer iostat traffic alongside the WAL.
//!
//! The on-disk layout is the same in both modes (one aligned header block
//! followed by fixed-stride slots, all block-aligned) so a DWB written in
//! one mode can be read in the other.

use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::os::unix::fs::OpenOptionsExt;
use std::os::unix::io::AsRawFd;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use crate::align::{AlignedBuf, DEFAULT_ALIGNMENT, is_aligned};
use crate::error::{Result, WalError};
use crate::record::{HEADER_SIZE, RecordHeader, WAL_MAGIC, WalRecord};

/// Maximum number of records kept in the double-write buffer.
/// Only the most recent records matter — torn writes affect the tail.
///
/// This is a compile-time constant used in slot offset arithmetic. It cannot
/// be made runtime-configurable without storing capacity in the struct and
/// adjusting all offset calculations accordingly. The value matches the
/// `WalTuning::dwb_capacity` default (64).
const DWB_CAPACITY: usize = 64;

/// Maximum payload bytes per slot (excluding the length prefix and header).
const DWB_SLOT_PAYLOAD_MAX: usize = 64 * 1024;

/// Raw slot content size: [len:4B][header][payload-up-to-64KiB].
const DWB_SLOT_RAW: usize = 4 + HEADER_SIZE + DWB_SLOT_PAYLOAD_MAX;

/// Per-slot on-disk stride, padded up to the O_DIRECT block size so every
/// slot offset is block-aligned. With `DWB_SLOT_RAW = 65570` and the default
/// 4 KiB alignment this rounds to 69632 bytes per slot.
const DWB_SLOT_STRIDE: usize = round_up_const(DWB_SLOT_RAW, DEFAULT_ALIGNMENT);

/// On-disk header occupies one aligned block (not the raw 12 bytes) so the
/// first slot starts at a block-aligned offset. The first 12 bytes of the
/// block carry the header fields; the remainder is zero-padded.
const DWB_HEADER_STRIDE: usize = DEFAULT_ALIGNMENT;
const DWB_HEADER_FIELDS: usize = 12;
const DWB_MAGIC: u32 = 0x4457_4246; // "DWBF"

/// Global counter: total bytes written to any DWB across the process.
/// Surfaces the duplicate-write cost of running the DWB alongside an
/// O_DIRECT WAL.
static DWB_BYTES_WRITTEN_TOTAL: AtomicU64 = AtomicU64::new(0);

/// Total bytes written to DWB files since process start.
pub fn wal_dwb_bytes_written_total() -> u64 {
    DWB_BYTES_WRITTEN_TOTAL.load(Ordering::Relaxed)
}

/// I/O mode for the double-write buffer file.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DwbMode {
    /// DWB disabled — no torn-write protection. `DoubleWriteBuffer::open`
    /// returns `None`.
    Off,
    /// Buffered I/O (page cache + `fsync`). Default when the parent WAL
    /// does not use `O_DIRECT`.
    Buffered,
    /// `O_DIRECT` I/O via an aligned buffer. The intended companion to an
    /// `O_DIRECT` WAL: keeps DWB bytes out of the page cache.
    Direct,
}

impl DwbMode {
    /// Choose the DWB mode that mirrors the parent writer's O_DIRECT setting
    /// when no explicit override is configured. With `O_DIRECT` on, the DWB
    /// should also be `O_DIRECT`, otherwise it undoes the cache-bypass.
    pub fn default_for_parent(parent_uses_direct_io: bool) -> Self {
        if parent_uses_direct_io {
            Self::Direct
        } else {
            Self::Buffered
        }
    }
}

const fn round_up_const(value: usize, align: usize) -> usize {
    (value + align - 1) & !(align - 1)
}

/// Slot stride in bytes. Exposed for tests and for callers that want to
/// size DWB files ahead of time.
pub const fn slot_stride() -> usize {
    DWB_SLOT_STRIDE
}

/// Byte offset of slot `idx` within the DWB file.
fn slot_offset(idx: u32) -> u64 {
    DWB_HEADER_STRIDE as u64 + (idx as u64 % DWB_CAPACITY as u64) * DWB_SLOT_STRIDE as u64
}

/// Double-write buffer file.
pub struct DoubleWriteBuffer {
    file: File,
    path: PathBuf,
    mode: DwbMode,
    /// Current write position (circular, wraps at DWB_CAPACITY).
    write_pos: u32,
    /// Number of valid records in the buffer.
    count: u32,
    /// Whether there are deferred writes that haven't been fsynced.
    dirty: bool,
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
    /// Returns `None`-wrapped errors for unsupported modes via
    /// `Err(WalError::…)`; callers that want "off" should not call this at all.
    pub fn open(path: &Path, mode: DwbMode) -> Result<Self> {
        if mode == DwbMode::Off {
            return Err(WalError::DwbOffNotOpenable);
        }

        let mut opts = OpenOptions::new();
        opts.read(true).write(true).create(true).truncate(false);
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

    /// Write a WAL record to the double-write buffer before WAL append.
    ///
    /// The record is written at the current circular position and the file
    /// is fsynced immediately. Use `write_record_deferred` + `flush` for
    /// batch mode (multiple records per fsync).
    pub fn write_record(&mut self, record: &WalRecord) -> Result<()> {
        self.write_record_deferred(record)?;
        self.flush()
    }

    /// Write a WAL record to the DWB without fsyncing.
    ///
    /// The data is written to the OS page cache (Buffered mode) or directly
    /// to the block device (Direct mode) but not guaranteed durable until
    /// `flush()` is called. Use this in batch mode: write all records in a
    /// group commit batch, then call `flush()` once — reducing fsync calls
    /// from N-per-batch to 1-per-batch.
    pub fn write_record_deferred(&mut self, record: &WalRecord) -> Result<()> {
        let total_size = HEADER_SIZE + record.payload.len();

        // Max 64 KiB per slot — larger records skip the double-write buffer
        // (they're multi-page and need different protection).
        if total_size > DWB_SLOT_PAYLOAD_MAX {
            return Ok(()); // Skip oversized records.
        }

        let header_bytes = record.header.to_bytes();
        let offset = slot_offset(self.write_pos);

        match self.mode {
            DwbMode::Off => unreachable!("Off never opens a DoubleWriteBuffer"),
            DwbMode::Buffered => {
                self.file
                    .seek(SeekFrom::Start(offset))
                    .map_err(WalError::Io)?;
                self.file
                    .write_all(&(total_size as u32).to_le_bytes())
                    .map_err(WalError::Io)?;
                self.file.write_all(&header_bytes).map_err(WalError::Io)?;
                self.file.write_all(&record.payload).map_err(WalError::Io)?;
                DWB_BYTES_WRITTEN_TOTAL.fetch_add(
                    (4 + header_bytes.len() + record.payload.len()) as u64,
                    Ordering::Relaxed,
                );
            }
            DwbMode::Direct => {
                let buf = self
                    .slot_buf
                    .as_mut()
                    .expect("slot_buf present in Direct mode");
                buf.clear();
                buf.write(&(total_size as u32).to_le_bytes());
                buf.write(&header_bytes);
                buf.write(&record.payload);
                // Zero the tail so the full aligned slot can be written
                // without leaking prior contents.
                zero_tail(buf);
                let slice = full_capacity_slice(buf);
                debug_assert_eq!(slice.len(), DWB_SLOT_STRIDE);
                debug_assert!(is_aligned(offset as usize, DEFAULT_ALIGNMENT));
                pwrite_all(&self.file, slice, offset)?;
                DWB_BYTES_WRITTEN_TOTAL.fetch_add(slice.len() as u64, Ordering::Relaxed);
            }
        }

        self.write_pos = self.write_pos.wrapping_add(1);
        self.count = self.count.saturating_add(1).min(DWB_CAPACITY as u32);
        self.dirty = true;

        Ok(())
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
            DwbMode::Off => unreachable!(),
            DwbMode::Buffered => {
                self.file.seek(SeekFrom::Start(0)).map_err(WalError::Io)?;
                self.file.write_all(&header).map_err(WalError::Io)?;
                DWB_BYTES_WRITTEN_TOTAL.fetch_add(header.len() as u64, Ordering::Relaxed);
            }
            DwbMode::Direct => {
                let buf = self
                    .header_buf
                    .as_mut()
                    .expect("header_buf present in Direct mode");
                buf.clear();
                buf.write(&header);
                zero_tail(buf);
                let slice = full_capacity_slice(buf);
                debug_assert_eq!(slice.len(), DWB_HEADER_STRIDE);
                pwrite_all(&self.file, slice, 0)?;
                DWB_BYTES_WRITTEN_TOTAL.fetch_add(slice.len() as u64, Ordering::Relaxed);
            }
        }

        self.file.sync_all().map_err(WalError::Io)?;
        self.dirty = false;

        Ok(())
    }

    /// Path to the double-write buffer file.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Try to recover a WAL record by LSN from the double-write buffer.
    ///
    /// Scans **all** DWB_CAPACITY slots for a record matching the given LSN
    /// with valid CRC. We scan every slot rather than relying on `count` or
    /// `write_pos` because the header itself may be stale or corrupted after
    /// a crash. Each slot is self-describing: the record's own CRC validates
    /// whether the slot contains usable data.
    pub fn recover_record(&mut self, target_lsn: u64) -> Result<Option<WalRecord>> {
        // Under O_DIRECT, reads must also use aligned buffers and aligned
        // lengths. Read one full aligned slot at a time, then parse.
        let mut slot = AlignedBuf::new(DWB_SLOT_STRIDE, DEFAULT_ALIGNMENT)?;

        for i in 0..DWB_CAPACITY as u32 {
            let offset = slot_offset(i);
            // SAFETY: slot.as_mut_ptr is valid for `capacity()` bytes.
            let read = unsafe {
                libc::pread(
                    self.file.as_raw_fd(),
                    slot.as_mut_ptr() as *mut libc::c_void,
                    DWB_SLOT_STRIDE,
                    offset as libc::off_t,
                )
            };
            if read <= 0 {
                continue;
            }
            // SAFETY: the kernel populated `read` bytes starting at the buffer.
            let bytes: &[u8] = unsafe { std::slice::from_raw_parts(slot.as_ptr(), read as usize) };
            if bytes.len() < 4 + HEADER_SIZE {
                continue;
            }

            let mut arr4 = [0u8; 4];
            arr4.copy_from_slice(&bytes[0..4]);
            let total_size = u32::from_le_bytes(arr4) as usize;
            if !(HEADER_SIZE..=DWB_SLOT_PAYLOAD_MAX).contains(&total_size)
                || bytes.len() < 4 + total_size
            {
                continue;
            }

            let mut header_buf = [0u8; HEADER_SIZE];
            header_buf.copy_from_slice(&bytes[4..4 + HEADER_SIZE]);
            let header = RecordHeader::from_bytes(&header_buf);
            if header.magic != WAL_MAGIC || header.lsn != target_lsn {
                continue;
            }

            let payload_len = total_size - HEADER_SIZE;
            let payload = bytes[4 + HEADER_SIZE..4 + HEADER_SIZE + payload_len].to_vec();
            let record = WalRecord { header, payload };
            if record.verify_checksum().is_ok() {
                return Ok(Some(record));
            }
        }

        Ok(None)
    }
}

/// Fill the unwritten tail of `buf` with zero bytes so an O_DIRECT write of
/// the entire aligned slot does not leak stale buffer contents to disk.
fn zero_tail(buf: &mut AlignedBuf) {
    let written = buf.len();
    let cap = buf.capacity();
    if written < cap {
        // SAFETY: `as_mut_ptr` is valid for `capacity` bytes; we write only
        // the uninitialized tail between `written..capacity`.
        unsafe {
            std::ptr::write_bytes(buf.as_mut_ptr().add(written), 0, cap - written);
        }
    }
}

/// View the entire allocated capacity of `buf` as a byte slice. Requires
/// that the caller has zeroed any unwritten tail (see `zero_tail`).
fn full_capacity_slice(buf: &AlignedBuf) -> &[u8] {
    // SAFETY: AlignedBuf guarantees `as_ptr` points to `capacity()` valid
    // bytes (alloc_zeroed) for the lifetime of the buffer.
    unsafe { std::slice::from_raw_parts(buf.as_ptr(), buf.capacity()) }
}

/// `pwrite`-retry helper that handles short writes.
fn pwrite_all(file: &File, mut data: &[u8], mut offset: u64) -> Result<()> {
    let fd = file.as_raw_fd();
    while !data.is_empty() {
        let n = unsafe {
            libc::pwrite(
                fd,
                data.as_ptr() as *const libc::c_void,
                data.len(),
                offset as libc::off_t,
            )
        };
        if n < 0 {
            return Err(WalError::Io(std::io::Error::last_os_error()));
        }
        let n = n as usize;
        data = &data[n..];
        offset += n as u64;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::record::RecordType;

    fn open_buffered(path: &Path) -> DoubleWriteBuffer {
        DoubleWriteBuffer::open(path, DwbMode::Buffered).unwrap()
    }

    #[test]
    fn write_and_recover() {
        let dir = tempfile::tempdir().unwrap();
        let dwb_path = dir.path().join("test.dwb");

        let mut dwb = open_buffered(&dwb_path);

        let record = WalRecord::new(
            RecordType::Put as u16,
            42,
            1,
            0,
            b"hello double-write".to_vec(),
            None,
        )
        .unwrap();

        dwb.write_record(&record).unwrap();

        // Recover by LSN.
        let recovered = dwb.recover_record(42).unwrap();
        assert!(recovered.is_some());
        let rec = recovered.unwrap();
        assert_eq!(rec.header.lsn, 42);
        assert_eq!(rec.payload, b"hello double-write");
    }

    #[test]
    fn recover_nonexistent_returns_none() {
        let dir = tempfile::tempdir().unwrap();
        let dwb_path = dir.path().join("test2.dwb");

        let mut dwb = open_buffered(&dwb_path);
        let result = dwb.recover_record(999).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn survives_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let dwb_path = dir.path().join("reopen.dwb");

        {
            let mut dwb = open_buffered(&dwb_path);
            let record =
                WalRecord::new(RecordType::Put as u16, 7, 1, 0, b"durable".to_vec(), None).unwrap();
            dwb.write_record(&record).unwrap();
        }

        let mut dwb = open_buffered(&dwb_path);
        let recovered = dwb.recover_record(7).unwrap();
        assert!(recovered.is_some());
        assert_eq!(recovered.unwrap().payload, b"durable");
    }

    #[test]
    fn batch_deferred_writes_and_flush() {
        let dir = tempfile::tempdir().unwrap();
        let dwb_path = dir.path().join("batch.dwb");

        let mut dwb = open_buffered(&dwb_path);

        for lsn in 1..=5u64 {
            let record = WalRecord::new(
                RecordType::Put as u16,
                lsn,
                1,
                0,
                format!("batch-{lsn}").into_bytes(),
                None,
            )
            .unwrap();
            dwb.write_record_deferred(&record).unwrap();
        }

        assert!(dwb.dirty);
        dwb.flush().unwrap();
        assert!(!dwb.dirty);

        for lsn in 1..=5u64 {
            let recovered = dwb.recover_record(lsn).unwrap();
            assert!(recovered.is_some(), "LSN {lsn} should be recoverable");
            assert_eq!(
                recovered.unwrap().payload,
                format!("batch-{lsn}").into_bytes()
            );
        }
    }

    #[test]
    fn flush_is_idempotent() {
        let dir = tempfile::tempdir().unwrap();
        let dwb_path = dir.path().join("idem.dwb");

        let mut dwb = open_buffered(&dwb_path);

        dwb.flush().unwrap();
        assert!(!dwb.dirty);

        let record =
            WalRecord::new(RecordType::Put as u16, 1, 1, 0, b"data".to_vec(), None).unwrap();
        dwb.write_record_deferred(&record).unwrap();
        dwb.flush().unwrap();
        dwb.flush().unwrap();
        assert!(!dwb.dirty);
    }

    #[test]
    fn slot_stride_is_o_direct_aligned() {
        // The DWB slot stride must be a multiple of the WAL alignment
        // (4 KiB) so the file can be opened with O_DIRECT alongside an
        // O_DIRECT WAL. With a non-aligned stride, every slot after the
        // first lands at an unaligned offset and the kernel rejects the
        // write with -EINVAL.
        assert!(
            is_aligned(DWB_SLOT_STRIDE, DEFAULT_ALIGNMENT),
            "DWB slot stride {DWB_SLOT_STRIDE} bytes is not a multiple of {DEFAULT_ALIGNMENT}"
        );
        assert!(is_aligned(DWB_HEADER_STRIDE, DEFAULT_ALIGNMENT));
        for i in 0..DWB_CAPACITY as u32 {
            assert!(is_aligned(slot_offset(i) as usize, DEFAULT_ALIGNMENT));
        }
    }

    #[test]
    fn recover_after_wraparound() {
        let dir = tempfile::tempdir().unwrap();
        let dwb_path = dir.path().join("wrap.dwb");

        let mut dwb = open_buffered(&dwb_path);

        let total = DWB_CAPACITY as u64 + 5;
        for lsn in 1..=total {
            let record = WalRecord::new(
                RecordType::Put as u16,
                lsn,
                1,
                0,
                format!("wrap-{lsn}").into_bytes(),
                None,
            )
            .unwrap();
            dwb.write_record_deferred(&record).unwrap();
        }
        dwb.flush().unwrap();

        for lsn in (total - 4)..=total {
            let recovered = dwb.recover_record(lsn).unwrap();
            assert!(
                recovered.is_some(),
                "LSN {lsn} should be recoverable after wrap-around"
            );
            assert_eq!(
                recovered.unwrap().payload,
                format!("wrap-{lsn}").into_bytes()
            );
        }

        for lsn in 1..=5u64 {
            let recovered = dwb.recover_record(lsn).unwrap();
            assert!(
                recovered.is_none(),
                "LSN {lsn} should have been overwritten by wrap-around"
            );
        }
    }

    #[test]
    fn bytes_written_counter_increments() {
        let dir = tempfile::tempdir().unwrap();
        let dwb_path = dir.path().join("counter.dwb");
        let before = wal_dwb_bytes_written_total();

        let mut dwb = open_buffered(&dwb_path);
        let rec =
            WalRecord::new(RecordType::Put as u16, 1, 1, 0, b"counted".to_vec(), None).unwrap();
        dwb.write_record(&rec).unwrap();

        assert!(wal_dwb_bytes_written_total() > before);
    }
}
