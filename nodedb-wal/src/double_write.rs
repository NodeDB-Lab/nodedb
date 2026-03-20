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

use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use crate::error::{Result, WalError};
use crate::record::{HEADER_SIZE, RecordHeader, WAL_MAGIC, WalRecord};

/// Maximum number of records kept in the double-write buffer.
/// Only the most recent records matter — torn writes affect the tail.
const DWB_CAPACITY: usize = 64;

/// On-disk header: [magic: 4B][count: 4B][write_pos: 4B] = 12 bytes.
const DWB_HEADER_SIZE: usize = 12;
const DWB_MAGIC: u32 = 0x4457_4246; // "DWBF"

/// Double-write buffer file.
pub struct DoubleWriteBuffer {
    file: File,
    path: PathBuf,
    /// Current write position (circular, wraps at DWB_CAPACITY).
    write_pos: u32,
    /// Number of valid records in the buffer.
    count: u32,
}

impl std::fmt::Debug for DoubleWriteBuffer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DoubleWriteBuffer")
            .field("path", &self.path)
            .field("write_pos", &self.write_pos)
            .field("count", &self.count)
            .finish()
    }
}

impl DoubleWriteBuffer {
    /// Open or create the double-write buffer file.
    pub fn open(path: &Path) -> Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)
            .map_err(|e| {
                tracing::warn!(path = %path.display(), error = %e, "failed to open double-write buffer");
                WalError::Io(e)
            })?;

        let mut dwb = Self {
            file,
            path: path.to_path_buf(),
            write_pos: 0,
            count: 0,
        };

        // Try to read existing header.
        let file_len = dwb.file.metadata().map(|m| m.len()).unwrap_or(0);
        if file_len >= DWB_HEADER_SIZE as u64 {
            let mut header = [0u8; DWB_HEADER_SIZE];
            dwb.file.seek(SeekFrom::Start(0)).map_err(WalError::Io)?;
            if dwb.file.read_exact(&mut header).is_ok() {
                let mut arr4 = [0u8; 4];
                arr4.copy_from_slice(&header[0..4]);
                let magic = u32::from_le_bytes(arr4);
                if magic == DWB_MAGIC {
                    arr4.copy_from_slice(&header[4..8]);
                    dwb.count = u32::from_le_bytes(arr4);
                    arr4.copy_from_slice(&header[8..12]);
                    dwb.write_pos = u32::from_le_bytes(arr4);
                }
            }
        }

        Ok(dwb)
    }

    /// Write a WAL record to the double-write buffer before WAL append.
    ///
    /// The record is written at the current circular position. The header
    /// is updated with the new count and position. The file is fsynced.
    pub fn write_record(&mut self, record: &WalRecord) -> Result<()> {
        let record_bytes = record.header.to_bytes();
        let total_size = HEADER_SIZE + record.payload.len();

        // Write record at current slot position.
        // Each slot stores: [total_size: 4B][header: 30B][payload: N bytes]
        let slot_offset = DWB_HEADER_SIZE as u64
            + (self.write_pos as u64 % DWB_CAPACITY as u64) * (4 + HEADER_SIZE as u64 + 64 * 1024);
        // Max 64 KiB per slot — larger records skip the double-write buffer
        // (they're multi-page and need different protection).
        if total_size > 64 * 1024 {
            return Ok(()); // Skip oversized records.
        }

        self.file
            .seek(SeekFrom::Start(slot_offset))
            .map_err(WalError::Io)?;
        self.file
            .write_all(&(total_size as u32).to_le_bytes())
            .map_err(WalError::Io)?;
        self.file.write_all(&record_bytes).map_err(WalError::Io)?;
        self.file.write_all(&record.payload).map_err(WalError::Io)?;

        // Update position.
        self.write_pos = self.write_pos.wrapping_add(1);
        self.count = self.count.saturating_add(1).min(DWB_CAPACITY as u32);

        // Write header.
        self.file.seek(SeekFrom::Start(0)).map_err(WalError::Io)?;
        self.file
            .write_all(&DWB_MAGIC.to_le_bytes())
            .map_err(WalError::Io)?;
        self.file
            .write_all(&self.count.to_le_bytes())
            .map_err(WalError::Io)?;
        self.file
            .write_all(&self.write_pos.to_le_bytes())
            .map_err(WalError::Io)?;

        // Fsync the double-write buffer before the WAL write.
        self.file.sync_all().map_err(WalError::Io)?;

        Ok(())
    }

    /// Path to the double-write buffer file.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Try to recover a WAL record by LSN from the double-write buffer.
    ///
    /// Scans all slots for a record matching the given LSN with valid CRC.
    /// Returns `Some(WalRecord)` if found, `None` if not.
    pub fn recover_record(&mut self, target_lsn: u64) -> Result<Option<WalRecord>> {
        let slots_to_check = self.count.min(DWB_CAPACITY as u32) as usize;

        for i in 0..slots_to_check {
            let slot_offset =
                DWB_HEADER_SIZE as u64 + (i as u64) * (4 + HEADER_SIZE as u64 + 64 * 1024);

            self.file
                .seek(SeekFrom::Start(slot_offset))
                .map_err(WalError::Io)?;

            let mut size_buf = [0u8; 4];
            if self.file.read_exact(&mut size_buf).is_err() {
                continue;
            }
            let total_size = u32::from_le_bytes(size_buf) as usize;
            if !(HEADER_SIZE..=64 * 1024).contains(&total_size) {
                continue;
            }

            let mut header_buf = [0u8; HEADER_SIZE];
            if self.file.read_exact(&mut header_buf).is_err() {
                continue;
            }
            let header = RecordHeader::from_bytes(&header_buf);

            if header.magic != WAL_MAGIC || header.lsn != target_lsn {
                continue;
            }

            let payload_len = total_size - HEADER_SIZE;
            let mut payload = vec![0u8; payload_len];
            if self.file.read_exact(&mut payload).is_err() {
                continue;
            }

            let record = WalRecord { header, payload };
            if record.verify_checksum().is_ok() {
                return Ok(Some(record));
            }
        }

        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::record::RecordType;

    #[test]
    fn write_and_recover() {
        let dir = tempfile::tempdir().unwrap();
        let dwb_path = dir.path().join("test.dwb");

        let mut dwb = DoubleWriteBuffer::open(&dwb_path).unwrap();

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

        let mut dwb = DoubleWriteBuffer::open(&dwb_path).unwrap();
        let result = dwb.recover_record(999).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn survives_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let dwb_path = dir.path().join("reopen.dwb");

        {
            let mut dwb = DoubleWriteBuffer::open(&dwb_path).unwrap();
            let record =
                WalRecord::new(RecordType::Put as u16, 7, 1, 0, b"durable".to_vec(), None).unwrap();
            dwb.write_record(&record).unwrap();
        }

        let mut dwb = DoubleWriteBuffer::open(&dwb_path).unwrap();
        let recovered = dwb.recover_record(7).unwrap();
        assert!(recovered.is_some());
        assert_eq!(recovered.unwrap().payload, b"durable");
    }
}
