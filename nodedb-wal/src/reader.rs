//! WAL reader for crash recovery and replay.
//!
//! Reads records sequentially from a WAL file, validating checksums and
//! magic numbers. Stops at the first corruption point — everything before
//! that point is the committed prefix.
//!
//! ## Replay invariants
//!
//! - Replay is **deterministic**: the same WAL file always produces the
//!   same sequence of records.
//! - Replay is **idempotent**: replaying the same record twice has the
//!   same effect as replaying it once.
//! - Unknown optional record types (bit 15 clear) are skipped.
//! - Unknown required record types (bit 15 set) cause a replay failure.

use std::fs::File;
use std::io::Read;
use std::path::Path;

use crate::error::{Result, WalError};
use crate::record::{HEADER_SIZE, RecordHeader, RecordType, WalRecord};

/// Sequential WAL reader.
pub struct WalReader {
    file: File,
    offset: u64,
    /// Optional double-write buffer for torn write recovery.
    double_write: Option<crate::double_write::DoubleWriteBuffer>,
}

impl WalReader {
    /// Open a WAL file for reading.
    ///
    /// Automatically opens the companion double-write buffer file
    /// (`*.dwb`) if it exists alongside the WAL file.
    pub fn open(path: &Path) -> Result<Self> {
        let file = File::open(path)?;
        let dwb_path = path.with_extension("dwb");
        let double_write = if dwb_path.exists() {
            crate::double_write::DoubleWriteBuffer::open(&dwb_path).ok()
        } else {
            None
        };
        Ok(Self {
            file,
            offset: 0,
            double_write,
        })
    }

    /// Read the next record from the WAL.
    ///
    /// Returns `None` at EOF (clean end) or at the first corruption point.
    /// Returns `Err` only for I/O errors or unknown required record types.
    pub fn next_record(&mut self) -> Result<Option<WalRecord>> {
        // Read header.
        let mut header_buf = [0u8; HEADER_SIZE];
        match self.read_exact(&mut header_buf) {
            Ok(()) => {}
            Err(WalError::Io(e)) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                return Ok(None); // Clean EOF.
            }
            Err(e) => return Err(e),
        }

        let header = RecordHeader::from_bytes(&header_buf);

        // Validate magic and version.
        if header.validate(self.offset - HEADER_SIZE as u64).is_err() {
            // Corruption or end of valid data — treat as end of committed prefix.
            return Ok(None);
        }

        // Read payload.
        let mut payload = vec![0u8; header.payload_len as usize];
        if !payload.is_empty() {
            match self.read_exact(&mut payload) {
                Ok(()) => {}
                Err(WalError::Io(e)) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                    // Torn write — record is incomplete. This is the end of committed prefix.
                    return Ok(None);
                }
                Err(e) => return Err(e),
            }
        }

        let record = WalRecord { header, payload };

        // Verify checksum.
        if record.verify_checksum().is_err() {
            // Checksum mismatch — torn write or corruption.
            // Try to recover from double-write buffer if available.
            if let Some(dwb) = &mut self.double_write
                && let Ok(Some(recovered)) = dwb.recover_record(header.lsn)
            {
                tracing::info!(
                    lsn = header.lsn,
                    "recovered torn write from double-write buffer"
                );
                self.offset += recovered.payload.len() as u64;
                return Ok(Some(recovered));
            }
            // No DWB recovery possible — end of committed prefix.
            return Ok(None);
        }

        // Check if the record type is known (strip encrypted flag for lookup).
        let logical_type = record.logical_record_type();
        if RecordType::from_raw(logical_type).is_none() {
            if RecordType::is_required(logical_type) {
                return Err(WalError::UnknownRequiredRecordType {
                    record_type: header.record_type,
                    lsn: header.lsn,
                });
            }
            // Unknown optional record — skip it and continue.
            // (The record is already consumed, so just recurse.)
            return self.next_record();
        }

        Ok(Some(record))
    }

    /// Iterator over all valid records in the WAL.
    pub fn records(self) -> WalRecordIter {
        WalRecordIter { reader: self }
    }

    /// Current read offset in the file.
    pub fn offset(&self) -> u64 {
        self.offset
    }

    fn read_exact(&mut self, buf: &mut [u8]) -> Result<()> {
        self.file.read_exact(buf)?;
        self.offset += buf.len() as u64;
        Ok(())
    }
}

/// Iterator over WAL records.
pub struct WalRecordIter {
    reader: WalReader,
}

impl Iterator for WalRecordIter {
    type Item = Result<WalRecord>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.reader.next_record() {
            Ok(Some(record)) => Some(Ok(record)),
            Ok(None) => None,
            Err(e) => Some(Err(e)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::record::RecordType;
    use crate::writer::WalWriter;

    #[test]
    fn write_then_read_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.wal");

        // Write records.
        {
            let mut writer = WalWriter::open_without_direct_io(&path).unwrap();
            writer
                .append(RecordType::Put as u16, 1, 0, b"first")
                .unwrap();
            writer
                .append(RecordType::Put as u16, 2, 1, b"second")
                .unwrap();
            writer
                .append(RecordType::Delete as u16, 1, 0, b"third")
                .unwrap();
            writer.sync().unwrap();
        }

        // Read them back.
        let reader = WalReader::open(&path).unwrap();
        let records: Vec<_> = reader.records().collect::<Result<_>>().unwrap();

        assert_eq!(records.len(), 3);
        assert_eq!(records[0].header.lsn, 1);
        assert_eq!(records[0].header.tenant_id, 1);
        assert_eq!(records[0].payload, b"first");

        assert_eq!(records[1].header.lsn, 2);
        assert_eq!(records[1].header.tenant_id, 2);
        assert_eq!(records[1].header.vshard_id, 1);
        assert_eq!(records[1].payload, b"second");

        assert_eq!(records[2].header.lsn, 3);
        assert_eq!(records[2].header.record_type, RecordType::Delete as u16);
        assert_eq!(records[2].payload, b"third");
    }

    #[test]
    fn empty_wal_yields_no_records() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("empty.wal");

        // Create an empty file.
        {
            let mut writer = WalWriter::open_without_direct_io(&path).unwrap();
            writer.sync().unwrap();
        }

        let reader = WalReader::open(&path).unwrap();
        let records: Vec<_> = reader.records().collect::<Result<_>>().unwrap();
        assert!(records.is_empty());
    }

    #[test]
    fn truncated_file_stops_at_committed_prefix() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("truncated.wal");

        // Write records.
        {
            let mut writer = WalWriter::open_without_direct_io(&path).unwrap();
            writer
                .append(RecordType::Put as u16, 1, 0, b"good-record")
                .unwrap();
            writer.sync().unwrap();
        }

        // Append garbage (simulating a torn write).
        {
            use std::io::Write;
            let mut file = std::fs::OpenOptions::new()
                .append(true)
                .open(&path)
                .unwrap();
            file.write_all(b"GARBAGE_PARTIAL_RECORD").unwrap();
        }

        // Reader should return only the valid record.
        let reader = WalReader::open(&path).unwrap();
        let records: Vec<_> = reader.records().collect::<Result<_>>().unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].payload, b"good-record");
    }
}
