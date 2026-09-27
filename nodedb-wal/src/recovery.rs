// SPDX-License-Identifier: Apache-2.0

//! WAL recovery: scan an existing WAL file to determine the last committed LSN
//! and file offset, enabling safe reopening for continued writes.
//!
//! ## Recovery algorithm
//!
//! 1. Open the WAL file read-only.
//! 2. Scan forward, validating each record (magic, checksum).
//! 3. Stop at first corruption.
//! 4. Classify that stop: an unfsynced torn tail bounds the committed prefix,
//!    but damage with intact higher-LSN records behind it is a hole and fails
//!    recovery rather than silently dropping those records.
//! 5. Return the last valid LSN and the byte offset past the last valid record.
//!
//! ## Invariants
//!
//! - Recovery is deterministic: same file → same result.
//! - Recovery is idempotent: running twice gives the same answer.
//! - Truncated/torn writes are not errors — they're the boundary of committed data.
//! - Corruption that hides committed records IS an error — see [`crate::torn_tail`].

use std::path::Path;

use crate::error::{Result, WalError};
use crate::reader::WalReader;

/// Result of scanning a WAL file for recovery.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecoveryInfo {
    /// Last valid LSN found in the WAL. 0 if the WAL is empty.
    pub last_lsn: u64,

    /// Number of valid records found.
    pub record_count: u64,

    /// Byte offset past the last valid record (where new writes should begin).
    pub end_offset: u64,
}

impl RecoveryInfo {
    /// The next LSN to assign for new writes.
    pub fn next_lsn(&self) -> u64 {
        self.last_lsn + 1
    }
}

/// Scan a WAL file and recover the committed prefix.
///
/// Returns `RecoveryInfo` describing the state of the WAL, or an error
/// if the file cannot be opened.
pub fn recover(path: &Path) -> Result<RecoveryInfo> {
    if !path.exists() {
        return Ok(RecoveryInfo {
            last_lsn: 0,
            record_count: 0,
            end_offset: 0,
        });
    }

    // Structural scan only: recovery reads framing (LSN, lengths, offsets) and
    // never a payload. It runs from `WalWriter::open`, which resumes a segment
    // for appending and holds no key ring, so demanding keys here would make an
    // encrypted WAL impossible to reopen for writing.
    let mut reader = WalReader::open_raw(path)?;
    let mut last_lsn = 0u64;
    let mut record_count = 0u64;

    loop {
        match reader.next_record() {
            Ok(Some(record)) => {
                last_lsn = record.header.lsn;
                record_count += 1;
            }
            Ok(None) => {
                // End of committed prefix (EOF or corruption).
                break;
            }
            Err(e @ WalError::UnknownRequiredRecordType { .. }) => {
                // Cannot proceed past unknown required records.
                return Err(e);
            }
            Err(e) => return Err(e),
        }
    }

    // Refuse to call a hole "the end of the log".
    crate::torn_tail::verify_committed_prefix(path, reader.stop_reason(), last_lsn)?;

    Ok(RecoveryInfo {
        last_lsn,
        record_count,
        // The reader's own accounting is authoritative: it covers alignment
        // padding that trails the final record and torn records rebuilt from
        // the double-write buffer, neither of which can be reconstructed from
        // the returned records' headers.
        end_offset: reader.committed_end(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::record::RecordType;
    use crate::writer::WalWriter;

    #[test]
    fn recover_empty_wal() {
        let dir = crate::test_tempdir().unwrap();
        let path = dir.path().join("empty.wal");

        // Create empty WAL.
        {
            let mut writer = WalWriter::open_without_direct_io(&path).unwrap();
            writer.sync().unwrap();
        }

        let info = recover(&path).unwrap();
        assert_eq!(info.last_lsn, 0);
        assert_eq!(info.record_count, 0);
        assert_eq!(info.next_lsn(), 1);
    }

    #[test]
    fn recover_nonexistent_file() {
        let dir = crate::test_tempdir().unwrap();
        let path = dir.path().join("nonexistent.wal");

        let info = recover(&path).unwrap();
        assert_eq!(info.last_lsn, 0);
        assert_eq!(info.record_count, 0);
    }

    #[test]
    fn recover_with_records() {
        let dir = crate::test_tempdir().unwrap();
        let path = dir.path().join("test.wal");

        {
            let mut writer = WalWriter::open_without_direct_io(&path).unwrap();
            writer
                .append(RecordType::Put as u32, 1, 0, 0, b"first")
                .unwrap();
            writer
                .append(RecordType::Put as u32, 1, 0, 0, b"second")
                .unwrap();
            writer
                .append(RecordType::Delete as u32, 2, 1, 0, b"third")
                .unwrap();
            writer.sync().unwrap();
        }

        let info = recover(&path).unwrap();
        assert_eq!(info.last_lsn, 3);
        assert_eq!(info.record_count, 3);
        assert_eq!(info.next_lsn(), 4);
        assert!(info.end_offset > 0);
    }

    #[test]
    fn recover_truncated_wal() {
        let dir = crate::test_tempdir().unwrap();
        let path = dir.path().join("truncated.wal");

        {
            let mut writer = WalWriter::open_without_direct_io(&path).unwrap();
            writer
                .append(RecordType::Put as u32, 1, 0, 0, b"good")
                .unwrap();
            writer
                .append(RecordType::Put as u32, 1, 0, 0, b"also-good")
                .unwrap();
            writer.sync().unwrap();
        }

        // Append garbage (simulating torn write).
        {
            use std::io::Write;
            let mut file = std::fs::OpenOptions::new()
                .append(true)
                .open(&path)
                .unwrap();
            file.write_all(b"GARBAGE_TORN_WRITE_PARTIAL").unwrap();
        }

        let info = recover(&path).unwrap();
        assert_eq!(info.last_lsn, 2);
        assert_eq!(info.record_count, 2);
        assert_eq!(info.next_lsn(), 3);
    }
}
