// SPDX-License-Identifier: Apache-2.0

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
//!
//! ## Encryption
//!
//! [`WalReader::open`] is the replay constructor: it takes the key ring and
//! hands out plaintext, so a consumer cannot act on ciphertext by forgetting to
//! decrypt. [`WalReader::open_raw`] is the opt-in structural constructor for
//! code that only walks record framing.

use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;

use crate::crypto::KeyRing;
use crate::error::{Result, WalError};
use crate::preamble::{SegmentPreamble, WAL_PREAMBLE_MAGIC, read_leading_preamble};
use crate::record::{HEADER_SIZE, RecordHeader, RecordType, WalRecord};
use crate::segment::SegmentDecryptor;

fn checked_offset_add(offset: u64, len: usize) -> Result<u64> {
    let len = u64::try_from(len).map_err(|_| {
        WalError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "WAL read length does not fit u64",
        ))
    })?;
    offset.checked_add(len).ok_or_else(|| {
        WalError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "WAL read offset overflow",
        ))
    })
}

/// Why a reader stopped producing records.
///
/// `Ok(None)` alone cannot distinguish "the segment ends here" from "the bytes
/// here are damaged". The difference decides whether the records that were
/// read are the whole committed prefix or only the part before a hole, so
/// readers record it and callers classify it via [`crate::torn_tail`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StopReason {
    /// The segment ended cleanly on a record boundary.
    Eof,

    /// A record at this offset failed structural validation (bad magic,
    /// unsupported version, short read, or checksum mismatch).
    Corruption { offset: u64 },
}

/// Sequential WAL reader.
pub struct WalReader {
    file: File,
    offset: u64,
    /// Preamble read from offset 0 of this segment (present when encryption
    /// is active). The epoch is used as part of the AAD for decryption.
    segment_preamble: Option<SegmentPreamble>,
    /// Optional double-write buffer for torn write recovery.
    double_write: Option<crate::double_write::DoubleWriteBuffer>,
    /// Why iteration stopped, once it has.
    stop_reason: Option<StopReason>,
    /// End of the last fully consumed record, padding included.
    committed_end: u64,
    /// Applied to every record before it is surfaced. `None` only in raw
    /// structural mode, where payloads may still be ciphertext.
    decryptor: Option<SegmentDecryptor>,
}

impl WalReader {
    /// Open a WAL file for replay.
    ///
    /// Every record is decrypted on the way out: what the caller receives never
    /// has `ENCRYPTED_FLAG` set, and its checksum verifies against the
    /// plaintext. `keys` must be the key ring the segment was written under, or
    /// `None` for a segment that was never encrypted — an encrypted record met
    /// with no key is [`WalError::EncryptedRecordWithoutKey`], never ciphertext
    /// handed to the caller.
    pub fn open(path: &Path, keys: Option<&KeyRing>) -> Result<Self> {
        let mut reader = Self::open_raw(path)?;
        reader.decryptor = Some(SegmentDecryptor::new(
            reader.segment_preamble.as_ref(),
            keys,
        ));
        Ok(reader)
    }

    /// Open a WAL file for structural scanning only.
    ///
    /// **Payloads may be ciphertext.** Records come out exactly as they are on
    /// disk, `ENCRYPTED_FLAG` and all. This exists for code that reads nothing
    /// but record framing — LSNs, lengths, offsets — and specifically for
    /// [`crate::recovery::recover`], which runs from `WalWriter::open` where no
    /// key ring exists and resuming an encrypted segment for appending must
    /// still work. Never use it for replay.
    ///
    /// If the file begins with a valid `WALP` preamble (16 bytes), it is
    /// consumed and stored for use as AAD during decryption. Files without a
    /// preamble (unencrypted segments) start reading from offset 0 directly.
    ///
    /// Automatically opens the companion double-write buffer file
    /// (`*.dwb`) if it exists alongside the WAL file.
    pub fn open_raw(path: &Path) -> Result<Self> {
        let mut file = File::open(path)?;
        let dwb_path = path.with_extension("dwb");
        let double_write = if dwb_path.exists() {
            crate::double_write::DoubleWriteBuffer::open(
                &dwb_path,
                crate::double_write::DwbMode::Buffered,
            )
            .ok()
        } else {
            None
        };

        let (segment_preamble, start_offset) =
            read_leading_preamble(&mut file, &WAL_PREAMBLE_MAGIC)?;

        Ok(Self {
            file,
            offset: start_offset,
            segment_preamble,
            double_write,
            stop_reason: None,
            committed_end: start_offset,
            decryptor: None,
        })
    }

    /// Hand a record to the caller, decrypting it first unless this reader was
    /// opened in raw structural mode.
    fn deliver(&self, record: WalRecord) -> Result<Option<WalRecord>> {
        match &self.decryptor {
            Some(decryptor) => decryptor.decrypt_record(record).map(Some),
            None => Ok(Some(record)),
        }
    }

    /// Byte offset just past the last fully consumed record.
    ///
    /// This is where a writer resumes appending. It is not the same as
    /// [`Self::offset`]: it includes any alignment padding that followed the
    /// final record, and it excludes a partially-read damaged record whose
    /// header the reader had to consume before rejecting it.
    pub fn committed_end(&self) -> u64 {
        self.committed_end
    }

    /// Why iteration stopped, or `None` while records are still being read.
    ///
    /// [`StopReason::Corruption`] does not by itself mean data was lost — it
    /// is the input to [`crate::torn_tail::verify_committed_prefix`], which
    /// decides whether the damage is an unfsynced tail or a hole with
    /// committed records behind it.
    pub fn stop_reason(&self) -> Option<StopReason> {
        self.stop_reason
    }

    fn stop(&mut self, reason: StopReason) -> Result<Option<WalRecord>> {
        self.stop_reason = Some(reason);
        Ok(None)
    }

    /// The preamble read from this segment file, if present.
    ///
    /// Returns `None` for unencrypted segments (no preamble written).
    pub fn segment_preamble(&self) -> Option<&SegmentPreamble> {
        self.segment_preamble.as_ref()
    }

    /// Read the next record from the WAL.
    ///
    /// Returns `None` at EOF (clean end) or at the first corruption point.
    /// Returns `Err` only for I/O errors or unknown required record types.
    pub fn next_record(&mut self) -> Result<Option<WalRecord>> {
        loop {
            // Read header.
            let record_offset = self.offset;
            let mut header_buf = [0u8; HEADER_SIZE];
            match self.read_exact(&mut header_buf) {
                Ok(()) => {}
                Err(WalError::Io(e)) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                    // A short trailing header is either a clean end or a torn
                    // tail; both are the boundary of the committed prefix.
                    let reason = if record_offset == self.file_len()? {
                        StopReason::Eof
                    } else {
                        StopReason::Corruption {
                            offset: record_offset,
                        }
                    };
                    return self.stop(reason);
                }
                Err(e) => return Err(e),
            }

            let header = RecordHeader::from_bytes(&header_buf);

            // Validate before using the attacker-controlled payload length for
            // allocation. Malformed oversized declarations are explicit errors;
            // ordinary corruption remains the end of the committed prefix.
            let header_offset = self
                .offset
                .checked_sub(u64::try_from(HEADER_SIZE).map_err(|_| {
                    WalError::Io(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "WAL header size does not fit u64",
                    ))
                })?)
                .ok_or_else(|| {
                    WalError::Io(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "WAL header offset underflow",
                    ))
                })?;
            match header.validate(header_offset) {
                Ok(()) => {}
                Err(error @ WalError::PayloadTooLarge { .. }) => return Err(error),
                Err(_) => {
                    return self.stop(StopReason::Corruption {
                        offset: header_offset,
                    });
                }
            }

            // Read payload only after the bounded header validation above.
            let payload_len =
                usize::try_from(header.payload_len).map_err(|_| WalError::PayloadTooLarge {
                    size: usize::MAX,
                    max: crate::record::MAX_WAL_PAYLOAD_SIZE,
                })?;
            let mut payload = vec![0u8; payload_len];
            if !payload.is_empty() {
                match self.read_exact(&mut payload) {
                    Ok(()) => {}
                    Err(WalError::Io(e)) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                        // The header promised more payload than the file holds.
                        return self.stop(StopReason::Corruption {
                            offset: header_offset,
                        });
                    }
                    Err(e) => return Err(e),
                }
            }

            let record = WalRecord { header, payload };

            // Verify checksum.
            if record.verify_checksum().is_err() {
                if let Some(recovered) = self.repair_torn_record(&header, header_offset)? {
                    return self.deliver(recovered);
                }
                return self.stop(StopReason::Corruption {
                    offset: header_offset,
                });
            }

            // The record is intact, so the bytes it occupies are committed
            // whether or not it is surfaced to the caller.
            self.committed_end = self.offset;

            // Check if the record type is known (strip encrypted flag for lookup).
            let logical_type = record.logical_record_type();
            let Some(kind) = RecordType::from_raw(logical_type) else {
                if RecordType::is_required(logical_type) {
                    return Err(WalError::UnknownRequiredRecordType {
                        record_type: header.record_type,
                        lsn: header.lsn,
                    });
                }
                // Unknown optional record — skip and continue loop.
                continue;
            };

            // Alignment padding is framing, not data — never surface it.
            if kind == RecordType::Noop {
                continue;
            }

            return self.deliver(record);
        }
    }

    /// Rebuild a record whose on-disk bytes failed CRC from the double-write
    /// buffer, if it holds an intact copy.
    ///
    /// Also re-anchors the read cursor: the torn header's own `payload_len` is
    /// not trustworthy, so the next record's position is derived from the DWB's
    /// authoritative copy rather than from wherever the read of the damaged
    /// bytes happened to leave the cursor.
    fn repair_torn_record(
        &mut self,
        header: &RecordHeader,
        header_offset: u64,
    ) -> Result<Option<WalRecord>> {
        let Some(dwb) = &mut self.double_write else {
            return Ok(None);
        };
        let Ok(Some(recovered)) = dwb.recover_record(header.lsn) else {
            return Ok(None);
        };
        tracing::info!(
            lsn = header.lsn,
            "recovered torn write from double-write buffer"
        );
        let resume = checked_offset_add(
            checked_offset_add(header_offset, HEADER_SIZE)?,
            recovered.payload.len(),
        )?;
        self.file.seek(SeekFrom::Start(resume))?;
        self.offset = resume;
        self.committed_end = resume;
        Ok(Some(recovered))
    }

    /// Iterator over all valid records in the WAL.
    pub fn records(self) -> WalRecordIter {
        WalRecordIter { reader: self }
    }

    /// Current read offset in the file.
    pub fn offset(&self) -> u64 {
        self.offset
    }

    fn file_len(&self) -> Result<u64> {
        Ok(self.file.metadata()?.len())
    }

    fn read_exact(&mut self, buf: &mut [u8]) -> Result<()> {
        let next_offset = checked_offset_add(self.offset, buf.len())?;
        self.file.read_exact(buf)?;
        self.offset = next_offset;
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
                .append(RecordType::Put as u32, 1, 0, 0, b"first")
                .unwrap();
            writer
                .append(RecordType::Put as u32, 2, 1, 0, b"second")
                .unwrap();
            writer
                .append(RecordType::Delete as u32, 1, 0, 0, b"third")
                .unwrap();
            writer.sync().unwrap();
        }

        // Read them back.
        let reader = WalReader::open(&path, None).unwrap();
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
        assert_eq!(records[2].header.record_type, RecordType::Delete as u32);
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

        let reader = WalReader::open(&path, None).unwrap();
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
                .append(RecordType::Put as u32, 1, 0, 0, b"good-record")
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
        let reader = WalReader::open(&path, None).unwrap();
        let records: Vec<_> = reader.records().collect::<Result<_>>().unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].payload, b"good-record");
    }

    #[test]
    fn oversized_payload_header_is_a_typed_error_before_allocation() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("oversized.wal");
        let header = RecordHeader {
            magic: crate::record::WAL_MAGIC,
            format_version: crate::record::WAL_FORMAT_VERSION,
            record_type: RecordType::Put as u32,
            lsn: 1,
            tenant_id: 1,
            vshard_id: 0,
            payload_len: (crate::record::MAX_WAL_PAYLOAD_SIZE + 1) as u32,
            database_id: 0,
            apply_key: 0,
            event_source: crate::record::NO_EVENT_SOURCE,
            crc32c: 0,
        };
        std::fs::write(&path, header.to_bytes()).unwrap();

        let mut reader = WalReader::open(&path, None).unwrap();
        assert!(matches!(
            reader.next_record(),
            Err(WalError::PayloadTooLarge { .. })
        ));
    }

    #[test]
    fn checked_offset_add_rejects_overflow() {
        assert!(checked_offset_add(u64::MAX, 1).is_err());
    }

    #[test]
    fn skip_many_unknown_optional_records_is_iterative() {
        // Record type 99 has bit 15 clear (99 & 0x8000 == 0) and is not a
        // known variant, so the reader must skip it as an unknown optional.
        // A recursive skip (`return self.next_record()`) would exhaust the
        // stack and panic on 50 000 consecutive unknown optional records.
        // The skip must be a loop, so all 50 000 are skipped without
        // overflow and the one valid record at the end is returned.
        const UNKNOWN_OPTIONAL: u32 = 99; // no 0x8000 bit → optional, not in enum
        const SKIP_COUNT: usize = 50_000;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("many_unknown.wal");

        {
            let mut writer = WalWriter::open_without_direct_io(&path).unwrap();
            for _ in 0..SKIP_COUNT {
                writer
                    .append(UNKNOWN_OPTIONAL, 1, 0, 0, b"skip-me")
                    .unwrap();
            }
            writer
                .append(RecordType::Put as u32, 1, 0, 0, b"keep-me")
                .unwrap();
            writer.sync().unwrap();
        }

        let reader = WalReader::open(&path, None).unwrap();
        let records: Vec<_> = reader.records().collect::<Result<_>>().unwrap();

        // Only the single known Put record survives; all unknown optional
        // records are silently discarded.
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].payload, b"keep-me");
    }
}
