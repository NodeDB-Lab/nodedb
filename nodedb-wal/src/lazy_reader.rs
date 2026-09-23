// SPDX-License-Identifier: Apache-2.0

//! Lazy WAL reader: reads headers without payload for selective replay.
//!
//! Unlike `WalReader` which reads every payload into a `Vec<u8>`, this
//! reader reads only the 30-byte header first. The caller inspects the
//! header (record_type, vshard_id, lsn) and decides whether to read or
//! skip the payload.
//!
//! This is critical for startup replay performance: with 100M timeseries
//! records, a vector core can skip TS payloads (potentially GBs) by
//! seeking forward instead of allocating and reading.
//!
//! ## Usage
//!
//! ```text
//! let mut reader = LazyWalReader::open(path, keys)?;
//! while let Some(header) = reader.next_header()? {
//!     if header.record_type == RecordType::VectorPut as u32 {
//!         let payload = reader.read_payload(&header)?;
//!         // process vector record — always plaintext
//!     } else {
//!         reader.skip_payload(&header)?;
//!     }
//! }
//! ```
//!
//! ## Encryption
//!
//! The key ring is supplied at `open` time and applied inside `read_payload`,
//! so a caller that reads a payload can never observe ciphertext: an encrypted
//! record opened without a ring is a hard error, not a passthrough.

use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;

use crate::crypto::KeyRing;
use crate::error::{Result, WalError};
use crate::preamble::{SegmentPreamble, WAL_PREAMBLE_MAGIC, read_leading_preamble};
use crate::reader::StopReason;
use crate::record::{HEADER_SIZE, MAX_WAL_PAYLOAD_SIZE, RecordHeader, RecordType, WalRecord};
use crate::segment::SegmentDecryptor;

fn checked_offset_add(offset: u64, len: u64) -> Result<u64> {
    offset.checked_add(len).ok_or_else(|| {
        WalError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "WAL lazy-reader offset overflow",
        ))
    })
}

/// Lazy WAL reader that separates header reading from payload reading.
pub struct LazyWalReader {
    file: File,
    offset: u64,
    /// Preamble read from offset 0 of this segment (present when encryption is
    /// active). The epoch is part of the AAD used to decrypt payloads.
    segment_preamble: Option<SegmentPreamble>,
    double_write: Option<crate::double_write::DoubleWriteBuffer>,
    stop_reason: Option<StopReason>,
    /// Turns encrypted payloads back into plaintext. Built once at open so the
    /// reader carries no borrow of the caller's ring, and so the per-segment
    /// AAD is derived once rather than on every payload read.
    decryptor: SegmentDecryptor,
}

impl LazyWalReader {
    /// Open a WAL file for lazy reading.
    ///
    /// A leading `WALP` preamble is consumed so headers are read from the first
    /// real record; segments written without encryption have none and start at
    /// offset 0.
    ///
    /// `keys` must be the key ring this segment was written under, or `None`
    /// for a segment that was never encrypted.
    pub fn open(path: &Path, keys: Option<&KeyRing>) -> Result<Self> {
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
        let decryptor = SegmentDecryptor::new(segment_preamble.as_ref(), keys);

        Ok(Self {
            file,
            offset: start_offset,
            segment_preamble,
            double_write,
            stop_reason: None,
            decryptor,
        })
    }

    /// The preamble read from this segment file, if present.
    ///
    /// Returns `None` for unencrypted segments (no preamble written).
    pub fn segment_preamble(&self) -> Option<&SegmentPreamble> {
        self.segment_preamble.as_ref()
    }

    /// Why iteration stopped, or `None` while headers are still being read.
    ///
    /// Feed this to [`crate::torn_tail::verify_committed_prefix`] to tell an
    /// unfsynced tail apart from a hole with committed records behind it.
    pub fn stop_reason(&self) -> Option<StopReason> {
        self.stop_reason
    }

    fn stop(&mut self, reason: StopReason) -> Result<Option<RecordHeader>> {
        self.stop_reason = Some(reason);
        Ok(None)
    }

    /// Read the next record header (54 bytes) without reading the payload.
    ///
    /// Returns `None` at EOF or first corruption. After this call, use
    /// either `read_payload()` to get the payload or `skip_payload()` to
    /// seek past it.
    ///
    /// Alignment padding records are consumed internally — the caller only
    /// ever sees real records.
    pub fn next_header(&mut self) -> Result<Option<RecordHeader>> {
        loop {
            let record_offset = self.offset;
            let mut header_buf = [0u8; HEADER_SIZE];
            match self.read_exact(&mut header_buf) {
                Ok(()) => {}
                Err(WalError::Io(e)) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                    let reason = if record_offset == self.file.metadata()?.len() {
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

            match header.validate(record_offset) {
                Ok(()) => {}
                Err(error @ WalError::PayloadTooLarge { .. }) => return Err(error),
                Err(_) => {
                    return self.stop(StopReason::Corruption {
                        offset: record_offset,
                    });
                }
            }

            // Check for unknown required record types.
            let logical_type = header.logical_record_type();
            match RecordType::from_raw(logical_type) {
                // Alignment padding is framing, not data: step over it and
                // keep going so batches after the first stay reachable.
                Some(RecordType::Noop) => {
                    self.skip_payload(&header)?;
                    continue;
                }
                Some(_) => {}
                None if RecordType::is_required(logical_type) => {
                    return Err(WalError::UnknownRequiredRecordType {
                        record_type: header.record_type,
                        lsn: header.lsn,
                    });
                }
                None => {}
            }

            return Ok(Some(header));
        }
    }

    /// Read the payload for a header that was just returned by `next_header()`,
    /// decrypting it if the record is encrypted.
    ///
    /// Must be called exactly once after `next_header()` returns `Some`,
    /// and before calling `next_header()` again (unless `skip_payload()`
    /// was called instead).
    pub fn read_payload(&mut self, header: &RecordHeader) -> Result<Vec<u8>> {
        let raw = self.read_raw_payload(header)?;
        self.decryptor.decrypt_payload(header, raw)
    }

    /// Read the on-disk payload bytes, repairing a torn write from the
    /// double-write buffer when possible. The result is still ciphertext for an
    /// encrypted record — only [`Self::read_payload`] is public so no caller
    /// can stop here.
    fn read_raw_payload(&mut self, header: &RecordHeader) -> Result<Vec<u8>> {
        header.validate(self.offset)?;
        let payload_len =
            usize::try_from(header.payload_len).map_err(|_| WalError::PayloadTooLarge {
                size: usize::MAX,
                max: MAX_WAL_PAYLOAD_SIZE,
            })?;
        let mut payload = vec![0u8; payload_len];
        if !payload.is_empty() {
            match self.read_exact(&mut payload) {
                Ok(()) => {}
                Err(WalError::Io(e)) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                    return Err(WalError::Io(std::io::Error::new(
                        std::io::ErrorKind::UnexpectedEof,
                        "torn write: incomplete payload",
                    )));
                }
                Err(e) => return Err(e),
            }
        }

        // Verify checksum without cloning the payload: constructing a WalRecord
        // just to call verify_checksum would allocate a duplicate buffer that
        // is thrown away immediately after.
        let checksum_ok = header.crc32c == header.compute_checksum(&payload);
        if !checksum_ok {
            // Try double-write buffer recovery.
            if let Some(dwb) = &mut self.double_write
                && let Ok(Some(recovered)) = dwb.recover_record(header.lsn)
            {
                // The torn header's `payload_len` decided how far the read
                // above advanced, and it cannot be trusted. Re-anchor on the
                // DWB's authoritative length so the next header is read from
                // the right place instead of from inside a record body.
                // `payload_len` and `recovered.payload.len()` are `usize`,
                // which always fits `u64` on every supported target, so this
                // widening conversion cannot fail.
                let payload_start =
                    self.offset.checked_sub(payload_len as u64).ok_or_else(|| {
                        WalError::Io(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            "WAL lazy-reader payload offset underflow",
                        ))
                    })?;
                let resume = checked_offset_add(payload_start, recovered.payload.len() as u64)?;
                self.file.seek(SeekFrom::Start(resume))?;
                self.offset = resume;
                return Ok(recovered.payload);
            }
            return Err(WalError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "checksum mismatch",
            )));
        }

        Ok(payload)
    }

    /// Read the payload and return a full plaintext WalRecord.
    ///
    /// The returned record is indistinguishable from one written without
    /// encryption: the flag is cleared and the CRC is recomputed, so downstream
    /// `logical_record_type()` dispatch and `verify_checksum()` both hold.
    pub fn read_record(&mut self, header: &RecordHeader) -> Result<WalRecord> {
        let raw = self.read_raw_payload(header)?;
        let record = WalRecord {
            header: *header,
            payload: raw,
        };
        self.decryptor.decrypt_record(record)
    }

    /// Skip the payload for a header, seeking forward without reading.
    ///
    /// This is the key optimization: non-matching records skip I/O entirely.
    pub fn skip_payload(&mut self, header: &RecordHeader) -> Result<()> {
        header.validate(self.offset)?;
        let len = u64::from(header.payload_len);
        let target = checked_offset_add(self.offset, len)?;
        let file_len = self.file.metadata()?.len();
        if target > file_len {
            return Err(WalError::Io(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "torn write: incomplete payload while skipping",
            )));
        }
        if len > 0 {
            let signed_len = i64::try_from(len).map_err(|_| {
                WalError::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "WAL lazy-reader skip length does not fit i64",
                ))
            })?;
            let position = self.file.seek(SeekFrom::Current(signed_len))?;
            if position != target {
                return Err(WalError::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "WAL lazy-reader seek did not reach bounded target",
                )));
            }
            self.offset = target;
        }
        Ok(())
    }

    /// Current file offset.
    pub fn offset(&self) -> u64 {
        self.offset
    }

    fn read_exact(&mut self, buf: &mut [u8]) -> Result<()> {
        let len = u64::try_from(buf.len()).map_err(|_| {
            WalError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "WAL lazy-reader read length does not fit u64",
            ))
        })?;
        let next_offset = checked_offset_add(self.offset, len)?;
        self.file.read_exact(buf)?;
        self.offset = next_offset;
        Ok(())
    }
}

/// Open a WAL segment for lazy reading and iterate with a callback.
///
/// Convenience function for single-pass replay: the callback receives each
/// header and decides whether to read or skip the payload. Payloads the
/// callback reads are plaintext; see [`LazyWalReader::open`] for `keys`.
pub fn replay_segment_lazy<F>(path: &Path, keys: Option<&KeyRing>, handler: F) -> Result<()>
where
    F: FnMut(&mut LazyWalReader, &RecordHeader) -> Result<()>,
{
    replay_one_segment(path, keys, handler).map(|_| ())
}

/// Replay a single segment and report the highest LSN it contains.
///
/// A reader stops at the first byte it cannot parse. That is the legal end of
/// the log only when nothing committed lies behind it, so the stop point is
/// classified before the segment is accepted as complete.
fn replay_one_segment<F>(path: &Path, keys: Option<&KeyRing>, mut handler: F) -> Result<u64>
where
    F: FnMut(&mut LazyWalReader, &RecordHeader) -> Result<()>,
{
    let mut reader = LazyWalReader::open(path, keys)?;
    let mut last_lsn = 0u64;
    while let Some(header) = reader.next_header()? {
        last_lsn = header.lsn;
        handler(&mut reader, &header)?;
    }
    crate::torn_tail::verify_committed_prefix(path, reader.stop_reason(), last_lsn)?;
    Ok(last_lsn)
}

/// Replay all WAL segments in a directory with lazy reading.
///
/// Segments are read in LSN order. The callback decides per-record whether
/// to read or skip the payload; payloads it reads are plaintext.
pub fn replay_all_segments_lazy<F>(
    wal_dir: &Path,
    keys: Option<&KeyRing>,
    mut handler: F,
) -> Result<()>
where
    F: FnMut(&mut LazyWalReader, &RecordHeader) -> Result<()>,
{
    let segments = crate::segment::discover_segments(wal_dir)?;
    let mut continuity = crate::segment::SegmentContinuity::new();
    for seg in &segments {
        continuity.check(seg)?;
        let last_lsn = replay_one_segment(&seg.path, keys, &mut handler)?;
        continuity.completed(seg, last_lsn);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::record::RecordType;
    use crate::writer::WalWriter;

    #[test]
    fn lazy_read_all_payloads() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.wal");

        {
            let mut w = WalWriter::open_without_direct_io(&path).unwrap();
            w.append(RecordType::Put as u32, 1, 0, 0, b"hello").unwrap();
            w.append(RecordType::VectorPut as u32, 1, 0, 0, b"vector-data")
                .unwrap();
            w.append(RecordType::Put as u32, 2, 1, 0, b"world").unwrap();
            w.sync().unwrap();
        }

        let mut reader = LazyWalReader::open(&path, None).unwrap();
        let mut records = Vec::new();
        while let Some(header) = reader.next_header().unwrap() {
            let payload = reader.read_payload(&header).unwrap();
            records.push((header, payload));
        }
        assert_eq!(records.len(), 3);
        assert_eq!(records[0].1, b"hello");
        assert_eq!(records[1].1, b"vector-data");
        assert_eq!(records[2].1, b"world");
    }

    #[test]
    fn lazy_skip_non_matching() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.wal");

        {
            let mut w = WalWriter::open_without_direct_io(&path).unwrap();
            // 3 big TS records, 1 small vector record.
            w.append(RecordType::TimeseriesBatch as u32, 1, 0, 0, &[0u8; 10000])
                .unwrap();
            w.append(RecordType::TimeseriesBatch as u32, 1, 0, 0, &[0u8; 10000])
                .unwrap();
            w.append(RecordType::VectorPut as u32, 1, 0, 0, b"small-vec")
                .unwrap();
            w.append(RecordType::TimeseriesBatch as u32, 1, 0, 0, &[0u8; 10000])
                .unwrap();
            w.sync().unwrap();
        }

        // A "vector core" reads only VectorPut, skips TimeseriesBatch.
        let mut reader = LazyWalReader::open(&path, None).unwrap();
        let mut vector_payloads = Vec::new();
        let mut skipped = 0;

        while let Some(header) = reader.next_header().unwrap() {
            let rt = RecordType::from_raw(header.record_type);
            if rt == Some(RecordType::VectorPut) {
                let payload = reader.read_payload(&header).unwrap();
                vector_payloads.push(payload);
            } else {
                reader.skip_payload(&header).unwrap();
                skipped += 1;
            }
        }

        assert_eq!(vector_payloads.len(), 1);
        assert_eq!(vector_payloads[0], b"small-vec");
        assert_eq!(skipped, 3);
    }

    #[test]
    fn oversized_payload_header_is_a_typed_error_before_skip_or_allocation() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("oversized.wal");
        let header = RecordHeader {
            magic: crate::record::WAL_MAGIC,
            format_version: crate::record::WAL_FORMAT_VERSION,
            record_type: RecordType::Put as u32,
            lsn: 1,
            tenant_id: 1,
            vshard_id: 0,
            payload_len: (MAX_WAL_PAYLOAD_SIZE + 1) as u32,
            database_id: 0,
            apply_key: 0,
            crc32c: 0,
        };
        std::fs::write(&path, header.to_bytes()).unwrap();

        let mut reader = LazyWalReader::open(&path, None).unwrap();
        assert!(matches!(
            reader.next_header(),
            Err(WalError::PayloadTooLarge { .. })
        ));
    }

    #[test]
    fn skip_payload_rejects_truncated_tail_without_seeking_beyond_eof() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("truncated.wal");
        let mut writer = WalWriter::open_without_direct_io(&path).unwrap();
        writer
            .append(RecordType::Put as u32, 1, 0, 0, b"payload")
            .unwrap();
        writer.sync().unwrap();
        drop(writer);
        let length = std::fs::metadata(&path).unwrap().len();
        std::fs::OpenOptions::new()
            .write(true)
            .open(&path)
            .unwrap()
            .set_len(length - 1)
            .unwrap();

        let mut reader = LazyWalReader::open(&path, None).unwrap();
        let header = reader.next_header().unwrap().expect("header");
        assert!(
            matches!(reader.skip_payload(&header), Err(WalError::Io(error)) if error.kind() == std::io::ErrorKind::UnexpectedEof)
        );
        assert_eq!(reader.offset(), HEADER_SIZE as u64);
    }

    #[test]
    fn checked_offset_add_rejects_overflow() {
        assert!(checked_offset_add(u64::MAX, 1).is_err());
    }

    #[test]
    fn replay_all_segments_lazy_works() {
        let dir = tempfile::tempdir().unwrap();
        // Use proper segment filename pattern: wal-{lsn:020}.seg
        let path = dir.path().join("wal-00000000000000000001.seg");

        {
            let mut w = WalWriter::open_without_direct_io(&path).unwrap();
            w.append(RecordType::Put as u32, 1, 0, 0, b"a").unwrap();
            w.append(RecordType::Put as u32, 1, 0, 0, b"b").unwrap();
            w.sync().unwrap();
        }

        let mut count = 0;
        replay_all_segments_lazy(dir.path(), None, |reader, header| {
            reader.skip_payload(header)?;
            count += 1;
            Ok(())
        })
        .unwrap();
        assert_eq!(count, 2);
    }

    #[test]
    fn empty_wal_no_records() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("empty.wal");

        {
            let mut w = WalWriter::open_without_direct_io(&path).unwrap();
            w.sync().unwrap();
        }

        let mut reader = LazyWalReader::open(&path, None).unwrap();
        assert!(reader.next_header().unwrap().is_none());
    }
}
