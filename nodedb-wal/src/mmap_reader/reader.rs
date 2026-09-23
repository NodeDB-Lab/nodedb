// SPDX-License-Identifier: Apache-2.0

//! Memory-mapped WAL segment reader for Event Plane catchup.
//!
//! Unlike the standard `WalReader` (which uses sequential `read_exact`),
//! this reader maps sealed WAL segments into the process address space via
//! `mmap`. The kernel manages the page cache — no slab allocator memory is
//! pinned, and mmap reads from page cache don't contend with the Data Plane's
//! O_DIRECT WAL append path (O_DIRECT bypasses page cache entirely).
//!
//! **Tier progression:**
//! 1. In-memory Arc slabs (hot, zero-copy from ring buffer)
//! 2. Mmap WAL segment reads (warm, kernel-managed pages)
//! 3. Shed consumer + cold WAL replay (last resort)
//!
//! This reader is used in tier 2: when the Event Plane enters WAL Catchup
//! Mode, it mmap's the relevant sealed segments and iterates records.
//!
//! ## Encryption
//!
//! [`MmapWalReader::open`] is the replay constructor: it takes the key ring and
//! hands out plaintext. [`MmapWalReader::open_raw`] is the opt-in structural
//! constructor for code that only walks record framing.

use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

#[cfg(target_os = "linux")]
use std::os::fd::AsRawFd;

use memmap2::Mmap;

use crate::crypto::KeyRing;
use crate::error::{Result, WalError};
use crate::preamble::{PREAMBLE_SIZE, SegmentPreamble, WAL_PREAMBLE_MAGIC, parse_leading_preamble};
use crate::reader::StopReason;
use crate::record::{HEADER_SIZE, RecordHeader, RecordType, WAL_MAGIC, WalRecord};
use crate::segment::SegmentDecryptor;

fn checked_range_end(start: usize, len: usize, limit: usize) -> Result<Option<usize>> {
    let end = start.checked_add(len).ok_or_else(|| {
        WalError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "mmap WAL offset overflow",
        ))
    })?;
    Ok((end <= limit).then_some(end))
}

/// Module-scoped atomic counters for observing mmap and fadvise behaviour in
/// production. These counters are incremented by the live code paths (open,
/// madvise, fadvise) and may be read from tests or from a metrics scrape.
pub mod observability {
    use super::{AtomicU64, Ordering};
    pub(super) static SEGMENTS_OPENED: AtomicU64 = AtomicU64::new(0);
    pub(super) static FADV_DONTNEED_COUNT: AtomicU64 = AtomicU64::new(0);
    pub(super) static MADV_SEQUENTIAL_COUNT: AtomicU64 = AtomicU64::new(0);

    pub fn segments_opened() -> u64 {
        SEGMENTS_OPENED.load(Ordering::Relaxed)
    }
    pub fn fadv_dontneed_count() -> u64 {
        FADV_DONTNEED_COUNT.load(Ordering::Relaxed)
    }
    pub fn madv_sequential_count() -> u64 {
        MADV_SEQUENTIAL_COUNT.load(Ordering::Relaxed)
    }
}

/// Call `posix_fadvise(POSIX_FADV_DONTNEED)` on an open WAL segment fd.
///
/// Once a segment has been iterated end-to-end during catchup, we don't
/// need its pages in cache any longer. Release them back to the kernel so
/// replay doesn't pin GiBs of page cache.
fn fadv_dontneed(fd: &std::fs::File, len: usize, path: &Path) {
    if len == 0 {
        return;
    }
    #[cfg(target_os = "linux")]
    {
        let len = match libc::off_t::try_from(len) {
            Ok(len) => len,
            Err(_) => {
                tracing::warn!(
                    path = %path.display(),
                    "WAL segment length does not fit posix_fadvise offset; skipping cache release",
                );
                return;
            }
        };
        let rc = unsafe { libc::posix_fadvise(fd.as_raw_fd(), 0, len, libc::POSIX_FADV_DONTNEED) };
        if rc == 0 {
            observability::FADV_DONTNEED_COUNT.fetch_add(1, Ordering::Relaxed);
        } else {
            tracing::warn!(
                path = %path.display(),
                errno = rc,
                "posix_fadvise(DONTNEED) failed on exhausted WAL segment",
            );
        }
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = (fd, path);
    }
}

/// Memory-mapped WAL segment reader.
///
/// Opens a sealed WAL segment file via mmap and provides zero-copy
/// iteration over records. The mmap'd region is read-only and the
/// kernel manages page residency — no application-level memory pinning.
pub struct MmapWalReader {
    mmap: Mmap,
    offset: usize,
    /// Preamble mapped at offset 0 of this segment (present when encryption is
    /// active). The epoch is part of the AAD used to decrypt payloads.
    segment_preamble: Option<SegmentPreamble>,
    file: std::fs::File,
    path: std::path::PathBuf,
    madvise_state: Option<libc::c_int>,
    stop_reason: Option<StopReason>,
    /// Applied to every record before it is surfaced. `None` only in raw
    /// structural mode, where payloads may still be ciphertext.
    decryptor: Option<SegmentDecryptor>,
}

impl MmapWalReader {
    /// Open a WAL segment file for mmap'd replay.
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

    /// Open a WAL segment file for structural scanning only.
    ///
    /// **Payloads may be ciphertext.** Records come out exactly as they are on
    /// disk, `ENCRYPTED_FLAG` and all. Use it only for code that reads record
    /// framing and never the payload; replay must use [`Self::open`].
    ///
    /// A leading `WALP` preamble is skipped so iteration begins at the first
    /// real record; segments written without encryption have none and start at
    /// offset 0.
    pub fn open_raw(path: &Path) -> Result<Self> {
        observability::SEGMENTS_OPENED.fetch_add(1, Ordering::Relaxed);
        let file = std::fs::File::open(path)?;
        // SAFETY: The file is a sealed WAL segment (not being written to).
        // The Data Plane writes to the ACTIVE segment via O_DIRECT; sealed
        // segments are immutable after rollover.
        let mmap = unsafe { Mmap::map(&file)? };

        // Catchup iterates forward through a segment. MADV_SEQUENTIAL
        // doubles readahead and drops already-consumed pages eagerly so
        // replay doesn't grow buff/cache by the full WAL size.
        let mut madvise_state = None;
        if !mmap.is_empty() {
            let rc = unsafe {
                libc::madvise(
                    mmap.as_ptr() as *mut libc::c_void,
                    mmap.len(),
                    libc::MADV_SEQUENTIAL,
                )
            };
            if rc == 0 {
                madvise_state = Some(libc::MADV_SEQUENTIAL);
                observability::MADV_SEQUENTIAL_COUNT.fetch_add(1, Ordering::Relaxed);
            } else {
                tracing::warn!(
                    path = %path.display(),
                    errno = std::io::Error::last_os_error().raw_os_error().unwrap_or(0),
                    "madvise(MADV_SEQUENTIAL) failed on WAL segment; continuing",
                );
            }
        }

        let segment_preamble = parse_leading_preamble(&mmap, &WAL_PREAMBLE_MAGIC)?;
        let offset = if segment_preamble.is_some() {
            PREAMBLE_SIZE
        } else {
            0
        };

        Ok(Self {
            mmap,
            offset,
            segment_preamble,
            file,
            path: path.to_path_buf(),
            madvise_state,
            stop_reason: None,
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

    /// The preamble read from this segment file, if present.
    ///
    /// Returns `None` for unencrypted segments (no preamble written).
    pub fn segment_preamble(&self) -> Option<&SegmentPreamble> {
        self.segment_preamble.as_ref()
    }

    /// Why iteration stopped, or `None` while records are still being read.
    ///
    /// Feed this to [`crate::torn_tail::verify_committed_prefix`] to tell an
    /// unfsynced tail apart from a hole with committed records behind it.
    pub fn stop_reason(&self) -> Option<StopReason> {
        self.stop_reason
    }

    fn stop(&mut self, reason: StopReason) -> Result<Option<WalRecord>> {
        self.stop_reason = Some(reason);
        Ok(None)
    }

    /// The madvise hint applied to the mapped segment (if any).
    pub fn madvise_state(&self) -> Option<libc::c_int> {
        self.madvise_state
    }

    /// Hint to the kernel that pages for this segment can be dropped from
    /// cache. Call this after a segment has been iterated end-to-end.
    pub fn release_pages(&self) {
        fadv_dontneed(&self.file, self.mmap.len(), &self.path);
    }

    /// Read the next record from the mmap'd region.
    ///
    /// Returns `None` at EOF or at the first corruption point.
    /// Header parsing avoids extra copies, but the payload is copied out of
    /// the mmap'd region into an owned `Vec<u8>` on `WalRecord`: records must
    /// outlive the reader (they cross thread boundaries in parallel replay
    /// and get mutated in place during decryption), which a borrow of the
    /// mapping cannot support.
    pub fn next_record(&mut self) -> Result<Option<WalRecord>> {
        let data = &self.mmap[..];

        loop {
            // Check if we have enough bytes for a header without unchecked
            // offset arithmetic. A short trailing header is a torn tail.
            let record_offset = self.offset;
            let Some(header_end) = checked_range_end(self.offset, HEADER_SIZE, data.len())? else {
                let reason = if record_offset == data.len() {
                    StopReason::Eof
                } else {
                    StopReason::Corruption {
                        offset: record_offset as u64,
                    }
                };
                return self.stop(reason);
            };

            // Parse header.
            let header_bytes: &[u8; HEADER_SIZE] = data
                .get(self.offset..header_end)
                .and_then(|bytes| bytes.try_into().ok())
                .ok_or_else(|| {
                    WalError::Io(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "header slice conversion failed",
                    ))
                })?;
            let header = RecordHeader::from_bytes(header_bytes);

            // Validate magic — corruption or end of valid data.
            if header.magic != WAL_MAGIC {
                return self.stop(StopReason::Corruption {
                    offset: record_offset as u64,
                });
            }

            // Oversized declarations are explicit errors before allocation;
            // ordinary malformed headers preserve committed-prefix semantics.
            let header_offset = u64::try_from(self.offset).map_err(|_| {
                WalError::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "mmap WAL offset does not fit u64",
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

            let payload_len =
                usize::try_from(header.payload_len).map_err(|_| WalError::PayloadTooLarge {
                    size: usize::MAX,
                    max: crate::record::MAX_WAL_PAYLOAD_SIZE,
                })?;
            let Some(record_end) = checked_range_end(header_end, payload_len, data.len())? else {
                // The header promised more payload than the segment holds.
                return self.stop(StopReason::Corruption {
                    offset: header_offset,
                });
            };

            // Extract payload only after the bounded header validation above.
            let payload = data
                .get(header_end..record_end)
                .ok_or_else(|| {
                    WalError::Io(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "payload slice conversion failed",
                    ))
                })?
                .to_vec();
            self.offset = record_end;

            let record = WalRecord { header, payload };

            // Verify checksum.
            if record.verify_checksum().is_err() {
                return self.stop(StopReason::Corruption {
                    offset: header_offset,
                });
            }

            // Check record type.
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

    /// Iterator over all valid records in the mmap'd segment.
    pub fn records(self) -> MmapRecordIter {
        MmapRecordIter { reader: self }
    }

    /// Current read offset.
    pub fn offset(&self) -> usize {
        self.offset
    }

    /// Total size of the mmap'd region.
    pub fn len(&self) -> usize {
        self.mmap.len()
    }

    /// Whether the mmap'd region is empty.
    pub fn is_empty(&self) -> bool {
        self.mmap.is_empty()
    }
}

/// Iterator over records in a mmap'd WAL segment.
pub struct MmapRecordIter {
    reader: MmapWalReader,
}

impl Iterator for MmapRecordIter {
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
    use crate::writer::{WalWriter, WalWriterConfig};

    fn test_writer(path: &Path) -> WalWriter {
        let config = WalWriterConfig {
            use_direct_io: false, // Tests run without O_DIRECT.
            ..Default::default()
        };
        WalWriter::open(path, config).unwrap()
    }

    #[test]
    fn mmap_reader_basic() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.wal");

        // Write some records with the standard writer.
        {
            let mut writer = test_writer(&path);
            writer
                .append(RecordType::Put as u32, 1, 0, 0, b"hello")
                .unwrap();
            writer
                .append(RecordType::Put as u32, 1, 0, 0, b"world")
                .unwrap();
            writer.sync().unwrap();
        }

        // Read back with mmap reader.
        let reader = MmapWalReader::open(&path, None).unwrap();
        assert!(
            reader.segment_preamble().is_none(),
            "unencrypted segments carry no preamble"
        );
        let records: Vec<WalRecord> = reader.records().collect::<Result<Vec<_>>>().unwrap();

        assert_eq!(records.len(), 2);
        assert_eq!(records[0].payload, b"hello");
        assert_eq!(records[1].payload, b"world");
    }

    #[test]
    fn mmap_reader_empty_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("empty.wal");
        std::fs::write(&path, []).unwrap();

        let reader = MmapWalReader::open(&path, None).unwrap();
        let records: Vec<WalRecord> = reader.records().collect::<Result<Vec<_>>>().unwrap();
        assert!(records.is_empty());
    }

    #[test]
    fn mmap_reader_truncated_header() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("truncated.wal");
        // Write 10 bytes — not enough for a header (30 bytes).
        std::fs::write(&path, [0u8; 10]).unwrap();

        let reader = MmapWalReader::open(&path, None).unwrap();
        let records: Vec<WalRecord> = reader.records().collect::<Result<Vec<_>>>().unwrap();
        assert!(records.is_empty());
    }

    #[test]
    fn mmap_reader_truncated_payload_stops_at_committed_prefix() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("truncated-payload.wal");
        let header = RecordHeader {
            magic: WAL_MAGIC,
            format_version: crate::record::WAL_FORMAT_VERSION,
            record_type: RecordType::Put as u32,
            lsn: 1,
            tenant_id: 1,
            vshard_id: 0,
            payload_len: 1,
            database_id: 0,
            apply_key: 0,
            crc32c: 0,
        };
        std::fs::write(&path, header.to_bytes()).unwrap();

        let mut reader = MmapWalReader::open(&path, None).unwrap();
        assert!(reader.next_record().unwrap().is_none());
    }

    #[test]
    fn oversized_payload_header_is_a_typed_error_before_allocation() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("oversized.wal");
        let header = RecordHeader {
            magic: WAL_MAGIC,
            format_version: crate::record::WAL_FORMAT_VERSION,
            record_type: RecordType::Put as u32,
            lsn: 1,
            tenant_id: 1,
            vshard_id: 0,
            payload_len: (crate::record::MAX_WAL_PAYLOAD_SIZE + 1) as u32,
            database_id: 0,
            apply_key: 0,
            crc32c: 0,
        };
        std::fs::write(&path, header.to_bytes()).unwrap();

        let mut reader = MmapWalReader::open(&path, None).unwrap();
        assert!(matches!(
            reader.next_record(),
            Err(WalError::PayloadTooLarge { .. })
        ));
    }

    #[test]
    fn checked_range_end_rejects_overflow_and_short_ranges() {
        assert!(checked_range_end(usize::MAX, 1, usize::MAX).is_err());
        assert_eq!(checked_range_end(5, 2, 6).unwrap(), None);
    }
}
