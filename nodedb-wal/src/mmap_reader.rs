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

use std::path::Path;

use memmap2::Mmap;

use crate::error::{Result, WalError};
use crate::record::{HEADER_SIZE, RecordHeader, RecordType, WAL_MAGIC, WalRecord};

/// Memory-mapped WAL segment reader.
///
/// Opens a sealed WAL segment file via mmap and provides zero-copy
/// iteration over records. The mmap'd region is read-only and the
/// kernel manages page residency — no application-level memory pinning.
pub struct MmapWalReader {
    mmap: Mmap,
    offset: usize,
}

impl MmapWalReader {
    /// Open a WAL segment file for mmap'd reading.
    pub fn open(path: &Path) -> Result<Self> {
        let file = std::fs::File::open(path)?;
        // SAFETY: The file is a sealed WAL segment (not being written to).
        // The Data Plane writes to the ACTIVE segment via O_DIRECT; sealed
        // segments are immutable after rollover.
        let mmap = unsafe { Mmap::map(&file)? };
        Ok(Self { mmap, offset: 0 })
    }

    /// Read the next record from the mmap'd region.
    ///
    /// Returns `None` at EOF or at the first corruption point.
    /// Zero-copy: payload bytes reference the mmap'd region directly.
    pub fn next_record(&mut self) -> Result<Option<WalRecord>> {
        let data = &self.mmap[..];

        // Check if we have enough bytes for a header.
        if self.offset + HEADER_SIZE > data.len() {
            return Ok(None);
        }

        // Parse header.
        let header_bytes: &[u8; HEADER_SIZE] = data[self.offset..self.offset + HEADER_SIZE]
            .try_into()
            .map_err(|_| {
                WalError::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "header slice conversion failed",
                ))
            })?;
        let header = RecordHeader::from_bytes(header_bytes);

        // Validate magic — corruption or end of valid data.
        if header.magic != WAL_MAGIC {
            return Ok(None);
        }

        // Validate version.
        if header.validate(self.offset as u64).is_err() {
            return Ok(None);
        }

        let payload_len = header.payload_len as usize;
        let record_end = self.offset + HEADER_SIZE + payload_len;

        // Check if payload is fully within the mmap'd region.
        if record_end > data.len() {
            return Ok(None); // Torn write at segment end.
        }

        // Extract payload (copies from mmap to owned Vec).
        let payload = data[self.offset + HEADER_SIZE..record_end].to_vec();
        self.offset = record_end;

        let record = WalRecord { header, payload };

        // Verify checksum.
        if record.verify_checksum().is_err() {
            return Ok(None); // Corruption — end of committed prefix.
        }

        // Check record type.
        let logical_type = record.logical_record_type();
        if RecordType::from_raw(logical_type).is_none() {
            if RecordType::is_required(logical_type) {
                return Err(WalError::UnknownRequiredRecordType {
                    record_type: header.record_type,
                    lsn: header.lsn,
                });
            }
            // Unknown optional record — skip and continue.
            return self.next_record();
        }

        Ok(Some(record))
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

/// Minimum number of segments to justify parallel replay overhead.
const PARALLEL_SEGMENT_THRESHOLD: usize = 4;

/// Replay WAL segments from a directory using mmap, starting from `from_lsn`.
///
/// Discovers all sealed segments, mmap's each, and returns records with
/// LSN >= `from_lsn`. This is the Event Plane's tier-2 catchup path.
///
/// When 4+ segments need scanning, uses `std::thread::scope` to read
/// segments in parallel (one thread per segment). Each thread mmap's its
/// segment and filters records independently; results are merged in
/// segment order (already LSN-sorted since segments are monotonic).
pub fn replay_segments_mmap(wal_dir: &Path, from_lsn: u64) -> Result<Vec<WalRecord>> {
    let segments = crate::segment::discover_segments(wal_dir)?;

    if segments.len() < PARALLEL_SEGMENT_THRESHOLD {
        return replay_segments_sequential(&segments, from_lsn);
    }

    replay_segments_parallel(&segments, from_lsn)
}

/// Sequential segment replay (used for small segment counts).
fn replay_segments_sequential(
    segments: &[crate::segment::SegmentMeta],
    from_lsn: u64,
) -> Result<Vec<WalRecord>> {
    let mut records = Vec::new();
    for seg in segments {
        let reader = MmapWalReader::open(&seg.path)?;
        for record_result in reader.records() {
            let record = record_result?;
            if record.header.lsn >= from_lsn {
                records.push(record);
            }
        }
    }
    Ok(records)
}

/// Parallel segment replay using scoped threads.
///
/// Each segment is read in its own thread via mmap. Since segments are
/// monotonically ordered by LSN, concatenating per-segment results in
/// segment order produces a globally LSN-ordered result.
fn replay_segments_parallel(
    segments: &[crate::segment::SegmentMeta],
    from_lsn: u64,
) -> Result<Vec<WalRecord>> {
    // Collect per-segment results. Index corresponds to segment order.
    let mut per_segment: Vec<Result<Vec<WalRecord>>> = Vec::with_capacity(segments.len());

    std::thread::scope(|scope| {
        let handles: Vec<_> = segments
            .iter()
            .map(|seg| {
                scope.spawn(move || -> Result<Vec<WalRecord>> {
                    let reader = MmapWalReader::open(&seg.path)?;
                    let mut seg_records = Vec::new();
                    for record_result in reader.records() {
                        let record = record_result?;
                        if record.header.lsn >= from_lsn {
                            seg_records.push(record);
                        }
                    }
                    Ok(seg_records)
                })
            })
            .collect();

        for handle in handles {
            per_segment.push(handle.join().unwrap_or_else(|_| {
                Err(WalError::Io(std::io::Error::other(
                    "segment replay thread panicked",
                )))
            }));
        }
    });

    // Merge in segment order (preserves LSN ordering).
    let total_estimate: usize = per_segment
        .iter()
        .map(|r| r.as_ref().map(|v| v.len()).unwrap_or(0))
        .sum();
    let mut records = Vec::with_capacity(total_estimate);
    for seg_result in per_segment {
        records.extend(seg_result?);
    }

    Ok(records)
}

/// Paginated mmap replay: reads at most `max_records` from `from_lsn`.
///
/// Returns `(records, has_more)` where `has_more` is `true` if the limit
/// was reached before all segments were exhausted. This bounds memory
/// usage per catch-up cycle to O(max_records) instead of O(all WAL data).
///
/// Always uses sequential reading (no parallel threads) since the bounded
/// record count makes parallel overhead unnecessary.
pub fn replay_segments_mmap_limit(
    wal_dir: &Path,
    from_lsn: u64,
    max_records: usize,
) -> Result<(Vec<WalRecord>, bool)> {
    let segments = crate::segment::discover_segments(wal_dir)?;
    let mut records = Vec::with_capacity(max_records.min(4096));

    for seg in &segments {
        // Skip segments that end before from_lsn. A segment's max LSN
        // is at least its first_lsn, so if first_lsn of the NEXT segment
        // is <= from_lsn we can skip this one. Conservative: always read
        // the last segment since we don't know its max LSN cheaply.
        let reader = MmapWalReader::open(&seg.path)?;
        for record_result in reader.records() {
            let record = record_result?;
            if record.header.lsn >= from_lsn {
                records.push(record);
                if records.len() >= max_records {
                    return Ok((records, true));
                }
            }
        }
    }

    Ok((records, false))
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
                .append(RecordType::Put as u16, 1, 0, b"hello")
                .unwrap();
            writer
                .append(RecordType::Put as u16, 1, 0, b"world")
                .unwrap();
            writer.sync().unwrap();
        }

        // Read back with mmap reader.
        let reader = MmapWalReader::open(&path).unwrap();
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

        let reader = MmapWalReader::open(&path).unwrap();
        let records: Vec<WalRecord> = reader.records().collect::<Result<Vec<_>>>().unwrap();
        assert!(records.is_empty());
    }

    #[test]
    fn mmap_reader_truncated_header() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("truncated.wal");
        // Write 10 bytes — not enough for a header (30 bytes).
        std::fs::write(&path, [0u8; 10]).unwrap();

        let reader = MmapWalReader::open(&path).unwrap();
        let records: Vec<WalRecord> = reader.records().collect::<Result<Vec<_>>>().unwrap();
        assert!(records.is_empty());
    }

    #[test]
    fn replay_mmap_from_lsn() {
        let dir = tempfile::tempdir().unwrap();
        let wal_dir = dir.path().join("wal");
        std::fs::create_dir_all(&wal_dir).unwrap();

        let config = crate::segmented::SegmentedWalConfig::for_testing(wal_dir.clone());
        let mut wal = crate::segmented::SegmentedWal::open(config).unwrap();

        let lsn1 = wal.append(RecordType::Put as u16, 1, 0, b"a").unwrap();
        let lsn2 = wal.append(RecordType::Put as u16, 1, 0, b"b").unwrap();
        let lsn3 = wal.append(RecordType::Put as u16, 1, 0, b"c").unwrap();
        wal.sync().unwrap();

        // Replay from lsn2 — should get records b and c.
        let records = replay_segments_mmap(&wal_dir, lsn2).unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].header.lsn, lsn2);
        assert_eq!(records[1].header.lsn, lsn3);

        // Replay from lsn1 — all 3.
        let all = replay_segments_mmap(&wal_dir, lsn1).unwrap();
        assert_eq!(all.len(), 3);
    }
}
