// SPDX-License-Identifier: BUSL-1.1

//! In-memory walk of one archived WAL segment image: record framing, time
//! anchors, write-abort markers, and the physical cut at a target LSN.
//!
//! The walk reads framing the way the segment reader does: an optional
//! preamble, then back-to-back records, each checked against its CRC32C. It
//! stops at the first record that does not check. `Noop` records are
//! alignment padding and carry no LSN.

use nodedb_types::temporal::LsnTimeAnchor;
use nodedb_wal::WalError;
use nodedb_wal::crypto::KeyRing;
use nodedb_wal::preamble::{
    PREAMBLE_SIZE, SegmentPreamble, WAL_PREAMBLE_MAGIC, parse_leading_preamble,
};
use nodedb_wal::record::{
    HEADER_SIZE, RecordHeader, RecordType, RestorePointPayload, SurrogateAllocPayload,
    TimeAnchorPayload, WalRecord, WalRecordArgs, WriteAbortedPayload, padding_record, padding_span,
};
use nodedb_wal::segment::SegmentDecryptor;

use super::error::RestoreError;

/// A write-abort marker: the record at `marker_lsn` refuses the record at
/// `aborted_lsn`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AbortMarker {
    pub marker_lsn: u64,
    pub aborted_lsn: u64,
}

/// What one walk of a segment found.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SegmentScan {
    /// Highest record LSN, `None` for a segment with no record.
    pub last_lsn: Option<u64>,
    /// Time anchors, in LSN order.
    pub anchors: Vec<LsnTimeAnchor>,
    pub aborts: Vec<AbortMarker>,
    /// Cluster restore point records with their LSNs, in LSN order.
    pub restore_points: Vec<(u64, RestorePointPayload)>,
    /// Highest surrogate a `SurrogateAlloc` record reserved.
    pub surrogate_hwm: Option<u32>,
    /// Every `WriteGroup` record's LSN and group descriptor, in LSN order.
    pub groups: Vec<(u64, crate::wal::WriteGroup)>,
    /// LSNs of the `SnapshotInstalled` records, in LSN order.
    pub installs: Vec<u64>,
}

/// A segment image cut at a target LSN.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CutSegment {
    pub bytes: Vec<u8>,
    /// Highest LSN the cut image holds, the appended markers included.
    pub last_lsn: Option<u64>,
    /// Records above the target the cut removed.
    pub dropped_records: u64,
}

/// One intact record in a segment image.
pub(super) struct Frame<'a> {
    pub(super) header: RecordHeader,
    payload: &'a [u8],
    end: usize,
}

impl Frame<'_> {
    pub(super) fn kind(&self) -> Option<RecordType> {
        RecordType::from_raw(self.header.logical_record_type())
    }

    pub(super) fn is_padding(&self) -> bool {
        self.kind() == Some(RecordType::Noop)
    }
}

/// The intact records of a segment image, in file order.
pub(super) struct Frames<'a> {
    bytes: &'a [u8],
    offset: usize,
    /// Where the segment's first record starts. The WAL format version is
    /// judged here and nowhere else, as the segment reader does.
    records_start: usize,
    done: bool,
    /// Version and supported version of an unreadable first record. A walk
    /// that ends here holds no records, and [`Frames::finish`] reports why.
    refused: Option<(u16, u16)>,
}

impl<'a> Frames<'a> {
    pub(super) fn open(bytes: &'a [u8]) -> Result<(Option<SegmentPreamble>, Self), RestoreError> {
        let preamble = parse_leading_preamble(bytes, &WAL_PREAMBLE_MAGIC)?;
        let offset = Self::records_start(preamble.as_ref());
        Ok((
            preamble,
            Self {
                bytes,
                offset,
                records_start: offset,
                done: false,
                refused: None,
            },
        ))
    }

    /// Report a first record in a WAL format version this build cannot read.
    ///
    /// Call it after the walk. An archive from another build holds intact
    /// records, so reading it as an empty segment would drop all of them
    /// without an error. `key` names the segment in the error.
    pub(super) fn finish(&self, key: &str) -> Result<(), RestoreError> {
        match self.refused {
            Some((version, supported)) => Err(RestoreError::Wal(WalError::SegmentFormatVersion {
                path: key.to_string(),
                version,
                supported,
            })),
            None => Ok(()),
        }
    }

    /// Byte offset where records begin: past the preamble, if any.
    fn records_start(preamble: Option<&SegmentPreamble>) -> usize {
        if preamble.is_some() { PREAMBLE_SIZE } else { 0 }
    }
}

impl<'a> Iterator for Frames<'a> {
    type Item = Frame<'a>;

    fn next(&mut self) -> Option<Frame<'a>> {
        if self.done {
            return None;
        }
        let frame = self.read_at(self.offset);
        match &frame {
            Some(f) => self.offset = f.end,
            None => self.done = true,
        }
        frame
    }
}

impl<'a> Frames<'a> {
    fn read_at(&mut self, start: usize) -> Option<Frame<'a>> {
        let bytes: &'a [u8] = self.bytes;
        let head: &[u8; HEADER_SIZE] = bytes
            .get(start..start.checked_add(HEADER_SIZE)?)?
            .try_into()
            .ok()?;
        let header = RecordHeader::from_bytes(head);
        match header.validate(start as u64) {
            Ok(()) => {}
            Err(WalError::UnsupportedVersion { version, supported }) => {
                // A zeroed version is a torn write, and a mismatch after the
                // first record is damage. Both end the walk without an error.
                if version != 0 && start == self.records_start {
                    self.refused = Some((version, supported));
                }
                return None;
            }
            Err(_) => return None,
        }
        let payload_start = start + HEADER_SIZE;
        let end = payload_start.checked_add(usize::try_from(header.payload_len).ok()?)?;
        let payload = bytes.get(payload_start..end)?;
        (header.compute_checksum(payload) == header.crc32c).then_some(Frame {
            header,
            payload,
            end,
        })
    }
}

/// Walk `bytes`. `ring` decrypts the anchor and abort payloads of an
/// encrypted segment.
pub fn scan_segment(
    key: &str,
    bytes: &[u8],
    ring: Option<&KeyRing>,
) -> Result<SegmentScan, RestoreError> {
    let (preamble, mut frames) = Frames::open(bytes)?;
    let decryptor = SegmentDecryptor::new(preamble.as_ref(), ring);
    let mut scan = SegmentScan::default();
    for frame in frames.by_ref() {
        if frame.is_padding() {
            continue;
        }
        let lsn = frame.header.lsn;
        advance(key, &mut scan.last_lsn, lsn)?;
        match frame.kind() {
            Some(RecordType::TimeAnchor) => {
                let payload = decryptor.decrypt_payload(&frame.header, frame.payload.to_vec())?;
                let anchor = TimeAnchorPayload::from_bytes(&payload)?;
                scan.anchors
                    .push(LsnTimeAnchor::new(lsn, anchor.hlc_wall_ns));
            }
            Some(RecordType::WriteAborted) => {
                let payload = decryptor.decrypt_payload(&frame.header, frame.payload.to_vec())?;
                let abort = WriteAbortedPayload::from_bytes(&payload)?;
                scan.aborts.push(AbortMarker {
                    marker_lsn: lsn,
                    aborted_lsn: abort.aborted_lsn,
                });
            }
            Some(RecordType::RestorePoint) => {
                let payload = decryptor.decrypt_payload(&frame.header, frame.payload.to_vec())?;
                scan.restore_points
                    .push((lsn, RestorePointPayload::from_bytes(&payload)?));
            }
            Some(RecordType::SurrogateAlloc) => {
                let payload = decryptor.decrypt_payload(&frame.header, frame.payload.to_vec())?;
                let hi = SurrogateAllocPayload::from_bytes(&payload)?.hi;
                scan.surrogate_hwm = scan.surrogate_hwm.max(Some(hi));
            }
            Some(RecordType::WriteGroup) => {
                let payload = decryptor.decrypt_payload(&frame.header, frame.payload.to_vec())?;
                let record = crate::wal::WriteGroupRecord::from_bytes(&payload)
                    .map_err(RestoreError::Node)?;
                scan.groups.push((lsn, record.group));
            }
            Some(RecordType::SnapshotInstalled) => scan.installs.push(lsn),
            _ => {}
        }
    }
    frames.finish(key)?;
    Ok(scan)
}

/// Cut `bytes` after its last record at or below `target`.
///
/// Every record above the target and every byte after the cut goes. One
/// plaintext `WriteAborted` marker per LSN in `refused` follows, numbered from
/// `target + 1`: each names a record at or below the target that replay must
/// drop. With `surrogate_hwm`, a plaintext `SurrogateAlloc` record reserving
/// through it follows. A padding record then brings the image to an
/// `alignment` boundary, so a writer reopening it under `O_DIRECT` resumes at
/// its end.
///
/// The markers are plaintext on purpose. The source segment encrypted its own
/// records above the target under the same key, epoch and LSNs, and reusing
/// that nonce breaks AES-GCM.
pub fn cut_segment(
    key: &str,
    bytes: &[u8],
    target: u64,
    refused: &[u64],
    surrogate_hwm: Option<u32>,
    alignment: usize,
) -> Result<CutSegment, RestoreError> {
    if !alignment.is_power_of_two() {
        return Err(RestoreError::BadAlignment { alignment });
    }
    let (preamble, mut frames) = Frames::open(bytes)?;
    let mut keep_end = Frames::records_start(preamble.as_ref());
    let mut last_kept: Option<u64> = None;
    let mut last_seen: Option<u64> = None;
    let mut dropped_records = 0u64;
    for frame in frames.by_ref() {
        if frame.is_padding() {
            continue;
        }
        let lsn = frame.header.lsn;
        advance(key, &mut last_seen, lsn)?;
        if lsn <= target {
            keep_end = frame.end;
            last_kept = Some(lsn);
        } else {
            dropped_records += 1;
        }
    }
    frames.finish(key)?;

    let mut out = bytes
        .get(..keep_end)
        .map(<[u8]>::to_vec)
        .unwrap_or_default();
    let mut last_lsn = last_kept;
    let mut next = target.saturating_add(1);
    let markers = refused.iter().map(|&aborted| {
        (
            RecordType::WriteAborted,
            WriteAbortedPayload::new(aborted).to_bytes().to_vec(),
        )
    });
    let reservation = surrogate_hwm.map(|hi| {
        (
            RecordType::SurrogateAlloc,
            SurrogateAllocPayload::new(hi).to_bytes().to_vec(),
        )
    });
    for (record_type, payload) in markers.chain(reservation) {
        let record = WalRecord::new(WalRecordArgs {
            record_type: record_type as u32,
            lsn: next,
            tenant_id: 0,
            vshard_id: 0,
            database_id: 0,
            payload,
            encryption_key: None,
            preamble_bytes: None,
        })?;
        append_record(&mut out, &record);
        last_lsn = Some(next);
        next = next.saturating_add(1);
    }
    if let Some(span) = padding_span(out.len(), alignment) {
        append_record(&mut out, &padding_record(span)?);
    }
    Ok(CutSegment {
        bytes: out,
        last_lsn,
        dropped_records,
    })
}

/// Move `last` to `lsn`, refusing an LSN at or below it.
pub(super) fn advance(key: &str, last: &mut Option<u64>, lsn: u64) -> Result<(), RestoreError> {
    if let Some(previous) = *last
        && lsn <= previous
    {
        return Err(RestoreError::SegmentOutOfOrder {
            key: key.to_string(),
            previous,
            lsn,
        });
    }
    *last = Some(lsn);
    Ok(())
}

fn append_record(out: &mut Vec<u8>, record: &WalRecord) {
    out.extend_from_slice(&record.header.to_bytes());
    out.extend_from_slice(&record.payload);
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_wal::WalReader;
    use nodedb_wal::crypto::WalEncryptionKey;
    use nodedb_wal::writer::{WalWriter, WalWriterConfig};

    const ALIGN: usize = 4096;

    fn key() -> WalEncryptionKey {
        WalEncryptionKey::from_bytes(&[0x5A; 32]).expect("test key")
    }

    /// A segment holding one `Put` per LSN in `1..=records`, one batch each.
    fn segment(records: u64, key: Option<WalEncryptionKey>) -> Vec<u8> {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal-00000000000000000001.seg");
        let mut writer = WalWriter::open_without_direct_io(&path).unwrap();
        if let Some(key) = key {
            writer.set_encryption_key(key).unwrap();
        }
        for i in 1..=records {
            let lsn = writer
                .append(
                    RecordType::Put as u32,
                    1,
                    0,
                    0,
                    format!("row-{i}").as_bytes(),
                )
                .unwrap();
            assert_eq!(lsn, i);
            writer.sync().unwrap();
        }
        drop(writer);
        std::fs::read(&path).unwrap()
    }

    /// Write `bytes` as a segment file and read every record back.
    fn reopen(bytes: &[u8], ring: Option<&KeyRing>) -> (tempfile::TempDir, Vec<WalRecord>) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal-00000000000000000001.seg");
        std::fs::write(&path, bytes).unwrap();
        let records = WalReader::open(&path, ring)
            .unwrap()
            .records()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        (dir, records)
    }

    fn lsns(records: &[WalRecord]) -> Vec<u64> {
        records.iter().map(|r| r.header.lsn).collect()
    }

    #[test]
    fn a_cut_at_six_reopens_with_exactly_one_through_six() {
        let bytes = segment(10, None);
        let cut = cut_segment("seg", &bytes, 6, &[], None, ALIGN).unwrap();
        assert_eq!(cut.last_lsn, Some(6));
        assert_eq!(cut.dropped_records, 4);
        assert_eq!(
            cut.bytes.len() % ALIGN,
            0,
            "the cut ends on a block boundary"
        );

        let (dir, records) = reopen(&cut.bytes, None);
        assert_eq!(lsns(&records), (1..=6).collect::<Vec<_>>());
        assert_eq!(records[5].payload, b"row-6");

        let path = dir.path().join("wal-00000000000000000001.seg");
        let info = nodedb_wal::recovery::recover(&path).unwrap();
        assert_eq!(info.last_lsn, 6);
        assert_eq!(
            info.end_offset,
            cut.bytes.len() as u64,
            "a writer resumes after the padding, never inside a kept record"
        );
        let writer = WalWriter::open(
            &path,
            WalWriterConfig {
                use_direct_io: false,
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(writer.next_lsn(), 7);
    }

    #[test]
    fn a_cut_keeps_the_preamble_of_an_encrypted_segment() {
        let bytes = segment(10, Some(key()));
        let cut = cut_segment("seg", &bytes, 4, &[], None, ALIGN).unwrap();
        let ring = KeyRing::new(key());
        let (_dir, records) = reopen(&cut.bytes, Some(&ring));
        assert_eq!(lsns(&records), vec![1, 2, 3, 4]);
        assert_eq!(records[3].payload, b"row-4");
    }

    #[test]
    fn a_cut_past_the_last_record_keeps_every_record() {
        let bytes = segment(5, None);
        let cut = cut_segment("seg", &bytes, 99, &[], None, ALIGN).unwrap();
        assert_eq!(cut.dropped_records, 0);
        let (_dir, records) = reopen(&cut.bytes, None);
        assert_eq!(lsns(&records), (1..=5).collect::<Vec<_>>());
    }

    #[test]
    fn refused_writes_get_markers_above_the_target() {
        let bytes = segment(10, None);
        let cut = cut_segment("seg", &bytes, 6, &[3], None, ALIGN).unwrap();
        assert_eq!(cut.last_lsn, Some(7));
        let (_dir, records) = reopen(&cut.bytes, None);
        assert_eq!(lsns(&records), (1..=7).collect::<Vec<_>>());
        let filters = nodedb_wal::extract_replay_filters(&records).unwrap();
        assert!(
            filters.aborted.contains(3),
            "replay drops the refused write"
        );
        assert!(!filters.aborted.contains(6));
    }

    #[test]
    fn a_scan_reads_anchors_aborts_and_the_last_lsn() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal-00000000000000000001.seg");
        let mut writer = WalWriter::open_without_direct_io(&path).unwrap();
        writer.set_encryption_key(key()).unwrap();
        writer
            .append(RecordType::Put as u32, 1, 0, 0, b"a")
            .unwrap();
        writer
            .append(
                RecordType::TimeAnchor as u32,
                0,
                0,
                0,
                &TimeAnchorPayload::new(1_000).to_bytes(),
            )
            .unwrap();
        writer
            .append(
                RecordType::WriteAborted as u32,
                0,
                0,
                0,
                &WriteAbortedPayload::new(1).to_bytes(),
            )
            .unwrap();
        writer
            .append(
                RecordType::TimeAnchor as u32,
                0,
                0,
                0,
                &TimeAnchorPayload::new(2_000).to_bytes(),
            )
            .unwrap();
        writer.sync().unwrap();
        drop(writer);
        let bytes = std::fs::read(&path).unwrap();
        let ring = KeyRing::new(key());

        let scan = scan_segment("seg", &bytes, Some(&ring)).unwrap();
        assert_eq!(scan.last_lsn, Some(4));
        assert_eq!(
            scan.anchors,
            vec![LsnTimeAnchor::new(2, 1_000), LsnTimeAnchor::new(4, 2_000)]
        );
        assert_eq!(
            scan.aborts,
            vec![AbortMarker {
                marker_lsn: 3,
                aborted_lsn: 1
            }]
        );
    }

    #[test]
    fn a_scan_stops_at_damage() {
        let mut bytes = segment(6, None);
        let tail = bytes.len() - 3;
        bytes[tail] ^= 0xFF;
        let scan = scan_segment("seg", &bytes, None).unwrap();
        assert_eq!(scan.last_lsn, Some(5));
        assert_eq!(scan_segment("seg", &[], None).unwrap().last_lsn, None);
    }

    /// Where each record of `bytes` starts.
    fn record_starts(bytes: &[u8]) -> Vec<usize> {
        let (preamble, frames) = Frames::open(bytes).unwrap();
        let mut starts = vec![Frames::records_start(preamble.as_ref())];
        starts.extend(frames.map(|frame| frame.end));
        starts.pop();
        starts
    }

    /// Declare `version` in the header of the record that starts at `start`.
    fn set_format_version(bytes: &mut [u8], start: usize, version: u16) {
        let head: &[u8; HEADER_SIZE] = bytes[start..start + HEADER_SIZE].try_into().unwrap();
        let mut header = RecordHeader::from_bytes(head);
        header.format_version = version;
        bytes[start..start + HEADER_SIZE].copy_from_slice(&header.to_bytes());
    }

    fn assert_format_gap<T: std::fmt::Debug>(result: Result<T, RestoreError>, version: u16) {
        match result {
            Err(RestoreError::Wal(WalError::SegmentFormatVersion {
                path,
                version: found,
                supported,
            })) => {
                assert_eq!(path, "seg");
                assert_eq!(found, version);
                assert_eq!(supported, nodedb_wal::record::WAL_FORMAT_VERSION);
            }
            other => panic!("expected a format gap, got {other:?}"),
        }
    }

    /// An archive from another build holds intact records. Reading it as an
    /// empty segment would drop all of them without an error.
    #[test]
    fn an_archive_in_another_format_is_refused_not_read_as_empty() {
        let mut bytes = segment(3, None);
        let starts = record_starts(&bytes);
        for &start in &starts {
            set_format_version(&mut bytes, start, 1);
        }

        assert_format_gap(scan_segment("seg", &bytes, None), 1);
        assert_format_gap(cut_segment("seg", &bytes, 3, &[], None, ALIGN), 1);
    }

    #[test]
    fn a_version_mismatch_after_the_first_record_stops_the_walk() {
        let mut bytes = segment(3, None);
        let starts = record_starts(&bytes);
        set_format_version(&mut bytes, starts[1], 1);

        let scan = scan_segment("seg", &bytes, None).unwrap();
        assert_eq!(scan.last_lsn, Some(1));
    }

    #[test]
    fn a_zeroed_first_version_stops_the_walk() {
        let mut bytes = segment(3, None);
        let starts = record_starts(&bytes);
        set_format_version(&mut bytes, starts[0], 0);

        let scan = scan_segment("seg", &bytes, None).unwrap();
        assert_eq!(scan.last_lsn, None);
    }
}
