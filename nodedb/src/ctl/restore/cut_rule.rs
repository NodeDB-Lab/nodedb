// SPDX-License-Identifier: BUSL-1.1

//! The rule a cluster restore splits one node's WAL with.
//!
//! A record that carries a commit HLC belongs to a write, or to the apply of
//! a stamped metadata entry. It is kept when that HLC is below the point's
//! watermark: the same rule decides the metadata entry itself. A record
//! with commit HLC `0` carries no clock at all (a checkpoint, a tombstone, a
//! surrogate bind). It is kept when it lies before this node's record of its
//! vShard's group at the point, in WAL order: the barrier of that group
//! applied there. A vShard no recorded group homes takes the metadata
//! group's record as its barrier.
//!
//! Time anchors, write-abort markers and surrogate reservations are kept
//! whatever their place, and never move the target: an anchor states when a
//! batch committed, a marker refuses a write whatever the target, and the
//! cut appends the highest reservation.

use std::collections::HashMap;

use nodedb_wal::record::{RecordHeader, RecordType};

use super::error::RestoreError;
use super::segment::{Frames, advance};

/// How a cluster restore splits one node's WAL.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CutRule {
    /// The point's watermark HLC.
    pub watermark: u64,
    /// LSN of this node's record of each vShard's group at the point.
    pub vshard_barrier: HashMap<u32, u64>,
    /// LSN of this node's record of the metadata group at the point.
    pub node_barrier: u64,
    /// The group each vShard homed at the point.
    pub vshard_group: HashMap<u32, u64>,
}

impl CutRule {
    /// Whether the restore keeps the record `header`. `None` for a record it
    /// always keeps and that moves no target.
    pub fn keeps(&self, header: &RecordHeader) -> Option<bool> {
        let kind = RecordType::from_raw(header.logical_record_type());
        if matches!(
            kind,
            Some(RecordType::TimeAnchor | RecordType::WriteAborted | RecordType::SurrogateAlloc)
        ) {
            return None;
        }
        if header.commit_hlc != 0 {
            return Some(header.commit_hlc < self.watermark);
        }
        let barrier = self
            .vshard_barrier
            .get(&header.vshard_id)
            .copied()
            .unwrap_or(self.node_barrier);
        Some(header.lsn < barrier)
    }
}

/// What a walk of a segment found against a [`CutRule`].
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CutScan {
    /// Highest LSN of a record the restore keeps and that moves the target.
    pub max_kept: Option<u64>,
    /// Lowest LSN of a record the restore drops.
    pub first_dropped: Option<u64>,
    /// LSNs of the dropped records at or below the listing bound.
    pub dropped: Vec<u64>,
    /// Newest kept commit HLC of each `(group, tenant)`, over the records
    /// above the marks bound and at or below the listing bound.
    pub marks: HashMap<(u64, u64), u64>,
}

/// Walk `bytes` against `rule`. Dropped LSNs at or below `list_through` are
/// listed, and kept commit HLCs in `(marks_after, list_through]` feed the
/// tenant marks.
pub fn scan_cut(
    key: &str,
    bytes: &[u8],
    rule: &CutRule,
    list_through: u64,
    marks_after: u64,
) -> Result<CutScan, RestoreError> {
    let (_, mut frames) = Frames::open(bytes)?;
    let mut scan = CutScan::default();
    let mut last = None;
    for frame in frames.by_ref() {
        if frame.is_padding() {
            continue;
        }
        let header = &frame.header;
        let lsn = header.lsn;
        advance(key, &mut last, lsn)?;
        match rule.keeps(header) {
            None => {}
            Some(true) => {
                scan.max_kept = Some(lsn);
                if header.commit_hlc != 0
                    && lsn > marks_after
                    && lsn <= list_through
                    && let Some(group) = rule.vshard_group.get(&header.vshard_id)
                {
                    let mark = scan.marks.entry((*group, header.tenant_id)).or_insert(0);
                    *mark = (*mark).max(header.commit_hlc);
                }
            }
            Some(false) => {
                scan.first_dropped.get_or_insert(lsn);
                if lsn <= list_through {
                    scan.dropped.push(lsn);
                }
            }
        }
    }
    frames.finish(key)?;
    Ok(scan)
}

#[cfg(test)]
mod tests {
    use nodedb_wal::WalReader;
    use nodedb_wal::record::{
        NO_EVENT_SOURCE, RecordTarget, SurrogateAllocPayload, WalRecord, WriteAbortedPayload,
    };
    use nodedb_wal::writer::WalWriter;

    use super::super::segment::{cut_segment, scan_segment};
    use super::*;

    const W: u64 = 100;

    /// A segment of `(kind, vshard, commit_hlc, payload)` records from LSN 1.
    fn segment(records: &[(RecordType, u32, u64, Vec<u8>)]) -> Vec<u8> {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal-00000000000000000001.seg");
        let mut writer = WalWriter::open_without_direct_io(&path).unwrap();
        for (kind, vshard_id, commit_hlc, payload) in records {
            let target = RecordTarget {
                record_type: *kind as u32,
                tenant_id: 1,
                vshard_id: *vshard_id,
                database_id: 0,
                event_source: NO_EVENT_SOURCE,
                commit_hlc: *commit_hlc,
            };
            writer.append_keyed(target, payload, 0).unwrap();
        }
        writer.sync().unwrap();
        drop(writer);
        std::fs::read(&path).unwrap()
    }

    fn records(bytes: &[u8]) -> Vec<WalRecord> {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal-00000000000000000001.seg");
        std::fs::write(&path, bytes).unwrap();
        WalReader::open(&path, None)
            .unwrap()
            .records()
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
    }

    fn rule() -> CutRule {
        CutRule {
            watermark: W,
            vshard_barrier: [(7, 5)].into_iter().collect(),
            node_barrier: 4,
            vshard_group: [(7, 2)].into_iter().collect(),
        }
    }

    /// A tombstone appended before its barrier survives the restore, however
    /// far the node's clock ran past the watermark: it carries no clock.
    #[test]
    fn a_record_without_a_clock_is_split_at_its_barrier() {
        use RecordType::*;
        let bytes = segment(&[
            (CollectionTombstoned, 0, 0, b"tomb".to_vec()),
            (Put, 7, 50, b"kept write".to_vec()),
            (Checkpoint, 7, 0, b"ckpt".to_vec()),
            (Put, 7, 0, b"metadata group record stand-in".to_vec()),
            (Put, 7, 150, b"dropped write".to_vec()),
            (CollectionTombstoned, 0, 0, b"late tomb".to_vec()),
            (Checkpoint, 7, 0, b"late ckpt".to_vec()),
            (
                SurrogateAlloc,
                7,
                0,
                SurrogateAllocPayload::new(9).to_bytes().to_vec(),
            ),
        ]);
        let scan = scan_cut("seg", &bytes, &rule(), 8, 0).unwrap();
        // LSN 1 tombstone (before the node barrier 4), 2 write (below W),
        // 3 checkpoint (before vShard 7's barrier 5) and 4 (unclocked, before
        // barrier 5) are kept. 5 write, 6 tombstone and 7 checkpoint go.
        assert_eq!(scan.max_kept, Some(4));
        assert_eq!(scan.first_dropped, Some(5));
        assert_eq!(scan.dropped, [5, 6, 7]);
        assert_eq!(
            scan.marks,
            [((2, 1), 50)]
                .into_iter()
                .collect::<HashMap<(u64, u64), u64>>()
        );
        assert_eq!(
            scan_segment("seg", &bytes, None).unwrap().surrogate_hwm,
            Some(9)
        );

        let cut = cut_segment("seg", &bytes, 4, &[], Some(9), 4096).unwrap();
        let kept = records(&cut.bytes);
        let lsns: Vec<u64> = kept.iter().map(|r| r.header.lsn).collect();
        assert_eq!(lsns, [1, 2, 3, 4, 5]);
        let reservation = &kept[4];
        assert_eq!(
            RecordType::from_raw(reservation.logical_record_type()),
            Some(SurrogateAlloc)
        );
        assert_eq!(
            SurrogateAllocPayload::from_bytes(&reservation.payload)
                .unwrap()
                .hi,
            9
        );
    }

    #[test]
    fn markers_and_anchors_never_move_the_target() {
        use RecordType::*;
        let bytes = segment(&[
            (Put, 7, 50, b"kept".to_vec()),
            (
                WriteAborted,
                7,
                0,
                WriteAbortedPayload::new(1).to_bytes().to_vec(),
            ),
        ]);
        let scan = scan_cut("seg", &bytes, &rule(), 9, 0).unwrap();
        assert_eq!(scan.max_kept, Some(1));
        assert!(scan.dropped.is_empty());
    }

    /// A segment whose first record is in another WAL format is refused. Read
    /// as empty, it would report that no record of the restore point exists.
    #[test]
    fn a_segment_in_another_format_is_refused_not_read_as_empty() {
        let mut bytes = segment(&[(RecordType::Put, 7, 50, b"kept".to_vec())]);
        let head: &[u8; nodedb_wal::record::HEADER_SIZE] =
            bytes[..nodedb_wal::record::HEADER_SIZE].try_into().unwrap();
        let mut header = nodedb_wal::record::RecordHeader::from_bytes(head);
        header.format_version = 1;
        bytes[..nodedb_wal::record::HEADER_SIZE].copy_from_slice(&header.to_bytes());

        match scan_cut("seg", &bytes, &rule(), 9, 0) {
            Err(RestoreError::Wal(nodedb_wal::WalError::SegmentFormatVersion {
                path,
                version,
                ..
            })) => {
                assert_eq!(path, "seg");
                assert_eq!(version, 1);
            }
            other => panic!("expected a format gap, got {other:?}"),
        }
    }
}
