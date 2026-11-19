// SPDX-License-Identifier: BUSL-1.1

//! Cut the replayed stream at the records boot judges, in one forward pass.
//!
//! Replay merges every record by the LSN its ops apply at: a write group's
//! parts and continuations apply at their origin's LSN, wherever they sit
//! in the WAL. So the stream is walked in that order, once: each cut hands
//! out the records that apply below the next judged record and were not
//! handed out yet. A refused record leaves the stream with every record that
//! applies at its LSN.
//!
//! The sync marks walk alongside: each cut raises the marks that take effect
//! below it, so the judge finds the marks its live validate found.

use std::collections::BTreeSet;

use nodedb_wal::WalRecord;
use nodedb_wal::record::{RecordType, SyncSeqAdvancePayload};

use crate::wal::SyncHwmReplayMaps;
use crate::wal::WriteGroupRecord;

/// The records of a stream in the order they apply, and how far the walk
/// went.
pub(super) struct ApplyingOrder {
    /// `(applying LSN, index into the stream)`, sorted by the LSN. The sort
    /// is stable, so records that apply at one LSN keep their WAL order.
    order: Vec<(u64, usize)>,
    next: usize,
    /// `(effective LSN, mark)` of every sync mark, sorted by the LSN.
    marks: Vec<(u64, SyncSeqAdvancePayload)>,
    next_mark: usize,
}

impl ApplyingOrder {
    pub(super) fn new(records: &[WalRecord]) -> crate::Result<Self> {
        let mut order = Vec::with_capacity(records.len());
        let mut marks = Vec::new();
        for (index, record) in records.iter().enumerate() {
            let lsn = record.header.lsn;
            match RecordType::from_raw(record.logical_record_type()) {
                Some(RecordType::WriteGroup) => {
                    let origin = WriteGroupRecord::from_bytes(&record.payload)?
                        .group
                        .origin_at(lsn);
                    order.push((origin, index));
                }
                Some(RecordType::SyncSeqAdvance) => {
                    let mark = SyncSeqAdvancePayload::from_bytes(&record.payload)
                        .map_err(crate::Error::Wal)?;
                    marks.push((mark.effective_lsn(lsn), mark));
                    order.push((lsn, index));
                }
                _ => order.push((lsn, index)),
            }
        }
        order.sort_by_key(|(lsn, _)| *lsn);
        marks.sort_by_key(|(lsn, _)| *lsn);
        Ok(Self {
            order,
            next: 0,
            marks,
            next_mark: 0,
        })
    }

    /// The records of `records`, the stream this order walks, that apply
    /// below `below` and at no LSN of `refused`, past the last cut. Only
    /// these records are copied, once each.
    pub(super) fn take_below(
        &mut self,
        records: &[WalRecord],
        below: u64,
        refused: &BTreeSet<u64>,
    ) -> Vec<WalRecord> {
        let mut taken = Vec::new();
        while let Some(&(lsn, index)) = self.order.get(self.next) {
            if lsn >= below {
                break;
            }
            self.next += 1;
            if !refused.contains(&lsn) {
                taken.push(records[index].clone());
            }
        }
        taken
    }

    /// The sync marks that take effect below `below` and were not raised
    /// yet.
    pub(super) fn marks_below(&mut self, below: u64) -> SyncHwmReplayMaps {
        let mut maps = SyncHwmReplayMaps::default();
        while let Some((lsn, mark)) = self.marks.get(self.next_mark) {
            if *lsn >= below {
                break;
            }
            crate::wal::replay::sync_hwm::apply_sync_seq_advance(mark, &mut maps);
            self.next_mark += 1;
        }
        maps
    }
}

/// Whether replay of `records` writes to any store: every record is one
/// no engine applies, such as a commit boundary, a redo chunk, a sync mark,
/// a cancel marker, or a group's start that carries no row.
pub(super) fn writes_nothing(records: &[WalRecord]) -> crate::Result<bool> {
    for record in records {
        let inert = match RecordType::from_raw(record.logical_record_type()) {
            Some(
                RecordType::RedoBoundary
                | RecordType::RedoChunk
                | RecordType::SyncSeqAdvance
                | RecordType::WriteAborted
                | RecordType::ChangePosition
                | RecordType::ProposalApplied,
            ) => true,
            Some(RecordType::WriteGroup) => WriteGroupRecord::from_bytes(&record.payload)?
                .ops
                .is_empty(),
            _ => false,
        };
        if !inert {
            return Ok(false);
        }
    }
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{DatabaseId, TenantId, VShardId};
    use crate::wal::WriteGroup;
    use crate::wal::manager::{NO_APPLY_KEY, WalManager};

    /// A part journalled after a later write applies at its origin: it
    /// falls in the origin's cut, and leaves with a refused origin. Each
    /// record is handed out once.
    #[test]
    fn a_part_applies_at_its_origin() {
        let dir = tempfile::tempdir().expect("tempdir");
        let wal = WalManager::open_for_testing(&dir.path().join("w.wal")).expect("wal");
        let appender = wal
            .appender(NO_APPLY_KEY)
            .with_event_source(crate::event::EventSource::User);
        let (t, v, d) = (TenantId::new(1), VShardId::new(0), DatabaseId::DEFAULT);
        let origin = appender
            .append_put(t, v, d, b"origin")
            .expect("put")
            .as_u64();
        let later = appender
            .append_put(t, v, d, b"later")
            .expect("put")
            .as_u64();
        appender
            .append_write_group(
                t,
                v,
                d,
                &WriteGroupRecord {
                    schemas: Vec::new(),
                    group: WriteGroup::part_of(origin, 1, 1),
                    ops: Vec::new(),
                    redo: None,
                },
            )
            .expect("part");
        wal.sync().expect("sync");
        let records = wal.replay().expect("replay");

        let mut order = ApplyingOrder::new(&records).expect("order");
        let below_later = order.take_below(&records, later, &BTreeSet::new());
        assert_eq!(below_later.len(), 2, "the origin and its part");
        assert_eq!(
            order.take_below(&records, u64::MAX, &BTreeSet::new()).len(),
            1
        );

        let mut order = ApplyingOrder::new(&records).expect("order");
        let rest = order.take_below(&records, u64::MAX, &BTreeSet::from([origin]));
        assert_eq!(rest.len(), 1);
        assert_eq!(rest[0].header.lsn, later);
    }

    /// The marks below a cut are those a write there found: a mark at its
    /// own record below it, and a mark appended above it that names a
    /// refused frame below it. A later mark waits for a later cut.
    #[test]
    fn a_cut_raises_the_marks_below_it() {
        let dir = tempfile::tempdir().expect("tempdir");
        let wal = WalManager::open_for_testing(&dir.path().join("w.wal")).expect("wal");
        let appender = wal.appender(NO_APPLY_KEY);
        let mark = |seq| SyncSeqAdvancePayload::new(1, 1, 10, seq);
        let early = appender.append_sync_mark(mark(1)).expect("mark").as_u64();
        let cut = appender.append_sync_mark(mark(5)).expect("mark").as_u64();
        appender
            .append_sync_mark(mark(2).at_frame(early))
            .expect("late mark");
        wal.sync().expect("sync");
        let records = wal.replay().expect("replay");

        let mut order = ApplyingOrder::new(&records).expect("order");
        let below = order.marks_below(cut);
        assert_eq!(below.sync_hwm.get(&(1, 10)).copied(), Some(2));
        let rest = order.marks_below(u64::MAX);
        assert_eq!(rest.sync_hwm.get(&(1, 10)).copied(), Some(5));
    }

    /// The start of a committed record, its boundary and its announcement,
    /// writes nothing, so the verdicts on either side of it share one
    /// commit. A row write does write.
    #[test]
    fn a_record_start_writes_nothing() {
        let dir = tempfile::tempdir().expect("tempdir");
        let wal = WalManager::open_for_testing(&dir.path().join("w.wal")).expect("wal");
        let appender = wal
            .appender(NO_APPLY_KEY)
            .with_event_source(crate::event::EventSource::User);
        let (t, v, d) = (TenantId::new(1), VShardId::new(0), DatabaseId::DEFAULT);
        appender
            .append_write_group(
                t,
                v,
                d,
                &WriteGroupRecord {
                    schemas: Vec::new(),
                    group: WriteGroup::announcing(7),
                    ops: Vec::new(),
                    redo: None,
                },
            )
            .expect("announce");
        wal.sync().expect("sync");
        let start = wal.replay().expect("replay");
        assert!(writes_nothing(&start).expect("inert"));
        appender.append_put(t, v, d, b"row").expect("put");
        wal.sync().expect("sync");
        assert!(!writes_nothing(&wal.replay().expect("replay")).expect("inert"));
    }
}
