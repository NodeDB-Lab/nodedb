// SPDX-License-Identifier: BUSL-1.1

//! Turn each source group's tail into cuts.
//!
//! Every data group's log carries the cut's barrier, placed in a Calvin-safe
//! order, so everything before each group's barrier is one consistent batch.
//! A cut applies only once every group fetched its barrier.
//!
//! Three things can break the plain case, and each stays a valid place:
//!
//! - A source leader that died inside a cut placed its barrier in some groups
//!   only. The next cut covers those entries.
//! - A former leader's late barrier sits after a new leader's. Its number is
//!   lower, so it is ignored.
//! - A group lags. The other groups wait for it.
//!
//! A batch holds what the source's retention bound holds, at most, because a
//! fetched entry stays at the source until the mirror acks it.

use nodedb_types::{DatabaseId, MirrorCutMark, MirrorFailure, MirrorGroupPosition};

use nodedb_cluster::METADATA_GROUP_ID;
use nodedb_cluster::mirror::{MirrorTailBatch, MirrorTailEntry};

use crate::control::catalog_entry::{self, CatalogEntry};
use crate::control::wal_replication::ReplicatedEntry;
use crate::control::wal_replication::types::MirrorCalvinFloor;

use super::rewrite::{CatalogCheck, CheckedEntry, check_data_entry, poison};

/// One source entry, with where it came from.
#[derive(Debug)]
pub(super) struct SourceEntry {
    pub group: u64,
    pub index: u64,
    pub entry: ReplicatedEntry,
}

#[derive(Debug)]
struct HeldBarrier {
    index: u64,
    mark: MirrorCutMark,
    hlc: u64,
    floors: Vec<MirrorCalvinFloor>,
    /// How many of the group's held entries sit before this barrier.
    entries_before: usize,
}

/// One source data group's tail.
#[derive(Debug)]
pub(super) struct GroupCursor {
    pub group: u64,
    /// The next index to fetch.
    pub next_index: u64,
    /// The last index a cut covered.
    pub position: u64,
    entries: Vec<(u64, ReplicatedEntry)>,
    barriers: Vec<HeldBarrier>,
    /// The newest cut number this group's log showed.
    top_seq: u64,
}

impl GroupCursor {
    pub(super) fn new(group: u64, position: u64) -> Self {
        Self {
            group,
            next_index: position.saturating_add(1),
            position,
            entries: Vec::new(),
            barriers: Vec::new(),
            top_seq: 0,
        }
    }

    /// Take one fetched batch, in log order. A barrier at or below `ready_cut`
    /// or below a newer one this log already showed never starts a cut.
    pub(super) fn ingest(
        &mut self,
        batch: MirrorTailBatch,
        database: DatabaseId,
        ready_cut: u64,
    ) -> Result<(), MirrorFailure> {
        for tail in batch.entries {
            match check_data_entry(&tail.data, self.group, tail.index, database)? {
                CheckedEntry::Data(entry) => self.entries.push((tail.index, *entry)),
                CheckedEntry::Barrier { hlc, barrier } => {
                    let seq = barrier.mark.seq;
                    if seq <= ready_cut || seq <= self.top_seq {
                        continue;
                    }
                    self.top_seq = seq;
                    self.barriers.push(HeldBarrier {
                        index: tail.index,
                        mark: barrier.mark,
                        hlc,
                        floors: barrier.calvin_floors,
                        entries_before: self.entries.len(),
                    });
                }
            }
        }
        self.next_index = self.next_index.max(batch.next_index);
        Ok(())
    }

    fn holds_seq(&self, seq: u64) -> bool {
        self.barriers.iter().any(|barrier| barrier.mark.seq == seq)
    }

    /// Take what the cut `seq` covers. The group's barriers below it go too:
    /// each marks a cut some other group never held.
    fn take_through(&mut self, seq: u64) -> Option<(Vec<(u64, ReplicatedEntry)>, HeldBarrier)> {
        let at = self
            .barriers
            .iter()
            .position(|barrier| barrier.mark.seq == seq)?;
        let barrier = self.barriers.drain(..=at).next_back()?;
        let later = self.entries.split_off(barrier.entries_before);
        let taken = std::mem::replace(&mut self.entries, later);
        for held in &mut self.barriers {
            held.entries_before -= barrier.entries_before;
        }
        self.position = barrier.index;
        Some((taken, barrier))
    }
}

/// The source metadata group's tail: catalog payloads that name the database.
#[derive(Debug)]
pub(super) struct MetadataCursor {
    pub next_index: u64,
    pub position: u64,
    held: Vec<MirrorTailEntry>,
}

impl MetadataCursor {
    pub(super) fn new(position: u64) -> Self {
        Self {
            next_index: position.saturating_add(1),
            position,
            held: Vec::new(),
        }
    }

    pub(super) fn ingest(&mut self, batch: MirrorTailBatch) {
        self.held.extend(batch.entries);
        self.next_index = self.next_index.max(batch.next_index);
    }

    /// Whether this cursor fetched past `target`.
    pub(super) fn reached(&self, target: u64) -> bool {
        self.next_index > target
    }

    /// Take the payloads through `target`, checked. The position never moves
    /// back: an earlier cut can have covered more.
    pub(super) fn take_through(
        &mut self,
        target: u64,
        check: &mut CatalogCheck<'_>,
    ) -> Result<Vec<Vec<u8>>, MirrorFailure> {
        let split = self.held.partition_point(|entry| entry.index <= target);
        let mut payloads = Vec::with_capacity(split);
        for entry in self.held.drain(..split) {
            check.check(&entry.data, entry.index)?;
            payloads.push(entry.data);
        }
        self.position = self.position.max(target);
        Ok(payloads)
    }

    /// The sequence counter entries past the taken position, through `horizon`.
    /// They stay held, so they apply again in their log place.
    ///
    /// A source node hands out a value only once its reservation applied, and a
    /// mark reads its metadata index ahead of the barriers. So a cut can hold a
    /// value whose reservation sits past the mark. Each cut carries these entries,
    /// and a promoted mirror reserves above every value its final cut holds.
    pub(super) fn counters_through(&self, horizon: u64) -> Result<Vec<Vec<u8>>, MirrorFailure> {
        let split = self.held.partition_point(|entry| entry.index <= horizon);
        let mut payloads = Vec::new();
        for entry in &self.held[..split] {
            let decoded = catalog_entry::decode(&entry.data)
                .map_err(|error| poison(METADATA_GROUP_ID, entry.index, error.to_string()))?;
            if matches!(
                decoded,
                CatalogEntry::ReserveSequenceRange { .. } | CatalogEntry::PutSequenceState(_)
            ) {
                payloads.push(entry.data.clone());
            }
        }
        Ok(payloads)
    }
}

/// The newest barrier any group showed.
pub(super) fn top_seq(cursors: &[GroupCursor]) -> u64 {
    cursors
        .iter()
        .map(|cursor| cursor.top_seq)
        .max()
        .unwrap_or(0)
}

/// Whether `cursor` still has to fetch to reach the newest barrier any group
/// showed. A fetch past it only grows the batch.
pub(super) fn wants_fetch(cursor: &GroupCursor, top: u64) -> bool {
    cursor.top_seq < top || top == 0 || cursor.barriers.is_empty()
}

/// The oldest cut every group holds a barrier of.
pub(super) fn next_cut(cursors: &[GroupCursor]) -> Option<u64> {
    let first = cursors.first()?;
    first
        .barriers
        .iter()
        .map(|barrier| barrier.mark.seq)
        .find(|seq| cursors.iter().all(|cursor| cursor.holds_seq(*seq)))
}

/// Everything one cut covers, before any rewrite.
#[derive(Debug)]
pub(super) struct CutBatch {
    pub cut: u64,
    pub mark: MirrorCutMark,
    /// The barrier's HLC wall time on the source, in nanoseconds.
    pub hlc_ns: u64,
    pub entries: Vec<SourceEntry>,
    pub floors: Vec<MirrorCalvinFloor>,
    /// The source metadata index the cut's catalog must cover.
    pub metadata_target: u64,
}

/// Take cut `seq` out of every group's tail. Every group holds its barrier:
/// [`next_cut`] chose it.
pub(super) fn take_cut(cursors: &mut [GroupCursor], seq: u64) -> Option<CutBatch> {
    let mut batch = None::<CutBatch>;
    for cursor in cursors.iter_mut() {
        let group = cursor.group;
        let (taken, barrier) = cursor.take_through(seq)?;
        let current = batch.get_or_insert_with(|| CutBatch {
            cut: seq,
            mark: barrier.mark,
            hlc_ns: barrier.hlc,
            entries: Vec::new(),
            floors: Vec::new(),
            metadata_target: barrier.mark.metadata_index,
        });
        current.hlc_ns = current.hlc_ns.max(barrier.hlc);
        current.floors.extend(barrier.floors);
        for (index, entry) in taken {
            current.metadata_target = current.metadata_target.max(entry.metadata_floor);
            current.entries.push(SourceEntry {
                group,
                index,
                entry,
            });
        }
    }
    batch
}

/// Where every tailed group resumes after the cut these cursors just took.
pub(super) fn positions(
    cursors: &[GroupCursor],
    metadata: &MetadataCursor,
) -> Vec<MirrorGroupPosition> {
    cursors
        .iter()
        .map(|cursor| MirrorGroupPosition {
            group_id: cursor.group,
            log_index: cursor.position,
        })
        .chain(std::iter::once(MirrorGroupPosition {
            group_id: METADATA_GROUP_ID,
            log_index: metadata.position,
        }))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::wal_replication::ReplicatedWrite;
    use crate::control::wal_replication::types::MirrorBarrier;

    const DB: DatabaseId = DatabaseId::new(1500);

    fn write(index: u64, floor: u64) -> MirrorTailEntry {
        let mut entry = ReplicatedEntry::new(
            1,
            DB.as_u64(),
            2,
            ReplicatedWrite::PointPut {
                collection: "docs".into(),
                document_id: format!("d{index}"),
                value: vec![1],
                surrogate: 7,
                resolved_sum_targets: Vec::new(),
                returning: None,
                rls_filters: Vec::new(),
            },
        );
        entry.metadata_floor = floor;
        MirrorTailEntry {
            index,
            data: entry.encode().expect("encode"),
        }
    }

    fn barrier(index: u64, seq: u64, metadata_index: u64) -> MirrorTailEntry {
        let entry = ReplicatedEntry::new(
            0,
            0,
            2,
            ReplicatedWrite::CutBarrier {
                hlc: seq * 10,
                restore_point: 0,
                capture: None,
                mirror: Some(MirrorBarrier {
                    mark: MirrorCutMark {
                        seq,
                        cap_ordinal: 5,
                        metadata_index,
                        surrogate_hwm: 0,
                    },
                    calvin_floors: vec![MirrorCalvinFloor {
                        vshard: 2,
                        epoch: 3,
                    }],
                }),
            },
        );
        MirrorTailEntry {
            index,
            data: entry.encode().expect("encode"),
        }
    }

    fn cursor(group: u64, entries: Vec<MirrorTailEntry>, ready_cut: u64) -> GroupCursor {
        let mut cursor = GroupCursor::new(group, 0);
        let next_index = entries.last().map_or(1, |entry| entry.index + 1);
        cursor
            .ingest(
                MirrorTailBatch {
                    entries,
                    next_index,
                    commit_index: next_index,
                    source_hlc_ns: 0,
                    first_available: 1,
                },
                DB,
                ready_cut,
            )
            .expect("ingest");
        cursor
    }

    #[test]
    fn a_cut_waits_for_every_groups_barrier() {
        let cursors = vec![
            cursor(1, vec![write(1, 0), barrier(2, 7, 0)], 0),
            cursor(2, vec![write(1, 0)], 0),
        ];
        assert_eq!(next_cut(&cursors), None);
        assert!(!wants_fetch(&cursors[0], top_seq(&cursors)));
        assert!(wants_fetch(&cursors[1], top_seq(&cursors)));
    }

    #[test]
    fn a_batch_takes_only_what_sits_before_its_barrier() {
        let mut cursors = vec![
            cursor(1, vec![write(1, 0), barrier(2, 7, 40), write(3, 0)], 0),
            cursor(
                2,
                vec![barrier(1, 7, 40), write(2, 55), barrier(3, 8, 41)],
                0,
            ),
        ];
        assert_eq!(next_cut(&cursors), Some(7));
        let batch = take_cut(&mut cursors, 7).expect("a batch");
        let taken: Vec<(u64, u64)> = batch.entries.iter().map(|e| (e.group, e.index)).collect();
        assert_eq!(taken, vec![(1, 1)]);
        assert_eq!(batch.metadata_target, 40);
        assert_eq!(batch.floors.len(), 2);
        assert_eq!(cursors[0].position, 2);
        assert_eq!(cursors[1].position, 1);
        assert_eq!(next_cut(&cursors), None, "group 1 holds no second barrier");

        cursors[0]
            .ingest(
                MirrorTailBatch {
                    entries: vec![barrier(4, 8, 41)],
                    next_index: 5,
                    commit_index: 5,
                    source_hlc_ns: 0,
                    first_available: 1,
                },
                DB,
                7,
            )
            .expect("ingest");
        assert_eq!(next_cut(&cursors), Some(8));
        let batch = take_cut(&mut cursors, 8).expect("a batch");
        let taken: Vec<(u64, u64)> = batch.entries.iter().map(|e| (e.group, e.index)).collect();
        assert_eq!(taken, vec![(1, 3), (2, 2)]);
        assert_eq!(batch.metadata_target, 55, "an entry's floor raises it");
    }

    #[test]
    fn a_barrier_at_or_below_the_current_cut_is_ignored() {
        let cursors = vec![cursor(1, vec![barrier(1, 7, 0), barrier(2, 9, 0)], 7)];
        assert_eq!(next_cut(&cursors), Some(9));
    }

    #[test]
    fn a_former_leaders_late_barrier_is_ignored() {
        let cursors = vec![cursor(1, vec![barrier(1, 9, 0), barrier(2, 8, 0)], 0)];
        assert_eq!(next_cut(&cursors), Some(9));
        assert_eq!(cursors[0].barriers.len(), 1);
    }

    /// A dead source leader placed cut 7 in group 1 only.
    #[test]
    fn a_partial_cut_folds_into_the_next_one() {
        let mut cursors = vec![
            cursor(
                1,
                vec![write(1, 0), barrier(2, 7, 0), write(3, 0), barrier(4, 8, 0)],
                0,
            ),
            cursor(2, vec![write(1, 0), barrier(2, 8, 0)], 0),
        ];
        assert_eq!(next_cut(&cursors), Some(8));
        let batch = take_cut(&mut cursors, 8).expect("a batch");
        assert_eq!(batch.entries.len(), 3);
        assert_eq!(cursors[0].position, 4);
    }

    #[test]
    fn positions_name_every_group_and_the_metadata_group() {
        let cursors = vec![GroupCursor::new(1, 4), GroupCursor::new(2, 5)];
        let held = positions(&cursors, &MetadataCursor::new(6));
        assert_eq!(held.len(), 3);
        assert!(held.contains(&MirrorGroupPosition {
            group_id: METADATA_GROUP_ID,
            log_index: 6
        }));
        assert_eq!(
            cursors[0].next_index, 5,
            "the tail resumes past its position"
        );
    }
}
