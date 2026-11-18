// SPDX-License-Identifier: BUSL-1.1

//! The undo log of a batched install, in two lanes that share one order.
//!
//! - The spill lane holds every entry with plain data ([`SpilledUndo`]),
//!   in a [`RevSpool`] that spills into one file of the install's space
//!   past its threshold, and reads back newest first.
//! - The RAM lane holds the snapshots of in-memory engine state that have
//!   no spilled form. Each records the spill-lane position it follows, so
//!   the two lanes merge back into write order.
//!
//! A snapshot that the undo of an earlier entry overwrites whole is not
//! kept ([`SnapshotKeys`]): only the first touch of a key reverses the
//! install.
//!
//! The whole-record install keeps its undo log in a `Vec`, and never builds
//! this.

use std::collections::{HashMap, HashSet};

use nodedb_array::types::{ArrayId, TileId};

use crate::data::executor::handlers::spill::tiered::{RevSpool, SpillSpace, SpillWeight};
use crate::data::executor::handlers::transaction::undo::spilled::SpilledUndo;
use crate::data::executor::handlers::transaction::undo::{UndoEntry, UndoError};
use crate::types::{DatabaseId, TenantId};
use crate::wal::redo::replay_arm::RedoArm;

use super::super::state::CollectionKey;

/// The arm and the batch whose write an undo entry reverses.
#[derive(Debug, Clone, Copy, PartialEq, Eq, zerompk::ToMessagePack, zerompk::FromMessagePack)]
pub(in crate::data::executor::handlers::transaction::redo_apply) struct BatchAt {
    pub arm: RedoArm,
    /// The batch's index among every batch of the stream.
    pub batch: u32,
}

impl SpillWeight for BatchAt {
    fn spill_weight(&self) -> usize {
        std::mem::size_of::<Self>()
    }
}

/// A snapshot entry of the RAM lane.
struct RamUndo {
    /// The spill-lane position the entry follows: every spill-lane entry
    /// below it was recorded before it, every one at or above it after.
    follows: u64,
    at: BatchAt,
    entry: UndoEntry,
}

/// The keys whose pre-install state a kept snapshot holds.
///
/// Each undo of such a snapshot overwrites its key's whole state, and no
/// other undo entry reads or writes that state. So among the snapshots of
/// one key, the oldest decides the state a rollback leaves, and the later
/// ones change nothing once it applies. A write that reads or writes the
/// key's state outside a snapshot ends the run: the next snapshot of the
/// key is kept, so that write's undo sees the state it saw.
///
/// - A timeseries collection: memtable, last-value cache and series
///   catalog. A `TRUNCATE` and an ended-partition install end the run. A
///   flush between two batches does not: its undo reads and writes none of
///   that state. The
///   ingest timer each snapshot also holds is core-wide: the install's
///   oldest ingest snapshot is a first touch, so it applies last and puts
///   the timer back.
/// - A CRDT collection's Loro document.
/// - An array memtable tile.
#[derive(Default)]
struct SnapshotKeys {
    timeseries: HashSet<CollectionKey>,
    crdt: HashMap<(DatabaseId, TenantId), HashSet<String>>,
    tiles: HashMap<ArrayId, HashSet<TileId>>,
}

impl SnapshotKeys {
    /// Drop from `entry` what an earlier snapshot already reverses. `false`
    /// when nothing of it is left to keep.
    fn keep(&mut self, entry: &mut UndoEntry) -> bool {
        match entry {
            UndoEntry::TimeseriesIngest(undo) => {
                if self.timeseries.contains(&undo.collection_key) {
                    return false;
                }
                self.timeseries.insert(undo.collection_key.clone());
                true
            }
            UndoEntry::TimeseriesTruncate(undo) => {
                self.timeseries.remove(&undo.collection_key);
                true
            }
            UndoEntry::TimeseriesTruncateFlush(undo) => {
                self.timeseries.remove(&undo.collection_key);
                true
            }
            UndoEntry::TimeseriesEndedInstall(undo) => {
                self.timeseries.remove(&undo.collection_key);
                true
            }
            // The flush drains the memtable, and its undo leaves the
            // memtable alone: the run's first snapshot, which applies after
            // it, puts the memtable back whole. So the run goes on, and the
            // RAM lane holds one memtable copy however often the install
            // flushes.
            UndoEntry::TimeseriesInstallFlush(_) => true,
            UndoEntry::CrdtCollection(undo) => {
                let collections = self
                    .crdt
                    .entry((undo.database_id, undo.tenant_id))
                    .or_default();
                if collections.contains(undo.collection.as_str()) {
                    return false;
                }
                collections.insert(undo.collection.clone());
                true
            }
            UndoEntry::ArrayTiles { array_id, snapshot } => {
                let tiles = self.tiles.entry(array_id.clone()).or_default();
                snapshot.retain_tiles(|tile| tiles.insert(*tile));
                !snapshot.is_empty()
            }
            _ => true,
        }
    }
}

/// Why a drain stopped.
pub(in crate::data::executor::handlers::transaction::redo_apply) enum DrainStop {
    /// An entry did not reverse.
    Undo(UndoError),
    /// A spilled entry could not be read back.
    Read(crate::Error),
}

/// The undo log of one batched install. See the module doc.
pub(in crate::data::executor::handlers::transaction::redo_apply) struct SpillableUndo {
    /// The batch the arms run.
    at: BatchAt,
    spilled: RevSpool<(BatchAt, SpilledUndo)>,
    /// Ascending by `follows`, in record order among equal ones.
    ram: Vec<RamUndo>,
    ram_bytes: usize,
    keys: SnapshotKeys,
}

impl Default for SpillableUndo {
    fn default() -> Self {
        Self {
            at: BatchAt {
                arm: RedoArm::Reconstitute,
                batch: 0,
            },
            spilled: RevSpool::default(),
            ram: Vec::new(),
            ram_bytes: 0,
            keys: SnapshotKeys::default(),
        }
    }
}

/// The forward position in the log of an entry recorded after
/// `spill_before` spill-lane and `ram_before` RAM-lane entries.
fn forward(spill_before: u64, ram_before: usize) -> usize {
    usize::try_from(spill_before).map_or(usize::MAX, |spill| spill.saturating_add(ram_before))
}

/// The RAM a snapshot entry holds: its own size, and the heap its copies
/// and the state the engine gave up for it hold. A detached vector index
/// and the rows a columnar truncate moved out count by their headers: the
/// engine held the same bytes before.
fn ram_weight(entry: &UndoEntry) -> usize {
    let held = match entry {
        UndoEntry::TimeseriesIngest(undo) => undo.held_bytes(),
        UndoEntry::TimeseriesTruncate(undo) => undo.held_bytes(),
        UndoEntry::TimeseriesTruncateFlush(undo) => undo.held_bytes(),
        UndoEntry::VectorTruncate(undo) => {
            undo.collection.len()
                + undo.index_key.2.len()
                + undo.sidecars.len()
                    * std::mem::size_of::<
                        crate::data::executor::handlers::transaction::undo::row_version::RowVersionFloor,
                    >()
        }
        _ => 0,
    };
    std::mem::size_of::<UndoEntry>() + held
}

impl SpillableUndo {
    /// Name the batch whose writes the next entries reverse.
    pub(in crate::data::executor::handlers::transaction::redo_apply) fn enter(
        &mut self,
        at: BatchAt,
    ) {
        self.at = at;
    }

    /// Record the entries that reverse one write, in write order.
    pub(in crate::data::executor::handlers::transaction::redo_apply) fn record(
        &mut self,
        entries: impl IntoIterator<Item = UndoEntry>,
    ) {
        for mut entry in entries {
            if !self.keys.keep(&mut entry) {
                continue;
            }
            match SpilledUndo::try_from(entry) {
                Ok(spilled) => {
                    self.spilled.push((self.at, spilled));
                }
                Err(entry) => {
                    self.ram_bytes += ram_weight(&entry);
                    self.ram.push(RamUndo {
                        follows: self.spilled.len(),
                        at: self.at,
                        entry,
                    });
                }
            }
        }
    }

    /// Whether the log keeps a snapshot of the timeseries collection `key`
    /// that a rollback applies after every later entry of the collection.
    pub(in crate::data::executor::handlers::transaction::redo_apply) fn holds_ts_snapshot(
        &self,
        key: &CollectionKey,
    ) -> bool {
        self.keys.timeseries.contains(key)
    }

    /// The entries the log holds, in both lanes.
    pub(in crate::data::executor::handlers::transaction::redo_apply) fn len(&self) -> u64 {
        self.spilled.len().saturating_add(self.ram.len() as u64)
    }

    /// Whether the log holds no entry.
    pub(in crate::data::executor::handlers::transaction::redo_apply) fn is_empty(&self) -> bool {
        self.spilled.len() == 0 && self.ram.is_empty()
    }

    /// RAM the spill lane holds and can spill.
    pub(in crate::data::executor::handlers::transaction::redo_apply) fn hot_bytes(&self) -> usize {
        self.spilled.tail_bytes()
    }

    /// RAM the RAM lane holds: it cannot spill.
    pub(in crate::data::executor::handlers::transaction::redo_apply) fn pinned_bytes(
        &self,
    ) -> usize {
        self.ram_bytes
    }

    /// Append the spill lane's RAM entries to its file in `space`.
    pub(in crate::data::executor::handlers::transaction::redo_apply) fn spill(
        &mut self,
        space: &mut SpillSpace,
    ) -> crate::Result<()> {
        self.spilled.spill(space, "undo")
    }

    /// The entries of the RAM lane, for the settle of an install that
    /// succeeded.
    pub(in crate::data::executor::handlers::transaction::redo_apply) fn ram_entries_mut(
        &mut self,
    ) -> impl Iterator<Item = &mut UndoEntry> {
        self.ram.iter_mut().map(|ram| &mut ram.entry)
    }

    /// Hand every entry to `undo`, newest first, with its forward position
    /// in the log and the batch whose write it reverses. The entries of one
    /// batch come together, the newest batch first. The first error stops
    /// the drain.
    pub(in crate::data::executor::handlers::transaction::redo_apply) fn drain_newest_first(
        self,
        mut undo: impl FnMut(usize, BatchAt, UndoEntry) -> Result<(), UndoError>,
    ) -> Result<(), DrainStop> {
        let Self {
            spilled, mut ram, ..
        } = self;
        let mut spilled = spilled.into_newest_first();
        loop {
            // Every spill-lane entry at or above the newest RAM entry's
            // position was recorded after it, so it goes first. Its forward
            // position counts the RAM entries left: all came before it.
            let follows = ram.last().map_or(0, |last| last.follows);
            while spilled.remaining() > follows {
                let position = forward(spilled.remaining() - 1, ram.len());
                let Some((at, entry)) = spilled.next_newest().map_err(DrainStop::Read)? else {
                    break;
                };
                undo(position, at, UndoEntry::from(entry)).map_err(DrainStop::Undo)?;
            }
            let Some(last) = ram.pop() else {
                return Ok(());
            };
            undo(forward(last.follows, ram.len()), last.at, last.entry).map_err(DrainStop::Undo)?;
        }
    }

    /// Entries the spill lane took.
    #[cfg(test)]
    pub(in crate::data::executor::handlers::transaction::redo_apply) fn spill_lane_len(
        &self,
    ) -> u64 {
        self.spilled.len()
    }

    /// Entries the RAM lane holds.
    #[cfg(test)]
    pub(in crate::data::executor::handlers::transaction::redo_apply) fn ram_lane_len(
        &self,
    ) -> usize {
        self.ram.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::executor::handlers::transaction::undo::{
        TimeseriesIngestUndo, TimeseriesTruncateUndo,
    };
    use crate::engine::array::ArrayTileSnapshot;

    fn collection(name: &str) -> CollectionKey {
        (DatabaseId::DEFAULT, TenantId::new(1), name.to_string())
    }

    fn ingest(name: &str) -> UndoEntry {
        UndoEntry::TimeseriesIngest(TimeseriesIngestUndo {
            collection_key: collection(name),
            memtable_before: None,
            memtable_config_before: None,
            memtable_memory_bytes_before: None,
            last_value_cache_before: None,
            series_catalog_before: None,
            last_ts_ingest_before: None,
            reservation_bytes_before: None,
        })
    }

    fn truncate(name: &str) -> UndoEntry {
        UndoEntry::TimeseriesTruncate(Box::new(TimeseriesTruncateUndo {
            collection_key: collection(name),
            ended: Vec::new(),
            replay_stamp: None,
            stamp_file: None,
            cagg_image: None,
            newest_truncate: None,
            last_value_cache: None,
            series_catalog: None,
        }))
    }

    fn stats(label: &str) -> UndoEntry {
        UndoEntry::StatsRestore {
            key: label.to_string(),
            prior: None,
        }
    }

    fn label(entry: &UndoEntry) -> String {
        match entry {
            UndoEntry::StatsRestore { key, .. } => key.clone(),
            UndoEntry::TimeseriesIngest(undo) => undo.collection_key.2.clone(),
            _ => panic!("the tests record stats and ingest entries only"),
        }
    }

    fn at(arm: RedoArm, batch: u32) -> BatchAt {
        BatchAt { arm, batch }
    }

    fn tiles(prefixes: &[u64]) -> UndoEntry {
        UndoEntry::ArrayTiles {
            array_id: ArrayId::new(TenantId::new(1), "grid"),
            snapshot: ArrayTileSnapshot::of_tiles(
                prefixes
                    .iter()
                    .map(|prefix| (TileId::new(*prefix, 0), None))
                    .collect(),
            ),
        }
    }

    /// Both lanes, spilled between batches, drain in the reverse of the
    /// order they were recorded in, with each entry's forward position and
    /// batch, the newest batch first.
    #[test]
    fn rollback_reads_spilled_entries_newest_first() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut space = SpillSpace::new(dir.path().join("undo"), "test", 0);
        let mut undo = SpillableUndo::default();
        let mut recorded = Vec::new();
        for batch in 0..6u32 {
            let arm = if batch < 3 {
                RedoArm::Document
            } else {
                RedoArm::Timeseries
            };
            undo.enter(at(arm, batch));
            for index in 0..300u32 {
                let name = format!("b{batch}-{index}");
                let entry = if index % 97 == 0 {
                    ingest(&name)
                } else {
                    stats(&name)
                };
                recorded.push((at(arm, batch), name));
                undo.record([entry]);
            }
            if batch % 2 == 0 {
                undo.spill(&mut space).expect("spill");
                assert_eq!(undo.hot_bytes(), 0, "the spill lane holds no RAM entry");
                assert_eq!(
                    std::fs::read_dir(dir.path().join("undo")).map_or(0, |files| files.count()),
                    1,
                    "every spill appends to one file"
                );
            }
        }
        assert!(undo.ram_lane_len() > 0 && undo.spill_lane_len() > 0);

        let mut drained = Vec::new();
        undo.drain_newest_first(|index, batch, entry| {
            drained.push((index, batch, label(&entry)));
            Ok(())
        })
        .unwrap_or_else(|_| panic!("drain"));
        assert_eq!(
            std::fs::read_dir(dir.path().join("undo")).map_or(0, |files| files.count()),
            0,
            "every spill went to one file, which the drain removed"
        );

        let total = recorded.len();
        assert_eq!(drained.len(), total);
        for (position, (index, batch, name)) in drained.into_iter().enumerate() {
            let forward = total - 1 - position;
            assert_eq!(index, forward, "entry {name} keeps its forward position");
            assert_eq!((batch, name), recorded[forward].clone());
        }
    }

    /// The first error stops the drain, and no older entry applies.
    #[test]
    fn a_drain_stops_at_the_entry_that_does_not_reverse() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut space = SpillSpace::new(dir.path().join("undo"), "test", 0);
        let mut undo = SpillableUndo::default();
        undo.record([stats("a"), stats("b")]);
        undo.spill(&mut space).expect("spill");
        undo.record([stats("c"), ingest("d")]);
        let mut applied = Vec::new();
        let drained = undo.drain_newest_first(|index, _, entry| {
            let name = label(&entry);
            if name == "b" {
                return Err(UndoError::mismatch(index, "b does not reverse"));
            }
            applied.push(name);
            Ok(())
        });
        match drained {
            Err(DrainStop::Undo(error)) => assert_eq!(error.entry_index, 1),
            _ => panic!("the drain stops at b"),
        }
        assert_eq!(applied, ["d", "c"]);
    }

    /// A timeseries collection keeps the snapshot of its first ingest, and
    /// a truncate of it ends the run: the next ingest keeps its own.
    #[test]
    fn a_truncate_ends_a_timeseries_snapshot_run() {
        let mut undo = SpillableUndo::default();
        undo.record([ingest("m"), ingest("m"), ingest("n"), ingest("m")]);
        assert_eq!(undo.ram_lane_len(), 2, "one snapshot per collection");
        undo.record([truncate("m"), ingest("m"), ingest("m"), ingest("n")]);
        assert_eq!(
            undo.ram_lane_len(),
            4,
            "the truncate and the first ingest after it"
        );
    }

    /// An array tile keeps the snapshot of its first write: a later
    /// snapshot keeps only the tiles no earlier one holds.
    #[test]
    fn array_tiles_collapse_per_tile() {
        let mut undo = SpillableUndo::default();
        undo.record([tiles(&[1, 2])]);
        undo.record([tiles(&[2, 1])]);
        assert_eq!(undo.spill_lane_len(), 1, "every tile was touched before");
        undo.record([tiles(&[2, 3])]);
        assert_eq!(undo.spill_lane_len(), 2, "tile 3 is new");
    }
}
