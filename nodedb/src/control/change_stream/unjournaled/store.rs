// SPDX-License-Identifier: BUSL-1.1

//! The applied entries whose change events the change journal does not hold
//! yet.
//!
//! The settle pass journals a group's change events when it saves the
//! group's applied floor. A crash before that save loses the events every
//! entry above the floor staged. The restart delivers each such entry again,
//! and the proposal ledger skips it as a repeat. Its events then stage again
//! from the WAL, at the positions and the LSN the first apply gave them:
//!
//! - from its change-position marker's tail, the rows the write funnel
//!   derived from its plan;
//! - from its stream's chunks, for a chunked redo's marker.
//!
//! Each entry holds the WAL floor at its marker, and at its stream's first
//! chunk, from its apply until its group's saved floor covers it. A
//! checkpoint therefore never truncates a record of an entry that a restart
//! delivers again. These release an entry:
//!
//! - a floor saved at or above it: the journal holds its events;
//! - a snapshot install that covers it: it never applies here;
//! - this node leaving its group: it publishes no event of the group;
//! - for an entry boot read and no delivery restaged, the end of the boot's
//!   data-group replay: its group's saved floor covered it.

use std::collections::BTreeMap;
use std::sync::{Mutex, MutexGuard};

use crate::types::{DatabaseId, Lsn, TenantId, VShardId};
use crate::wal::manager::WalFloorHold;
use crate::wal::{RedoRowChange, RedoStreamRef};

/// The change events of an entry boot read from the WAL, for the delivery
/// that stages them again.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecoveredChanges {
    /// The LSN of the entry's change-position marker. Every event of the
    /// entry names it.
    pub lsn: Lsn,
    /// Where the entry's write applied: its marker's header.
    pub tenant_id: TenantId,
    pub database_id: DatabaseId,
    pub vshard_id: VShardId,
    /// The commit HLC the marker carries, which dates the events.
    pub commit_hlc: u64,
    /// The rows the marker's tail names. They stage first.
    pub rows: Vec<RedoRowChange>,
    /// The stream of a chunked redo's marker, whose row changes stage after
    /// `rows`.
    pub stream: Option<RedoStreamRef>,
}

/// One entry above its group's saved floor.
struct Unjournaled {
    /// What boot read for the entry. Taken by the delivery that restages it.
    recovered: Option<RecoveredChanges>,
    /// Keep the entry's records in the WAL until it releases.
    holds: Vec<WalFloorHold>,
}

/// The applied entries above their groups' saved floors, by
/// `(group_id, log_index)`.
#[derive(Default)]
pub struct UnjournaledEntries {
    entries: Mutex<BTreeMap<(u64, u64), Unjournaled>>,
}

impl UnjournaledEntries {
    pub fn new() -> Self {
        Self::default()
    }

    fn lock(&self) -> MutexGuard<'_, BTreeMap<(u64, u64), Unjournaled>> {
        self.entries.lock().unwrap_or_else(|p| p.into_inner())
    }

    /// Note that the entry at `log_index` of `group_id` appended its change
    /// marker, which `hold` keeps in the WAL. An entry noted before, such as
    /// one boot read, is replaced: this apply's records are the entry's now.
    pub fn note_marker(&self, group_id: u64, log_index: u64, hold: WalFloorHold) {
        let replaced = self.lock().insert(
            (group_id, log_index),
            Unjournaled {
                recovered: None,
                holds: vec![hold],
            },
        );
        // A replaced entry's holds release outside the lock.
        drop(replaced);
    }

    /// Keep the records `hold` holds for the entry at `log_index` of
    /// `group_id` too: the chunks its marker reads its row changes from.
    pub fn hold_from(&self, group_id: u64, log_index: u64, hold: WalFloorHold) {
        let mut entries = self.lock();
        let entry = entries
            .entry((group_id, log_index))
            .or_insert_with(|| Unjournaled {
                recovered: None,
                holds: Vec::new(),
            });
        entry.holds.push(hold);
    }

    /// Keep an entry boot read, with `holds` on its records.
    pub(super) fn insert_recovered(
        &self,
        group_id: u64,
        log_index: u64,
        recovered: RecoveredChanges,
        holds: Vec<WalFloorHold>,
    ) {
        let replaced = self.lock().insert(
            (group_id, log_index),
            Unjournaled {
                recovered: Some(recovered),
                holds,
            },
        );
        drop(replaced);
    }

    /// The change events boot read for the entry at `log_index` of
    /// `group_id`, which a delivery stages again. The entry stays, with its
    /// holds, until its group's saved floor covers it.
    pub fn take_recovered(&self, group_id: u64, log_index: u64) -> Option<RecoveredChanges> {
        self.lock()
            .get_mut(&(group_id, log_index))
            .and_then(|entry| entry.recovered.take())
    }

    /// Release every entry of `group_id` at or below `floor`: the group's
    /// saved applied floor, or the index a snapshot install covers.
    pub fn release_through(&self, group_id: u64, floor: u64) {
        let released = self.remove_where(|(group, index), _| group == group_id && index <= floor);
        drop(released);
    }

    /// Release every entry boot read that no delivery restaged, except in
    /// the groups `keep` names. Boot's data-group replay delivered every
    /// entry above each replayed group's saved floor, so such an entry sits
    /// at or below the floor: the journal holds its events.
    pub fn release_recovered(&self, keep: impl Fn(u64) -> bool) {
        let released =
            self.remove_where(|(group, _), entry| entry.recovered.is_some() && !keep(group));
        drop(released);
    }

    /// Release every entry of a group `hosted` rejects: this node publishes
    /// no event of a group it does not host.
    pub fn release_unhosted(&self, hosted: impl Fn(u64) -> bool) {
        let released = self.remove_where(|(group, _), _| !hosted(group));
        drop(released);
    }

    /// Remove every entry `released` names. The caller drops them outside
    /// the lock.
    fn remove_where(
        &self,
        released: impl Fn((u64, u64), &Unjournaled) -> bool,
    ) -> Vec<Unjournaled> {
        let mut entries = self.lock();
        let keys: Vec<(u64, u64)> = entries
            .iter()
            .filter(|(key, entry)| released(**key, entry))
            .map(|(key, _)| *key)
            .collect();
        keys.into_iter()
            .filter_map(|key| entries.remove(&key))
            .collect()
    }

    #[cfg(test)]
    pub(super) fn len(&self) -> usize {
        self.lock().len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wal::WalManager;

    fn wal(dir: &tempfile::TempDir) -> WalManager {
        WalManager::open_for_testing(&dir.path().join("wal")).expect("wal")
    }

    fn recovered(lsn: u64) -> RecoveredChanges {
        RecoveredChanges {
            lsn: Lsn::new(lsn),
            tenant_id: TenantId::new(1),
            database_id: DatabaseId::DEFAULT,
            vshard_id: VShardId::new(0),
            commit_hlc: 7,
            rows: Vec::new(),
            stream: None,
        }
    }

    /// An entry holds its records until its group's saved floor covers it.
    /// A floor releases no other group's entries.
    #[test]
    fn a_saved_floor_releases_the_entries_it_covers() {
        let dir = tempfile::tempdir().expect("tempdir");
        let wal = wal(&dir);
        let entries = UnjournaledEntries::new();
        entries.note_marker(4, 9, wal.floor_holds().hold(Lsn::new(30)));
        entries.note_marker(4, 12, wal.floor_holds().hold(Lsn::new(40)));
        entries.note_marker(5, 3, wal.floor_holds().hold(Lsn::new(45)));
        assert_eq!(wal.floor_holds().lowest(), Some(Lsn::new(30)));
        entries.release_through(4, 10);
        assert_eq!(entries.len(), 2);
        assert_eq!(wal.floor_holds().lowest(), Some(Lsn::new(40)));
        entries.release_unhosted(|group_id| group_id != 5);
        assert_eq!(entries.len(), 1);
        entries.release_through(4, 12);
        assert_eq!(wal.floor_holds().lowest(), None);
    }

    /// A delivery takes what boot read once. The entry keeps its holds
    /// until a floor covers it.
    #[test]
    fn a_recovered_entry_restages_once_and_holds_until_its_floor() {
        let dir = tempfile::tempdir().expect("tempdir");
        let wal = wal(&dir);
        let entries = UnjournaledEntries::new();
        entries.insert_recovered(
            4,
            9,
            recovered(50),
            vec![wal.floor_holds().hold(Lsn::new(30))],
        );
        entries.hold_from(4, 9, wal.floor_holds().hold(Lsn::new(20)));
        assert_eq!(entries.take_recovered(4, 9), Some(recovered(50)));
        assert_eq!(entries.take_recovered(4, 9), None);
        assert_eq!(wal.floor_holds().lowest(), Some(Lsn::new(20)));
        // A restaged entry is not one boot read and no delivery restaged.
        entries.release_recovered(|_| false);
        assert_eq!(entries.len(), 1);
        entries.release_through(4, 9);
        assert_eq!(wal.floor_holds().lowest(), None);
    }

    /// The end of boot's replay releases every entry no delivery restaged,
    /// except in a group that waits for a snapshot install.
    #[test]
    fn the_end_of_replay_releases_the_entries_no_delivery_restaged() {
        let dir = tempfile::tempdir().expect("tempdir");
        let wal = wal(&dir);
        let entries = UnjournaledEntries::new();
        entries.insert_recovered(
            4,
            9,
            recovered(50),
            vec![wal.floor_holds().hold(Lsn::new(30))],
        );
        entries.insert_recovered(
            6,
            2,
            recovered(60),
            vec![wal.floor_holds().hold(Lsn::new(35))],
        );
        entries.note_marker(4, 10, wal.floor_holds().hold(Lsn::new(70)));
        entries.release_recovered(|group_id| group_id == 6);
        assert_eq!(entries.take_recovered(4, 9), None);
        assert_eq!(entries.take_recovered(6, 2), Some(recovered(60)));
        assert_eq!(entries.len(), 2);
        assert_eq!(wal.floor_holds().lowest(), Some(Lsn::new(35)));
    }

    /// An entry applied again replaces what boot read for it.
    #[test]
    fn a_new_apply_replaces_what_boot_read() {
        let dir = tempfile::tempdir().expect("tempdir");
        let wal = wal(&dir);
        let entries = UnjournaledEntries::new();
        entries.insert_recovered(
            4,
            9,
            recovered(50),
            vec![wal.floor_holds().hold(Lsn::new(30))],
        );
        entries.note_marker(4, 9, wal.floor_holds().hold(Lsn::new(80)));
        assert_eq!(entries.take_recovered(4, 9), None);
        assert_eq!(wal.floor_holds().lowest(), Some(Lsn::new(80)));
    }
}
