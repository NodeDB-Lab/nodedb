// SPDX-License-Identifier: BUSL-1.1

//! Each mirror's replicated cut state, in memory, and the verdict it gives a
//! held entry.
//!
//! A mirror's entry commits to its data group before its cut is ready, and
//! holds the group's lane until it is. The verdict depends only on the metadata
//! log, so every replica reaches the same one, however far its data group
//! trails its metadata group:
//!
//! - A cut at or below `ready_cut` applies for the epoch that raised it. Any
//!   other epoch's copy skips.
//! - A cut above `ready_cut` waits, unless no `MirrorCut` can raise it. An
//!   epoch below the fence never raises it, and a sealed mirror takes no cut.
//!
//! The set follows the catalog through the metadata applier's synchronous
//! post-apply, which runs before the applied index moves. Boot and a metadata
//! image install rebuild it. The catalog is the source of truth.

use std::collections::HashMap;
use std::sync::RwLock;

use nodedb_types::{DatabaseId, MirrorCutState, MirrorStatus};
use tokio::sync::watch;

use crate::control::security::catalog::SystemCatalog;
use crate::control::security::catalog::database_types::DatabaseDescriptor;

/// What a stamped entry does at its turn in its group's log.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MirrorVerdict {
    /// The cut is ready and the entry's epoch owns it.
    Apply,
    /// The entry never applies, on any replica.
    Skip,
    /// The metadata log has not decided the entry yet.
    Wait,
}

#[derive(Debug, Default)]
struct WatchedMirror {
    state: MirrorCutState,
    /// The last cut the mirror applies, once PROMOTE sealed it.
    final_cut: Option<u64>,
    /// Whether the mirror is promoted. Its row no longer moves.
    promoted: bool,
}

impl WatchedMirror {
    fn verdict(&self, cut: u64, epoch: u64) -> MirrorVerdict {
        if cut > self.state.ready_cut {
            if self.final_cut.is_some() || epoch < self.state.fence_epoch {
                return MirrorVerdict::Skip;
            }
            return MirrorVerdict::Wait;
        }
        let in_range = self.final_cut.is_none_or(|last| cut <= last);
        if in_range && self.state.epoch_of(cut) == Some(epoch) {
            MirrorVerdict::Apply
        } else {
            MirrorVerdict::Skip
        }
    }

    fn follow_status(&mut self, status: &MirrorStatus) {
        match status {
            MirrorStatus::Bootstrapping { .. }
            | MirrorStatus::Following
            | MirrorStatus::Degraded
            | MirrorStatus::Disconnected
            | MirrorStatus::Failed { .. } => {
                self.final_cut = None;
                self.promoted = false;
            }
            MirrorStatus::Promoting { final_cut } => {
                self.final_cut = Some(*final_cut);
                self.promoted = false;
            }
            // A node that restarted past the promotion knows only the row.
            // PROMOTE leaves `ready_cut` at the final cut.
            MirrorStatus::Promoted => {
                self.final_cut = Some(self.final_cut.unwrap_or(self.state.ready_cut));
                self.promoted = true;
            }
        }
    }
}

/// The cut state of every database that follows or followed a source.
#[derive(Debug)]
pub struct MirrorCutWatch {
    mirrors: RwLock<HashMap<DatabaseId, WatchedMirror>>,
    /// The newest cut each mirror group applied on this node, by database and
    /// group. It lives in memory: the applier's next seal sets it again.
    seals: RwLock<HashMap<(DatabaseId, u64), u64>>,
    /// Moves whenever a verdict can change.
    changed: watch::Sender<u64>,
}

impl Default for MirrorCutWatch {
    fn default() -> Self {
        Self {
            mirrors: RwLock::default(),
            seals: RwLock::default(),
            changed: watch::channel(0).0,
        }
    }
}

impl MirrorCutWatch {
    pub fn new() -> Self {
        Self::default()
    }

    /// A receiver that wakes whenever a verdict can change.
    pub fn subscribe(&self) -> watch::Receiver<u64> {
        self.changed.subscribe()
    }

    fn wake(&self) {
        self.changed.send_modify(|version| {
            *version = version.wrapping_add(1);
        });
    }

    /// The verdict on an entry of `database_id` stamped with `cut` and
    /// `epoch`.
    ///
    /// The entry's metadata floor names a catalog that holds the mirror, so a
    /// database absent here is dropped, and its entry skips.
    pub fn verdict(&self, database_id: DatabaseId, cut: u64, epoch: u64) -> MirrorVerdict {
        self.mirrors
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(&database_id)
            .map_or(MirrorVerdict::Skip, |mirror| mirror.verdict(cut, epoch))
    }

    /// Follow one applied descriptor.
    pub fn apply_descriptor(&self, descriptor: &DatabaseDescriptor) {
        {
            let mut mirrors = self
                .mirrors
                .write()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            match &descriptor.mirror_origin {
                Some(origin) => mirrors
                    .entry(descriptor.id)
                    .or_default()
                    .follow_status(&origin.status),
                None => {
                    mirrors.remove(&descriptor.id);
                }
            }
        }
        self.wake();
    }

    /// Follow one applied cut state. A promoted mirror or a dropped database
    /// wrote no row, and a state behind the row is a late copy.
    pub fn apply_state(&self, database_id: DatabaseId, state: &MirrorCutState) {
        {
            let mut mirrors = self
                .mirrors
                .write()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let Some(mirror) = mirrors.get_mut(&database_id) else {
                return;
            };
            if mirror.promoted || !state.is_at_or_past(&mirror.state) {
                return;
            }
            mirror.state.clone_from(state);
        }
        self.wake();
    }

    /// Take `state` as a rebuild left it, though it sits behind the row this
    /// mirror held, and forget every seal. Its fence skips each former copy.
    pub fn restart(&self, database_id: DatabaseId, state: &MirrorCutState) {
        {
            let mut mirrors = self
                .mirrors
                .write()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if let Some(mirror) = mirrors.get_mut(&database_id) {
                mirror.state.clone_from(state);
            }
        }
        self.seals
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .retain(|(database, _), _| *database != database_id);
        self.wake();
    }

    /// Follow one applied database drop.
    pub fn remove(&self, database_id: DatabaseId) {
        self.mirrors
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(&database_id);
        self.seals
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .retain(|(database, _), _| *database != database_id);
        self.wake();
    }

    /// Replace the set with what `catalog` holds.
    ///
    /// A read error leaves the set unchanged, and the caller refuses to
    /// continue: a set that misses a mirror skips its entries.
    pub fn load_from_catalog(&self, catalog: &SystemCatalog) -> crate::Result<()> {
        let mut loaded = HashMap::new();
        for descriptor in catalog.list_databases()? {
            let Some(origin) = descriptor.mirror_origin.as_ref() else {
                continue;
            };
            let mut mirror = WatchedMirror {
                state: catalog
                    .get_mirror_cut_state(descriptor.id)?
                    .unwrap_or_default(),
                ..WatchedMirror::default()
            };
            mirror.follow_status(&origin.status);
            loaded.insert(descriptor.id, mirror);
        }
        *self
            .mirrors
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = loaded;
        self.wake();
        Ok(())
    }

    /// Record that `group_id` applied every entry of `database_id` through
    /// `cut` on this node.
    pub fn note_seal(&self, database_id: DatabaseId, group_id: u64, cut: u64) {
        let mut seals = self
            .seals
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let sealed = seals.entry((database_id, group_id)).or_default();
        *sealed = cut.max(*sealed);
    }

    /// The newest cut `group_id` applied for `database_id` on this node.
    pub fn sealed_cut(&self, database_id: DatabaseId, group_id: u64) -> Option<u64> {
        self.seals
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(&(database_id, group_id))
            .copied()
    }
}

#[cfg(test)]
mod tests {
    use nodedb_types::MirrorCutAdvance;

    use super::super::test_fixture::mirror_descriptor;
    use super::*;

    const DB: DatabaseId = DatabaseId::new(1100);

    fn descriptor(status: MirrorStatus) -> DatabaseDescriptor {
        mirror_descriptor(DB.as_u64(), "mirror_db", status)
    }

    fn state_at(cut: u64, epoch: u64) -> MirrorCutState {
        MirrorCutState::default()
            .advanced(&MirrorCutAdvance {
                cut,
                epoch,
                ..MirrorCutAdvance::default()
            })
            .expect("raises")
    }

    fn following() -> MirrorCutWatch {
        let watch = MirrorCutWatch::new();
        watch.apply_descriptor(&descriptor(MirrorStatus::Following));
        watch
    }

    #[test]
    fn an_entry_waits_for_its_cut_and_applies_for_the_epoch_that_raised_it() {
        let watch = following();
        assert_eq!(watch.verdict(DB, 1, 3), MirrorVerdict::Wait);
        watch.apply_state(DB, &state_at(1, 3));
        assert_eq!(watch.verdict(DB, 1, 3), MirrorVerdict::Apply);
    }

    /// A former applier's copy skips on a replica that decides after the new
    /// epoch's cut, and on one that decides before it, once the fence lands.
    #[test]
    fn a_stale_epochs_copy_skips() {
        let watch = following();
        watch.apply_state(DB, &state_at(1, 5));
        assert_eq!(watch.verdict(DB, 1, 4), MirrorVerdict::Skip);

        let fenced = following();
        fenced.apply_state(DB, &state_at(1, 4));
        assert_eq!(fenced.verdict(DB, 2, 4), MirrorVerdict::Wait);
        let fence = state_at(1, 4)
            .advanced(&MirrorCutAdvance {
                cut: 1,
                epoch: 5,
                ..MirrorCutAdvance::default()
            })
            .expect("fences");
        fenced.apply_state(DB, &fence);
        assert_eq!(
            fenced.verdict(DB, 2, 4),
            MirrorVerdict::Skip,
            "the former applier's partial cut releases its lanes"
        );
        assert_eq!(fenced.verdict(DB, 2, 5), MirrorVerdict::Wait);
        assert_eq!(fenced.verdict(DB, 1, 4), MirrorVerdict::Apply);
    }

    #[test]
    fn a_cut_past_the_seal_skips_and_one_at_it_applies() {
        let watch = following();
        watch.apply_state(DB, &state_at(3, 1));
        watch.apply_descriptor(&descriptor(MirrorStatus::Promoting { final_cut: 3 }));
        assert_eq!(watch.verdict(DB, 3, 1), MirrorVerdict::Apply);
        assert_eq!(watch.verdict(DB, 4, 1), MirrorVerdict::Skip);
    }

    #[test]
    fn a_cut_raised_past_the_final_one_skips() {
        let watch = following();
        watch.apply_state(DB, &state_at(4, 1));
        watch.apply_descriptor(&descriptor(MirrorStatus::Promoting { final_cut: 3 }));
        assert_eq!(watch.verdict(DB, 4, 1), MirrorVerdict::Skip);
    }

    /// A replica whose data group lags the promotion still applies the cuts
    /// the mirror took, and a late state moves nothing.
    #[test]
    fn a_promoted_mirror_decides_from_its_row() {
        let watch = following();
        watch.apply_state(DB, &state_at(3, 1));
        watch.apply_descriptor(&descriptor(MirrorStatus::Promoted));
        assert_eq!(watch.verdict(DB, 3, 1), MirrorVerdict::Apply);
        assert_eq!(watch.verdict(DB, 4, 1), MirrorVerdict::Skip);
        watch.apply_state(DB, &state_at(9, 1));
        assert_eq!(watch.verdict(DB, 9, 1), MirrorVerdict::Skip);
    }

    #[test]
    fn a_dropped_or_unknown_database_skips_and_takes_no_state() {
        let watch = MirrorCutWatch::new();
        assert_eq!(watch.verdict(DB, 1, 1), MirrorVerdict::Skip);
        watch.apply_state(DB, &state_at(1, 1));
        assert_eq!(watch.verdict(DB, 1, 1), MirrorVerdict::Skip);

        let dropped = following();
        dropped.note_seal(DB, 4, 1);
        dropped.remove(DB);
        assert_eq!(dropped.verdict(DB, 1, 1), MirrorVerdict::Skip);
        assert_eq!(dropped.sealed_cut(DB, 4), None);
    }

    #[test]
    fn a_late_state_moves_nothing_back() {
        let watch = following();
        watch.apply_state(DB, &state_at(3, 1));
        watch.apply_state(DB, &state_at(2, 1));
        assert_eq!(watch.verdict(DB, 3, 1), MirrorVerdict::Apply);
    }

    #[test]
    fn seals_only_rise() {
        let watch = following();
        watch.note_seal(DB, 4, 5);
        watch.note_seal(DB, 4, 3);
        assert_eq!(watch.sealed_cut(DB, 4), Some(5));
        assert_eq!(watch.sealed_cut(DB, 5), None);
    }

    /// Every change that can move a verdict wakes the apply loop.
    #[test]
    fn every_change_wakes_a_subscriber() {
        let watch = following();
        let mut changed = watch.subscribe();
        assert!(!changed.has_changed().expect("sender lives"));
        watch.apply_state(DB, &state_at(1, 1));
        assert!(changed.has_changed().expect("sender lives"));
        changed.borrow_and_update();
        watch.apply_descriptor(&descriptor(MirrorStatus::Promoted));
        assert!(changed.has_changed().expect("sender lives"));
        changed.borrow_and_update();
        watch.remove(DB);
        assert!(changed.has_changed().expect("sender lives"));
    }

    /// Boot rebuilds the verdicts from the catalog alone.
    #[test]
    fn a_catalog_load_replaces_the_set() {
        let catalog = SystemCatalog::open_in_memory().expect("catalog");
        catalog
            .put_database(&descriptor(MirrorStatus::Following))
            .expect("put");
        catalog
            .raise_mirror_cut_state(DB, &state_at(2, 7))
            .expect("raise");
        let watch = MirrorCutWatch::new();
        let gone = DatabaseId::new(9);
        watch.apply_descriptor(&DatabaseDescriptor {
            id: gone,
            ..descriptor(MirrorStatus::Following)
        });
        assert_eq!(watch.verdict(gone, 1, 1), MirrorVerdict::Wait);
        watch.load_from_catalog(&catalog).expect("load");
        assert_eq!(watch.verdict(DB, 2, 7), MirrorVerdict::Apply);
        assert_eq!(watch.verdict(DB, 3, 7), MirrorVerdict::Wait);
        assert_eq!(watch.verdict(gone, 1, 1), MirrorVerdict::Skip);
    }
}
