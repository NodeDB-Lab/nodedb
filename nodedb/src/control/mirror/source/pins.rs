// SPDX-License-Identifier: BUSL-1.1

//! Each mirror link's hold on this cluster's Raft logs.
//!
//! A pin keeps every log entry above its link's acked position, so a mirror
//! resumes where it stopped. The pins replicate through the metadata group, and
//! every node follows them here through the synchronous post-apply. Each group
//! has one compaction ceiling on this node: the lowest position any live pin
//! holds. A group a live pin never acked is held whole.
//!
//! A retained log lives in memory, so a pin has two bounds. A pin past its age
//! or past its bytes on any replica expires: compaction resumes, and the link's
//! next fetch answers `PinExpired`.
//!
//! `GroupStatus::retained_bytes` counts entry bytes alone. Every retained entry
//! also costs its own header and an allocator slot, which dominate when entries
//! are small, so the bound charges them too (see [`retained_cost`]).

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

use nodedb_cluster::GroupStatus;
use nodedb_raft::message::LogEntry;
use nodedb_types::config::tuning::MirrorTuning;
use nodedb_types::{MirrorPin, MirrorPinState};

use crate::control::security::catalog::SystemCatalog;

/// The owner name of a pin's ceiling on a group's log compaction.
pub const PIN_CEILING: &str = "mirror_pin";

/// What the allocator adds to one entry's data buffer, on average.
const ENTRY_ALLOCATION_BYTES: u64 = 16;

/// The memory `status`'s replica holds for its log: the entry bytes, plus each
/// retained entry's header and allocator slot.
pub fn retained_cost(status: &GroupStatus) -> u64 {
    let entries = status.last_log_index.saturating_sub(status.snapshot_index);
    let per_entry = std::mem::size_of::<LogEntry>() as u64 + ENTRY_ALLOCATION_BYTES;
    status
        .retained_bytes
        .saturating_add(entries.saturating_mul(per_entry))
}

/// Why a pin expires. The pure decision over one node's replicas.
///
/// A pin past its age expires. A replica past its byte bound expires the live
/// pin that holds it lowest, when that pin holds back compaction at all. The
/// next pass takes the next lowest, if the replica is still over.
pub fn pins_to_expire(
    pins: &[MirrorPin],
    statuses: &[GroupStatus],
    now_ms: u64,
    tuning: &MirrorTuning,
) -> Vec<u64> {
    let max_age_ms = tuning.max_pin_age_secs.saturating_mul(1_000);
    let live = || {
        pins.iter()
            .filter(|pin| matches!(pin.state, MirrorPinState::Live))
    };
    let mut expired: Vec<u64> = live()
        .filter(|pin| now_ms.saturating_sub(pin.acked_ms) > max_age_ms)
        .map(|pin| pin.link_id)
        .collect();
    for status in statuses {
        if retained_cost(status) <= tuning.max_pinned_bytes_per_group {
            continue;
        }
        let lowest = live()
            .filter(|pin| !expired.contains(&pin.link_id))
            .min_by_key(|pin| pin.ceiling(status.group_id));
        if let Some(pin) = lowest
            && pin.ceiling(status.group_id) < status.last_applied
        {
            expired.push(pin.link_id);
        }
    }
    expired
}

/// Every link's pin on this node, and one compaction ceiling per group.
#[derive(Debug, Default)]
pub struct MirrorPinTable {
    inner: RwLock<PinState>,
}

#[derive(Debug, Default)]
struct PinState {
    pins: HashMap<u64, MirrorPin>,
    ceilings: HashMap<u64, Arc<AtomicU64>>,
}

impl PinState {
    /// The lowest index of `group_id` any pin holds, `u64::MAX` for none.
    fn ceiling(&self, group_id: u64) -> u64 {
        self.pins
            .values()
            .map(|pin| pin.ceiling(group_id))
            .min()
            .unwrap_or(u64::MAX)
    }

    fn publish(&self) {
        for (group_id, ceiling) in &self.ceilings {
            ceiling.store(self.ceiling(*group_id), Ordering::Release);
        }
    }
}

impl MirrorPinTable {
    fn write(&self) -> std::sync::RwLockWriteGuard<'_, PinState> {
        self.inner.write().unwrap_or_else(|p| p.into_inner())
    }

    fn read(&self) -> std::sync::RwLockReadGuard<'_, PinState> {
        self.inner.read().unwrap_or_else(|p| p.into_inner())
    }

    /// Follow one applied pin, merged as the catalog merged it.
    pub fn apply(&self, pin: &MirrorPin) {
        let mut state = self.write();
        let next = state
            .pins
            .get(&pin.link_id)
            .map_or_else(|| pin.clone(), |held| held.merged(pin));
        state.pins.insert(pin.link_id, next);
        state.publish();
    }

    /// Follow one applied pin removal.
    pub fn remove(&self, link_id: u64) {
        let mut state = self.write();
        state.pins.remove(&link_id);
        state.publish();
    }

    /// Follow one applied database drop.
    pub fn source_dropped(&self, database: u64) {
        let mut state = self.write();
        for pin in state.pins.values_mut() {
            if pin.database == database {
                pin.state = MirrorPinState::SourceDropped;
            }
        }
        state.publish();
    }

    /// Replace the table with what `catalog` holds.
    pub fn load_from_catalog(&self, catalog: &SystemCatalog) -> crate::Result<()> {
        let pins = catalog.list_mirror_pins()?;
        let mut state = self.write();
        state.pins = pins.into_iter().map(|pin| (pin.link_id, pin)).collect();
        state.publish();
        Ok(())
    }

    /// The pin of `link_id`.
    pub fn get(&self, link_id: u64) -> Option<MirrorPin> {
        self.read().pins.get(&link_id).cloned()
    }

    /// Every pin on this node.
    pub fn all(&self) -> Vec<MirrorPin> {
        self.read().pins.values().cloned().collect()
    }

    /// Whether any link holds a live pin.
    pub fn any_live(&self) -> bool {
        self.read()
            .pins
            .values()
            .any(|pin| matches!(pin.state, MirrorPinState::Live))
    }

    /// The compaction ceiling of `group_id`, created on first use at what the
    /// pins hold now.
    pub fn ceiling_for(&self, group_id: u64) -> Arc<AtomicU64> {
        let mut state = self.write();
        let value = state.ceiling(group_id);
        Arc::clone(
            state
                .ceilings
                .entry(group_id)
                .or_insert_with(|| Arc::new(AtomicU64::new(value))),
        )
    }

    /// Whether `group_id`'s ceiling exists on this node.
    pub fn has_ceiling(&self, group_id: u64) -> bool {
        self.read().ceilings.contains_key(&group_id)
    }
}

#[cfg(test)]
mod tests {
    use nodedb_types::MirrorGroupPosition;

    use super::*;

    fn pin(link_id: u64, positions: &[(u64, u64)], acked_ms: u64) -> MirrorPin {
        MirrorPin {
            link_id,
            client: "dr".to_string(),
            database: 1024,
            positions: positions
                .iter()
                .map(|(group_id, log_index)| MirrorGroupPosition {
                    group_id: *group_id,
                    log_index: *log_index,
                })
                .collect(),
            acked_ms,
            state: MirrorPinState::Live,
        }
    }

    fn status(group_id: u64, retained_bytes: u64, entries: u64, last_applied: u64) -> GroupStatus {
        GroupStatus {
            group_id,
            role: "Leader".to_string(),
            leader_id: 1,
            term: 1,
            commit_index: last_applied,
            last_applied,
            last_log_index: entries,
            snapshot_index: 0,
            retained_bytes,
            replica_state: nodedb_raft::ReplicaState::Whole,
            member_count: 1,
            learner_count: 0,
            vshard_count: 1,
        }
    }

    fn tuning(max_bytes: u64) -> MirrorTuning {
        MirrorTuning {
            cut_interval_ms: 1_000,
            max_pinned_bytes_per_group: max_bytes,
            max_pin_age_secs: 100,
        }
    }

    #[test]
    fn the_lowest_live_pin_binds_and_a_new_pin_holds_the_whole_log() {
        let table = MirrorPinTable::default();
        let ceiling = table.ceiling_for(1);
        assert_eq!(ceiling.load(Ordering::Acquire), u64::MAX);
        table.apply(&pin(1, &[(1, 40)], 5));
        table.apply(&pin(2, &[(1, 30)], 5));
        assert_eq!(ceiling.load(Ordering::Acquire), 30);
        table.apply(&pin(3, &[], 5));
        assert_eq!(ceiling.load(Ordering::Acquire), 0);
        table.remove(3);
        assert_eq!(ceiling.load(Ordering::Acquire), 30);
    }

    /// An expiry releases compaction at once, and a late ack never holds again.
    #[test]
    fn an_expired_pin_releases_its_hold() {
        let table = MirrorPinTable::default();
        let ceiling = table.ceiling_for(1);
        table.apply(&pin(1, &[(1, 40)], 5));
        table.apply(&MirrorPin {
            state: MirrorPinState::Expired,
            ..pin(1, &[], 6)
        });
        assert_eq!(ceiling.load(Ordering::Acquire), u64::MAX);
        table.apply(&pin(1, &[(1, 50)], 7));
        assert_eq!(ceiling.load(Ordering::Acquire), u64::MAX);
        assert!(!table.any_live());
    }

    #[test]
    fn a_database_drop_releases_its_pins() {
        let table = MirrorPinTable::default();
        let ceiling = table.ceiling_for(1);
        table.apply(&pin(1, &[(1, 40)], 5));
        table.source_dropped(1024);
        assert_eq!(ceiling.load(Ordering::Acquire), u64::MAX);
        assert_eq!(
            table.get(1).map(|held| held.state),
            Some(MirrorPinState::SourceDropped)
        );
    }

    #[test]
    fn a_pin_past_its_age_expires() {
        let pins = [pin(1, &[(1, 40)], 1_000), pin(2, &[(1, 40)], 90_000)];
        assert_eq!(
            pins_to_expire(&pins, &[], 101_001, &tuning(u64::MAX)),
            vec![1]
        );
    }

    /// The bound counts what `retained_bytes` leaves out. A million empty
    /// entries hold no data bytes and still cost real memory.
    #[test]
    fn the_byte_bound_charges_each_entrys_overhead() {
        let per_entry = std::mem::size_of::<LogEntry>() as u64 + ENTRY_ALLOCATION_BYTES;
        let small = status(1, 0, 1_000_000, 1_000_000);
        assert_eq!(retained_cost(&small), per_entry * 1_000_000);
        let pins = [pin(1, &[(1, 5)], 100_000)];
        assert_eq!(
            pins_to_expire(&pins, &[small], 100_000, &tuning(per_entry * 1_000)),
            vec![1]
        );
    }

    #[test]
    fn the_byte_bound_expires_the_lowest_pin_only() {
        let pins = [pin(1, &[(1, 40)], 100_000), pin(2, &[(1, 90)], 100_000)];
        let over = status(1, 2_000, 100, 100);
        assert_eq!(
            pins_to_expire(&pins, &[over], 100_000, &tuning(1_000)),
            vec![1]
        );
    }

    /// A log the pins do not hold back shrinks on its own.
    #[test]
    fn a_pin_past_the_applied_index_expires_nothing() {
        let pins = [pin(1, &[(1, 100)], 100_000)];
        let over = status(1, 2_000, 100, 100);
        assert!(pins_to_expire(&pins, &[over], 100_000, &tuning(1_000)).is_empty());
    }

    #[test]
    fn a_stopped_pin_never_expires_again() {
        let stopped = MirrorPin {
            state: MirrorPinState::Expired,
            ..pin(1, &[(1, 1)], 0)
        };
        assert!(
            pins_to_expire(
                &[stopped],
                &[status(1, 9_000, 100, 100)],
                u64::MAX,
                &tuning(1)
            )
            .is_empty()
        );
    }

    #[test]
    fn a_catalog_load_replaces_the_table() {
        let catalog = SystemCatalog::open_in_memory().expect("catalog");
        catalog
            .raise_mirror_pin(&pin(1, &[(1, 40)], 5))
            .expect("raise");
        let table = MirrorPinTable::default();
        let ceiling = table.ceiling_for(1);
        table.apply(&pin(9, &[(1, 1)], 5));
        table.load_from_catalog(&catalog).expect("load");
        assert!(table.get(9).is_none());
        assert_eq!(ceiling.load(Ordering::Acquire), 40);
    }
}
