// SPDX-License-Identifier: BUSL-1.1

//! `DatabaseRegistry` — monotonic database-id allocator.
//!
//! ## Id ranges
//!
//! `DatabaseId(0)` is the built-in `default` database. `1..=1023` is
//! reserved for system databases. User databases start at
//! [`USER_DB_START`] inside their cluster's id space.
//!
//! ## Id spaces
//!
//! A user database id is `(space << ID_SPACE_SHIFT) | counter`. The space
//! comes from the cluster id, so two clusters issue disjoint ids unless both
//! draw one space. An id then names one database across clusters.
//!
//! Every id stays below 2^53, so a JSON client reads it exactly. A cluster
//! issues under 2^30 ids in its life, one per reservation.
//!
//! ## Allocation
//!
//! Every id comes from a `DatabaseIdReserve` log entry of the metadata Raft
//! group, a one-node cluster included. The entry carries the id space.
//! [`DatabaseRegistry::reserve_at_index`] runs at apply time on every node,
//! in log order, so every node computes the same id with no input outside the
//! log. It persists the hwm together with the entry's log index.
//!
//! The hwm is persisted before the id leaves the registry. A persist error
//! returns `Err` and leaves the counter unchanged, so a restart never
//! reissues an id.
//!
//! ## Replay
//!
//! The metadata log replays from its first entry on every boot. The
//! registry is seeded with the persisted `(hwm, reserve_index)` pair, and
//! `reserve_at_index` skips every entry at or below `reserve_index`. Those
//! reservations are already folded into the seeded hwm.

use std::collections::HashMap;
use std::sync::Mutex;

use nodedb_types::DatabaseId;

use super::persist::DatabaseHwmPersist;

/// First user-assignable database id. `0..=1023` reserved.
pub const USER_DB_START: u64 = 1024;

/// Bit position of the id space inside a database id.
const ID_SPACE_SHIFT: u32 = 30;

/// Mask of the cluster id bits that form an id space: 23 bits, so a full id
/// fits 53 bits.
const ID_SPACE_MASK: u64 = (1 << 23) - 1;

/// The id space of the cluster `cluster_id`. Never `0`, the space of a fixture
/// that runs no cluster.
pub fn database_id_space(cluster_id: u64) -> u64 {
    (cluster_id & ID_SPACE_MASK).max(1)
}

/// The first user database id of `id_space`.
fn first_id_of_space(id_space: u64) -> u64 {
    (id_space << ID_SPACE_SHIFT) | USER_DB_START
}

/// Allocation errors. `From` wires this into the crate's central `Error`.
#[derive(Debug, thiserror::Error)]
pub enum DatabaseAllocError {
    #[error("database hwm persist failed: {detail}")]
    PersistFailed { detail: String },
}

/// Counter state guarded as one unit so the id and its cursor move together.
struct Counter {
    /// Next id to issue. Always `>= USER_DB_START`.
    next: u64,
    /// Highest metadata log index whose reservation is folded into `next`.
    reserve_index: u64,
}

/// Thread-safe database-id allocator.
pub struct DatabaseRegistry {
    counter: Mutex<Counter>,
    /// In-flight replicated reservations of this node: request id -> the id
    /// the applier carved for it, once applied.
    pending: Mutex<HashMap<u64, Option<DatabaseId>>>,
}

impl DatabaseRegistry {
    /// Create an empty registry — the first id issued is `USER_DB_START`.
    pub fn new() -> Self {
        Self::from_persisted(0, 0)
    }

    /// Restore from the persisted hwm and applied-reservation cursor.
    pub fn from_persisted(hwm: u64, reserve_index: u64) -> Self {
        Self {
            counter: Mutex::new(Counter {
                next: hwm.saturating_add(1).max(USER_DB_START),
                reserve_index,
            }),
            pending: Mutex::new(HashMap::new()),
        }
    }

    /// Reset the counter to the persisted hwm and applied-reservation
    /// cursor, after the catalog rows were replaced by a metadata snapshot.
    /// Waiting requests stay registered: their entries fall inside the
    /// snapshot, so they time out and retry.
    pub fn restore_persisted(&self, hwm: u64, reserve_index: u64) {
        let mut counter = self.lock_counter();
        counter.next = hwm.saturating_add(1).max(USER_DB_START);
        counter.reserve_index = reserve_index;
    }

    /// Apply the `DatabaseIdReserve` entry at `raft_index`.
    ///
    /// Returns `None` for an entry already folded into the seeded hwm
    /// (replay or duplicate delivery). Otherwise issues the next id of
    /// `id_space`, or a later one when the hwm already passed that space, and
    /// persists it with `raft_index` before returning it.
    pub fn reserve_at_index(
        &self,
        raft_index: u64,
        id_space: u64,
        persist: &dyn DatabaseHwmPersist,
    ) -> Result<Option<DatabaseId>, DatabaseAllocError> {
        let mut counter = self.lock_counter();
        if raft_index <= counter.reserve_index {
            return Ok(None);
        }
        let id = counter.next.max(first_id_of_space(id_space));
        persist
            .checkpoint_reserve(id, raft_index)
            .map_err(persist_failed)?;
        counter.next = id + 1;
        counter.reserve_index = raft_index;
        Ok(Some(DatabaseId::new(id)))
    }

    /// Highest id ever issued, or `USER_DB_START - 1` before the first one.
    pub fn current_hwm(&self) -> u64 {
        self.lock_counter().next - 1
    }

    /// Register an in-flight replicated reservation. The request id is
    /// random, so a request of this process never matches an entry a
    /// previous process of this node proposed.
    pub fn begin_request(&self) -> u64 {
        let mut pending = self.lock_pending();
        loop {
            let request_id = rand::random::<u64>();
            if let std::collections::hash_map::Entry::Vacant(slot) = pending.entry(request_id) {
                slot.insert(None);
                return request_id;
            }
        }
    }

    /// Record the id the applier carved for `request_id`. A request this
    /// process does not wait on is ignored.
    pub fn complete_request(&self, request_id: u64, id: DatabaseId) {
        if let Some(slot) = self.lock_pending().get_mut(&request_id) {
            *slot = Some(id);
        }
    }

    /// Stop waiting on `request_id` and return its id, if it was applied.
    pub fn finish_request(&self, request_id: u64) -> Option<DatabaseId> {
        self.lock_pending().remove(&request_id).flatten()
    }

    fn lock_counter(&self) -> std::sync::MutexGuard<'_, Counter> {
        self.counter.lock().unwrap_or_else(|p| p.into_inner())
    }

    fn lock_pending(&self) -> std::sync::MutexGuard<'_, HashMap<u64, Option<DatabaseId>>> {
        self.pending.lock().unwrap_or_else(|p| p.into_inner())
    }
}

impl Default for DatabaseRegistry {
    fn default() -> Self {
        Self::new()
    }
}

fn persist_failed(e: crate::Error) -> DatabaseAllocError {
    DatabaseAllocError::PersistFailed {
        detail: e.to_string(),
    }
}

impl From<DatabaseAllocError> for crate::Error {
    fn from(e: DatabaseAllocError) -> Self {
        match e {
            DatabaseAllocError::PersistFailed { detail } => crate::Error::Storage {
                engine: "database_registry".into(),
                detail,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, Ordering};

    use super::*;
    use crate::control::security::catalog::SystemCatalog;

    #[derive(Default)]
    struct MemPersist {
        state: Mutex<(u64, u64)>,
        fail: AtomicBool,
    }

    impl MemPersist {
        fn check_fail(&self) -> crate::Result<()> {
            if self.fail.load(Ordering::Acquire) {
                return Err(crate::Error::Storage {
                    engine: "test".into(),
                    detail: "injected".into(),
                });
            }
            Ok(())
        }
    }

    impl DatabaseHwmPersist for MemPersist {
        fn checkpoint_reserve(&self, hwm: u64, reserve_index: u64) -> crate::Result<()> {
            self.check_fail()?;
            *self.state.lock().unwrap() = (hwm, reserve_index);
            Ok(())
        }

        fn load(&self) -> crate::Result<u64> {
            Ok(self.state.lock().unwrap().0)
        }

        fn load_reserve_index(&self) -> crate::Result<u64> {
            Ok(self.state.lock().unwrap().1)
        }
    }

    /// The space of a fixture that runs no cluster.
    const NO_CLUSTER: u64 = 0;

    fn reopen(persist: &dyn DatabaseHwmPersist) -> DatabaseRegistry {
        DatabaseRegistry::from_persisted(
            persist.load().unwrap(),
            persist.load_reserve_index().unwrap(),
        )
    }

    #[test]
    fn first_reservation_is_user_db_start_and_persisted() {
        let persist = MemPersist::default();
        let reg = DatabaseRegistry::new();
        let id = reg.reserve_at_index(1, NO_CLUSTER, &persist).unwrap();
        assert_eq!(id, Some(DatabaseId::new(USER_DB_START)));
        assert_eq!(persist.load().unwrap(), USER_DB_START);
        assert_eq!(persist.load_reserve_index().unwrap(), 1);
    }

    #[test]
    fn hwm_below_user_start_is_floored() {
        let persist = MemPersist::default();
        let reg = DatabaseRegistry::from_persisted(500, 0);
        assert_eq!(
            reg.reserve_at_index(1, NO_CLUSTER, &persist).unwrap(),
            Some(DatabaseId::new(USER_DB_START))
        );
    }

    #[test]
    fn persist_error_is_returned_and_leaves_the_counter() {
        let persist = MemPersist::default();
        let reg = DatabaseRegistry::new();
        persist.fail.store(true, Ordering::Release);
        assert!(reg.reserve_at_index(1, NO_CLUSTER, &persist).is_err());
        persist.fail.store(false, Ordering::Release);
        assert_eq!(
            reg.reserve_at_index(1, NO_CLUSTER, &persist).unwrap(),
            Some(DatabaseId::new(USER_DB_START))
        );
    }

    /// Two nodes applying the same log compute the same ids.
    #[test]
    fn reservations_agree_across_nodes() {
        let (pa, pb) = (MemPersist::default(), MemPersist::default());
        let (a, b) = (DatabaseRegistry::new(), DatabaseRegistry::new());
        for index in [3, 8, 9] {
            assert_eq!(
                a.reserve_at_index(index, NO_CLUSTER, &pa).unwrap(),
                b.reserve_at_index(index, NO_CLUSTER, &pb).unwrap()
            );
        }
        assert_eq!(a.current_hwm(), USER_DB_START + 2);
    }

    /// Two clusters issue disjoint ids, each below the JSON exact-integer
    /// bound and above the reserved range.
    #[test]
    fn two_clusters_issue_disjoint_ids() {
        let (pa, pb) = (MemPersist::default(), MemPersist::default());
        let (a, b) = (DatabaseRegistry::new(), DatabaseRegistry::new());
        let (space_a, space_b) = (database_id_space(0xABCD), database_id_space(0x1234));
        for index in 1..=3 {
            let id_a = a.reserve_at_index(index, space_a, &pa).unwrap().unwrap();
            let id_b = b.reserve_at_index(index, space_b, &pb).unwrap().unwrap();
            assert_ne!(id_a, id_b);
            for id in [id_a, id_b] {
                assert!(id.as_u64() > USER_DB_START);
                assert!(id.as_u64() < (1 << 53), "{} is not JSON-exact", id.as_u64());
            }
        }
        assert_eq!(a.current_hwm() >> ID_SPACE_SHIFT, space_a);
        assert_eq!(b.current_hwm() >> ID_SPACE_SHIFT, space_b);
    }

    #[test]
    fn the_id_space_is_never_zero_and_ignores_high_bits() {
        assert_eq!(database_id_space(0), 1);
        assert_eq!(database_id_space(1 << 23), 1);
        assert_eq!(database_id_space(ID_SPACE_MASK), ID_SPACE_MASK);
        assert_eq!(database_id_space(u64::MAX), ID_SPACE_MASK);
    }

    /// Ids of one space stay consecutive across a restart.
    #[test]
    fn a_space_continues_past_its_hwm_after_a_restart() {
        let persist = MemPersist::default();
        let space = database_id_space(7);
        let reg = DatabaseRegistry::new();
        let first = reg.reserve_at_index(1, space, &persist).unwrap().unwrap();
        assert_eq!(first.as_u64(), first_id_of_space(space));
        let reg = reopen(&persist);
        let next = reg.reserve_at_index(2, space, &persist).unwrap().unwrap();
        assert_eq!(next.as_u64(), first.as_u64() + 1);
    }

    /// An hwm past the entry's space keeps rising, so no id is issued twice.
    #[test]
    fn the_hwm_never_moves_back_into_a_lower_space() {
        let persist = MemPersist::default();
        let reg = DatabaseRegistry::new();
        let high = reg
            .reserve_at_index(1, database_id_space(9), &persist)
            .unwrap()
            .unwrap();
        let low = reg
            .reserve_at_index(2, database_id_space(2), &persist)
            .unwrap()
            .unwrap();
        assert_eq!(low.as_u64(), high.as_u64() + 1);
    }

    /// A full-log replay after a restart skips every folded reservation and
    /// issues fresh ids only for entries past the cursor.
    #[test]
    fn replay_after_restart_skips_folded_reservations() {
        let persist = MemPersist::default();
        let reg = DatabaseRegistry::new();
        let first = reg.reserve_at_index(4, NO_CLUSTER, &persist).unwrap();
        let second = reg.reserve_at_index(6, NO_CLUSTER, &persist).unwrap();
        assert_eq!(first, Some(DatabaseId::new(USER_DB_START)));
        assert_eq!(second, Some(DatabaseId::new(USER_DB_START + 1)));

        let reg = reopen(&persist);
        assert_eq!(reg.reserve_at_index(4, NO_CLUSTER, &persist).unwrap(), None);
        assert_eq!(reg.reserve_at_index(6, NO_CLUSTER, &persist).unwrap(), None);
        assert_eq!(
            reg.reserve_at_index(11, NO_CLUSTER, &persist).unwrap(),
            Some(DatabaseId::new(USER_DB_START + 2))
        );
    }

    /// The hwm and cursor survive a reopen of the real catalog.
    #[test]
    fn catalog_backed_hwm_survives_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("system.redb");
        let before = {
            let catalog = SystemCatalog::open(&path).unwrap();
            let reg = reopen(&catalog);
            reg.reserve_at_index(5, NO_CLUSTER, &catalog).unwrap();
            reg.reserve_at_index(7, NO_CLUSTER, &catalog)
                .unwrap()
                .unwrap()
        };
        let catalog = SystemCatalog::open(&path).unwrap();
        let reg = reopen(&catalog);
        assert_eq!(reg.current_hwm(), before.as_u64());
        assert_eq!(reg.reserve_at_index(7, NO_CLUSTER, &catalog).unwrap(), None);
        let after = reg
            .reserve_at_index(12, NO_CLUSTER, &catalog)
            .unwrap()
            .unwrap();
        assert!(after.as_u64() > before.as_u64());
    }

    #[test]
    fn request_routing_only_completes_waited_requests() {
        let reg = DatabaseRegistry::new();
        let request = reg.begin_request();
        reg.complete_request(request.wrapping_add(1), DatabaseId::new(9));
        reg.complete_request(request, DatabaseId::new(2000));
        assert_eq!(reg.finish_request(request), Some(DatabaseId::new(2000)));
        assert_eq!(reg.finish_request(request), None);
    }
}
