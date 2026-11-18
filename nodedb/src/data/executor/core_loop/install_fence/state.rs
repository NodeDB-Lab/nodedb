// SPDX-License-Identifier: BUSL-1.1

//! The install fence: the state an install holds back while it installs.
//!
//! A task proceeds only when its footprint is disjoint from the fence keys
//! and from the footprint of every task already waiting. A waiting task of
//! a database holds every later task of that database. A waiting write
//! holds every later write of its event vShard. A waiting core task holds
//! every later task with a footprint. Background core work waits and holds
//! nothing.
//! A task with an empty footprint always proceeds.

use std::collections::{BTreeSet, VecDeque};

use nodedb_types::{NodeDbError, QualifiedCollection};

use crate::data::executor::task::ExecutionTask;
use crate::types::{DatabaseId, RequestId, TenantId, VShardId};

use super::footprint::{Footprint, footprint};

/// One piece of state an install writes.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(in crate::data::executor) enum FenceKey {
    /// A collection of `db`. `name` is the stored name a plan carries.
    Collection { db: DatabaseId, name: String },
    /// The node labels of `tenant` in `db`.
    GraphLabels { db: DatabaseId, tenant: TenantId },
    /// The event partition of a vShard: the install's held events carry
    /// it, and a write whose events carry it would advance the partition
    /// past them.
    VShard(VShardId),
}

impl FenceKey {
    /// The key of the collection stored as `stored`, in the database the
    /// name carries. A name that carries no valid database is an error.
    pub(in crate::data::executor) fn of_stored_collection(
        stored: &str,
    ) -> Result<Self, NodeDbError> {
        Ok(FenceKey::Collection {
            db: QualifiedCollection::database_of_stored(stored)?,
            name: stored.to_owned(),
        })
    }

    /// The keys of every collection in `stored`, such as the collections a
    /// committed redo writes.
    pub(in crate::data::executor) fn of_stored_collections(
        stored: &[String],
    ) -> Result<Vec<Self>, NodeDbError> {
        stored
            .iter()
            .map(|name| Self::of_stored_collection(name))
            .collect()
    }

    /// The database the key belongs to. A vShard spans databases.
    pub(in crate::data::executor) fn database(&self) -> Option<DatabaseId> {
        match self {
            FenceKey::Collection { db, .. } | FenceKey::GraphLabels { db, .. } => Some(*db),
            FenceKey::VShard(_) => None,
        }
    }
}

/// The union of the footprints of every waiting task.
#[derive(Debug, Default)]
pub(in crate::data::executor) struct Blocked {
    keys: BTreeSet<FenceKey>,
    databases: BTreeSet<DatabaseId>,
    /// The event vShards of the waiting writes.
    vshards: BTreeSet<VShardId>,
    core: bool,
}

impl Blocked {
    fn add(&mut self, footprint: Footprint) {
        match footprint {
            Footprint::Keys(keys) => self.keys.extend(keys),
            Footprint::Database(db) => {
                self.databases.insert(db);
            }
            Footprint::Write { db, vshard } => {
                self.databases.insert(db);
                self.vshards.insert(vshard);
            }
            Footprint::Core => self.core = true,
            // It orders against no later task.
            Footprint::Background => {}
        }
    }

    fn holds_database(&self, db: DatabaseId) -> bool {
        self.databases.contains(&db) || self.keys.iter().any(|key| key.database() == Some(db))
    }
}

/// The keys an install holds and the tasks that wait for its release.
pub(in crate::data::executor) struct InstallFence {
    /// The state the install writes.
    pub(in crate::data::executor) keys: BTreeSet<FenceKey>,
    /// Tasks held back, in arrival order.
    pub(in crate::data::executor) waiting: VecDeque<ExecutionTask>,
    /// The union of the waiting tasks' footprints.
    pub(in crate::data::executor) blocked: Blocked,
    /// Maintenance came due while the fence was armed and skipped. The
    /// release runs it before any waiter re-enters.
    pub(in crate::data::executor) maintenance_owed: bool,
    /// The lowest LSN of an install whose events are still held. A heartbeat
    /// publishes below it.
    pub(in crate::data::executor) event_floor: Option<u64>,
}

impl InstallFence {
    /// A fence over `keys` with no task waiting.
    pub(in crate::data::executor) fn new(keys: BTreeSet<FenceKey>) -> Self {
        Self {
            keys,
            waiting: VecDeque::new(),
            blocked: Blocked::default(),
            maintenance_owed: false,
            event_floor: None,
        }
    }

    /// Whether a task of `footprint` proceeds past the fence now.
    pub(in crate::data::executor) fn admits(&self, footprint: &Footprint) -> bool {
        match footprint {
            Footprint::Keys(keys) => {
                keys.is_empty()
                    || (!self.blocked.core
                        && keys.iter().all(|key| {
                            !self.keys.contains(key)
                                && !self.blocked.keys.contains(key)
                                && key
                                    .database()
                                    .is_none_or(|db| !self.blocked.databases.contains(&db))
                        }))
            }
            Footprint::Database(db) => self.admits_database(*db),
            Footprint::Write { db, vshard } => {
                self.admits_database(*db)
                    && !self.keys.contains(&FenceKey::VShard(*vshard))
                    && !self.blocked.vshards.contains(vshard)
            }
            Footprint::Core | Footprint::Background => false,
        }
    }

    /// Whether a task of any state of `db` proceeds now.
    fn admits_database(&self, db: DatabaseId) -> bool {
        !self.blocked.core
            && !self.blocked.holds_database(db)
            && !self.keys.iter().any(|key| key.database() == Some(db))
    }

    /// Hold `task`, whose footprint is `footprint`, behind every task
    /// already waiting.
    pub(in crate::data::executor) fn hold(&mut self, task: ExecutionTask, footprint: Footprint) {
        self.blocked.add(footprint);
        self.waiting.push_back(task);
    }

    /// Rebuild `blocked` from the tasks still waiting.
    pub(in crate::data::executor) fn recompute_blocked(&mut self) {
        let mut blocked = Blocked::default();
        for task in &self.waiting {
            blocked.add(footprint(task));
        }
        self.blocked = blocked;
    }

    /// Remove the waiting task of `request_id`. Returns whether one waited.
    pub(in crate::data::executor) fn remove_waiter(&mut self, request_id: RequestId) -> bool {
        let Some(pos) = self
            .waiting
            .iter()
            .position(|task| task.request_id() == request_id)
        else {
            return false;
        };
        self.waiting.remove(pos);
        self.recompute_blocked();
        true
    }

    /// Take every waiting task, in arrival order, and clear `blocked`.
    pub(in crate::data::executor) fn take_waiters(&mut self) -> VecDeque<ExecutionTask> {
        self.blocked = Blocked::default();
        std::mem::take(&mut self.waiting)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn collection(name: &str) -> FenceKey {
        FenceKey::Collection {
            db: DatabaseId::DEFAULT,
            name: name.into(),
        }
    }

    fn fence(keys: &[FenceKey]) -> InstallFence {
        InstallFence::new(keys.iter().cloned().collect())
    }

    #[test]
    fn a_read_of_another_collection_is_admitted() {
        let fence = fence(&[collection("a")]);
        assert!(fence.admits(&Footprint::Keys(vec![collection("b")])));
        assert!(!fence.admits(&Footprint::Keys(vec![collection("a")])));
    }

    #[test]
    fn a_database_task_waits_for_any_key_of_its_database() {
        let fence = fence(&[FenceKey::GraphLabels {
            db: DatabaseId::DEFAULT,
            tenant: TenantId::new(1),
        }]);
        assert!(!fence.admits(&Footprint::Database(DatabaseId::DEFAULT)));
        assert!(fence.admits(&Footprint::Database(DatabaseId::new(7))));
    }

    #[test]
    fn a_core_task_never_passes_an_armed_fence() {
        assert!(!fence(&[]).admits(&Footprint::Core));
    }

    #[test]
    fn an_empty_footprint_passes_a_waiting_core_task() {
        let mut fence = fence(&[]);
        fence.blocked.add(Footprint::Core);
        assert!(fence.admits(&Footprint::Keys(Vec::new())));
        assert!(!fence.admits(&Footprint::Keys(vec![collection("b")])));
    }

    #[test]
    fn waiting_background_work_holds_no_later_task() {
        let mut fence = fence(&[collection("a")]);
        assert!(
            !fence.admits(&Footprint::Background),
            "it waits for the install"
        );
        fence.blocked.add(Footprint::Background);
        assert!(fence.admits(&Footprint::Write {
            db: DatabaseId::new(7),
            vshard: VShardId::new(1),
        }));
        assert!(fence.admits(&Footprint::Keys(vec![collection("b")])));
    }

    #[test]
    fn a_write_of_a_fenced_vshard_waits_in_any_database() {
        let mut fence = fence(&[FenceKey::VShard(VShardId::new(3))]);
        let write = |vshard| Footprint::Write {
            db: DatabaseId::new(7),
            vshard: VShardId::new(vshard),
        };
        assert!(!fence.admits(&write(3)));
        assert!(fence.admits(&write(4)));
        fence.blocked.add(Footprint::Write {
            db: DatabaseId::new(8),
            vshard: VShardId::new(4),
        });
        assert!(!fence.admits(&write(4)), "a waiting write holds its vShard");
    }

    #[test]
    fn a_waiting_database_task_holds_every_later_task_of_its_database() {
        let mut fence = fence(&[collection("a")]);
        fence.blocked.add(Footprint::Database(DatabaseId::DEFAULT));
        assert!(!fence.admits(&Footprint::Keys(vec![collection("b")])));
        assert!(!fence.admits(&Footprint::Database(DatabaseId::DEFAULT)));
        assert!(fence.admits(&Footprint::Database(DatabaseId::new(7))));
    }
}
