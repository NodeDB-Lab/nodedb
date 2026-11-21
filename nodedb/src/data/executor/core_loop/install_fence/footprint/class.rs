// SPDX-License-Identifier: BUSL-1.1

//! The classes of state a task can touch, and how two of them join.

use crate::data::executor::task::ExecutionTask;
use crate::types::{DatabaseId, TenantId, VShardId};

use super::super::state::FenceKey;

/// The state a task can touch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::data::executor) enum Footprint {
    /// Reads of these keys only. Empty: the task touches no fenced state.
    Keys(Vec<FenceKey>),
    /// Writes of these keys, by a task whose events carry `vshard`. It
    /// also reads them.
    Write {
        keys: Vec<FenceKey>,
        vshard: VShardId,
    },
    /// Any state of database `db`, by a task whose events carry `vshard`.
    /// An op the plan cannot bound to keys gets this class.
    WholeDatabase { db: DatabaseId, vshard: VShardId },
    /// Any state of the core.
    Core,
    /// Core work with no client order to keep, such as a sampler or a
    /// checkpoint. It waits while a fence is armed, so it never sees an
    /// install part way, and holds no later task.
    Background,
}

/// What a task's footprint is derived from beside its plan.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::data::executor) struct Scope {
    /// The request's database. Sum targets and graph state key by it.
    pub(in crate::data::executor) db: DatabaseId,
    /// The request's tenant. Graph state keys by it.
    pub(in crate::data::executor) tenant: TenantId,
    /// The vShard the task's events carry.
    pub(in crate::data::executor) vshard: VShardId,
}

impl Scope {
    /// The scope of `task`'s request.
    pub(in crate::data::executor) fn of(task: &ExecutionTask) -> Self {
        Self {
            db: task.request.database_id,
            tenant: task.request.tenant_id,
            vshard: task.request.vshard_id,
        }
    }

    /// Any state of the request's database.
    pub(in crate::data::executor) fn whole_database(&self) -> Footprint {
        self.whole_database_of(self.db)
    }

    /// Any state of database `db`.
    pub(in crate::data::executor) fn whole_database_of(&self, db: DatabaseId) -> Footprint {
        Footprint::WholeDatabase {
            db,
            vshard: self.vshard,
        }
    }
}

impl Footprint {
    /// The footprint of a task that does both.
    ///
    /// Keys and writes union their keys. Two writes join only on one
    /// vShard. A whole database absorbs keys of that database only. Every
    /// other pair joins to `Core`, the one class that covers both.
    pub(in crate::data::executor) fn join(self, other: Footprint) -> Footprint {
        match (self, other) {
            (Footprint::Keys(mut a), Footprint::Keys(b)) => {
                a.extend(b);
                Footprint::Keys(a)
            }
            (Footprint::Keys(mut a), Footprint::Write { keys, vshard })
            | (
                Footprint::Write {
                    keys: mut a,
                    vshard,
                },
                Footprint::Keys(keys),
            ) => {
                a.extend(keys);
                Footprint::Write { keys: a, vshard }
            }
            (
                Footprint::Write {
                    keys: mut a,
                    vshard: left,
                },
                Footprint::Write {
                    keys: b,
                    vshard: right,
                },
            ) if left == right => {
                a.extend(b);
                Footprint::Write {
                    keys: a,
                    vshard: left,
                }
            }
            (Footprint::Keys(keys), Footprint::WholeDatabase { db, vshard })
            | (Footprint::WholeDatabase { db, vshard }, Footprint::Keys(keys))
                if all_of(&keys, db) =>
            {
                Footprint::WholeDatabase { db, vshard }
            }
            (
                Footprint::Write { keys, vshard: left },
                Footprint::WholeDatabase { db, vshard: right },
            )
            | (
                Footprint::WholeDatabase { db, vshard: right },
                Footprint::Write { keys, vshard: left },
            ) if left == right && all_of(&keys, db) => {
                Footprint::WholeDatabase { db, vshard: left }
            }
            (
                Footprint::WholeDatabase {
                    db: a,
                    vshard: left,
                },
                Footprint::WholeDatabase {
                    db: b,
                    vshard: right,
                },
            ) if a == b && left == right => Footprint::WholeDatabase {
                db: a,
                vshard: left,
            },
            (Footprint::Keys(_), Footprint::WholeDatabase { .. })
            | (Footprint::WholeDatabase { .. }, Footprint::Keys(_))
            | (Footprint::Write { .. }, Footprint::Write { .. })
            | (Footprint::Write { .. }, Footprint::WholeDatabase { .. })
            | (Footprint::WholeDatabase { .. }, Footprint::Write { .. })
            | (Footprint::WholeDatabase { .. }, Footprint::WholeDatabase { .. })
            | (Footprint::Background, _)
            | (_, Footprint::Background)
            | (Footprint::Core, _)
            | (_, Footprint::Core) => Footprint::Core,
        }
    }
}

/// Whether every key of `keys` belongs to `db`. A vShard key belongs to
/// no database.
fn all_of(keys: &[FenceKey], db: DatabaseId) -> bool {
    keys.iter().all(|key| key.database() == Some(db))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(db: u64, name: &str) -> FenceKey {
        FenceKey::Collection {
            db: DatabaseId::new(db),
            name: name.into(),
        }
    }

    fn write(keys: Vec<FenceKey>, vshard: u32) -> Footprint {
        Footprint::Write {
            keys,
            vshard: VShardId::new(vshard),
        }
    }

    fn whole(db: u64, vshard: u32) -> Footprint {
        Footprint::WholeDatabase {
            db: DatabaseId::new(db),
            vshard: VShardId::new(vshard),
        }
    }

    #[test]
    fn two_writes_of_one_vshard_union_their_keys() {
        let joined = write(vec![key(0, "a")], 1).join(write(vec![key(0, "b")], 1));
        assert_eq!(joined, write(vec![key(0, "a"), key(0, "b")], 1));
        assert_eq!(
            write(Vec::new(), 1).join(write(Vec::new(), 2)),
            Footprint::Core
        );
    }

    #[test]
    fn a_read_joins_a_write_as_a_write() {
        let joined = Footprint::Keys(vec![key(0, "a")]).join(write(vec![key(0, "b")], 1));
        assert_eq!(joined, write(vec![key(0, "a"), key(0, "b")], 1));
    }

    #[test]
    fn a_whole_database_absorbs_only_keys_of_its_database() {
        assert_eq!(
            Footprint::Keys(vec![key(0, "a")]).join(whole(0, 1)),
            whole(0, 1)
        );
        assert_eq!(
            Footprint::Keys(vec![key(7, "a")]).join(whole(0, 1)),
            Footprint::Core
        );
        assert_eq!(write(vec![key(0, "a")], 1).join(whole(0, 1)), whole(0, 1));
        assert_eq!(whole(0, 1).join(whole(7, 1)), Footprint::Core);
        assert_eq!(whole(0, 1).join(whole(0, 1)), whole(0, 1));
    }

    #[test]
    fn core_and_background_join_to_core() {
        assert_eq!(
            Footprint::Background.join(Footprint::Keys(Vec::new())),
            Footprint::Core
        );
        assert_eq!(whole(0, 1).join(Footprint::Core), Footprint::Core);
    }
}
