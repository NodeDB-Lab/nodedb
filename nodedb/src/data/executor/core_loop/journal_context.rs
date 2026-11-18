// SPDX-License-Identifier: BUSL-1.1

//! The journal window of a task that gives the core back before it ends.
//!
//! A journalled task runs with the sparse and edge stores deferred and its
//! events held (see `super::write_set_journal`). A task that leaves the core
//! part way takes its window with it: [`CoreLoop::suspend_journal`] lifts the
//! deferral and empties the journal slots, so a task that runs meanwhile
//! commits durably, sends its events directly, and opens its own journal.
//! The window returns as it stood once `check_journal_resume` passes
//! (`restore_journal`), as `CoreLoop::enter_install` returns it.
//!
//! A window leaves only with every commit of it persisted. A deferred commit
//! left behind becomes durable with the next task's commit, or falls to a
//! seal meant for another task. Either refusal seals both stores and
//! fail-stops the core.

use super::CoreLoop;
use super::fail_stop::FailStopCause;
use super::held_events::HeldEvents;
use super::write_set_journal::RunningJournal;

/// A journal window out of the core: the journal slots and which stores
/// deferred.
pub(in crate::data::executor) struct SuspendedJournal {
    running: Option<RunningJournal>,
    held_events: Option<HeldEvents>,
    sparse_deferred: bool,
    edge_deferred: bool,
}

/// Why a journal window cannot leave or return to the core.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub(crate) enum JournalSuspendError {
    /// A store holds a deferred commit no durable commit persisted.
    #[error("the {store} store holds a deferred commit that no durable commit persisted")]
    Unpersisted { store: &'static str },
    /// Another task's journal window is open on the core.
    #[error("another task's journal window is open on the core")]
    Occupied,
}

impl CoreLoop {
    /// Whether a journal window is open on the core: a journal slot holds a
    /// task's journal, or a store defers.
    pub(in crate::data::executor) fn has_journal_window(&self) -> bool {
        self.write_set_journal.running.is_some()
            || self.write_set_journal.held_events.is_some()
            || self.sparse.db().is_deferred()
            || self.edge_store.db().is_deferred()
    }

    /// Take the journal window out of the core. The stores stop deferring,
    /// and the journal slots are empty until the window returns.
    pub(in crate::data::executor) fn suspend_journal(
        &mut self,
    ) -> Result<SuspendedJournal, JournalSuspendError> {
        if let Some(store) = self.unpersisted_store() {
            return Err(self.leak_journal_window(JournalSuspendError::Unpersisted { store }));
        }
        // `defer` raises both flags, and only a resolve lowers the
        // unresolved one. A deferred store is therefore unresolved, and
        // `defer` on resume restores both flags. A store that is not
        // deferred stays as it is.
        let sparse_deferred = self.sparse.db().is_deferred();
        if sparse_deferred {
            self.sparse.db().resume();
            self.sparse.db().resolve_journal();
        }
        let edge_deferred = self.edge_store.db().is_deferred();
        if edge_deferred {
            self.edge_store.db().resume();
            self.edge_store.db().resolve_journal();
        }
        Ok(SuspendedJournal {
            running: self.write_set_journal.running.take(),
            held_events: self.write_set_journal.held_events.take(),
            sparse_deferred,
            edge_deferred,
        })
    }

    /// Whether a journal window can return: the core holds no other window,
    /// and every commit since the suspend is persisted. A refusal seals both
    /// stores and fail-stops the core.
    pub(super) fn check_journal_resume(&mut self) -> Result<(), JournalSuspendError> {
        if self.has_journal_window() {
            return Err(self.leak_journal_window(JournalSuspendError::Occupied));
        }
        if let Some(store) = self.unpersisted_store() {
            return Err(self.leak_journal_window(JournalSuspendError::Unpersisted { store }));
        }
        Ok(())
    }

    /// Put `suspended` back. [`Self::check_journal_resume`] passed.
    pub(super) fn restore_journal(&mut self, suspended: SuspendedJournal) {
        if suspended.sparse_deferred {
            self.sparse.db().defer();
        }
        if suspended.edge_deferred {
            self.edge_store.db().defer();
        }
        self.write_set_journal.running = suspended.running;
        self.write_set_journal.held_events = suspended.held_events;
    }

    /// The first store that holds an unpersisted deferred commit.
    fn unpersisted_store(&self) -> Option<&'static str> {
        if self.sparse.db().has_unpersisted() {
            Some("sparse")
        } else if self.edge_store.db().has_unpersisted() {
            Some("edge")
        } else {
            None
        }
    }

    /// Seal both stores and stop the core: `error` leaks one task's window
    /// into another's. The seal keeps a leaked commit off disk, a normal
    /// close included.
    fn leak_journal_window(&mut self, error: JournalSuspendError) -> JournalSuspendError {
        self.sparse.db().seal();
        self.edge_store.db().seal();
        self.fail_stop_core(FailStopCause::JournalWindowLeaked, &error.to_string());
        error
    }
}

#[cfg(test)]
mod tests {
    use nodedb_types::{StorageKey, Surrogate};

    use super::super::write_set_journal::tests::{
        captures, crash_sparse, event_bus, ordinary_group, ordinary_task,
    };
    use super::*;
    use crate::bridge::envelope::Status;
    use crate::data::executor::core_loop::tests::make_core_with_dir;

    const PROBE: redb::TableDefinition<u64, u64> = redb::TableDefinition::new("journal_probe");

    /// Commit `key` to the edge store's probe table.
    fn put_edge_probe(core: &CoreLoop, key: u64) {
        let txn = core.edge_store.db().begin_write().expect("edge txn");
        txn.open_table(PROBE)
            .expect("probe table")
            .insert(key, key)
            .expect("probe insert");
        txn.commit().expect("probe commit");
    }

    /// The probe keys a crash at this instant leaves in the edge store.
    fn edge_probe_after_crash(directory: &std::path::Path) -> Vec<u64> {
        use redb::{ReadableDatabase, ReadableTable};
        let image = tempfile::tempdir().expect("image");
        let copy = image.path().join("graph.redb");
        std::fs::copy(
            crate::data::executor::snapshot::layout::graph_store_path(directory, 0),
            &copy,
        )
        .expect("copy edge crash image");
        let db = redb::Database::create(&copy).expect("open edge crash image");
        let txn = db.begin_read().expect("read");
        let Ok(table) = txn.open_table(PROBE) else {
            return Vec::new();
        };
        table
            .iter()
            .expect("scan probe")
            .map(|entry| entry.expect("probe entry").0.value())
            .collect()
    }

    /// Whether both stores refuse every later write: the refusal sealed them.
    fn sealed(core: &CoreLoop) -> bool {
        core.sparse.begin_durable_write().is_err()
            && core.edge_store.db().begin_durable_write().is_err()
            && core.edge_store.db().begin_write().is_err()
    }

    fn has_row(core: &CoreLoop, id: u32) -> bool {
        core.sparse
            .get(
                0,
                1,
                "ordinary_rows",
                &StorageKey::for_surrogate(Surrogate::new(id)),
            )
            .expect("read row")
            .is_some()
    }

    #[test]
    fn suspend_refuses_an_unpersisted_deferred_commit() {
        let directory = tempfile::tempdir().expect("directory");
        let (mut core, _requests, _responses) = make_core_with_dir(directory.path());
        let task = ordinary_task(1);
        assert!(core.begin_task_journal(&task, ordinary_group(1)).is_none());
        assert_eq!(core.execute(&task).status, Status::Ok);
        assert!(core.sparse.db().has_unpersisted());

        assert_eq!(
            core.suspend_journal().err(),
            Some(JournalSuspendError::Unpersisted { store: "sparse" })
        );
        assert!(core.is_fail_stopped());
        assert_eq!(
            core.fail_stop.cause().map(|(cause, _)| cause),
            Some(FailStopCause::JournalWindowLeaked)
        );
        assert!(core.write_set_journal.running.is_some());
        assert!(core.write_set_journal.held_events.is_some());
        assert!(sealed(&core), "the leaked commit never reaches disk");
    }

    /// A batched install persists each batch with a durable commit before it
    /// gives the core back, so its window leaves.
    #[test]
    fn suspend_takes_a_window_whose_deferred_commit_a_durable_one_persisted() {
        let directory = tempfile::tempdir().expect("directory");
        let (mut core, _requests, _responses) = make_core_with_dir(directory.path());
        let task = ordinary_task(1);
        assert!(core.begin_task_journal(&task, ordinary_group(1)).is_none());
        assert_eq!(core.execute(&task).status, Status::Ok);
        assert!(core.sparse.db().has_unpersisted());
        let txn = core.sparse.begin_durable_write().expect("durable txn");
        txn.commit().expect("durable commit");
        assert!(!core.sparse.db().has_unpersisted());

        let context = core.leave_install().expect("leave");
        assert!(!core.is_fail_stopped());
        assert!(!core.sparse.db().is_deferred());
        assert!(core.enter_install(context).is_ok(), "enter");
        assert!(core.sparse.db().is_deferred());
    }

    #[test]
    fn enter_refuses_an_unpersisted_commit_since_the_leave() {
        let directory = tempfile::tempdir().expect("directory");
        let (mut core, _requests, _responses) = make_core_with_dir(directory.path());
        let first = ordinary_task(1);
        assert!(core.begin_task_journal(&first, ordinary_group(1)).is_none());
        let context = core.leave_install().expect("leave");
        core.sparse.db().defer();
        assert_eq!(core.execute(&ordinary_task(2)).status, Status::Ok);
        core.sparse.db().resume();
        assert!(core.sparse.db().has_unpersisted());

        assert_eq!(
            core.enter_install(context)
                .err()
                .map(|refused| refused.error),
            Some(JournalSuspendError::Unpersisted { store: "sparse" })
        );
        assert_eq!(
            core.fail_stop.cause().map(|(cause, _)| cause),
            Some(FailStopCause::JournalWindowLeaked)
        );
        assert!(core.write_set_journal.running.is_none());
        assert!(sealed(&core));
    }

    #[test]
    fn a_task_between_leave_and_enter_commits_durably_and_sends_its_events() {
        let directory = tempfile::tempdir().expect("directory");
        let (mut core, _requests, _responses) = make_core_with_dir(directory.path());
        let mut events = event_bus(&mut core);
        let first = ordinary_task(1);
        assert!(core.begin_task_journal(&first, ordinary_group(1)).is_none());
        let context = core.leave_install().expect("leave");
        assert!(!core.sparse.db().is_deferred());
        assert!(!core.edge_store.db().is_deferred());

        let between = ordinary_task(2);
        assert_eq!(core.execute(&between).status, Status::Ok);
        assert_eq!(events(), 1, "the task between sends its event directly");
        put_edge_probe(&core, 2);
        assert!(!core.sparse.db().has_unpersisted());
        assert!(!core.edge_store.db().has_unpersisted());
        assert_eq!(
            edge_probe_after_crash(directory.path()),
            [2],
            "a crash keeps the edge commit of the task between"
        );
        let image = tempfile::tempdir().expect("image");
        crash_sparse(directory.path(), image.path());
        let (reopened, _req, _resp) = make_core_with_dir(image.path());
        assert!(
            has_row(&reopened, 2),
            "a crash keeps the row of the task between"
        );
        drop(reopened);

        assert!(core.enter_install(context).is_ok(), "enter");
        assert!(core.sparse.db().is_deferred());
        assert!(core.edge_store.db().is_deferred());
        let mut response = core.execute(&first);
        assert_eq!(response.status, Status::Ok);
        assert_eq!(events(), 0, "the resumed window holds its events");
        core.finish_journalled_task(&first, &mut response)
            .expect("finish journal");
        assert_eq!(captures(&core).len(), 1);
        assert_eq!(events(), 1);
        assert!(!core.is_fail_stopped());
    }

    #[test]
    fn a_journalled_task_between_keeps_its_own_journal() {
        let directory = tempfile::tempdir().expect("directory");
        let (mut core, _requests, _responses) = make_core_with_dir(directory.path());
        let mut events = event_bus(&mut core);
        let first = ordinary_task(1);
        assert!(core.begin_task_journal(&first, ordinary_group(1)).is_none());
        let context = core.leave_install().expect("leave");

        let between = ordinary_task(2);
        assert!(
            core.begin_task_journal(&between, ordinary_group(2))
                .is_none()
        );
        let mut response = core.execute(&between);
        assert_eq!(response.status, Status::Ok);
        assert_eq!(events(), 0);
        core.finish_journalled_task(&between, &mut response)
            .expect("finish the journal between");
        assert_eq!(captures(&core).len(), 1);
        assert_eq!(events(), 1);

        assert!(core.enter_install(context).is_ok(), "enter");
        assert!(matches!(
            core.write_set_journal.running,
            Some(RunningJournal::Pending { request_id: 1, .. })
        ));
        let mut response = core.execute(&first);
        assert_eq!(response.status, Status::Ok);
        core.finish_journalled_task(&first, &mut response)
            .expect("finish the first journal");
        let mut origins: Vec<u64> = captures(&core).iter().map(|c| c.origin).collect();
        origins.sort_unstable();
        assert_eq!(origins, [1, 2]);
        assert_eq!(events(), 1);
        assert!(!core.is_fail_stopped());
    }

    #[test]
    fn enter_refuses_an_occupied_slot() {
        let directory = tempfile::tempdir().expect("directory");
        let (mut core, _requests, _responses) = make_core_with_dir(directory.path());
        let first = ordinary_task(1);
        assert!(core.begin_task_journal(&first, ordinary_group(1)).is_none());
        let context = core.leave_install().expect("leave");
        let between = ordinary_task(2);
        assert!(
            core.begin_task_journal(&between, ordinary_group(2))
                .is_none()
        );

        assert_eq!(
            core.enter_install(context)
                .err()
                .map(|refused| refused.error),
            Some(JournalSuspendError::Occupied)
        );
        assert!(core.is_fail_stopped());
        assert!(matches!(
            core.write_set_journal.running,
            Some(RunningJournal::Pending { request_id: 2, .. })
        ));
        assert_eq!(
            core.fail_stop.cause().map(|(cause, _)| cause),
            Some(FailStopCause::JournalWindowLeaked)
        );
        assert!(sealed(&core));
    }
}
