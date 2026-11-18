// SPDX-License-Identifier: BUSL-1.1

//! The ambient core state a batched install holds for its whole run, out
//! of the core and back.
//!
//! While its arms run, an install keeps state on the core that every write
//! reads: its apply scope, the task's record scope, its KV write scope,
//! its record stamp, and the task's journal window. The KV write scope
//! leaves whole, an enclosing scope the install joined included.
//! [`CoreLoop::leave_install`] takes all of it out and leaves the core as
//! it stands between tasks. [`CoreLoop::enter_install`] puts it back.

use crate::data::executor::handlers::transaction::redo_apply::RedoApplyScope;
use crate::engine::kv::engine::SuspendedKvScope;

use super::CoreLoop;
use super::apply_scope::ApplyScope;
use super::journal_context::{JournalSuspendError, SuspendedJournal};
use super::write_note::RecordStamps;

/// An install's ambient core state, out of the core.
pub struct InstallContext {
    /// The arms' apply scope. `None` once the arms ran.
    scope: Option<RedoApplyScope>,
    /// The record scope the task's apply set: its ordinals, its journaled
    /// flag, and the version keys it forces.
    apply_scope: ApplyScope,
    /// Whatever KV write scope is open: the install's, or one an enclosing
    /// apply opened around it, as a Calvin slice opens its fixed ordinal.
    /// The guards stay with whoever opened the scopes.
    kv: SuspendedKvScope,
    /// The stamp each write of the record takes.
    stamps: RecordStamps,
    /// The task's journal window, empty when the task opened none.
    journal: SuspendedJournal,
}

/// Why an install's ambient state cannot return to the core, with the state
/// it holds. The core fail-stopped.
pub struct InstallEnterRefused {
    pub error: JournalSuspendError,
    pub context: Box<InstallContext>,
}

impl CoreLoop {
    /// Take the install's ambient state out of the core: its apply scope,
    /// the task's record scope, every open KV write scope, its record
    /// stamp, and the task's journal window. The core then stands as it
    /// stands between tasks. `Err` when the journal window cannot leave:
    /// nothing moved, and the core fail-stopped.
    pub fn leave_install(&mut self) -> Result<InstallContext, JournalSuspendError> {
        let journal = self.suspend_journal()?;
        Ok(InstallContext {
            scope: self.redo_apply.scope.take(),
            apply_scope: std::mem::take(&mut self.apply_scope),
            kv: self.kv_engine.suspend_write_scope(),
            stamps: std::mem::take(&mut self.record_stamps),
            journal,
        })
    }

    /// Put `context` back as it stood at [`Self::leave_install`]. `Err`
    /// when the journal window cannot return: the core holds another
    /// window, or a commit since the leave is unpersisted. Nothing moved,
    /// the core fail-stopped, and the refusal carries `context` back.
    pub fn enter_install(&mut self, context: InstallContext) -> Result<(), InstallEnterRefused> {
        if let Err(error) = self.check_journal_resume() {
            return Err(InstallEnterRefused {
                error,
                context: Box::new(context),
            });
        }
        let InstallContext {
            scope,
            apply_scope,
            kv,
            stamps,
            journal,
        } = context;
        self.restore_journal(journal);
        self.redo_apply.scope = scope;
        self.apply_scope = apply_scope;
        self.kv_engine.resume_write_scope(kv);
        self.record_stamps.absorb(stamps);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::super::tests::{make_core_with_dir, make_default_task};
    use super::super::write_note::WriteStamp;
    use super::*;
    use crate::bridge::dispatch::JournalGroup;
    use crate::data::executor::handlers::transaction::overlay::BitemporalStamp;
    use crate::data::executor::handlers::transaction::redo_apply::RedoApplyPass;
    use crate::engine::kv::engine::KvWriteScope;
    use crate::types::{DatabaseId, Lsn, TenantId, VShardId};

    const LSN: u64 = 900;

    /// The slots `leave_install` and `enter_install` move, as one value.
    #[derive(Debug, PartialEq)]
    struct Slots {
        scope: Option<(u32, u64)>,
        replicated_ordinal: Option<i64>,
        calvin_txn_ordinal: Option<i64>,
        journaled: bool,
        graph_system_from: Option<i64>,
        graph_applied: Option<i64>,
        bitemporal_stamps: Vec<(u32, BitemporalStamp)>,
        storage_schemas: usize,
        kv: KvWriteScope,
        stamp: Option<WriteStamp>,
        journal_window: bool,
        held_events: bool,
        sparse_deferred: bool,
        edge_deferred: bool,
    }

    fn slots(core: &CoreLoop) -> Slots {
        let mut bitemporal_stamps: Vec<(u32, BitemporalStamp)> = core
            .apply_scope
            .bitemporal_stamps
            .iter()
            .map(|(surrogate, stamp)| (*surrogate, *stamp))
            .collect();
        bitemporal_stamps.sort_by_key(|(surrogate, _)| *surrogate);
        Slots {
            scope: core
                .redo_apply
                .scope
                .as_ref()
                .map(|scope| (scope.batch, scope.op_base)),
            replicated_ordinal: core.apply_scope.replicated_ordinal,
            calvin_txn_ordinal: core.apply_scope.calvin_txn_ordinal,
            journaled: core.apply_scope.journaled,
            graph_system_from: core.apply_scope.graph_system_from,
            graph_applied: core.apply_scope.graph_applied,
            bitemporal_stamps,
            storage_schemas: core.apply_scope.storage_schemas.borrow().len(),
            kv: core.kv_engine.current_write_scope(),
            stamp: core.record_stamps.get(LSN),
            journal_window: core.has_journal_window(),
            held_events: core.write_set_journal.held_events.is_some(),
            sparse_deferred: core.sparse.db().is_deferred(),
            edge_deferred: core.edge_store.db().is_deferred(),
        }
    }

    #[test]
    fn leave_then_enter_restores_every_ambient_slot() {
        let directory = tempfile::tempdir().expect("directory");
        let (mut core, _requests, _responses) = make_core_with_dir(directory.path());
        let resting = slots(&core);

        let task = make_default_task();
        let _permit = core.begin_task_journal(
            &task,
            JournalGroup {
                origin: Lsn::new(LSN),
                collection: "rows".into(),
                apply_key: 0,
                commit_hlc: None,
                change_position: None,
            },
        );
        let mut scope = RedoApplyScope::new(RedoApplyPass::Install, Vec::new());
        scope.batch = 4;
        scope.op_base = 9;
        core.redo_apply.scope = Some(scope);
        core.apply_scope.replicated_ordinal = Some(77);
        core.apply_scope.calvin_txn_ordinal = Some(78);
        core.apply_scope.journaled = true;
        core.apply_scope.graph_system_from = Some(79);
        core.apply_scope.graph_applied = Some(80);
        core.apply_scope.bitemporal_stamps.insert(
            5,
            BitemporalStamp {
                sys_from: 81,
                valid_from_ms: i64::MIN,
                valid_until_ms: i64::MAX,
            },
        );
        core.apply_scope.storage_schemas.get_mut().insert(
            (DatabaseId::DEFAULT, TenantId::new(1), "rows".into()),
            std::sync::Arc::new(nodedb_physical::physical_plan::StorageMode::default()),
        );
        let guard = core.kv_engine.open_write_scope(None);
        core.record_stamps.insert(WriteStamp {
            vshard: VShardId::new(3),
            lsn: Lsn::new(LSN),
        });
        let installing = slots(&core);
        assert!(installing.journal_window && installing.sparse_deferred);
        assert_eq!(installing.kv, KvWriteScope::Drawn(None));

        let context = core.leave_install().expect("leave");
        assert_eq!(slots(&core), resting, "the core stands as between tasks");
        assert!(!core.is_fail_stopped());

        assert!(core.enter_install(context).is_ok(), "enter");
        assert_eq!(slots(&core), installing, "every slot is back as it stood");
        core.kv_engine.close_write_scope(guard);
        assert_eq!(core.kv_engine.current_write_scope(), KvWriteScope::Closed);
    }

    /// Put `key` into the probe collection and answer the ordinal its live
    /// version took.
    fn put_ordinal(core: &mut CoreLoop, key: &[u8]) -> Option<i64> {
        core.kv_engine
            .put(crate::engine::kv::KvPutParams {
                database_id: 0,
                tenant_id: 1,
                collection: "probe",
                key,
                value: b"v",
                ttl_ms: 0,
                now_ms: 1_000,
                surrogate: crate::engine::kv::test_support::row_surrogate(key),
            })
            .expect("a bound row writes");
        core.kv_engine
            .version_of_key(
                0,
                1,
                "probe",
                key,
                nodedb_types::SystemTimeScope::Current,
                1_000,
            )
            .map(|version| version.sys_from)
    }

    #[test]
    fn a_left_calvin_install_keeps_its_ordinal_from_a_foreign_write() {
        let directory = tempfile::tempdir().expect("directory");
        let (mut core, _requests, _responses) = make_core_with_dir(directory.path());
        let calvin_ordinal = 1_000_000;
        // A Calvin slice opens its fixed ordinal around the install, and the
        // install's own scope joins it.
        let calvin = core.kv_engine.open_write_scope(Some(calvin_ordinal));
        let install = core.kv_engine.open_write_scope(None);
        assert_eq!(put_ordinal(&mut core, b"before"), Some(calvin_ordinal));

        let context = core.leave_install().expect("leave");
        assert_eq!(core.kv_engine.current_write_scope(), KvWriteScope::Closed);
        let foreign = put_ordinal(&mut core, b"foreign");
        assert!(foreign.is_some());
        assert_ne!(
            foreign,
            Some(calvin_ordinal),
            "a foreign write draws its own ordinal"
        );

        assert!(core.enter_install(context).is_ok(), "enter");
        assert_eq!(put_ordinal(&mut core, b"after"), Some(calvin_ordinal));
        core.kv_engine.close_write_scope(install);
        core.kv_engine.close_write_scope(calvin);
        assert_eq!(core.kv_engine.current_write_scope(), KvWriteScope::Closed);
    }

    #[test]
    fn enter_refuses_a_window_leaked_between_leave_and_enter() {
        let directory = tempfile::tempdir().expect("directory");
        let (mut core, _requests, _responses) = make_core_with_dir(directory.path());
        core.redo_apply.scope = Some(RedoApplyScope::new(RedoApplyPass::Install, Vec::new()));
        core.record_stamps.insert(WriteStamp {
            vshard: VShardId::new(3),
            lsn: Lsn::new(LSN),
        });
        let context = core.leave_install().expect("leave");

        // A task between leaves its journal window open.
        let task = make_default_task();
        let _permit = core.begin_task_journal(
            &task,
            JournalGroup {
                origin: Lsn::new(LSN + 1),
                collection: "rows".into(),
                apply_key: 0,
                commit_hlc: None,
                change_position: None,
            },
        );

        let Err(refused) = core.enter_install(context) else {
            panic!("an occupied journal slot refuses the enter");
        };
        assert_eq!(refused.error, JournalSuspendError::Occupied);
        assert!(core.is_fail_stopped());
        assert!(core.redo_apply.scope.is_none(), "nothing moved back");
        assert!(
            refused.context.scope.is_some(),
            "the refusal carries the scope"
        );
        assert!(refused.context.stamps.get(LSN).is_some());
    }
}
