// SPDX-License-Identifier: BUSL-1.1

//! Durable write sets of journalled writes.
//!
//! The Control Plane journals a grouped write's write set after apply, as
//! the parts of the write's record group. A crash between the apply and that
//! append must leave restart state, point-in-time restore and the rebuilt
//! event stream in agreement. The core therefore runs a journalled task with
//! the durability of the sparse and edge stores deferred (see
//! `crate::engine::durability_gate`), and holds the task's events. Once the
//! task ran, one durable sparse commit stores the write set and persists
//! every effect before it. The edge store persists next, and the events go
//! out last.
//!
//! - A crash before that commit rolls the effects back, and no event left the
//!   core. Boot cancels the write's group.
//! - A crash after it leaves the effects and the stored write set. Boot
//!   journals the parts the WAL lacks from it, and replay rebuilds any edge
//!   the crash rolled back.
//!
//! Boot does both in `crate::bootstrap::write_group_settle`.

use std::collections::HashMap;

use crate::bridge::dispatch::{JournalGroup, WriteSetJournal};
use crate::bridge::envelope::{Response, Status, WriteSetEntry};
use crate::event::WriteEvent;
use crate::wal::{CapturedEntry, OriginAppend, WriteSetCapture};
use nodedb_physical::physical_plan::{MetaOp, PhysicalPlan};

use super::CoreLoop;
use super::fail_stop::FailStopCause;
use crate::data::executor::task::ExecutionTask;

/// The journal side of a core.
#[derive(Default)]
pub(in crate::data::executor) struct WriteSetJournalState {
    /// The record group of each queued request that journals one.
    groups: HashMap<u64, JournalGroup>,
    /// Origins whose parts are durable. The next store drops their write
    /// sets.
    settled: Vec<u64>,
    running: Option<RunningJournal>,
    /// The events of the running journalled tasks, sent once their write
    /// sets are stored. `None` outside a journalled task.
    pub(in crate::data::executor) held_events: Option<Vec<WriteEvent>>,
}

/// Authorization belongs only to the root redo dispatch that started the journal.
pub(in crate::data::executor) struct RootJournalPermit {
    request_id: u64,
    tenant_id: u64,
    database_id: u64,
    vshard_id: u32,
}

enum RunningJournal {
    Pending {
        request_id: u64,
        group: JournalGroup,
    },
    Durable {
        write_set: Vec<WriteSetEntry>,
    },
    Failed,
}

impl CoreLoop {
    /// Keep the journal a ring push handed over with request `request_id`.
    pub(in crate::data::executor) fn keep_write_set_journal(
        &mut self,
        request_id: u64,
        journal: WriteSetJournal,
    ) {
        if let Some(group) = journal.group {
            self.write_set_journal.groups.insert(request_id, group);
        }
        self.write_set_journal
            .settled
            .extend(journal.settled.iter().map(|lsn| lsn.as_u64()));
    }

    /// Whether `task` journals its write set.
    pub(in crate::data::executor) fn journals_write_set(&self, task: &ExecutionTask) -> bool {
        self.write_set_journal
            .groups
            .contains_key(&task.request_id().as_u64())
    }

    /// Take the record group `task` journals into, if any.
    pub(in crate::data::executor) fn take_journal_group(
        &mut self,
        task: &ExecutionTask,
    ) -> Option<JournalGroup> {
        self.write_set_journal
            .groups
            .remove(&task.request_id().as_u64())
    }

    /// Forget the group of request `request_id`, which never runs.
    pub(in crate::data::executor) fn drop_journal_group(&mut self, request_id: u64) {
        self.write_set_journal.groups.remove(&request_id);
    }

    /// Defer the durability of every commit on the sparse and edge stores,
    /// and hold every event, until [`Self::finish_journalled`].
    pub(in crate::data::executor) fn begin_journalled(&mut self) {
        self.sparse.db().defer();
        self.edge_store.db().defer();
        self.write_set_journal
            .held_events
            .get_or_insert_with(Vec::new);
    }

    /// Start the root task's journal and authorize only a root redo installer.
    pub(in crate::data::executor) fn begin_task_journal(
        &mut self,
        task: &ExecutionTask,
        group: JournalGroup,
    ) -> Option<RootJournalPermit> {
        self.begin_journalled();
        self.write_set_journal.running = Some(RunningJournal::Pending {
            request_id: task.request_id().as_u64(),
            group,
        });
        matches!(
            task.plan(),
            PhysicalPlan::Meta(MetaOp::ApplyTransactionRedo { .. })
        )
        .then(|| RootJournalPermit {
            request_id: task.request_id().as_u64(),
            tenant_id: task.request.tenant_id.as_u64(),
            database_id: task.request.database_id.as_u64(),
            vshard_id: task.request.vshard_id.as_u32(),
        })
    }

    /// Construct the capture while undo still owns every installed effect.
    pub(in crate::data::executor) fn prepare_redo_capture(
        &self,
        task: &ExecutionTask,
        permit: RootJournalPermit,
        write_set: &[WriteSetEntry],
    ) -> crate::Result<(u64, Vec<u8>)> {
        let valid = permit.request_id == task.request_id().as_u64()
            && permit.tenant_id == task.request.tenant_id.as_u64()
            && permit.database_id == task.request.database_id.as_u64()
            && permit.vshard_id == task.request.vshard_id.as_u32()
            && matches!(
                task.plan(),
                PhysicalPlan::Meta(MetaOp::ApplyTransactionRedo { .. })
            );
        let Some(RunningJournal::Pending { request_id, group }) = &self.write_set_journal.running
        else {
            return Err(crate::Error::Storage {
                engine: "journal".into(),
                detail: "redo capture requires a pending root journal".into(),
            });
        };
        if !valid || *request_id != permit.request_id {
            return Err(crate::Error::Storage {
                engine: "journal".into(),
                detail: "redo capture root request scope differs".into(),
            });
        }
        let capture = self.capture_entries(task, group.clone(), write_set)?;
        Ok((capture.origin, capture.to_bytes()?))
    }

    /// Persist source effects and their capture before irreversible settlement.
    pub(in crate::data::executor) fn persist_redo_capture(
        &mut self,
        capture: (u64, Vec<u8>),
        write_set: &[WriteSetEntry],
    ) -> crate::Result<()> {
        let write_set = write_set.to_vec();
        if let Err(error) = self.store_encoded_write_sets(std::slice::from_ref(&capture)) {
            self.fail_journalled(&error);
            return Err(error);
        }
        self.write_set_journal.running = Some(RunningJournal::Durable { write_set });
        Ok(())
    }

    /// Mark unknown durability and suppress every held event.
    pub(in crate::data::executor) fn fail_journalled(&mut self, error: &crate::Error) {
        self.seal_journalled();
        self.write_set_journal.running = Some(RunningJournal::Failed);
        self.fail_stop_core(FailStopCause::WriteSetUnpersisted, &error.to_string());
    }

    /// Fence an active journal before error cleanup can persist deferred effects.
    pub(in crate::data::executor) fn seal_journalled(&self) {
        if self.write_set_journal.held_events.is_some() {
            self.sparse.db().seal();
            self.edge_store.db().seal();
        }
    }

    fn end_journalled(&mut self, healthy: bool) {
        let healthy = healthy && !self.fail_stop.is_stopped();
        if healthy {
            self.sparse.db().resolve_journal();
            self.edge_store.db().resolve_journal();
        } else {
            self.seal_journalled();
        }
        self.sparse.db().resume();
        self.edge_store.db().resume();
        if let Some(events) = self.write_set_journal.held_events.take()
            && healthy
        {
            for event in events {
                self.send_write_event(event);
            }
        }
    }

    /// Persist ordinary writes and release events only after both stores succeed.
    pub(in crate::data::executor) fn finish_journalled(
        &mut self,
        captures: &[WriteSetCapture],
    ) -> crate::Result<()> {
        let stored = self.store_write_sets(captures);
        if let Err(error) = &stored {
            self.fail_journalled(error);
        }
        self.end_journalled(stored.is_ok());
        self.write_set_journal.running = None;
        stored
    }

    /// Finish the root journal exactly once and retain accepted target rows.
    pub(in crate::data::executor) fn finish_journalled_task(
        &mut self,
        task: &ExecutionTask,
        response: &mut Response,
    ) -> crate::Result<()> {
        let result = match self.write_set_journal.running.take() {
            Some(RunningJournal::Pending { group, .. }) => {
                match self.write_set_capture(task, group, response) {
                    Ok(capture) => self.finish_journalled(std::slice::from_ref(&capture)),
                    Err(error) => {
                        self.fail_journalled(&error);
                        self.end_journalled(false);
                        Err(error)
                    }
                }
            }
            Some(RunningJournal::Durable { write_set }) => {
                response.write_set = write_set;
                self.end_journalled(response.status == Status::Ok);
                Ok(())
            }
            Some(RunningJournal::Failed) => {
                self.end_journalled(false);
                Ok(())
            }
            None => Ok(()),
        };
        self.write_set_journal.running = None;
        result
    }

    /// Resume an empty batch before its tasks return to the queue.
    pub(in crate::data::executor) fn end_empty_journalled(&mut self) {
        self.end_journalled(true);
    }

    /// End a batch whose required capture cannot be constructed.
    pub(in crate::data::executor) fn abandon_journalled(&mut self, error: &crate::Error) {
        self.fail_journalled(error);
        self.end_journalled(false);
        self.write_set_journal.running = None;
    }

    /// The write set `response` reports for `task`, with what boot needs to
    /// journal the task's origin again.
    pub(in crate::data::executor) fn write_set_capture(
        &self,
        task: &ExecutionTask,
        group: JournalGroup,
        response: &Response,
    ) -> crate::Result<WriteSetCapture> {
        self.capture_entries(task, group, &response.write_set)
    }

    fn capture_entries(
        &self,
        task: &ExecutionTask,
        group: JournalGroup,
        entries: &[WriteSetEntry],
    ) -> crate::Result<WriteSetCapture> {
        let plan =
            zerompk::to_msgpack_vec(task.plan()).map_err(|e| crate::Error::Serialization {
                format: "msgpack".into(),
                detail: format!("write set capture plan encode: {e}"),
            })?;
        let mut schemas = Vec::new();
        let entries = entries
            .iter()
            .map(|entry| CapturedEntry::from_entry(entry, &mut schemas))
            .collect::<crate::Result<Vec<_>>>()?;
        Ok(WriteSetCapture {
            tenant_id: task.request.tenant_id.as_u64(),
            vshard_id: task.request.vshard_id.as_u32(),
            database_id: task.request.database_id.as_u64(),
            collection: group.collection,
            origin: group.origin.as_u64(),
            entries,
            schemas,
            origin_append: OriginAppend {
                plan,
                apply_key: group.apply_key,
                event_source: task.request.event_source.wal_code(),
                commit_hlc: group.commit_hlc,
                resolved_now_ms: task.request.resolved_now_ms,
                change_position: group
                    .change_position
                    .map(|position| (position.epoch, position.group_id, position.log_index)),
            },
        })
    }

    fn store_write_sets(&mut self, captures: &[WriteSetCapture]) -> crate::Result<()> {
        let encoded = captures
            .iter()
            .map(|capture| Ok((capture.origin, capture.to_bytes()?)))
            .collect::<crate::Result<Vec<_>>>()?;
        self.store_encoded_write_sets(&encoded)
    }

    fn store_encoded_write_sets(&mut self, captures: &[(u64, Vec<u8>)]) -> crate::Result<()> {
        let settled = std::mem::take(&mut self.write_set_journal.settled);
        let txn = self.sparse.begin_durable_write()?;
        self.sparse
            .remove_write_set_captures_in_txn(&txn, &settled)?;
        for (origin, bytes) in captures {
            self.sparse
                .put_write_set_capture_in_txn(&txn, *origin, bytes)?;
        }
        txn.commit().map_err(|e| crate::Error::Storage {
            engine: "sparse".into(),
            detail: format!("commit write set captures: {e}"),
        })?;
        self.edge_store.persist()
    }
}

#[cfg(test)]
#[path = "journal_redo_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "journal_close_tests.rs"]
mod close_tests;

#[cfg(test)]
#[path = "batch_capture_tests.rs"]
mod batch_capture_tests;

#[cfg(test)]
#[path = "batch_enforcement_tests.rs"]
mod batch_enforcement_tests;
