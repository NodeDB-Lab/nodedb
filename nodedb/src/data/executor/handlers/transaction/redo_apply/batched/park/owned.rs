// SPDX-License-Identifier: BUSL-1.1

//! The inputs of a parking stream install, owned so they outlive the run
//! of the task that started it.

use nodedb_physical::physical_plan::RedoOrigin;

use crate::data::executor::core_loop::write_set_journal::RootJournalPermit;
use crate::data::executor::task::ExecutionTask;
use crate::types::Lsn;
use crate::wal::{RedoRecord, RedoStreamRef};

use super::super::super::stream_validate::{
    StreamLimits, StreamValidateInput, string_bytes, sum_targets_bytes,
};
use super::super::install::driver::StreamInstallInput;

/// What a stream marker's validate and install read, beside the task.
pub(super) struct OwnedStream {
    tid: u64,
    marker: RedoRecord,
    reference: RedoStreamRef,
    origin: RedoOrigin,
    snapshot: Option<i64>,
    judged_at_ms: i64,
    lsn: Lsn,
    limits: StreamLimits,
    /// The root journal permit. `None` once the install took it, or when
    /// the task holds none.
    permit: Option<RootJournalPermit>,
    collections: Vec<String>,
}

impl OwnedStream {
    /// Own what `validate` and `install` borrow. Both name one marker.
    pub(super) fn of(validate: &StreamValidateInput<'_>, install: StreamInstallInput<'_>) -> Self {
        Self {
            tid: validate.tid,
            marker: validate.marker.clone(),
            reference: validate.reference.clone(),
            origin: validate.origin,
            snapshot: validate.snapshot,
            judged_at_ms: validate.judged_at_ms,
            lsn: validate.lsn,
            limits: validate.limits,
            permit: install.permit,
            collections: install.collections.to_vec(),
        }
    }

    /// The marker's LSN.
    pub(super) fn lsn(&self) -> Lsn {
        self.lsn
    }

    /// Whether the marker declares its keys. Keys that passed the final
    /// entry's room are not declared.
    pub(super) fn declares_keys(&self) -> bool {
        self.reference.keys.declared().is_some()
    }

    /// An estimate of the RAM the inputs hold: the marker's ops, row
    /// changes, roles and sum targets, the reference's chunks and keys, and
    /// the collection names. A marker's content holds the rest of its
    /// record.
    pub(super) fn held_bytes(&self) -> usize {
        let marker = &self.marker;
        let ops: usize = marker
            .ops
            .iter()
            .map(|op| std::mem::size_of_val(op) + op.payload.len())
            .sum();
        let changes: usize = marker
            .row_changes
            .iter()
            .map(|change| {
                std::mem::size_of_val(change) + change.collection.len() + change.row.len()
            })
            .sum();
        let roles: usize = marker
            .user_roles
            .iter()
            .map(|role| string_bytes(role))
            .sum();
        let collections: usize = self.collections.iter().map(|name| string_bytes(name)).sum();
        let reference = std::mem::size_of_val(self.reference.chunks.as_slice())
            + self.reference.keys.heap_bytes();
        std::mem::size_of::<Self>()
            + reference
            + ops
            + changes
            + roles
            + sum_targets_bytes(&marker.sum_targets)
            + collections
    }

    /// The validate's input for `task`, the task that started the install.
    pub(super) fn validate_input<'a>(&'a self, task: &'a ExecutionTask) -> StreamValidateInput<'a> {
        StreamValidateInput {
            task,
            tid: self.tid,
            marker: &self.marker,
            reference: &self.reference,
            origin: self.origin,
            snapshot: self.snapshot,
            judged_at_ms: self.judged_at_ms,
            lsn: self.lsn,
            limits: self.limits,
        }
    }

    /// The install's input for `task`. It takes the permit: the install
    /// opens once.
    pub(super) fn install_input<'a>(
        &'a mut self,
        task: &'a ExecutionTask,
    ) -> StreamInstallInput<'a> {
        let permit = self.permit.take();
        let this: &'a Self = self;
        StreamInstallInput {
            task,
            tid: this.tid,
            marker: &this.marker,
            reference: &this.reference,
            origin: this.origin,
            lsn: this.lsn,
            limits: this.limits,
            permit,
            collections: &this.collections,
        }
    }
}
