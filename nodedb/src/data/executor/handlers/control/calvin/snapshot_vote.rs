// SPDX-License-Identifier: BUSL-1.1

//! The serialization vote of a staged Calvin slice.
//!
//! A transaction with a snapshot commits only while no read it made and no
//! row it writes holds a committed version above the snapshot. The stage
//! checks this participant's slice against its stored versions.
//!
//! The vote is atomic with the install order. The scheduler releases a
//! transaction's locks only once its redo installed on the data-group
//! leader (`complete_applied`). A row write takes its row `Exclusive` and its
//! collection `IntentExclusive`, and a predicate read takes the collection
//! `Shared`. So a stage at position `p` runs only after every earlier
//! position that conflicts with it installed on the leader, and the vote
//! sees each such install.

use nodedb_types::calvin::VersionedReadEntry;

use crate::bridge::envelope::{ErrorCode, PhysicalPlan, StageVote};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::core_loop::txn_validate::plan_writes;
use crate::data::executor::task::ExecutionTask;

/// A Calvin slice's read set and the snapshot ordinal its reads observed.
pub(in crate::data::executor) struct CalvinSnapshot<'a> {
    pub reads: &'a [VersionedReadEntry],
    /// `None` for a transaction with no snapshot: its stage validates
    /// nothing.
    pub ordinal: Option<i64>,
}

#[cfg(test)]
impl CalvinSnapshot<'static> {
    /// A transaction with no snapshot and no read set.
    pub(in crate::data::executor) const NONE: Self = Self {
        reads: &[],
        ordinal: None,
    };
}

impl CoreLoop {
    /// The vote of a slice that staged `plans`: `Commit` when the
    /// transaction has no snapshot or nothing moved past it, else
    /// `SerializationConflict`. `txn_ordinal` is the transaction's own
    /// ordinal, from its epoch instant and position. `Err` when a store does
    /// not read: the stage fails and votes `ParticipantError`.
    pub(super) fn calvin_snapshot_vote(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        snapshot: &CalvinSnapshot<'_>,
        plans: &[PhysicalPlan],
        txn_ordinal: i64,
    ) -> Result<StageVote, ErrorCode> {
        let Some(ordinal) = snapshot.ordinal else {
            return Ok(StageVote::Commit);
        };
        // A snapshot this far behind the transaction's own ordinal can
        // predate a history purge on some replica. Every replica refuses it.
        if !self.snapshot_within_window(ordinal, Some(txn_ordinal)) {
            return Ok(StageVote::of_read_set(false));
        }
        let unreadable = |error: crate::Error| ErrorCode::Internal {
            detail: format!(
                "the serialization vote of a Calvin stage could not read a store: {error}"
            ),
        };
        let current = self
            .txn_reads_unchanged(task, tid, snapshot.reads, ordinal)
            .map_err(unreadable)?
            && self
                .txn_writes_unchanged(task, tid, &plan_writes(plans), ordinal)
                .map_err(unreadable)?;
        Ok(StageVote::of_read_set(current))
    }
}
