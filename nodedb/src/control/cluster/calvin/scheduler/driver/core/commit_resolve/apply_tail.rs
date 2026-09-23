// SPDX-License-Identifier: BUSL-1.1

//! Commit tail: runs once a flush/drop response has returned, depositing the
//! applied result, marking the apply durable, recording write versions, and
//! proposing the `CompletionAck`.

use crate::bridge::envelope::{Response, Status};
use crate::control::cluster::calvin::scheduler::driver::core::halt::{
    HaltReason, HaltStep, error_response_text,
};
use crate::control::cluster::calvin::scheduler::driver::core::owed::SchedulerProposal;
use crate::control::cluster::calvin::scheduler::driver::core::scheduler::Scheduler;
use crate::control::cluster::calvin::scheduler::lock_manager::TxnId;
use crate::control::cluster::calvin::scheduler::metrics::infra_abort_reason;

impl Scheduler {
    /// Run the commit tail once a flush/drop response has returned.
    ///
    /// On a successful flush the full commit tail runs (deposit applied result,
    /// `CalvinApplied` WAL + write-version recording, `CompletionAck`). On a
    /// successful drop only the `CompletionAck` is proposed — the coordinator's
    /// completion waiter still fires and the epoch advances, but nothing was
    /// written so there is no result to deposit, no apply LSN, and no versions
    /// to record.
    ///
    /// A non-`Ok` flush halts the scheduler. The flush handler removes the
    /// staged buffer before it applies, so a second flush applies nothing, and
    /// a skipped flush tears the committed txn on this replica. A non-`Ok` drop
    /// completes the txn: under an abort verdict no replica writes anything.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn finish_resolved_commit(
        &mut self,
        txn_id: TxnId,
        response: Response,
        committed: bool,
        redo_lsn: Option<crate::types::Lsn>,
    ) {
        if response.status != Status::Ok {
            if committed {
                self.halt_apply(
                    txn_id,
                    HaltReason::FlushFailed,
                    HaltStep::Flush,
                    error_response_text("CalvinFlush", &response),
                );
                return;
            }
            tracing::error!(
                vshard_id = self.vshard_id,
                epoch = txn_id.epoch,
                position = txn_id.position,
                "calvin: drop response was not Ok under an abort verdict; completing the \
                 aborted txn, since no replica writes it"
            );
            self.metrics.record_executor_error();
            self.metrics
                .record_infra_abort(infra_abort_reason::IO_ERROR);
            self.metrics.record_completed();
            self.on_txn_complete(txn_id);
            return;
        }

        let completed = if committed {
            self.commit_apply_tail(txn_id, response, redo_lsn)
        } else {
            self.propose_sequencer_entry(txn_id, SchedulerProposal::CompletionAck);
            true
        };
        // `false` means the commit tail halted the scheduler: the txn stays
        // pending and unapplied.
        if completed {
            self.metrics.record_completed();
            self.on_txn_complete(txn_id);
        }
    }

    /// Deposit the applied result, durably mark the apply, record the apply's
    /// write versions, and propose the `CompletionAck`.
    ///
    /// Shared by the flush-completion path and the direct-apply (dependent /
    /// active) apply path.
    ///
    /// Returns `false` once a failed `CalvinApplied` WAL append halted the
    /// scheduler: the position must not be marked applied without its marker,
    /// so the caller leaves the txn pending.
    ///
    /// `redo_lsn` is `Some(lsn)` when a `TransactionRedo` record was already
    /// WAL-appended for this commit's non-empty write set (`finish_redo_resolve`)
    /// — that record already IS the durable applied marker, so only write
    /// versions are recorded at it. `None` (a drop, an empty-ops staged commit,
    /// or the direct-apply dependent/active path, which carries no redo record)
    /// falls back to appending a `CalvinApplied` marker here, exactly as before
    /// this record existed.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn commit_apply_tail(
        &mut self,
        txn_id: TxnId,
        response: Response,
        redo_lsn: Option<crate::types::Lsn>,
    ) -> bool {
        // Deposit the FULL applied Response (affected-count + watermark + any
        // RETURNING rows) into the local sidecar BEFORE proposing the replicated
        // CompletionAck. The ack fires the coordinator's completion oneshot on
        // every sequencer member, so depositing first guarantees the result is
        // present by the time the coordinator drains it — no lost result, no
        // race.
        //
        // Gated on the PRIMARY-WRITE participant: any participant whose slice
        // carries the user's non-edge DML (Document/KV/Vector/etc.), as opposed
        // to the implicit graph-edge cleanup that dual-homes alongside it. A
        // multi-collection cross-shard COMMIT has MANY primary-write
        // participants — each a plain affected-count write — and they coalesce:
        // the first applied response stands for the coordinator (which discards
        // it for a COMMIT tag anyway), and the plain-write siblings do not
        // conflict. Only a genuine cross-shard RETURNING union — two
        // participants each carrying RETURNING rows — records `Conflict`.
        // Results travel via this in-process sidecar only — never the sequencer
        // Raft log.
        let (has_primary_write, has_returning) = self
            .pending
            .get(&txn_id)
            .map(|p| (p.has_primary_write, p.has_returning))
            .unwrap_or((false, false));
        if has_primary_write {
            use std::collections::hash_map::Entry;

            use crate::control::state::CalvinApplyResult;

            let key = nodedb_cluster::calvin::TxnId::new(txn_id.epoch, txn_id.position);
            let mut results = self
                .shared
                .calvin_apply_results
                .lock()
                .unwrap_or_else(|p| p.into_inner());
            match results.entry(key) {
                Entry::Vacant(slot) => {
                    slot.insert(CalvinApplyResult::Single {
                        response,
                        has_returning,
                    });
                }
                Entry::Occupied(mut slot) => {
                    // Derive both facts from the existing entry BEFORE any
                    // insert, so the immutable borrow does not outlive the
                    // mutable one.
                    let existing_returning = matches!(
                        slot.get(),
                        CalvinApplyResult::Single {
                            has_returning: true,
                            ..
                        }
                    );
                    let already_conflict = matches!(slot.get(), CalvinApplyResult::Conflict);

                    if already_conflict {
                        // A RETURNING union was already recorded; stays Conflict.
                    } else if has_returning && existing_returning {
                        // Two RETURNING-bearing participants for one Calvin txn:
                        // a cross-shard RETURNING union, which is unsupported.
                        // Record Conflict so the coordinator fails the statement
                        // loudly rather than returning one shard's partial rows.
                        tracing::error!(
                            epoch = txn_id.epoch,
                            position = txn_id.position,
                            vshard = self.vshard_id,
                            "two RETURNING-bearing participants for one Calvin txn — cross-shard \
                             RETURNING union unsupported"
                        );
                        slot.insert(CalvinApplyResult::Conflict);
                    } else if has_returning {
                        // The incoming participant carries the rows; the existing
                        // entry was a plain affected-count sibling. Rows win.
                        slot.insert(CalvinApplyResult::Single {
                            response,
                            has_returning: true,
                        });
                    } else {
                        // Incoming is a plain write; keep the existing entry — a
                        // multi-collection cross-shard COMMIT coalesces (the
                        // coordinator discards it for a COMMIT tag anyway).
                    }
                }
            }
        }
        let applied_lsn = match redo_lsn {
            // The TransactionRedo record already durably marks this apply — the
            // SAME shard-local WAL-LSN space fast-path writes and read
            // watermarks use. Record the apply's per-key write versions at it;
            // no second (CalvinApplied) marker is written.
            Some(lsn) => {
                self.record_calvin_write_versions(txn_id, lsn);
                Some(lsn)
            }
            None => match self.shared.wal.append_calvin_applied(
                crate::types::VShardId::new(self.vshard_id),
                txn_id.epoch,
                txn_id.position,
            ) {
                // The CalvinApplied WAL LSN is the committed write-LSN for this
                // apply — the SAME shard-local WAL-LSN space fast-path writes and
                // read watermarks use. Record the apply's per-key write versions
                // at it once it exists; it does not exist yet at dispatch time.
                Ok(applied_lsn) => {
                    self.record_calvin_write_versions(txn_id, applied_lsn);
                    Some(applied_lsn)
                }
                Err(e) => {
                    self.halt_apply(
                        txn_id,
                        HaltReason::WalAppendFailed,
                        HaltStep::AppliedMarker,
                        format!("CalvinApplied WAL append failed: {e}"),
                    );
                    None
                }
            },
        };
        let Some(lsn) = applied_lsn else {
            // The apply cannot be acknowledged without a durable participant
            // LSN: CDC and write-version consumers would otherwise observe a
            // successful commit with no authoritative ordering point. The
            // scheduler halted above.
            return false;
        };
        // Control change-stream events are distinct from Data-Plane
        // WriteEvents. Publish the participant-local logical manifests once,
        // from the data-group leader, at the authoritative committed LSN.
        if self.is_group_leader()
            && let Some(pending) = self.pending.get_mut(&txn_id)
        {
            let tenant_id = pending.txn.tx_class.tenant_id;
            let database_id = pending.txn.tx_class.database_id;
            for change_set in std::mem::take(&mut pending.change_sets) {
                crate::control::server::dispatch_utils::publish_change_set_with_lsn(
                    &self.shared,
                    tenant_id,
                    database_id,
                    change_set,
                    lsn,
                );
            }
        }
        self.propose_sequencer_entry(txn_id, SchedulerProposal::CompletionAck);
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bridge::envelope::ErrorCode;
    use crate::control::cluster::calvin::scheduler::driver::core::test_support::{
        error_response, scheduler_with_pending,
    };
    use crate::control::cluster::calvin::scheduler::driver::types::CommitState;

    fn internal_error() -> Response {
        error_response(ErrorCode::Internal {
            detail: "core failed".to_string(),
        })
    }

    /// A flush that returns an error under a COMMIT verdict holds the txn
    /// unapplied and halts: a second flush would apply nothing.
    #[tokio::test]
    async fn flush_error_response_holds_committed_txn_unapplied() {
        let txn_id = TxnId::new(9, 2);
        let (mut scheduler, _dir) = scheduler_with_pending(
            txn_id,
            CommitState::AwaitingResolve {
                committed: true,
                redo_lsn: None,
            },
        );

        scheduler.finish_resolved_commit(txn_id, internal_error(), true, None);

        assert!(!scheduler.applied.is_applied(9, 2));
        assert!(scheduler.pending.contains_key(&txn_id));
        assert_eq!(
            scheduler.apply_halt().map(|h| h.reason),
            Some(HaltReason::FlushFailed)
        );
        assert!(scheduler.shared.sequencer_halt.apply_halt().is_halted());
    }

    /// A drop that returns an error under an abort verdict still completes
    /// the txn: no replica writes an aborted txn.
    #[tokio::test]
    async fn drop_error_response_under_abort_completes_txn() {
        let txn_id = TxnId::new(9, 2);
        let (mut scheduler, _dir) = scheduler_with_pending(
            txn_id,
            CommitState::AwaitingResolve {
                committed: false,
                redo_lsn: None,
            },
        );

        scheduler.finish_resolved_commit(txn_id, internal_error(), false, None);

        assert!(scheduler.applied.is_applied(9, 2));
        assert!(!scheduler.pending.contains_key(&txn_id));
        assert!(!scheduler.is_apply_halted());
    }
}
