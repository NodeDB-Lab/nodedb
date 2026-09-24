// SPDX-License-Identifier: BUSL-1.1

//! Commit tail: runs once a flush/drop response has returned, depositing the
//! applied result, marking the apply durable, recording write versions, and
//! proposing the `CompletionAck`.

use crate::bridge::envelope::{ErrorCode, Response, Status};
use crate::control::cluster::calvin::scheduler::driver::core::commit_resolution_dispatch::CommitResolution;
use crate::control::cluster::calvin::scheduler::driver::core::deferred::{
    DispatchOutcome, DispatchStep,
};
use crate::control::cluster::calvin::scheduler::driver::core::halt::{
    HaltReason, HaltStep, error_response_text,
};
use crate::control::cluster::calvin::scheduler::driver::core::owed::SchedulerProposal;
use crate::control::cluster::calvin::scheduler::driver::core::scheduler::Scheduler;
use crate::control::cluster::calvin::scheduler::lock_manager::TxnId;
use crate::control::cluster::calvin::scheduler::metrics::infra_abort_reason;

/// The most flushes one committed txn sends. Each refused install rolled
/// every write back, so a resend is safe. A refusal that outlasts the bound
/// halts the scheduler.
pub(in crate::control::cluster::calvin::scheduler::driver::core) const MAX_FLUSH_SENDS: u32 = 8;

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
    /// A flush refused with `RetryableRefusal` rolled its install back and
    /// kept the staged buffer, so the scheduler sends the same flush again, up
    /// to [`MAX_FLUSH_SENDS`] sends. Any other non-`Ok` flush, or a refusal
    /// past the bound, halts the scheduler: a skipped flush tears the
    /// committed txn on this replica. A non-`Ok` drop completes the txn:
    /// under an abort verdict no replica writes anything.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn finish_resolved_commit(
        &mut self,
        txn_id: TxnId,
        response: Response,
        committed: bool,
        redo_lsn: Option<crate::types::Lsn>,
    ) {
        if response.status != Status::Ok {
            if committed && self.resend_refused_flush(txn_id, &response, redo_lsn) {
                return;
            }
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

    /// Send the flush of `txn_id` again when the install refused it as
    /// retryable and the send bound allows another. Returns whether the
    /// refusal is handled: the flush went out again, or its dispatch failed
    /// and the step's terminal handling ran.
    fn resend_refused_flush(
        &mut self,
        txn_id: TxnId,
        response: &Response,
        redo_lsn: Option<crate::types::Lsn>,
    ) -> bool {
        if !matches!(
            response.error_code.as_deref(),
            Some(ErrorCode::RetryableRefusal { .. })
        ) {
            return false;
        }
        let sends = self
            .pending
            .get(&txn_id)
            .map_or(MAX_FLUSH_SENDS, |pending| pending.flush_scope.sends);
        if sends >= MAX_FLUSH_SENDS {
            return false;
        }
        tracing::warn!(
            vshard_id = self.vshard_id,
            epoch = txn_id.epoch,
            position = txn_id.position,
            sends,
            "calvin: the flush install was refused as retryable; sending it again"
        );
        if let DispatchOutcome::Failed(error) =
            self.dispatch_commit_resolution(txn_id, CommitResolution::Flush { redo_lsn })
        {
            self.fail_dispatch_step(txn_id, DispatchStep::Flush, error);
        }
        true
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
        // The install folded materialized sums into target rows no redo
        // sub-record names. The redo record's stamp carries the sum targets,
        // so restart replay folds at the same LSN.
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

            let response = statement_reply(response);

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
            None => match self
                .shared
                .wal
                .appender(crate::wal::manager::NO_APPLY_KEY)
                .append_calvin_applied(
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

/// The response the statement drains. A flush whose install succeeded but
/// whose reply failed to render answers `Ok` with the render error in
/// `error_code`. The statement reports that error.
fn statement_reply(mut response: Response) -> Response {
    if response.status == Status::Ok && response.error_code.is_some() {
        response.status = Status::Error;
        response.payload = crate::bridge::envelope::Payload::empty();
    }
    response
}

#[cfg(test)]
mod tests {
    use super::*;
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
    fn retryable_refusal() -> Response {
        error_response(ErrorCode::RetryableRefusal {
            reason: "install rolled back".to_string(),
        })
    }

    /// A flush refused as retryable reaches the Data Plane again with the
    /// same redo record, and the txn stays pending and unhalted.
    #[tokio::test]
    async fn retryable_flush_refusal_resends_the_same_flush() {
        use crate::control::cluster::calvin::scheduler::driver::core::test_support::{
            await_data_plane_request, build_test_scheduler_with_data_side, make_sequenced_txn,
            staged_pending,
        };
        use nodedb_physical::physical_plan::PhysicalPlan;
        use nodedb_physical::physical_plan::meta::MetaOp;

        let txn_id = TxnId::new(9, 2);
        let registry = nodedb_cluster::calvin::CalvinCompletionRegistry::new_detached();
        let (mut scheduler, _dir, mut data_side) = build_test_scheduler_with_data_side(7, registry);
        let mut pending = staged_pending(make_sequenced_txn(9, 2), txn_id);
        pending.commit_state = Some(CommitState::AwaitingResolve {
            committed: true,
            redo_lsn: None,
        });
        pending.flush_scope.redo = vec![7, 7, 7];
        pending.flush_scope.sends = 1;
        scheduler.pending.insert(txn_id, pending);

        scheduler.finish_resolved_commit(txn_id, retryable_refusal(), true, None);

        assert!(!scheduler.is_apply_halted());
        assert!(!scheduler.applied.is_applied(9, 2));
        assert_eq!(
            scheduler.pending.get(&txn_id).map(|p| p.flush_scope.sends),
            Some(2)
        );
        assert!(
            await_data_plane_request(&mut data_side, |plan| matches!(
                plan,
                PhysicalPlan::Meta(MetaOp::CalvinFlush { epoch: 9, position: 2, redo, .. })
                    if redo == &vec![7, 7, 7]
            ))
            .await,
            "the resent flush carries the same redo record"
        );
    }

    /// A retryable refusal past the send bound halts like any flush error.
    #[tokio::test]
    async fn retryable_flush_refusal_past_the_bound_halts() {
        let txn_id = TxnId::new(9, 2);
        let (mut scheduler, _dir) = scheduler_with_pending(
            txn_id,
            CommitState::AwaitingResolve {
                committed: true,
                redo_lsn: None,
            },
        );
        if let Some(pending) = scheduler.pending.get_mut(&txn_id) {
            pending.flush_scope.sends = MAX_FLUSH_SENDS;
        }

        scheduler.finish_resolved_commit(txn_id, retryable_refusal(), true, None);

        assert!(scheduler.pending.contains_key(&txn_id));
        assert_eq!(
            scheduler.apply_halt().map(|h| h.reason),
            Some(HaltReason::FlushFailed)
        );
    }

    /// A render error on an installed flush reaches the statement as a typed
    /// error, and the txn completes.
    #[test]
    fn a_render_error_on_an_installed_flush_becomes_the_statement_error() {
        let mut response = error_response(ErrorCode::Internal {
            detail: "render".to_string(),
        });
        response.status = Status::Ok;

        let reply = statement_reply(response);

        assert_eq!(reply.status, Status::Error);
        assert!(matches!(
            reply.error_code.as_deref(),
            Some(ErrorCode::Internal { .. })
        ));
    }
}
