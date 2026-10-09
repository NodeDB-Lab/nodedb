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
use crate::control::cluster::calvin::scheduler::driver::core::write_version_record::VersionRecord;
use crate::control::cluster::calvin::scheduler::driver::types::CommitState;
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
    pub(in crate::control::cluster::calvin::scheduler::driver::core) async fn finish_resolved_commit(
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
        }

        let completed = if committed {
            self.commit_apply_tail(txn_id, response, redo_lsn).await
        } else {
            // A dropped txn installed nothing, so its ack reports nothing.
            // The coordinator's outcome waits for this ack, whatever the
            // drop response said.
            self.propose_sequencer_entry(
                txn_id,
                SchedulerProposal::CompletionAck { result: Vec::new() },
            );
            true
        };
        // `false` means the txn stays pending and unapplied: the commit tail
        // halted the scheduler, or its write-version record waits for capacity.
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
    /// Returns `true` when the caller completes the txn. Returns `false` when
    /// the txn stays pending, with its position unapplied:
    /// - a WAL append, its fsync, or the write-version record failed, and the
    ///   scheduler halted;
    /// - the write-version record waits for capacity. The txn then moves to
    ///   [`CommitState::AwaitingVersionRecord`] and completes once the record is sent.
    ///
    /// `redo_lsn` is `Some(lsn)` when a `TransactionRedo` record was already
    /// WAL-appended for this commit's non-empty write set (`finish_redo_resolve`)
    /// — that record already IS the durable applied marker, so only write
    /// versions are recorded at it. `None` (an empty-ops staged commit, which
    /// carries no redo record) appends a `CalvinApplied` marker here.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) async fn commit_apply_tail(
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
        // The primary slice's answer, its RETURNING rows or its affected
        // count, and the timeseries install counts also ride the replicated
        // CompletionAck, so a coordinator on a node with no replica of this
        // vShard reads them.
        let (has_primary_write, has_returning) = self
            .pending
            .get(&txn_id)
            .map(|p| (p.has_primary_write, p.has_returning))
            .unwrap_or((false, false));
        let ack_result = self.ack_result_of(
            &response,
            crate::control::state::AckSlice {
                primary_write: has_primary_write,
                returning: has_returning,
            },
        );
        if has_primary_write {
            use std::collections::hash_map::Entry;

            let response = statement_reply(response);

            use crate::control::state::CalvinApplyResult;

            let key = nodedb_cluster::calvin::TxnId::new(txn_id.epoch, txn_id.position);
            let vshard_id = self.vshard_id;
            self.shared.calvin.apply_results.deposit_with(key, |entry| match entry {
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
                            vshard = vshard_id,
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
                        // The timeseries install counts of both merge, so the
                        // COMMIT reports every participant's apply count.
                        merge_install_counts(slot.get_mut(), &response);
                    }
                }
            });
        }
        let applied_lsn = match redo_lsn {
            // The TransactionRedo record already durably marks this apply — the
            // SAME shard-local WAL-LSN space fast-path writes and read
            // watermarks use. Record the apply's per-key write versions at it;
            // no second (CalvinApplied) marker is written.
            Some(lsn) => Some((lsn, self.record_calvin_write_versions(txn_id, lsn))),
            None => match self
                .shared
                .wal
                .appender(crate::wal::manager::NO_APPLY_KEY)
                .with_commit_hlc(self.txn_commit_hlc(txn_id))
                .append_calvin_applied(
                    crate::types::VShardId::new(self.vshard_id),
                    txn_id.epoch,
                    txn_id.position,
                ) {
                // The CalvinApplied WAL LSN is the committed write-LSN for this
                // apply — the SAME shard-local WAL-LSN space fast-path writes and
                // read watermarks use. Record the apply's per-key write versions
                // at it once it exists; it does not exist yet at dispatch time.
                Ok(applied_lsn) => Some((
                    applied_lsn,
                    self.record_calvin_write_versions(txn_id, applied_lsn),
                )),
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
        let Some((lsn, version_record)) = applied_lsn else {
            // The apply cannot be acknowledged without a durable participant
            // LSN: CDC and write-version consumers will otherwise observe a
            // successful commit with no authoritative ordering point. The
            // scheduler halted above.
            return false;
        };
        if version_record == VersionRecord::Halted {
            // Read-set validation reads the lost versions. The scheduler
            // halted, and the txn stays pending and unapplied.
            return false;
        }
        // The record is whole at append: the flush reports no rows, so no
        // part follows it. A record split over the WAL record limit is
        // durable once its last continuation is.
        let durable_through = self
            .pending
            .get(&txn_id)
            .and_then(|pending| pending.redo_records.as_ref())
            .and_then(|records| records.last_lsn())
            .map_or(lsn, |last| last.max(lsn));
        // Control change-stream events are distinct from Data-Plane
        // WriteEvents. Every replica publishes the rows this participant's
        // redo installs at the transaction's sequencer position, which every
        // replica shares; the vShard's leader forwards them to the nodes
        // that do not replicate it. The publish journals them durably before
        // the applied marker's fsync below, so a restart that finds the
        // position applied also finds its changes.
        if let Some(pending) = self.pending.get_mut(&txn_id) {
            let calvin = crate::control::server::dispatch_utils::CalvinApply {
                tenant_id: pending.txn.tx_class.tenant_id,
                database_id: pending.txn.tx_class.database_id,
                vshard: self.vshard_id,
                sequencer_epoch: txn_id.epoch,
                position: txn_id.position,
                commit_hlc: self
                    .cut_floors
                    .commit_hlc(pending.txn.epoch, pending.txn.epoch_system_ms),
            };
            let change_sets = std::mem::take(&mut pending.change_sets);
            crate::control::server::dispatch_utils::publish_calvin_change_sets(
                &self.shared,
                calvin,
                change_sets,
                lsn,
            );
        }
        // The record at `lsn` is this position's only applied marker. An
        // append only buffers it, so it is durable before the mark and the
        // ack: a restart that lost it will take the position for unapplied,
        // run the transaction again, and never settle its ack. The wait joins
        // the WAL group commit, so concurrent Calvin commits share one fsync.
        if let Err(e) = self.shared.wal.wait_durable(durable_through).await {
            self.halt_apply(
                txn_id,
                HaltReason::WalAppendFailed,
                HaltStep::AppliedMarker,
                format!("applied marker fsync at lsn {} failed: {e}", lsn.as_u64()),
            );
            return false;
        }
        if let Some(origin) = redo_lsn {
            self.note_flush_settled(origin);
            self.record_applied_key(txn_id);
        }
        // The commit's mark lands before the ack, as a write through the
        // funnel records its mark before its response returns.
        self.record_calvin_write_mark(txn_id);
        self.propose_sequencer_entry(
            txn_id,
            SchedulerProposal::CompletionAck { result: ack_result },
        );
        if version_record == VersionRecord::Parked {
            // The txn keeps its locks until the record is sent, so no later
            // txn validates a read before the versions land.
            if let Some(pending) = self.pending.get_mut(&txn_id) {
                pending.commit_state = CommitState::AwaitingVersionRecord;
            }
            return false;
        }
        true
    }
}

impl Scheduler {
    /// Record the dedup key this participant's redo record carries, once the
    /// record is durable and installed. Every replica records it, as a
    /// Raft-applied redo's key is, so a request or trigger body sent again to
    /// any replica applies once.
    fn record_applied_key(&self, txn_id: TxnId) {
        let Some(pending) = self.pending.get(&txn_id) else {
            return;
        };
        let tx_class = &pending.txn.tx_class;
        if tx_class.applied_key.is_empty()
            || tx_class.applied_key_home().ok().flatten() != Some(self.vshard_id)
        {
            return;
        }
        let Some(dedup) = self.shared.cross_shard_dedup.get() else {
            return;
        };
        match zerompk::from_msgpack::<crate::wal::CrossShardAppliedKey>(&tx_class.applied_key) {
            Ok(key) => {
                if let Err(error) = dedup.record_applied(&key) {
                    tracing::error!(
                        vshard_id = self.vshard_id,
                        origin = %key.origin,
                        %error,
                        "calvin: dedup key held in memory only; the WAL restores it on restart"
                    );
                }
            }
            Err(error) => tracing::error!(
                vshard_id = self.vshard_id,
                %error,
                "calvin: the applied key does not decode; it is not recorded"
            ),
        }
    }

    /// The report the flush `response` owes the coordinator, encoded for the
    /// replicated `CompletionAck`: the apply's timeseries install counts, and
    /// the rows of a `returning` slice. The rows are bounded by the query
    /// result limit and by the ack's frame budget, whichever is lower.
    fn ack_result_of(
        &self,
        response: &Response,
        slice: crate::control::state::AckSlice,
    ) -> Vec<u8> {
        let limit = self
            .shared
            .tuning
            .network
            .max_query_result_bytes
            .min(crate::control::state::ACK_ROWS_FRAME_BUDGET);
        crate::control::state::CalvinAckResult::of(response, slice, limit)
            .to_bytes()
            .unwrap_or_else(|error| {
                tracing::warn!(
                    vshard_id = self.vshard_id,
                    %error,
                    "calvin: the completion ack carries no report"
                );
                Vec::new()
            })
    }
}

/// The response the statement drains. A flush whose install succeeded but
/// whose reply failed to render answers `Ok` with the render error in
/// `error_code`. The statement reports that error.
/// Fold the timeseries install counts `incoming` answers into the plain
/// result `held`. A RETURNING result keeps its rows untouched.
fn merge_install_counts(held: &mut crate::control::state::CalvinApplyResult, incoming: &Response) {
    let crate::control::state::CalvinApplyResult::Single {
        response,
        has_returning: false,
    } = held
    else {
        return;
    };
    if let Some(merged) = crate::engine::timeseries::install_counts::merge_count_payloads(
        response.payload.as_bytes(),
        incoming.payload.as_bytes(),
    ) {
        response.payload = crate::bridge::envelope::Payload::from_vec(merged);
    }
}

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
        error_response, scheduler_with_pending, staged_response,
    };
    use crate::control::cluster::calvin::scheduler::driver::types::CommitState;

    fn internal_error() -> Response {
        error_response(ErrorCode::Internal {
            detail: "core failed".to_string(),
        })
    }

    fn flushed_with_counts(collection: &str, accepted: u64, rejected: u64) -> Response {
        use crate::engine::timeseries::install_counts::{TsInstallCount, TsInstallCounts};
        let mut response = staged_response(Status::Ok, None);
        response.payload = crate::bridge::envelope::Payload::from_vec(
            TsInstallCounts::new(vec![TsInstallCount {
                collection: collection.into(),
                accepted,
                rejected,
            }])
            .to_bytes()
            .expect("encode install counts"),
        );
        response
    }

    /// Two plain participants of one Calvin transaction each installed a
    /// timeseries batch. The coordinator's result carries both installs'
    /// apply counts, so COMMIT reports every participant's rejected rows. A
    /// RETURNING result keeps its rows.
    #[test]
    fn plain_participants_merge_their_install_counts() {
        use crate::control::state::CalvinApplyResult;
        use crate::engine::timeseries::install_counts::TsInstallCounts;
        let mut held = CalvinApplyResult::Single {
            response: flushed_with_counts("cpu", 2, 1),
            has_returning: false,
        };
        merge_install_counts(&mut held, &flushed_with_counts("mem", 3, 2));
        let CalvinApplyResult::Single { response, .. } = &held else {
            panic!("a merged result stays single");
        };
        let totals = TsInstallCounts::from_payload(response.payload.as_bytes())
            .expect("install counts")
            .by_collection();
        assert_eq!(totals.get("cpu"), Some(&(2, 1)));
        assert_eq!(totals.get("mem"), Some(&(3, 2)));

        let rows = crate::bridge::envelope::Payload::from_vec(vec![0x90]);
        let mut returning = CalvinApplyResult::Single {
            response: Response {
                payload: rows,
                ..flushed_with_counts("cpu", 1, 0)
            },
            has_returning: true,
        };
        merge_install_counts(&mut returning, &flushed_with_counts("mem", 1, 1));
        let CalvinApplyResult::Single { response, .. } = &returning else {
            panic!("a RETURNING result stays single");
        };
        assert_eq!(response.payload.as_bytes(), &[0x90]);
    }

    /// A flush that returns an error under a COMMIT verdict holds the txn
    /// unapplied and halts: a second flush will apply nothing.
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

        scheduler
            .finish_resolved_commit(txn_id, internal_error(), true, None)
            .await;

        assert!(!scheduler.applied.is_applied(9, 2));
        assert!(scheduler.pending.contains_key(&txn_id));
        assert_eq!(
            scheduler.apply_halt().map(|h| h.reason),
            Some(HaltReason::FlushFailed)
        );
        assert!(scheduler.shared.sequencer_halt.apply_halt().is_halted());
    }

    /// A drop that returns an error under an abort verdict still completes
    /// the txn and owes its ack: no replica writes an aborted txn.
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

        scheduler
            .finish_resolved_commit(txn_id, internal_error(), false, None)
            .await;

        assert!(scheduler.applied.is_applied(9, 2));
        assert!(!scheduler.pending.contains_key(&txn_id));
        assert!(!scheduler.is_apply_halted());
        assert!(scheduler.owed.contains_key(&(
            txn_id,
            crate::control::cluster::calvin::scheduler::driver::core::owed::OwedKind::CompletionAck
        )));
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
        pending.commit_state = CommitState::AwaitingResolve {
            committed: true,
            redo_lsn: None,
        };
        pending.flush_scope.redo = vec![7, 7, 7];
        pending.flush_scope.sends = 1;
        scheduler.pending.insert(txn_id, pending);

        scheduler
            .finish_resolved_commit(txn_id, retryable_refusal(), true, None)
            .await;

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

        scheduler
            .finish_resolved_commit(txn_id, retryable_refusal(), true, None)
            .await;

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
