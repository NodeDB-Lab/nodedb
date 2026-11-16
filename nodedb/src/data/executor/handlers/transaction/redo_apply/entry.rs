// SPDX-License-Identifier: BUSL-1.1

//! `MetaOp::ApplyTransactionRedo`: install one committed transaction's redo
//! record on the core that owns its vShard.
//!
//! The record is applied through the SAME per-engine replay arms WAL replay
//! uses (`CoreLoop::replay_engines_in_lsn_order`), handed a one-record slice at
//! the LSN the write funnel minted for it on this node. What restart replay
//! reproduces from the WAL is therefore exactly what the live apply installed.
//!
//! The open apply scope switches the arms to their committed-redo policy
//! (`replay_policy`): no restart watermark skips a sub-record, and every
//! sub-record an arm cannot apply fails this response instead of being logged
//! and skipped or stopping the process.
//!
//! The record applies all or nothing (see `passes`): a validate pass checks
//! every sub-record before any arm writes, and the install pass rolls every
//! write back when one fails.
//!
//! Around the arms the apply adds what a live commit owes and restart replay
//! does not:
//!
//! 1. commit-boundary checks and the validate pass, before any write (see
//!    `validate` and `passes`);
//! 2. materialized-sum folds and hash-chain links on document rows, through
//!    the open [`RedoApplyScope`](super::state::RedoApplyScope);
//! 3. a write note for every collection written;
//! 4. the record's events, sent once the post-install work succeeded, with
//!    one publish event per message its transaction published (see
//!    `events`);
//! 5. the fold target rows in `Response::write_set`, so the funnel
//!    journals them.

use nodedb_physical::physical_plan::{RedoOrigin, RedoSumTargets};
use nodedb_types::calvin::VersionedReadEntry;

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::core_loop::write_set_journal::RootJournalPermit;
use crate::data::executor::enforcement::write_hook::target_write_set;
use crate::data::executor::task::ExecutionTask;
use crate::types::TenantId;
use crate::wal::{RedoRecord, RedoStreamRef};

use super::clone_copy::clone_probe_unreadable;
use super::passes::{RedoTarget, final_refusal, redo_wal_record};
use super::state::TsInstalled;
use super::stream_validate::{StreamLimits, StreamValidateInput};
use super::streamed::STREAM_BATCH_BYTES;
use super::sub_ops::{document_ops, kv_ops, label_ops};
use super::validate::CommitBoundary;
use crate::engine::timeseries::install_counts::TsInstallCounts;

/// The `MetaOp::ApplyTransactionRedo` fields the handler reads.
pub(in crate::data::executor) struct CommittedRedo<'a> {
    pub redo: &'a [u8],
    pub collections: &'a [String],
    pub sum_targets: &'a [RedoSumTargets],
}

/// The read set of a single-shard snapshot commit and the snapshot ordinal
/// every read of it observed.
#[derive(Clone, Copy)]
pub(in crate::data::executor) struct RedoSnapshot<'a> {
    pub reads: &'a [VersionedReadEntry],
    pub ordinal: i64,
}

#[derive(Default)]
struct RedoInstallBoundary<'a> {
    snapshot: Option<RedoSnapshot<'a>>,
    permit: Option<RootJournalPermit>,
    /// The instant the commit boundary judges retention at, in ms since the
    /// Unix epoch. A Calvin slice names its epoch instant, which its stage
    /// judged at. `None` judges at the request's system-time ordinal.
    judged_at_ms: Option<i64>,
}

/// What a committed Calvin slice's install carries to its commit boundary.
pub(super) struct CalvinBoundary {
    pub permit: Option<RootJournalPermit>,
    /// The slice's epoch instant, which its stage judged retention at.
    pub epoch_system_ms: i64,
}

impl CoreLoop {
    /// Apply one committed redo record. The request carries the LSN of the
    /// `TransactionRedo` WAL record the funnel appended for it.
    #[cfg(test)]
    pub(in crate::data::executor) fn execute_apply_transaction_redo(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        committed: CommittedRedo<'_>,
    ) -> Response {
        self.install_redo(task, tid, committed, RedoOrigin::Commit)
    }

    /// [`Self::install_redo_authorized`] for a committed record from `origin` that also
    /// hands back what each resolved timeseries batch of the record stored,
    /// in `ts_installs`.
    pub(super) fn install_committed_redo_into(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        committed: CommittedRedo<'_>,
        origin: RedoOrigin,
        ts_installs: &mut Vec<TsInstalled>,
        calvin: CalvinBoundary,
    ) -> Response {
        // A Calvin slice's stage vote validated its snapshot, and its stage
        // judged its commit boundary at the epoch instant.
        self.install_redo_into(
            task,
            tid,
            committed,
            origin,
            ts_installs,
            RedoInstallBoundary {
                snapshot: None,
                permit: calvin.permit,
                judged_at_ms: Some(calvin.epoch_system_ms),
            },
        )
    }

    /// Install one committed redo record at the LSN the request carries:
    /// validate every sub-record, install with undo, then settle. `origin`
    /// decides the commit-boundary checks the validate pass runs.
    /// Every committed transaction installs here, whichever path committed it,
    /// and restart replay drives the same arms over the same record.
    #[cfg(test)]
    pub(in crate::data::executor) fn install_redo(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        committed: CommittedRedo<'_>,
        origin: RedoOrigin,
    ) -> Response {
        self.install_redo_authorized(task, tid, committed, origin, None, None)
    }

    /// [`Self::install_redo_authorized`] for a single-shard snapshot commit: the record
    /// installs only while nothing its transaction read or replaces holds a
    /// committed version above `snapshot.ordinal`. A record that fails the
    /// check is refused with `SerializationConflict` on every replica.
    #[cfg(test)]
    pub(in crate::data::executor) fn install_snapshot_redo(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        committed: CommittedRedo<'_>,
        origin: RedoOrigin,
        snapshot: RedoSnapshot<'_>,
    ) -> Response {
        self.install_redo_authorized(task, tid, committed, origin, Some(snapshot), None)
    }

    pub(in crate::data::executor) fn install_redo_authorized(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        committed: CommittedRedo<'_>,
        origin: RedoOrigin,
        snapshot: Option<RedoSnapshot<'_>>,
        permit: Option<RootJournalPermit>,
    ) -> Response {
        self.install_redo_into(
            task,
            tid,
            committed,
            origin,
            &mut Vec::new(),
            RedoInstallBoundary {
                snapshot,
                permit,
                judged_at_ms: None,
            },
        )
    }

    /// Install one committed redo record from `origin`. When the record
    /// stored resolved timeseries batches, the response payload carries
    /// their [`TsInstallCounts`] and `ts_installs` receives what each stored.
    fn install_redo_into(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        committed: CommittedRedo<'_>,
        origin: RedoOrigin,
        ts_installs: &mut Vec<TsInstalled>,
        boundary: RedoInstallBoundary<'_>,
    ) -> Response {
        let RedoInstallBoundary {
            snapshot,
            permit,
            judged_at_ms,
        } = boundary;
        let Some(lsn) = task.wal_lsn() else {
            return self.response_error(
                task,
                ErrorCode::Internal {
                    detail: "committed transaction redo reached the Data Plane without the LSN \
                             of its WAL record"
                        .into(),
                },
            );
        };
        let redo = match RedoRecord::from_bytes(committed.redo) {
            Ok(redo) => redo,
            Err(error) => return self.response_error(task, final_refusal(error.into())),
        };
        let reference = match RedoStreamRef::of_marker(&redo) {
            Ok(reference) => reference,
            Err(error) => return self.response_error(task, final_refusal(error.into())),
        };
        // Retention is judged at an instant the entry carries: a Calvin
        // slice's epoch instant, else the system-time ordinal the entry's
        // leader assigned. So every replica reaches the same verdict. Only a
        // write with neither reads the wall clock.
        let judged_at_ms = crate::data::executor::enforcement::retention::judged_instant_ms(
            judged_at_ms,
            task.request.system_ordinal,
        );
        // A stream marker's content is validated as the stream pages in,
        // with every check the whole-record path runs below, and the same
        // first error. The validate and the install run in this one
        // synchronous task on this core, with no yield between them, so no
        // write lands between the verdict and the install. `stream_validated`
        // holds the validate's spill directory until the install ran.
        let stream_validated = match &reference {
            Some(reference) => {
                let input = StreamValidateInput {
                    task,
                    tid,
                    marker: &redo,
                    reference,
                    origin,
                    snapshot: snapshot.map(|snapshot| snapshot.ordinal),
                    judged_at_ms,
                    lsn,
                    limits: StreamLimits {
                        batch_bytes: STREAM_BATCH_BYTES,
                        spill_threshold: self.query_tuning.max_txn_overlay_bytes,
                    },
                };
                match self.validate_redo_stream(&input) {
                    Ok(validated) => Some(validated),
                    Err(code) => return self.response_error(task, code),
                }
            }
            None => None,
        };
        // A stream marker installs the content its chunk records hold, with
        // the sum targets and reads the content carries.
        let (redo, streamed) = match reference {
            Some(reference) => match self.streamed_content(&redo, reference) {
                Ok(content) => (content.record, Some(content.carried)),
                Err(code) => return self.response_error(task, code),
            },
            None => (redo, None),
        };
        let redo_bytes = streamed
            .as_ref()
            .map_or(committed.redo, |carried| carried.bytes.as_slice());
        let sum_targets = streamed.as_ref().map_or(committed.sum_targets, |carried| {
            carried.sum_targets.as_slice()
        });
        let snapshot = match (&streamed, snapshot) {
            (Some(carried), Some(snapshot)) => Some(RedoSnapshot {
                reads: &carried.reads,
                ordinal: snapshot.ordinal,
            }),
            (_, snapshot) => snapshot,
        };
        let database_id = task.request.database_id.as_u64();

        // A clone copy over a row the target holds writes nothing, so no
        // check judges it.
        let doc_ops =
            match self.without_skipped_clone_copies(database_id, tid, document_ops(&redo.ops)) {
                Ok(ops) => ops,
                Err(error) => return self.response_error(task, clone_probe_unreadable(error)),
            };
        // Nothing is written when a check refuses the record, so the refusal
        // is final on every replica. A role-guarded transition is judged
        // against the roles the record carries.
        let kv_writes = kv_ops(&redo.ops);
        if stream_validated.is_none()
            && let Err(error) = self.check_commit_boundary(&CommitBoundary {
                database_id,
                tid,
                documents: &doc_ops,
                kv: &kv_writes,
                sum_targets,
                origin,
                user_roles: &redo.user_roles,
                judged_at_ms,
            })
        {
            return self.response_error(task, error);
        }
        let kv_images = self.kv_prior_images(database_id, tid, kv_writes);
        let labels = label_ops(&redo.ops);

        let record = match redo_wal_record(task, lsn, redo_bytes.to_vec()) {
            Ok(record) => record,
            Err(code) => return self.response_error(task, code),
        };
        let target = RedoTarget {
            record: &record,
            sub_records: redo.ops.len(),
            database_id,
            tid,
            origin,
        };
        if stream_validated.is_none() {
            if let Err(refusal) = self.validate_redo_pass(&target, sum_targets) {
                return self.response_error(task, refusal.into_code());
            }
            // A snapshot transaction commits only while nothing it read or
            // replaces moved past its snapshot. Nothing is written yet.
            if let Some(snapshot) = snapshot
                && let Err(refusal) =
                    self.check_redo_snapshot(task, tid, snapshot.reads, &redo.ops, snapshot.ordinal)
            {
                return self.response_error(task, refusal);
            }
        }
        // The replay arms install the record. Each write they apply takes the
        // record's stamp: its vShard and its LSN.
        let stamp = self.task_write_stamp_at(task, lsn);
        self.record_stamps.insert(stamp);
        // Every KV write of the record, and every undo of a refused install,
        // stamps one system-time ordinal: a rolled-back write replaces its
        // own version in place and leaves no history.
        let apply_ordinal = self.kv_apply_ordinal();
        let kv_scope = self.kv_engine.open_write_scope(apply_ordinal);
        let installed = self.install_redo_pass(&target, sum_targets);
        self.kv_engine.close_write_scope(kv_scope);
        // The install ran over the validated content: its spill directory
        // goes.
        drop(stream_validated);
        self.record_stamps.remove(lsn);
        let mut scope = match installed {
            Ok(scope) => scope,
            Err(refusal) => return self.response_error(task, refusal.into_code()),
        };
        // A crash test kills the process here: the install folded its sums
        // into deferred store commits, and its write set is not stored. Each
        // collection the record writes has its own point, taken in name
        // order.
        #[cfg(feature = "failpoints")]
        {
            let mut collections: Vec<&String> = committed.collections.iter().collect();
            collections.sort();
            for collection in collections {
                crate::fail_point!(
                    self.fail_scope,
                    &format!("redo_apply::after_install_pass::{collection}")
                );
            }
        }
        let write_set = target_write_set(&scope.target_writes);
        if let Some(permit) = permit {
            let capture = match self.prepare_redo_capture(task, permit, &write_set) {
                Ok(capture) => capture,
                Err(error) => {
                    let kv_scope = self.kv_engine.open_write_scope(apply_ordinal);
                    let detail = error.to_string();
                    let refusal = self.roll_back_redo(
                        &target,
                        std::mem::take(&mut scope.undo),
                        ErrorCode::from(error),
                    );
                    self.kv_engine.close_write_scope(kv_scope);
                    let error = crate::Error::Storage {
                        engine: "journal".into(),
                        detail,
                    };
                    let code = refusal.into_code();
                    self.fail_stop_on_rollback_code(&code);
                    self.fail_journalled(&error);
                    return self.response_error(task, code);
                }
            };
            if let Err(error) = self.persist_redo_capture(capture, &write_set) {
                let mut response = self.response_error(task, error);
                response.write_set = write_set;
                return response;
            }
        }
        // The install applied the record: every flush the settle runs, and
        // every later checkpoint, names it. No artifact written before this
        // point names it: its stamp prefix is an outcome floor the record was
        // above, since the record's dispatcher window held the floor below it
        // until this apply answers, and its applied ranges name only records
        // applied before. So restart replay applies it, and nothing written
        // earlier needs publishing again.
        self.floors.applied_prefix.note_applied(lsn);
        // The settle cannot be rolled back once it started, so a failure
        // leaves live state restart replay does not rebuild: the core
        // fail-stops. The funnel keeps the record for restart replay.
        if let Err(error) = self.settle_redo_install(task, &mut scope) {
            return self.fail_after_install(task, lsn, error, write_set);
        }
        // Write notes and the watermark move only once the record is
        // settled.
        let notes = std::mem::take(&mut scope.deferred_notes);
        if let Err(error) = self.publish_deferred_notes(notes) {
            return self.fail_after_install(task, lsn, error, write_set);
        }
        // Events leave only once the record is settled.
        let rows = crate::wal::RowSourceIndex::new(&redo.row_sources);
        self.send_held_events(task, std::mem::take(&mut scope.pending_events), &rows, lsn);
        // Edge writes count only once the record settled: a rolled-back
        // install applied none.
        self.publish_edge_writes(scope.edge_writes);

        let tenant = TenantId::new(tid);
        // The arms noted every document and KV row by key, which folded
        // those collections into the newest-ordinal cache. Every other
        // collection's note forgets its cached newest ordinal.
        let keyed: std::collections::HashSet<&str> = doc_ops
            .iter()
            .map(|op| op.collection())
            .chain(kv_images.iter().map(|image| image.collection.as_str()))
            .collect();
        for collection in committed.collections {
            if keyed.contains(collection.as_str()) {
                self.publish_applied(stamp);
            } else if let Err(error) =
                self.note_settled_collection(task.request.database_id, tenant, collection, stamp)
            {
                return self.fail_after_install(task, lsn, error, write_set);
            }
        }
        self.emit_committed_redo_events(task, scope.doc_writes, kv_images, labels, &rows);
        self.emit_committed_publishes(task, &redo.publishes);

        let mut response = self.response_ok(task);
        response.write_set = write_set;
        // The writer reports each resolved batch's apply count, not its
        // resolve's: the install rejected the rows that conflict with the
        // schema at this record's position.
        let counts = TsInstallCounts::new(
            scope
                .ts_installs
                .iter()
                .map(|installed| installed.count.clone())
                .collect(),
        );
        if !counts.is_empty() {
            match counts.to_bytes() {
                Ok(bytes) => response.payload = crate::bridge::envelope::Payload::from_vec(bytes),
                Err(error) => response.error_code = Some(Box::new(ErrorCode::from(error))),
            }
        }
        ts_installs.append(&mut scope.ts_installs);
        response
    }
}

#[cfg(test)]
#[path = "committed_apply_tests.rs"]
mod tests;
