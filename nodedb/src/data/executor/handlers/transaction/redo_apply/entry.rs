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
//! write back when one fails. A stream marker's content validates batch by
//! batch as the stream pages in (see `stream_validate`), then installs batch
//! by batch through the same arms (see `batched`).
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

use std::collections::HashSet;

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::core_loop::write_set_journal::RootJournalPermit;
use crate::data::executor::handlers::spill::run::BlockReader;
use crate::data::executor::task::ExecutionTask;
use crate::types::Lsn;
use crate::wal::{RedoRecord, RedoStreamRef};

use super::batched::{InstallFinish, StreamInstallInput};
use super::clone_copy::clone_probe_unreadable;
use super::passes::{RedoTarget, final_refusal, redo_wal_record};
use super::state::TsInstalled;
use super::stream_validate::{StreamLimits, StreamValidateInput};
use super::sub_ops::{document_ops, kv_ops, label_ops};
use super::validate::CommitBoundary;

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

/// The refusal of an install of the record at `lsn` that `folds`
/// materialized sums with nothing `recorded` to keep its folds: no root
/// journal stores its write set, and no deferred store holds the folds
/// until one does. Its folds would be durable with no write set, so every
/// boot would judge the record never durable and fold it again. The funnel
/// journals every committed redo it installs, so the record is refused
/// before any write.
pub(super) fn unrecorded_fold(lsn: Lsn, folds: bool, recorded: bool) -> Option<ErrorCode> {
    (folds && !recorded).then(|| ErrorCode::Internal {
        detail: format!(
            "the committed redo at lsn {} folds materialized sums with no root journal to store \
             its write set",
            lsn.as_u64()
        ),
    })
}

impl CoreLoop {
    /// Apply one committed redo record. The request carries the LSN of the
    /// `TransactionRedo` WAL record the funnel appended for it.
    ///
    /// A record that folds installs under a root journal, as the funnel
    /// installs every committed redo. A test that opened a journal itself
    /// keeps it.
    #[cfg(test)]
    pub(in crate::data::executor) fn execute_apply_transaction_redo(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        committed: CommittedRedo<'_>,
    ) -> Response {
        let Some(lsn) = task.wal_lsn() else {
            return self.install_redo(task, tid, committed, RedoOrigin::Commit);
        };
        if committed.sum_targets.is_empty() || self.sparse.db().is_deferred() {
            return self.install_redo(task, tid, committed, RedoOrigin::Commit);
        }
        let mut request = task.request.clone();
        request.wal_lsn = Some(lsn);
        request.plan = crate::bridge::envelope::PhysicalPlan::Meta(
            nodedb_physical::physical_plan::MetaOp::ApplyTransactionRedo {
                redo: committed.redo.to_vec(),
                collections: committed.collections.to_vec(),
                sum_targets: committed.sum_targets.to_vec(),
                origin: RedoOrigin::Commit,
                calvin: None,
                reads: Vec::new(),
                snapshot_ordinal: None,
            },
        );
        let root = ExecutionTask::new(request);
        let group = crate::bridge::dispatch::JournalGroup {
            origin: lsn,
            collection: committed.collections.first().cloned().unwrap_or_default(),
            apply_key: 0,
            commit_hlc: None,
            change_position: None,
        };
        let permit = self.begin_task_journal(&root, group);
        let mut response =
            self.install_redo_authorized(&root, tid, committed, RedoOrigin::Commit, None, permit);
        if let Err(error) = self.finish_journalled_task(&root, &mut response) {
            response.status = crate::bridge::envelope::Status::Error;
            response.error_code = Some(Box::new(ErrorCode::from(error)));
        }
        response
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
    /// their `TsInstallCounts` and `ts_installs` receives what each stored.
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
        // write lands between the verdict and the install.
        if let Some(reference) = &reference {
            let limits = StreamLimits {
                batch_bytes: self.redo_apply.stream_batch_bytes,
                spill_threshold: self.query_tuning.max_txn_overlay_bytes,
            };
            let input = StreamValidateInput {
                task,
                tid,
                marker: &redo,
                reference,
                origin,
                snapshot: snapshot.map(|snapshot| snapshot.ordinal),
                judged_at_ms,
                lsn,
                limits,
            };
            let validated = match self.validate_redo_stream(&input) {
                Ok(validated) => validated,
                Err(code) => return self.response_error(task, code),
            };
            return self.install_redo_stream(
                StreamInstallInput {
                    task,
                    tid,
                    marker: &redo,
                    reference,
                    origin,
                    lsn,
                    limits,
                    permit,
                    collections: committed.collections,
                },
                validated,
                ts_installs,
            );
        }
        // A whole record's folds persist with its write set under a root
        // journal, or wait in an open journal's deferred commits.
        let recorded = permit.is_some() || self.sparse.db().is_deferred();
        if let Some(refusal) = unrecorded_fold(lsn, !committed.sum_targets.is_empty(), recorded) {
            return self.response_error(task, refusal);
        }
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
        if let Err(error) = self.check_commit_boundary(&CommitBoundary {
            database_id,
            tid,
            documents: &doc_ops,
            kv: &kv_writes,
            sum_targets: committed.sum_targets,
            origin,
            user_roles: &redo.user_roles,
            judged_at_ms,
        }) {
            return self.response_error(task, error);
        }
        let kv_images = self.kv_prior_images(database_id, tid, kv_writes);
        let labels = label_ops(&redo.ops);

        let record = match redo_wal_record(task, lsn, committed.redo.to_vec()) {
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
        if let Err(refusal) = self.validate_redo_pass(&target, committed.sum_targets) {
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
        // The replay arms install the record. Each write they apply takes the
        // record's stamp: its vShard and its LSN.
        let stamp = self.task_write_stamp_at(task, lsn);
        self.record_stamps.insert(stamp);
        // Every KV write of the record, and every undo of a refused install,
        // stamps one system-time ordinal: a rolled-back write replaces its
        // own version in place and leaves no history.
        let apply_ordinal = self.kv_apply_ordinal();
        let kv_scope = self.kv_engine.open_write_scope(apply_ordinal);
        let installed = self.install_redo_pass(&target, committed.sum_targets);
        self.kv_engine.close_write_scope(kv_scope);
        self.record_stamps.remove(lsn);
        let mut scope = match installed {
            Ok(scope) => scope,
            Err(refusal) => return self.response_error(task, refusal.into_code()),
        };
        // The arms note every document and KV row by key. The set borrows
        // its names: a KV collection's from the record's collection list,
        // since the images move into the spools below. A KV collection the
        // list does not name is never asked about.
        let keyed: HashSet<&str> = doc_ops
            .iter()
            .map(|op| op.collection())
            .chain(kv_images.iter().filter_map(|image| {
                committed
                    .collections
                    .iter()
                    .find(|collection| **collection == image.collection)
                    .map(String::as_str)
            }))
            .collect();
        // The whole record sits in RAM already, so its spools never spill.
        for image in kv_images {
            scope.held.kv_priors.push(image);
        }
        for label in labels {
            scope.held.labels.push(label);
        }
        for group in redo.row_sources {
            scope.held.row_sources.add(group);
        }
        for publish in redo.publishes {
            scope.held.publishes.push(publish);
        }
        let reader = BlockReader::default();
        self.finish_redo_install(
            InstallFinish {
                task,
                tid,
                lsn,
                stamp,
                apply_ordinal,
                permit,
                collections: committed.collections,
                keyed: &keyed,
                reader: &reader,
                batched: None,
            },
            scope,
            ts_installs,
        )
    }
}

#[cfg(test)]
#[path = "committed_apply_tests.rs"]
mod tests;
