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
mod tests {
    use super::*;
    use crate::bridge::envelope::Status;
    use crate::data::executor::core_loop::tests::{make_core_with_dir, make_default_task};
    use crate::types::{Lsn, TenantId};
    use crate::wal::RedoSubRecord;
    use nodedb_types::sync::wire::SyncProvenance;
    use nodedb_types::{StorageKey, Surrogate};
    use nodedb_wal::WalRecord;
    use nodedb_wal::record::{RecordType, WalRecordArgs};

    const TID: u64 = 1;

    fn doc_body(name: &str) -> Vec<u8> {
        let mut obj = std::collections::HashMap::new();
        obj.insert(
            "name".to_string(),
            nodedb_types::Value::String(name.to_string()),
        );
        nodedb_types::value_to_msgpack(&nodedb_types::Value::Object(obj)).expect("encode body")
    }

    fn redo_bytes(ops: Vec<RedoSubRecord>) -> Vec<u8> {
        RedoRecord {
            version: 1,
            ops,
            calvin_stamp: None,
            cross_shard_applied: None,
            row_sources: Vec::new(),
            publishes: Vec::new(),
            row_changes: Vec::new(),
            user_roles: Vec::new(),
            sum_targets: Vec::new(),
        }
        .to_bytes()
        .expect("encode redo")
    }

    /// A document put sub-record, stamped as every producer stamps one.
    fn doc_put(collection: &str, document_id: &str, surrogate: u32) -> RedoSubRecord {
        RedoSubRecord {
            record_type: RecordType::Put as u32,
            payload: zerompk::to_msgpack_vec(&(
                collection,
                document_id,
                doc_body(document_id),
                None::<SyncProvenance>,
                surrogate,
                super::super::test_commit::test_stamp(),
                i64::MIN,
                i64::MAX,
            ))
            .expect("encode doc put"),
        }
    }

    fn kv_put(collection: &str, key: &[u8], value: &[u8], surrogate: u32) -> RedoSubRecord {
        RedoSubRecord {
            record_type: RecordType::Put as u32,
            payload: zerompk::to_msgpack_vec(&(
                "kv_put",
                collection,
                key.to_vec(),
                value.to_vec(),
                0u64,
                None::<u64>,
                surrogate,
            ))
            .expect("encode kv put"),
        }
    }

    fn task_at(lsn: Option<u64>) -> ExecutionTask {
        let mut task = make_default_task();
        task.wal_lsn = lsn.map(Lsn::new);
        task
    }

    /// Every message a committed record carries leaves the install as one
    /// publish event on its topic's stream, at the record's LSN.
    #[test]
    fn an_installed_record_emits_one_publish_event_per_message() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _req, _resp) = make_core_with_dir(dir.path());
        let (mut producers, mut consumers) =
            crate::event::bus::create_event_bus_with_capacity(1, 64);
        core.set_event_producer(producers.pop().expect("producer"));
        let mut events = consumers.pop().expect("consumer");
        let redo = RedoRecord {
            version: 1,
            ops: vec![kv_put("cache", b"k1", b"v1", 12)],
            calvin_stamp: None,
            cross_shard_applied: None,
            row_sources: Vec::new(),
            publishes: vec![crate::wal::RedoPublish {
                owner: "trigger/0/notify".into(),
                database_id: 0,
                tenant_id: TID,
                topic: "cache_feed".into(),
                payload: "k1 set".into(),
                metadata_floor: 0,
                position: None,
            }],
            row_changes: Vec::new(),
            user_roles: Vec::new(),
            sum_targets: Vec::new(),
        }
        .to_bytes()
        .expect("encode redo");

        let response = core.execute_apply_transaction_redo(
            &task_at(Some(60)),
            TID,
            CommittedRedo {
                redo: &redo,
                collections: &["cache".to_string()],
                sum_targets: &[],
            },
        );
        assert_eq!(response.status, Status::Ok, "{:?}", response.error_code);

        let mut publishes = Vec::new();
        while let Some(event) = events.try_recv() {
            if event.op == crate::event::WriteOp::Publish {
                publishes.push(event);
            }
        }
        assert_eq!(publishes.len(), 1, "one event per message");
        assert_eq!(publishes[0].collection.as_ref(), "topic:cache_feed");
        assert_eq!(publishes[0].lsn, Lsn::new(60));
    }

    #[test]
    fn an_installed_record_is_named_by_the_next_replay_stamp() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _req, _resp) = make_core_with_dir(dir.path());
        core.floors
            .applied_prefix
            .observe_outcome_floor(Lsn::new(10));
        let redo = redo_bytes(vec![kv_put("cache", b"k1", b"v1", 12)]);

        let response = core.execute_apply_transaction_redo(
            &task_at(Some(50)),
            TID,
            CommittedRedo {
                redo: &redo,
                collections: &["cache".to_string()],
                sum_targets: &[],
            },
        );
        assert_eq!(response.status, Status::Ok, "{:?}", response.error_code);

        let stamp = core.floors.applied_prefix.stamp().expect("exact stamp");
        assert!(
            stamp.skips(50),
            "the installed record is in every later artifact"
        );
        assert!(
            !stamp.skips(49),
            "a record below it that never applied here must still replay"
        );
    }

    #[test]
    fn committed_redo_installs_its_document_and_kv_rows() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _req, _resp) = make_core_with_dir(dir.path());
        let redo = redo_bytes(vec![
            doc_put("notes", "n1", 11),
            kv_put("cache", b"k1", b"v1", 12),
        ]);
        let collections = vec!["cache".to_string(), "notes".to_string()];

        let response = core.execute_apply_transaction_redo(
            &task_at(Some(50)),
            TID,
            CommittedRedo {
                redo: &redo,
                collections: &collections,
                sum_targets: &[],
            },
        );

        assert_eq!(response.status, Status::Ok, "{:?}", response.error_code);
        let stored = core
            .sparse
            .get(
                0,
                TID,
                "notes",
                &StorageKey::for_surrogate(Surrogate::new(11)),
            )
            .expect("read document");
        assert!(stored.is_some(), "the document post-image is installed");
        let now_ms = crate::engine::kv::current_ms();
        assert_eq!(
            core.kv_engine
                .get(0, TID, "cache", b"k1", now_ms)
                .as_deref(),
            Some(b"v1".as_slice()),
            "the KV post-image is installed"
        );
        assert!(
            core.redo_apply.scope.is_none(),
            "the apply scope closes with the apply"
        );
    }

    #[test]
    fn committed_redo_without_its_wal_lsn_writes_nothing() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _req, _resp) = make_core_with_dir(dir.path());
        let redo = redo_bytes(vec![kv_put("cache", b"k1", b"v1", 12)]);

        let response = core.execute_apply_transaction_redo(
            &task_at(None),
            TID,
            CommittedRedo {
                redo: &redo,
                collections: &[],
                sum_targets: &[],
            },
        );

        assert_eq!(response.status, Status::Error);
        let now_ms = crate::engine::kv::current_ms();
        assert!(core.kv_engine.get(0, TID, "cache", b"k1", now_ms).is_none());
    }

    fn metric_samples_sub(collection: &str, timestamp_ms: i64, value: f64) -> RedoSubRecord {
        // ILP timestamps are nanoseconds.
        let line = format!("{collection} value={value} {}", timestamp_ms * 1_000_000);
        RedoSubRecord {
            record_type: RecordType::TimeseriesBatch as u32,
            payload: crate::control::server::wal_dispatch::encode_timeseries_ingest_payload(
                crate::control::server::wal_dispatch::TimeseriesIngestRecord {
                    rls_write_check: &nodedb_types::RlsWriteCheck::already_decided_elsewhere(),
                    collection,
                    payload: line.as_bytes(),
                    provenance: None,
                    format: "ilp",
                    default_timestamp_ms: 0,
                },
            )
            .expect("encode timeseries sub-record"),
        }
    }

    /// A committed KV record applied after a checkpoint that named a higher
    /// LSN is not named by that checkpoint: nothing publishes it again, and a
    /// restart replays it over the restored generation.
    #[test]
    fn a_kv_redo_applied_after_a_checkpoint_replays_after_a_restart() {
        let dir = tempfile::tempdir().expect("tempdir");
        let redo_record = |lsn: u64, redo: &[u8]| {
            WalRecord::new(WalRecordArgs {
                record_type: RecordType::TransactionRedo as u32,
                lsn,
                tenant_id: TID,
                vshard_id: 0,
                database_id: 0,
                payload: redo.to_vec(),
                encryption_key: None,
                preamble_bytes: None,
            })
            .expect("wal record")
        };
        let later = redo_bytes(vec![kv_put("cache", b"k2", b"v2", 22)]);
        let earlier = redo_bytes(vec![kv_put("cache", b"k1", b"v1", 21)]);
        let collections = vec!["cache".to_string()];
        let install = |core: &mut CoreLoop, lsn: u64, redo: &[u8]| {
            let response = core.execute_apply_transaction_redo(
                &task_at(Some(lsn)),
                TID,
                CommittedRedo {
                    redo,
                    collections: &collections,
                    sum_targets: &[],
                },
            );
            assert_eq!(response.status, Status::Ok, "{:?}", response.error_code);
        };

        {
            let (mut core, _req, _resp) = make_core_with_dir(dir.path());
            core.floors
                .applied_prefix
                .observe_outcome_floor(Lsn::new(10));
            install(&mut core, 100, &later);
            core.checkpoint_kv_engines().expect("checkpoint");
            install(&mut core, 50, &earlier);
        }

        let (mut core, _req, _resp) = make_core_with_dir(dir.path());
        core.load_kv_checkpoints().expect("load");
        assert!(
            !core.floors.replay_floors.kv.covers(50),
            "the checkpoint written before lsn 50 applied does not name it"
        );
        core.floors
            .applied_prefix
            .seed_replayed_through(Lsn::new(100));
        core.replay_transaction_redo_wal(
            &[redo_record(50, &earlier), redo_record(100, &later)],
            1,
            &nodedb_wal::TombstoneSet::new(),
        )
        .expect("replay");
        let now_ms = crate::engine::kv::current_ms();
        for (key, value) in [(&b"k1"[..], &b"v1"[..]), (&b"k2"[..], &b"v2"[..])] {
            assert_eq!(
                core.kv_engine.get(0, TID, "cache", key, now_ms).as_deref(),
                Some(value),
                "{key:?} is present after the restart"
            );
        }
    }

    /// A committed redo record applied after a flush that named a higher LSN
    /// is not named by that flush's stamp: its dispatcher window held the
    /// outcome floor below it. The flush needs no republish, and a restart
    /// replays the record once, beside the flushed one it does not repeat.
    #[test]
    fn a_redo_applied_below_a_flushed_record_replays_once_after_a_restart() {
        let dir = tempfile::tempdir().expect("tempdir");
        let tenant = TenantId::new(TID);
        let key = (
            crate::types::DatabaseId::DEFAULT,
            tenant,
            "metrics".to_string(),
        );
        let redo_record = |lsn: u64, redo: &[u8]| {
            WalRecord::new(WalRecordArgs {
                record_type: RecordType::TransactionRedo as u32,
                lsn,
                tenant_id: TID,
                vshard_id: 0,
                database_id: 0,
                payload: redo.to_vec(),
                encryption_key: None,
                preamble_bytes: None,
            })
            .expect("wal record")
        };
        let rows = |core: &CoreLoop| {
            core.columnar_memtables
                .get(&key)
                .map_or(0, |m| m.row_count())
                + core
                    .ts_registries
                    .get(&key)
                    .map_or(0, |registry| registry.total_row_count())
        };
        let later = redo_bytes(vec![metric_samples_sub("metrics", 1_700_000_000_000, 1.0)]);
        let earlier = redo_bytes(vec![metric_samples_sub("metrics", 1_700_000_000_001, 2.0)]);
        let collections = vec!["metrics".to_string()];
        let install = |core: &mut CoreLoop, lsn: u64, redo: &[u8]| {
            let response = core.execute_apply_transaction_redo(
                &task_at(Some(lsn)),
                TID,
                CommittedRedo {
                    redo,
                    collections: &collections,
                    sum_targets: &[],
                },
            );
            assert_eq!(response.status, Status::Ok, "{:?}", response.error_code);
        };

        {
            let (mut core, _req, _resp) = make_core_with_dir(dir.path());
            core.floors
                .applied_prefix
                .observe_outcome_floor(crate::types::Lsn::new(10));
            // The record at 100 applies first and flushes.
            install(&mut core, 100, &later);
            core.flush_ts_collection(tenant, crate::types::DatabaseId::DEFAULT, "metrics", 0)
                .expect("flush");
            let stamp = &core.ts_replay.stamps.get(&key).expect("stamp").rows;
            assert!(stamp.skips(100) && !stamp.skips(50), "{stamp:?}");
            // The record at 50 applies afterwards and stays in the memtable.
            install(&mut core, 50, &earlier);
            assert_eq!(rows(&core), 2);
        }

        let (mut core, _req, _resp) = make_core_with_dir(dir.path());
        core.load_ts_registries().expect("load");
        core.floors
            .applied_prefix
            .seed_replayed_through(crate::types::Lsn::new(100));
        core.replay_transaction_redo_wal(
            &[redo_record(50, &earlier), redo_record(100, &later)],
            1,
            &nodedb_wal::TombstoneSet::new(),
        )
        .expect("replay");
        assert_eq!(
            rows(&core),
            2,
            "the record at 50 replays once; the flushed record at 100 does not repeat"
        );
    }

    /// A sub-record no replay arm owns is refused by the validate pass, so
    /// the sub-records beside it write nothing.
    #[test]
    fn a_record_with_a_sub_record_no_arm_claims_writes_nothing() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _req, _resp) = make_core_with_dir(dir.path());
        let redo = redo_bytes(vec![
            kv_put("cache", b"k1", b"v1", 12),
            RedoSubRecord {
                record_type: RecordType::VectorParams as u32,
                payload: vec![0x90],
            },
        ]);

        let response = core.execute_apply_transaction_redo(
            &task_at(Some(70)),
            TID,
            CommittedRedo {
                redo: &redo,
                collections: &[],
                sum_targets: &[],
            },
        );

        assert!(
            matches!(
                response.error_code.as_deref(),
                Some(ErrorCode::RejectedPrevalidation { .. })
            ),
            "{:?}",
            response.error_code
        );
        let now_ms = crate::engine::kv::current_ms();
        assert!(core.kv_engine.get(0, TID, "cache", b"k1", now_ms).is_none());
    }

    /// A sub-record that passes validation and fails while it installs rolls
    /// back every write of the record, those before it and those after it:
    /// the overwritten KV key keeps its value, expiry and surrogate, and the
    /// document row is absent.
    #[test]
    fn an_install_failure_rolls_back_every_write_of_the_record() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _req, _resp) = make_core_with_dir(dir.path());
        let now_ms = crate::engine::kv::current_ms();
        let expire_at_ms = now_ms + 3_600_000;
        core.kv_engine
            .put_with_absolute_expiry(
                crate::engine::kv::KvPutParams {
                    database_id: 0,
                    tenant_id: TID,
                    collection: "cache",
                    key: b"k1",
                    value: b"old",
                    ttl_ms: 0,
                    now_ms,
                    surrogate: Surrogate::new(5),
                },
                expire_at_ms,
            )
            .expect("a bound row writes");
        let before = core
            .kv_engine
            .entry_image(0, TID, "cache", b"k1", now_ms)
            .expect("seeded key");
        let redo = redo_bytes(vec![
            kv_put("cache", b"k1", b"new", 12),
            mismatched_ingest(),
            doc_put("notes", "n1", 11),
        ]);
        let collections = vec!["cache".to_string(), "notes".to_string()];

        let response = core.execute_apply_transaction_redo(
            &task_at(Some(80)),
            TID,
            CommittedRedo {
                redo: &redo,
                collections: &collections,
                sum_targets: &[],
            },
        );

        assert!(
            matches!(
                response.error_code.as_deref(),
                Some(ErrorCode::RetryableRefusal { .. })
            ),
            "{:?}",
            response.error_code
        );
        assert_eq!(
            core.kv_engine.entry_image(0, TID, "cache", b"k1", now_ms),
            Some(before),
            "the KV key keeps its value, its expiry instant and its surrogate"
        );
        let stored = core
            .sparse
            .get(
                0,
                TID,
                "notes",
                &StorageKey::for_surrogate(Surrogate::new(11)),
            )
            .expect("read document");
        assert!(
            stored.is_none(),
            "the document row the record wrote is gone"
        );
        assert!(core.redo_apply.scope.is_none());
    }

    fn mismatched_ingest() -> RedoSubRecord {
        let lines = zerompk::to_msgpack_vec(&vec!["other,host=a value=1 1".to_string()])
            .expect("encode lines");
        RedoSubRecord {
            record_type: RecordType::TimeseriesBatch as u32,
            payload: crate::control::server::wal_dispatch::encode_timeseries_ingest_payload(
                crate::control::server::wal_dispatch::TimeseriesIngestRecord {
                    rls_write_check: &nodedb_types::RlsWriteCheck::already_decided_elsewhere(),
                    collection: "metrics",
                    payload: &lines,
                    provenance: None,
                    format: "ilp-msgpack",
                    default_timestamp_ms: 0,
                },
            )
            .expect("encode ingest"),
        }
    }

    /// A batch `VectorPut` carries every vector's surrogate, and the install
    /// binds each one.
    #[test]
    fn a_batch_vector_put_binds_every_surrogate() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _req, _resp) = make_core_with_dir(dir.path());
        let batch = RedoSubRecord {
            record_type: RecordType::VectorPut as u32,
            payload: crate::control::server::wal_dispatch::encode_vector_batch_put_payload(
                "docs",
                &[vec![0.5f32, 0.5], vec![0.25, 0.75]],
                2,
                &[Surrogate::new(31), Surrogate::new(32)],
            )
            .expect("encode vector batch put"),
        };
        let redo = redo_bytes(vec![batch]);

        let response = core.execute_apply_transaction_redo(
            &task_at(Some(91)),
            TID,
            CommittedRedo {
                redo: &redo,
                collections: &[],
                sum_targets: &[],
            },
        );

        assert_eq!(response.status, Status::Ok, "{:?}", response.error_code);
        let key = CoreLoop::vector_index_key(0, TID, "docs", "");
        let collection = core
            .vector_collections
            .get(&key)
            .expect("the batch's index");
        for surrogate in [Surrogate::new(31), Surrogate::new(32)] {
            assert!(
                collection.local_for_surrogate(surrogate).is_some(),
                "{surrogate:?} is bound after the install"
            );
        }
    }

    /// A `VectorPut` payload of neither current shape (here the retired
    /// identity-less `(collection, vector, dim)`) is refused, never installed
    /// as an unbound vector.
    #[test]
    fn a_vector_put_of_no_current_shape_is_refused() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _req, _resp) = make_core_with_dir(dir.path());
        let retired = RedoSubRecord {
            record_type: RecordType::VectorPut as u32,
            payload: zerompk::to_msgpack_vec(&("docs", vec![0.5f32, 0.5], 2usize))
                .expect("encode retired vector put"),
        };
        let redo = redo_bytes(vec![retired]);

        let response = core.execute_apply_transaction_redo(
            &task_at(Some(92)),
            TID,
            CommittedRedo {
                redo: &redo,
                collections: &[],
                sum_targets: &[],
            },
        );

        assert_eq!(response.status, Status::Error);
        let key = CoreLoop::vector_index_key(0, TID, "docs", "");
        assert!(
            core.vector_collections
                .get(&key)
                .is_none_or(|collection| collection.live_count() == 0),
            "no vector is installed"
        );
    }

    /// The vector node an install inserted is withdrawn with the rest of the
    /// record, and the index it created is gone.
    #[test]
    fn an_install_failure_withdraws_the_vector_node_it_inserted() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _req, _resp) = make_core_with_dir(dir.path());
        let vector_put = RedoSubRecord {
            record_type: RecordType::VectorPut as u32,
            payload: crate::control::server::wal_dispatch::encode_vector_put_payload(
                "docs",
                &[0.5f32, 0.5],
                2,
                "",
                nodedb_types::Surrogate::new(21),
                None,
            )
            .expect("encode vector put"),
        };
        let redo = redo_bytes(vec![vector_put, mismatched_ingest()]);

        let response = core.execute_apply_transaction_redo(
            &task_at(Some(90)),
            TID,
            CommittedRedo {
                redo: &redo,
                collections: &[],
                sum_targets: &[],
            },
        );

        assert!(
            matches!(
                response.error_code.as_deref(),
                Some(ErrorCode::RetryableRefusal { .. })
            ),
            "{:?}",
            response.error_code
        );
        let key = CoreLoop::vector_index_key(0, TID, "docs", "");
        assert!(!core.vector_collections.contains_key(&key));
    }

    /// The edge an install wrote leaves no version behind at any system time,
    /// and the nodes it created leave the CSR: the core reads as restart
    /// replay of the cancelled record would.
    #[test]
    fn an_install_failure_leaves_no_trace_of_the_edge_it_wrote() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _req, _resp) = make_core_with_dir(dir.path());
        let edge_put = RedoSubRecord {
            record_type: RecordType::Put as u32,
            payload: zerompk::to_msgpack_vec(&crate::wal::EdgePutRedo {
                collection: "knows".into(),
                src_id: "alice".into(),
                label: "KNOWS".into(),
                dst_id: "bob".into(),
                properties: Vec::new(),
                src_surrogate: 31,
                dst_surrogate: 32,
                system_from: Some(500),
                applied: None,
                valid_from_ms: None,
                valid_until_ms: None,
            })
            .expect("encode edge put"),
        };
        let redo = redo_bytes(vec![edge_put, mismatched_ingest()]);

        let response = core.execute_apply_transaction_redo(
            &task_at(Some(95)),
            TID,
            CommittedRedo {
                redo: &redo,
                collections: &[],
                sum_targets: &[],
            },
        );

        assert!(
            matches!(
                response.error_code.as_deref(),
                Some(ErrorCode::RetryableRefusal { .. })
            ),
            "{:?}",
            response.error_code
        );
        let edge = crate::engine::graph::edge_store::EdgeRef::new(
            crate::types::DatabaseId::DEFAULT,
            TenantId::new(TID),
            "knows",
            "alice",
            "KNOWS",
            "bob",
        );
        for as_of in [500, 501, i64::MAX] {
            assert_eq!(
                core.edge_store
                    .ceiling_resolve_edge(edge, as_of, None)
                    .expect("resolve edge"),
                None,
                "no version of the rolled-back edge is visible at system time {as_of}"
            );
        }
        assert!(
            core.edge_store
                .scan_all_node_surrogates()
                .expect("scan bindings")
                .is_empty()
        );
        assert!(
            core.csr_partition(0, TID)
                .is_none_or(|p| !p.contains_node("alice") && !p.contains_node("bob")),
            "the nodes the edge created are gone from the CSR"
        );
    }

    /// A label write on a node the CSR did not hold creates the node and
    /// interns the label. A rolled-back install withdraws both.
    #[test]
    fn an_install_failure_withdraws_the_node_a_label_write_created() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _req, _resp) = make_core_with_dir(dir.path());
        let label_set = RedoSubRecord {
            record_type: RecordType::GraphNodeLabelSet as u32,
            payload: crate::wal::NodeLabelRedo::live("carol", &["Person".to_string()])
                .encode()
                .expect("encode label set"),
        };
        let redo = redo_bytes(vec![label_set, mismatched_ingest()]);

        let response = core.execute_apply_transaction_redo(
            &task_at(Some(96)),
            TID,
            CommittedRedo {
                redo: &redo,
                collections: &[],
                sum_targets: &[],
            },
        );

        assert!(
            matches!(
                response.error_code.as_deref(),
                Some(ErrorCode::RetryableRefusal { .. })
            ),
            "{:?}",
            response.error_code
        );
        assert!(
            core.csr_partition(0, TID)
                .is_none_or(|p| !p.contains_node("carol") && !p.has_node_label_name("Person")),
            "the node and the label name the write created are gone"
        );
    }

    /// A non-document sub-record that cannot be applied fails the online
    /// apply response instead of being logged and skipped.
    #[test]
    fn a_failing_non_document_sub_record_fails_the_apply() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _req, _resp) = make_core_with_dir(dir.path());
        let redo = redo_bytes(vec![RedoSubRecord {
            record_type: RecordType::SpatialPut as u32,
            payload: vec![0xff, 0x00],
        }]);

        let response = core.execute_apply_transaction_redo(
            &task_at(Some(60)),
            TID,
            CommittedRedo {
                redo: &redo,
                collections: &[],
                sum_targets: &[],
            },
        );

        assert_eq!(response.status, Status::Error);
        assert!(
            core.redo_apply.scope.is_none(),
            "the apply scope closes with the failed apply"
        );
    }
}
