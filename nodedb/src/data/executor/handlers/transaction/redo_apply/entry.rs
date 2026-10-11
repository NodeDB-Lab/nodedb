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
//!    the open [`RedoApplyScope`];
//! 3. a collection-floor write version for every collection written, and the
//!    index-value versions of every document row;
//! 4. the record's events, sent once the post-install work succeeded, with
//!    one publish event per message its transaction published (see
//!    `events`);
//! 5. the fold target rows in `Response::write_set`, so the funnel
//!    journals them.

use nodedb_physical::physical_plan::{RedoOrigin, RedoSumTargets};
use nodedb_wal::WalRecord;
use nodedb_wal::record::{RecordType, WalRecordArgs};

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::core_loop::fail_stop::FailStopCause;
use crate::data::executor::enforcement::write_hook::target_write_set;
use crate::data::executor::task::ExecutionTask;
use crate::types::TenantId;
use crate::wal::RedoRecord;

use super::passes::{RedoTarget, final_refusal};
use super::state::{RedoApplyPass, RedoApplyScope, TsInstalled};
use super::sub_ops::{document_ops, kv_ops, label_ops};
use crate::engine::timeseries::install_counts::TsInstallCounts;

/// The `MetaOp::ApplyTransactionRedo` fields the handler reads.
pub(in crate::data::executor) struct CommittedRedo<'a> {
    pub redo: &'a [u8],
    pub collections: &'a [String],
    pub sum_targets: &'a [RedoSumTargets],
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

    /// [`Self::install_redo`] for a committed record from `origin` that also
    /// hands back what each resolved timeseries batch of the record stored,
    /// in `ts_installs`.
    pub(in crate::data::executor) fn install_committed_redo_into(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        committed: CommittedRedo<'_>,
        origin: RedoOrigin,
        ts_installs: &mut Vec<TsInstalled>,
    ) -> Response {
        self.install_redo_into(task, tid, committed, origin, ts_installs)
    }

    /// Install one committed redo record at the LSN the request carries:
    /// validate every sub-record, install with undo, then settle. `origin`
    /// decides the commit-boundary checks the validate pass runs.
    /// Every committed transaction installs here, whichever path committed it,
    /// and restart replay drives the same arms over the same record.
    pub(in crate::data::executor) fn install_redo(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        committed: CommittedRedo<'_>,
        origin: RedoOrigin,
    ) -> Response {
        self.install_redo_into(task, tid, committed, origin, &mut Vec::new())
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
    ) -> Response {
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
        let database_id = task.request.database_id.as_u64();

        let doc_ops = document_ops(&redo.ops);
        // Nothing is written when a check refuses the record, so the refusal
        // is final on every replica.
        let check_scope =
            RedoApplyScope::new(RedoApplyPass::Validate, committed.sum_targets.to_vec());
        if let Err(error) =
            self.validate_redo_document_ops(database_id, tid, &doc_ops, &check_scope, origin)
        {
            return self.response_error(task, error);
        }
        let kv_images = self.kv_prior_images(database_id, tid, kv_ops(&redo.ops));
        let labels = label_ops(&redo.ops);

        let record = match WalRecord::new(WalRecordArgs {
            record_type: RecordType::TransactionRedo as u32,
            lsn: lsn.as_u64(),
            tenant_id: task.request.tenant_id.as_u64(),
            vshard_id: task.request.vshard_id.as_u32(),
            database_id,
            payload: committed.redo.to_vec(),
            encryption_key: None,
            preamble_bytes: None,
        }) {
            Ok(record) => record,
            Err(error) => {
                return self.response_error(task, final_refusal(crate::Error::from(error).into()));
            }
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
        // The replay arms install the record. Each write they apply takes the
        // record's stamp: its vShard and the entry it applies.
        let stamp = self.task_write_stamp_at(task, lsn);
        self.write_index.record_stamps.insert(stamp);
        let installed = self.install_redo_pass(&target, committed.sum_targets);
        self.write_index.record_stamps.remove(lsn);
        let mut scope = match installed {
            Ok(scope) => scope,
            Err(refusal) => return self.response_error(task, refusal.into_code()),
        };
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
        let settled = self.settle_redo_install(task, &mut scope);
        if let Err(error) = settled {
            self.fail_stop_core(
                FailStopCause::PostInstallFailed,
                &format!(
                    "committed redo record at lsn {} installed, then failed: {error:?}",
                    lsn.as_u64()
                ),
            );
            return self.response_error(task, error);
        }
        // Write versions and the watermark move only once the record is
        // settled.
        for version in std::mem::take(&mut scope.write_versions) {
            self.publish_write_version(
                version.db,
                version.tenant,
                &version.collection,
                version.key,
                version.stamp,
            );
        }
        // Events leave only once the record is settled. Every
        // one names the record's LSN: the install held the watermark back.
        // A handler stamped each with the record's source: it takes the
        // committed source of its kind of row, or its row-source group's.
        // A handler that ran under a replay task stamped no commit HLC or
        // author: each takes the record's.
        let rows = crate::wal::RowSourceIndex::new(&redo.row_sources);
        let record_source = task.request.event_source;
        for mut event in std::mem::take(&mut scope.pending_events) {
            event.lsn = lsn;
            if event.commit_hlc.is_none() {
                event.commit_hlc = task.request.commit_hlc;
            }
            if event.user_id.is_none() {
                event.user_id = task.request.user_id.clone();
            }
            if event.statement_digest.is_none() {
                event.statement_digest = task.request.statement_digest.clone();
            }
            event.source = super::events::committed_source(
                &rows,
                record_source,
                record_source.committed_other_source(),
                &event.collection,
                event.row_id.as_str(),
            );
            self.send_write_event(event);
        }

        let tenant = TenantId::new(tid);
        for collection in committed.collections {
            self.note_write(task.request.database_id, tenant, collection, None, stamp);
        }
        for write in &scope.doc_writes {
            self.note_index_write_values(
                task.request.database_id,
                tenant,
                &write.collection,
                &write.index_tuples,
                stamp,
            );
        }
        let write_set = target_write_set(&scope.target_writes);
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
mod tests {
    use super::*;
    use crate::bridge::envelope::Status;
    use crate::data::executor::core_loop::tests::{make_core_with_dir, make_default_task};
    use crate::types::Lsn;
    use crate::wal::RedoSubRecord;
    use nodedb_types::sync::wire::SyncProvenance;
    use nodedb_types::{StorageKey, Surrogate};

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
        }
        .to_bytes()
        .expect("encode redo")
    }

    fn doc_put(collection: &str, document_id: &str, surrogate: u32) -> RedoSubRecord {
        RedoSubRecord {
            record_type: RecordType::Put as u32,
            payload: zerompk::to_msgpack_vec(&(
                collection,
                document_id,
                doc_body(document_id),
                None::<SyncProvenance>,
                surrogate,
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
        let batch = nodedb_types::timeseries::TimeseriesWalBatch {
            collection: collection.to_string(),
            samples: vec![(1u64, timestamp_ms, value)],
            provenance: None,
        };
        let batch_bytes = zerompk::to_msgpack_vec(&batch).expect("encode samples");
        RedoSubRecord {
            record_type: RecordType::TimeseriesBatch as u32,
            payload: crate::control::server::wal_dispatch::encode_timeseries_ingest_payload(
                crate::control::server::wal_dispatch::TimeseriesIngestRecord {
                    collection,
                    payload: &batch_bytes,
                    provenance: None,
                    format: "samples",
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
            let stamp = &core.ts_replay_stamps.get(&key).expect("stamp").rows;
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
            payload: zerompk::to_msgpack_vec(&("carol".to_string(), vec!["Person".to_string()]))
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
