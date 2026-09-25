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
//! 4. the record's events, sent once the post-install work succeeded (see
//!    `events`);
//! 5. the fold target rows in `Response::write_set`, so the funnel journals
//!    them.

use nodedb_physical::physical_plan::RedoSumTargets;
use nodedb_wal::WalRecord;
use nodedb_wal::record::{RecordType, WalRecordArgs};

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::core_loop::fail_stop::FailStopCause;
use crate::data::executor::enforcement::write_hook::target_write_set;
use crate::data::executor::task::ExecutionTask;
use crate::types::TenantId;
use crate::wal::RedoRecord;

use super::cover::WrittenEngines;
use super::passes::{RedoTarget, final_refusal};
use super::state::{RedoApplyPass, RedoApplyScope};
use super::sub_ops::{document_ops, kv_ops, label_ops};

/// The `MetaOp::ApplyTransactionRedo` fields the handler reads.
pub(in crate::data::executor) struct CommittedRedo<'a> {
    pub redo: &'a [u8],
    pub collections: &'a [String],
    pub sum_targets: &'a [RedoSumTargets],
}

impl CoreLoop {
    /// Apply one committed redo record. The request carries the LSN of the
    /// `TransactionRedo` WAL record the funnel appended for it.
    pub(in crate::data::executor) fn execute_apply_transaction_redo(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        committed: CommittedRedo<'_>,
    ) -> Response {
        self.install_committed_redo(task, tid, committed)
    }

    /// Install one committed redo record at the LSN the request carries:
    /// validate every sub-record, install with undo, then settle and cover.
    /// Every committed transaction installs here, whichever path committed it,
    /// and restart replay drives the same arms over the same record.
    pub(in crate::data::executor) fn install_committed_redo(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        committed: CommittedRedo<'_>,
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
            self.validate_redo_document_ops(database_id, tid, &doc_ops, &check_scope)
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
        };
        if let Err(refusal) = self.validate_redo_pass(&target, committed.sum_targets) {
            return self.response_error(task, refusal.into_code());
        }
        let mut scope = match self.install_redo_pass(&target, committed.sum_targets) {
            Ok(scope) => scope,
            Err(refusal) => return self.response_error(task, refusal.into_code()),
        };
        // Settle, then publish again every artifact whose watermark covers
        // the record, so restart replay does not skip it. Neither step can be
        // rolled back once it started, so a failure leaves live state restart
        // replay does not rebuild: the core fail-stops. The funnel keeps the
        // record for restart replay.
        let settled = self.settle_redo_install(task, &mut scope).and_then(|()| {
            // Applied from here on: a checkpoint the cover writes names it.
            self.floors.applied_prefix.note_applied(lsn);
            self.cover_applied_record(lsn, &WrittenEngines::of(&redo), &scope.arrays_written)
        });
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
        // settled and covered.
        for version in std::mem::take(&mut scope.write_versions) {
            self.publish_write_version(
                version.db,
                version.tenant,
                &version.collection,
                version.key,
                version.lsn,
            );
        }
        // Events leave only once the record is settled and covered. Every
        // one names the record's LSN: the install held the watermark back.
        for mut event in std::mem::take(&mut scope.pending_events) {
            event.lsn = lsn;
            self.send_write_event(event);
        }

        let tenant = TenantId::new(tid);
        for collection in committed.collections {
            self.note_write_lsn(task.request.database_id, tenant, collection, None, lsn);
        }
        for write in &scope.doc_writes {
            self.note_index_write_values(
                task.request.database_id,
                tenant,
                &write.collection,
                &write.index_tuples,
                lsn,
            );
        }
        let write_set = target_write_set(&scope.target_writes);
        self.emit_committed_redo_events(task, scope.doc_writes, kv_images, labels);

        let mut response = self.response_ok(task);
        response.write_set = write_set;
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
        zerompk::to_msgpack_vec(&nodedb_types::Value::Object(obj)).expect("encode body")
    }

    fn redo_bytes(ops: Vec<RedoSubRecord>) -> Vec<u8> {
        RedoRecord {
            version: 1,
            ops,
            calvin_stamp: None,
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
            payload:
                crate::control::server::wal_dispatch::encode_timeseries_batch_payload_with_format(
                    collection,
                    &batch_bytes,
                    None,
                    "samples",
                )
                .expect("encode timeseries sub-record"),
        }
    }

    /// Restart replay skips a timeseries record at or below the highest
    /// flushed partition stamp. A committed redo applied online after a live
    /// flush stamped past its LSN must still install, and must flush so the
    /// stamp's claim holds for it on the next restart.
    #[test]
    fn an_online_redo_apply_below_a_flushed_partition_stamp_still_installs() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _req, _resp) = make_core_with_dir(dir.path());
        let tenant = TenantId::new(TID);
        let key = (
            crate::types::DatabaseId::DEFAULT,
            tenant,
            "metrics".to_string(),
        );

        // A write at LSN 100 lands and flushes: the partition stamps LSN 100.
        let earlier = RedoRecord {
            version: 1,
            ops: vec![metric_samples_sub("metrics", 1_700_000_000_000, 1.0)],
            calvin_stamp: None,
        };
        let record = WalRecord::new(WalRecordArgs {
            record_type: RecordType::TransactionRedo as u32,
            lsn: 100,
            tenant_id: TID,
            vshard_id: 0,
            database_id: 0,
            payload: earlier.to_bytes().expect("encode redo"),
            encryption_key: None,
            preamble_bytes: None,
        })
        .expect("wal record");
        core.replay_transaction_redo_wal(
            std::slice::from_ref(&record),
            1,
            &nodedb_wal::TombstoneSet::new(),
        )
        .expect("seed replay");
        core.flush_ts_collection(tenant, crate::types::DatabaseId::DEFAULT, "metrics", 0)
            .expect("flush the seeded partition");

        // A committed redo minted at LSN 50 applies afterwards.
        let redo = redo_bytes(vec![metric_samples_sub("metrics", 1_700_000_000_001, 2.0)]);
        let collections = vec!["metrics".to_string()];
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

        let memtable_rows = core
            .columnar_memtables
            .get(&key)
            .map_or(0, |m| m.row_count());
        let flushed_rows = core
            .ts_registries
            .get(&key)
            .map_or(0, |registry| registry.total_row_count());
        assert_eq!(
            memtable_rows + flushed_rows,
            2,
            "the online apply installs its sample despite the higher partition stamp"
        );
        assert_eq!(
            memtable_rows, 0,
            "the sample applied below the stamp is flushed, so a restart that skips its \
             record still finds it on disk"
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
        core.kv_engine.put_with_absolute_expiry(
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
        );
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
            payload:
                crate::control::server::wal_dispatch::encode_timeseries_batch_payload_with_format(
                    "metrics",
                    &lines,
                    None,
                    "ilp-msgpack",
                )
                .expect("encode ingest"),
        }
    }

    /// The vector node an install inserted is withdrawn with the rest of the
    /// record, and the index it created is gone.
    #[test]
    fn an_install_failure_withdraws_the_vector_node_it_inserted() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _req, _resp) = make_core_with_dir(dir.path());
        let vector_put = RedoSubRecord {
            record_type: RecordType::VectorPut as u32,
            payload: zerompk::to_msgpack_vec(&(
                "docs",
                vec![0.5f32, 0.5],
                2usize,
                "",
                None::<String>,
                21u32,
                None::<SyncProvenance>,
            ))
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
