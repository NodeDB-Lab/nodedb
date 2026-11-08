// SPDX-License-Identifier: BUSL-1.1

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
        user_roles: Vec::new(),
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
    let (mut producers, mut consumers) = crate::event::bus::create_event_bus_with_capacity(1, 64);
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
    let lines =
        zerompk::to_msgpack_vec(&vec!["other,host=a value=1 1".to_string()]).expect("encode lines");
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
