// SPDX-License-Identifier: BUSL-1.1

use nodedb_types::sync::wire::{AckStatus, SyncAckResult, SyncOutcome, SyncProvenance};
use nodedb_types::{RlsWriteCheck, Value};

use super::*;
use crate::bridge::scan_filter::{FilterOp, ScanFilter};
use crate::data::executor::core_loop::tests::{make_core_with_dir, make_default_task};
use crate::data::executor::handlers::timeseries::{TimeseriesApplyMode, TimeseriesIngestExec};

fn provenance(seq: u64) -> SyncProvenance {
    SyncProvenance {
        producer_id: 19,
        epoch: 1,
        stream_id: 7,
        seq,
    }
}

fn policy(field: &str, value: Value) -> RlsWriteCheck {
    RlsWriteCheck::Predicate(
        zerompk::to_msgpack_vec(&vec![ScanFilter {
            field: field.into(),
            op: FilterOp::Eq,
            value,
            clauses: Vec::new(),
            expr: None,
        }])
        .expect("encode predicate"),
    )
}

fn assert_refused(response: &Response, prov: &SyncProvenance) {
    assert_eq!(response.status, Status::Error);
    assert!(response.write_set.is_empty());
    assert!(
        matches!(response.error_code.as_deref(), Some(ErrorCode::SyncRejected {
        provenance, applied_seq: 1,
        violation: nodedb_types::sync::violation::ViolationType::RlsPolicyViolation { .. },
    }) if provenance == prov)
    );
}

fn assert_applied(response: &Response, seq: u64) {
    assert_eq!(response.status, Status::Ok, "{:?}", response.error_code);
    let ack: SyncAckResult = zerompk::from_msgpack(&response.payload).expect("decode sync ACK");
    assert_eq!(ack.outcome, SyncOutcome::Ack(AckStatus::Applied));
    assert_eq!(ack.applied_seq, seq);
}

#[test]
fn columnar_batch_policy_refusal_consumes_without_installing_a_prefix() {
    use nodedb_types::columnar::{ColumnDef, ColumnType, ColumnarSchema};
    let dir = tempfile::tempdir().expect("tempdir");
    let (mut core, _requests, _responses) = make_core_with_dir(dir.path());
    let task = make_default_task();
    let schema = ColumnarSchema::new(vec![
        ColumnDef::required("id", ColumnType::String).with_primary_key(),
        ColumnDef::required("v", ColumnType::Int64),
    ])
    .expect("schema");
    let schema = zerompk::to_msgpack_vec(&schema).expect("encode schema");
    let row = |id: &str, v: i64| {
        Value::Object(std::collections::HashMap::from([
            ("id".into(), Value::String(id.into())),
            ("v".into(), Value::Integer(v)),
        ]))
    };
    let denied =
        nodedb_types::value_to_msgpack(&Value::Array(vec![row("first", 2), row("second", 1)]))
            .expect("payload");
    let check = policy("v", Value::Integer(2));
    let first = provenance(1);
    fn params<'a>(
        payload: &'a [u8],
        prov: &'a SyncProvenance,
        schema: &'a [u8],
        check: &'a RlsWriteCheck,
    ) -> ColumnarInsertParams<'a> {
        ColumnarInsertParams {
            collection: "sync_rows",
            payload,
            format: "msgpack",
            intent: ColumnarInsertIntent::Insert,
            on_conflict_updates: &[],
            surrogates: &[],
            schema_bytes: schema,
            provenance: Some(prov),
            system_from: None,
            restored_versions: &[],
            rls_write_check: check,
            returning: None,
            rls_filters: &[],
            spatial_undo: None,
        }
    }
    assert_refused(
        &core.execute_columnar_insert(&task, params(&denied, &first, &schema, &check)),
        &first,
    );
    let key = (
        task.request.database_id,
        task.request.tenant_id,
        "sync_rows".to_string(),
    );
    assert_eq!(
        core.columnar_engines
            .get(&key)
            .expect("columnar engine")
            .live_row_count(),
        0
    );
    let accepted =
        nodedb_types::value_to_msgpack(&Value::Array(vec![row("next", 2)])).expect("payload");
    assert_applied(
        &core.execute_columnar_insert(&task, params(&accepted, &provenance(2), &schema, &check)),
        2,
    );
    assert_eq!(
        core.columnar_engines
            .get(&key)
            .expect("columnar engine")
            .live_row_count(),
        1
    );
}

#[test]
fn timeseries_batch_policy_refusal_consumes_before_schema_or_rows_exist() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (mut core, _requests, _responses) = make_core_with_dir(dir.path());
    let task = make_default_task();
    let check = policy("owner", Value::String("alice".into()));
    let first = provenance(1);
    fn params<'a>(
        task: &'a ExecutionTask,
        payload: &'a [u8],
        prov: &'a SyncProvenance,
        check: &'a RlsWriteCheck,
    ) -> TimeseriesIngestExec<'a> {
        TimeseriesIngestExec {
            task,
            tid: task.request.tenant_id,
            collection: "cpu",
            payload,
            format: "ilp",
            wal_lsn: None,
            provenance: Some(prov),
            mode: TimeseriesApplyMode::Immediate,
            rls_write_check: check,
            returning: None,
            rls_filters: &[],
        }
    }
    let refused = core.execute_timeseries_ingest(params(
        &task,
        b"cpu,owner=alice value=1i 1000000\ncpu,owner=mallory value=2i 2000000\n",
        &first,
        &check,
    ));
    assert_refused(&refused, &first);
    let key = (
        task.request.database_id,
        task.request.tenant_id,
        "cpu".to_string(),
    );
    assert!(!core.columnar_memtables.contains_key(&key));
    assert!(!core.ts_registries.contains_key(&key));
    let accepted = core.execute_timeseries_ingest(params(
        &task,
        b"cpu,owner=alice value=3i 3000000\n",
        &provenance(2),
        &check,
    ));
    assert_applied(&accepted, 2);
    assert_eq!(
        core.columnar_memtables
            .get(&key)
            .expect("timeseries memtable")
            .row_count(),
        1
    );
}

#[derive(Clone, Copy)]
enum BatchKind {
    Columnar,
    Timeseries,
}

fn batch_image(kind: BatchKind, value: i64) -> Vec<u8> {
    match kind {
        BatchKind::Columnar => nodedb_types::value_to_msgpack(&Value::Array(vec![Value::Object(
            std::collections::HashMap::from([
                ("id".into(), Value::String("row".into())),
                ("v".into(), Value::Integer(value)),
            ]),
        )]))
        .expect("encode row"),
        BatchKind::Timeseries => format!("cpu value={value}i 3000000\n").into_bytes(),
    }
}

fn batch_check(kind: BatchKind) -> RlsWriteCheck {
    policy(
        match kind {
            BatchKind::Columnar => "v",
            BatchKind::Timeseries => "value",
        },
        Value::Integer(2),
    )
}

fn record_payload(kind: BatchKind, value: i64, seq: u64) -> Vec<u8> {
    use crate::control::server::wal_dispatch::{
        ColumnarBatchRecord, TimeseriesIngestRecord, encode_columnar_batch_payload,
        encode_timeseries_ingest_payload,
    };
    let payload = batch_image(kind, value);
    let prov = provenance(seq);
    let check = batch_check(kind);
    match kind {
        BatchKind::Columnar => encode_columnar_batch_payload(ColumnarBatchRecord {
            collection: "sync_rows",
            payload: &payload,
            provenance: Some(&prov),
            surrogates: &[],
            conflict_policy: &crate::wal::ColumnarConflictPolicy {
                intent: ColumnarInsertIntent::Insert,
                on_conflict_updates: Vec::new(),
                rls_write_check: check,
            },
            system_from: None,
            restored_versions: &[],
        })
        .expect("encode columnar intent"),
        BatchKind::Timeseries => encode_timeseries_ingest_payload(TimeseriesIngestRecord {
            collection: "cpu",
            payload: &payload,
            provenance: Some(&prov),
            format: "ilp",
            default_timestamp_ms: 1000,
            rls_write_check: &check,
        })
        .expect("encode timeseries intent"),
    }
}

fn apply_batch(core: &mut CoreLoop, kind: BatchKind, value: i64, seq: u64) -> Response {
    let task = make_default_task();
    let payload = batch_image(kind, value);
    let prov = provenance(seq);
    let check = batch_check(kind);
    match kind {
        BatchKind::Columnar => core.execute_columnar_insert(
            &task,
            ColumnarInsertParams {
                collection: "sync_rows",
                payload: &payload,
                format: "msgpack",
                intent: ColumnarInsertIntent::Insert,
                on_conflict_updates: &[],
                surrogates: &[],
                schema_bytes: &[],
                provenance: Some(&prov),
                system_from: None,
                restored_versions: &[],
                rls_write_check: &check,
                returning: None,
                rls_filters: &[],
                spatial_undo: None,
            },
        ),
        BatchKind::Timeseries => core.execute_timeseries_ingest(TimeseriesIngestExec {
            task: &task,
            tid: task.request.tenant_id,
            collection: "cpu",
            payload: &payload,
            format: "ilp",
            wal_lsn: None,
            provenance: Some(&prov),
            mode: TimeseriesApplyMode::Immediate,
            rls_write_check: &check,
            returning: None,
            rls_filters: &[],
        }),
    }
}

fn row_count(core: &CoreLoop, kind: BatchKind) -> usize {
    let task = make_default_task();
    let collection = match kind {
        BatchKind::Columnar => "sync_rows",
        BatchKind::Timeseries => "cpu",
    };
    let key = (
        task.request.database_id,
        task.request.tenant_id,
        collection.to_string(),
    );
    match kind {
        BatchKind::Columnar => core
            .columnar_engines
            .get(&key)
            .map_or(0, |engine| engine.live_row_count()),
        BatchKind::Timeseries => core
            .columnar_memtables
            .get(&key)
            .map_or(0, |table| table.row_count() as usize),
    }
}

#[test]
fn refused_intents_without_abort_markers_replay_the_policy_and_allow_the_next_sequence() {
    use crate::types::{DatabaseId, TenantId, VShardId};
    use crate::wal::manager::{NO_APPLY_KEY, WalManager};
    use nodedb_wal::TombstoneSet;
    for kind in [BatchKind::Columnar, BatchKind::Timeseries] {
        let directory = tempfile::tempdir().expect("tempdir");
        let wal_path = directory.path().join("writes.wal");
        let core_path = directory.path().join("core");
        {
            let wal = WalManager::open_for_testing(&wal_path).expect("WAL");
            wal.appender(NO_APPLY_KEY)
                .with_event_source(crate::event::EventSource::CrdtSync)
                .append_timeseries_batch(
                    TenantId::new(1),
                    VShardId::new(0),
                    DatabaseId::DEFAULT,
                    &record_payload(kind, 1, 1),
                )
                .expect("append intent");
            let (mut core, _requests, _responses) = make_core_with_dir(&core_path);
            assert_refused(&apply_batch(&mut core, kind, 1, 1), &provenance(1));
            assert_eq!(row_count(&core, kind), 0);
            wal.sync().expect("sync intent without outcome markers");
        }
        {
            let wal = WalManager::open_for_testing(&wal_path).expect("WAL");
            let records = wal.replay().expect("records");
            assert_eq!(
                records.len(),
                1,
                "crash precedes abort and frontier records"
            );
            let (mut core, _requests, _responses) = make_core_with_dir(&core_path);
            core.replay_all_wal(&records, 1, &TombstoneSet::new())
                .expect("replay refusal");
            assert_eq!(core.sync_hwm_value(19, 7), 1);
            assert_eq!(row_count(&core, kind), 0);
            wal.appender(NO_APPLY_KEY)
                .with_event_source(crate::event::EventSource::CrdtSync)
                .append_timeseries_batch(
                    TenantId::new(1),
                    VShardId::new(0),
                    DatabaseId::DEFAULT,
                    &record_payload(kind, 2, 2),
                )
                .expect("append next intent");
            assert_applied(&apply_batch(&mut core, kind, 2, 2), 2);
            assert_eq!(row_count(&core, kind), 1);
            wal.sync().expect("sync next intent");
        }
        {
            let wal = WalManager::open_for_testing(&wal_path).expect("WAL");
            let records = wal.replay().expect("records");
            let (mut core, _requests, _responses) = make_core_with_dir(&core_path);
            core.replay_all_wal(&records, 1, &TombstoneSet::new())
                .expect("replay twice");
            assert_eq!(core.sync_hwm_value(19, 7), 2);
            assert_eq!(row_count(&core, kind), 1);
        }
    }
}

#[test]
fn a_grouped_policy_refusal_halts_recovery_and_restores_the_prior_frontier() {
    use crate::wal::{RedoRecord, RedoSubRecord};
    use nodedb_wal::{
        TombstoneSet, WalRecord,
        record::{RecordType, WalRecordArgs},
    };
    for kind in [BatchKind::Columnar, BatchKind::Timeseries] {
        let directory = tempfile::tempdir().expect("tempdir");
        let (mut core, _requests, _responses) = make_core_with_dir(directory.path());
        let redo = RedoRecord {
            version: 1,
            ops: vec![RedoSubRecord {
                record_type: RecordType::TimeseriesBatch as u32,
                payload: record_payload(kind, 1, 1),
            }],
            calvin_stamp: None,
            cross_shard_applied: None,
            row_sources: Vec::new(),
            publishes: Vec::new(),
            row_changes: Vec::new(),
            user_roles: Vec::new(),
            sum_targets: Vec::new(),
        };
        let record = WalRecord::new(WalRecordArgs {
            record_type: RecordType::TransactionRedo as u32,
            lsn: 10,
            tenant_id: 1,
            vshard_id: 0,
            database_id: 0,
            payload: redo.to_bytes().expect("encode redo"),
            encryption_key: None,
            preamble_bytes: None,
        })
        .expect("record");
        assert!(
            core.replay_all_wal(&[record], 1, &TombstoneSet::new())
                .is_err()
        );
        assert!(core.is_fail_stopped());
        assert_eq!(core.sync_hwm_value(19, 7), 0);
        assert_eq!(row_count(&core, kind), 0);
    }
}
