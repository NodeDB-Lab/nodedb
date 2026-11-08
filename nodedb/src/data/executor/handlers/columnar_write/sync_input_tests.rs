// SPDX-License-Identifier: BUSL-1.1

use super::*;
use crate::control::server::wal_dispatch::{
    ColumnarBatchRecord, TimeseriesIngestRecord, encode_columnar_batch_payload,
    encode_timeseries_ingest_payload,
};
use crate::data::executor::core_loop::tests::{make_core_with_dir, make_default_task};
use crate::data::executor::handlers::timeseries::{TimeseriesApplyMode, TimeseriesIngestExec};
use crate::types::{DatabaseId, TenantId, VShardId};
use crate::wal::manager::{NO_APPLY_KEY, WalManager};
use nodedb_types::sync::wire::{SyncAckResult, SyncOutcome};
use nodedb_types::{RlsWriteCheck, Value};
use nodedb_wal::TombstoneSet;

#[derive(Clone, Copy)]
enum Source {
    Columnar,
    Timeseries,
}

fn provenance(seq: u64) -> SyncProvenance {
    SyncProvenance {
        producer_id: 21,
        epoch: 1,
        stream_id: 9,
        seq,
    }
}

fn accepted(source: Source) -> Vec<u8> {
    match source {
        Source::Columnar => nodedb_types::value_to_msgpack(&Value::Array(vec![Value::Object(
            std::collections::HashMap::from([
                ("id".into(), Value::String("next".into())),
                ("value".into(), Value::Float(2.0)),
            ]),
        )]))
        .expect("encode rows"),
        Source::Timeseries => b"cpu value=2i 3000000\n".to_vec(),
    }
}

fn cases() -> Vec<(Source, Vec<u8>)> {
    vec![
        (
            Source::Columnar,
            nodedb_types::value_to_msgpack(&Value::Array(vec![Value::Integer(1)]))
                .expect("encode nonobject row"),
        ),
        (Source::Timeseries, vec![0xff]),
        (Source::Timeseries, b"cpu missing-fields".to_vec()),
        (Source::Timeseries, Vec::new()),
        (Source::Timeseries, b"other value=1i 1000000\n".to_vec()),
    ]
}

fn apply(core: &mut CoreLoop, source: Source, payload: &[u8], seq: u64) -> Response {
    let task = make_default_task();
    let prov = provenance(seq);
    match source {
        Source::Columnar => core.execute_columnar_insert(
            &task,
            ColumnarInsertParams {
                collection: "rows",
                payload,
                format: "msgpack",
                intent: ColumnarInsertIntent::Insert,
                on_conflict_updates: &[],
                surrogates: &[],
                schema_bytes: &[],
                provenance: Some(&prov),
                system_from: None,
                restored_versions: &[],
                rls_write_check: &RlsWriteCheck::NoPolicyApplies,
                returning: None,
                rls_filters: &[],
                spatial_undo: None,
            },
        ),
        Source::Timeseries => core.execute_timeseries_ingest(TimeseriesIngestExec {
            task: &task,
            tid: task.request.tenant_id,
            collection: "cpu",
            payload,
            format: "ilp",
            wal_lsn: None,
            provenance: Some(&prov),
            mode: TimeseriesApplyMode::Immediate,
            rls_write_check: &RlsWriteCheck::NoPolicyApplies,
            returning: None,
            rls_filters: &[],
        }),
    }
}

fn encoded(source: Source, payload: &[u8], seq: u64) -> Vec<u8> {
    let prov = provenance(seq);
    match source {
        Source::Columnar => encode_columnar_batch_payload(ColumnarBatchRecord {
            collection: "rows",
            payload,
            provenance: Some(&prov),
            surrogates: &[],
            conflict_policy: &crate::wal::ColumnarConflictPolicy::replace(),
            system_from: None,
            restored_versions: &[],
        })
        .expect("encode columnar intent"),
        Source::Timeseries => encode_timeseries_ingest_payload(TimeseriesIngestRecord {
            collection: "cpu",
            payload,
            provenance: Some(&prov),
            format: "ilp",
            default_timestamp_ms: 1000,
            rls_write_check: &RlsWriteCheck::NoPolicyApplies,
        })
        .expect("encode timeseries intent"),
    }
}

fn count(core: &CoreLoop, source: Source) -> usize {
    let task = make_default_task();
    let key = (
        task.request.database_id,
        task.request.tenant_id,
        match source {
            Source::Columnar => "rows",
            Source::Timeseries => "cpu",
        }
        .to_string(),
    );
    match source {
        Source::Columnar => core
            .columnar_engines
            .get(&key)
            .map_or(0, |engine| engine.live_row_count()),
        Source::Timeseries => core
            .columnar_memtables
            .get(&key)
            .map_or(0, |table| table.row_count() as usize),
    }
}

fn assert_consumed(response: &Response) {
    assert_eq!(response.status, Status::Error);
    assert!(response.write_set.is_empty());
    assert!(
        matches!(response.error_code.as_deref(), Some(ErrorCode::SyncRejected {
        provenance: actual, applied_seq: 1, ..
    }) if actual == &provenance(1)),
        "{:?}",
        response.error_code
    );
}

fn assert_next(response: &Response) {
    assert_eq!(response.status, Status::Ok, "{:?}", response.error_code);
    let ack: SyncAckResult = zerompk::from_msgpack(&response.payload).expect("typed ACK");
    assert_eq!(ack.outcome, SyncOutcome::Ack(AckStatus::Applied));
    assert_eq!(ack.applied_seq, 2);
}

#[test]
fn input_only_refusals_leave_no_rows_and_admit_the_next_sequence() {
    for (source, denied) in cases() {
        let directory = tempfile::tempdir().expect("tempdir");
        let (mut core, _requests, _responses) = make_core_with_dir(directory.path());
        assert_consumed(&apply(&mut core, source, &denied, 1));
        assert_eq!(count(&core, source), 0);
        assert_next(&apply(&mut core, source, &accepted(source), 2));
        assert_eq!(count(&core, source), 1);
    }
}

#[test]
fn input_only_refusals_replay_without_abort_markers_across_two_reopens() {
    for (source, denied) in cases() {
        let directory = tempfile::tempdir().expect("tempdir");
        let wal_path = directory.path().join("input.wal");
        let core_path = directory.path().join("core");
        {
            let wal = WalManager::open_for_testing(&wal_path).expect("WAL");
            wal.appender(NO_APPLY_KEY)
                .with_event_source(crate::event::EventSource::CrdtSync)
                .append_timeseries_batch(
                    TenantId::new(1),
                    VShardId::new(0),
                    DatabaseId::DEFAULT,
                    &encoded(source, &denied, 1),
                )
                .expect("append intent");
            let (mut core, _requests, _responses) = make_core_with_dir(&core_path);
            assert_consumed(&apply(&mut core, source, &denied, 1));
            wal.sync().expect("sync without abort or frontier markers");
        }
        {
            let wal = WalManager::open_for_testing(&wal_path).expect("WAL");
            let records = wal.replay().expect("records");
            assert_eq!(records.len(), 1);
            let (mut core, _requests, _responses) = make_core_with_dir(&core_path);
            core.replay_all_wal(&records, 1, &TombstoneSet::new())
                .expect("replay refusal");
            assert_eq!(core.sync_hwm_value(21, 9), 1);
            assert_eq!(count(&core, source), 0);
            let next = accepted(source);
            wal.appender(NO_APPLY_KEY)
                .with_event_source(crate::event::EventSource::CrdtSync)
                .append_timeseries_batch(
                    TenantId::new(1),
                    VShardId::new(0),
                    DatabaseId::DEFAULT,
                    &encoded(source, &next, 2),
                )
                .expect("append next");
            assert_next(&apply(&mut core, source, &next, 2));
            wal.sync().expect("sync next");
        }
        {
            let wal = WalManager::open_for_testing(&wal_path).expect("WAL");
            let records = wal.replay().expect("records");
            let (mut core, _requests, _responses) = make_core_with_dir(&core_path);
            core.replay_all_wal(&records, 1, &TombstoneSet::new())
                .expect("replay twice");
            assert_eq!(core.sync_hwm_value(21, 9), 2);
            assert_eq!(count(&core, source), 1);
        }
    }
}
