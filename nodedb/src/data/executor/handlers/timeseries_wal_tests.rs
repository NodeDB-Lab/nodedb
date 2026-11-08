// SPDX-License-Identifier: BUSL-1.1

use crate::data::executor::core_loop::CoreLoop;
use crate::types::{DatabaseId, Lsn, TenantId};
use crate::wal::{DecodedBatchRecord, decode_batch_record};
use nodedb_types::Surrogate;
use nodedb_types::columnar::ColumnarWalRecord;
use nodedb_types::sync::wire::SyncProvenance;
use nodedb_wal::WalRecord;
use nodedb_wal::record::{RecordType, WalRecordArgs};
use std::sync::Arc;

/// Holds the bridge endpoints + tempdir alive for the core's lifetime.
/// The test drives replay directly and never ticks the event loop, so
/// the far ends of the bridge are unused — they just must not be
/// dropped mid-test.
struct CoreHarness {
    core: CoreLoop,
    _req_tx: nodedb_bridge::buffer::Producer<crate::bridge::dispatch::BridgeRequest>,
    _resp_rx: nodedb_bridge::buffer::Consumer<crate::bridge::dispatch::BridgeResponse>,
    _dir: tempfile::TempDir,
}

fn make_core() -> CoreHarness {
    use crate::bridge::dispatch::{BridgeRequest, BridgeResponse};
    use nodedb_bridge::buffer::RingBuffer;

    let dir = tempfile::tempdir().expect("tempdir");
    let (req_tx, req_rx) = RingBuffer::channel::<BridgeRequest>(64);
    let (resp_tx, resp_rx) = RingBuffer::channel::<BridgeResponse>(64);
    let core = CoreLoop::open(
        0,
        req_rx,
        resp_tx,
        dir.path(),
        Arc::new(nodedb_types::OrdinalClock::new()),
        crate::data::executor::core_loop::test_governor(),
    )
    .expect("open core");
    CoreHarness {
        core,
        _req_tx: req_tx,
        _resp_rx: resp_rx,
        _dir: dir,
    }
}

/// A one-row msgpack `Value::Object` columnar payload: `{id: "r1", col: "v"}`.
fn row_payload(col: &str, value: &str) -> Vec<u8> {
    let mut obj = std::collections::HashMap::new();
    obj.insert(
        col.to_string(),
        nodedb_types::Value::String(value.to_string()),
    );
    // Every columnar row carries its `id` identity, as the planner fills
    // it.
    obj.insert("id".to_string(), nodedb_types::Value::String("r1".into()));
    // Mirror the production columnar-insert write path, which encodes rows
    // with the PLAIN msgpack writer (`value_to_msgpack`) and reads them back
    // with `value_from_msgpack`. `zerompk::to_msgpack_vec(&Value)` would emit
    // a tagged `[variant, payload]` array that the plain reader mis-parses.
    nodedb_types::value_to_msgpack(&nodedb_types::Value::Object(obj)).expect("encode row")
}

/// A `TimeseriesBatch`-typed WAL record carrying one ILP line, in the
/// format-preserving five-element timeseries tuple, at `lsn`.
fn ilp_wal_record(collection: &str, lsn: u64, tenant_id: u64, line: &str) -> WalRecord {
    let payload = crate::control::server::wal_dispatch::encode_timeseries_ingest_payload(
        crate::control::server::wal_dispatch::TimeseriesIngestRecord {
            rls_write_check: &nodedb_types::RlsWriteCheck::already_decided_elsewhere(),
            collection,
            payload: line.as_bytes(),
            provenance: None,
            format: "ilp",
            default_timestamp_ms: 1_700_000_000_000,
        },
    )
    .expect("encode timeseries ingest record");
    WalRecord::new(WalRecordArgs {
        record_type: RecordType::TimeseriesBatch as u32,
        lsn,
        tenant_id,
        vshard_id: 0,
        database_id: 0,
        payload,
        encryption_key: None,
        preamble_bytes: None,
    })
    .expect("wal record")
}

/// A flush fired from INSIDE replay may only claim the records whose rows it
/// actually holds.
///
/// Replaying a record can itself fire the ingest handler's record-boundary
/// flush — a full tag dictionary is resolved by flushing first, then taking
/// the record whole. The partition that flush writes contains everything up
/// to the PREVIOUS record and nothing of the in-flight one, so its stamp
/// must name the previous record. Stamping it with the in-flight record
/// makes boot replay skip a record no partition holds: the rows are gone.
///
/// This fails if the replay cursor passes a record before its rows land.
#[test]
fn a_replay_flush_is_stamped_with_the_last_fully_applied_record() {
    let mut h = make_core();
    // One tag value fits; the second record's new host has no headroom, so
    // replaying it flushes at the record boundary before any of its rows
    // land.
    h.core.ts_tuning.max_tag_cardinality = 1;

    let records = vec![
        ilp_wal_record("metrics_stamp", 10, 7, "metrics_stamp,host=h0 value=1i"),
        ilp_wal_record("metrics_stamp", 11, 7, "metrics_stamp,host=h1 value=2i"),
    ];
    h.core
        .replay_timeseries_wal(&records, 1, &nodedb_wal::TombstoneSet::new());

    let key = (
        DatabaseId::new(0),
        TenantId::new(7),
        "metrics_stamp".to_string(),
    );
    let registry = h
        .core
        .ts_registries
        .get(&key)
        .expect("the record-boundary flush registered a partition");
    let stamps: Vec<u64> = registry
        .iter()
        .map(|(_, entry)| entry.meta.last_flushed_wal_lsn)
        .collect();
    assert_eq!(
        stamps,
        vec![10],
        "the partition holds record 10 and none of record 11, so it may \
         claim only record 10"
    );
    let stamp = &h.core.ts_replay.stamps.get(&key).expect("stamp").rows;
    assert!(
        stamp.skips(10) && !stamp.skips(11),
        "the stamp names record 10 only: {stamp:?}"
    );
    assert_eq!(h.core.ts_replay.cursor, None, "the pass clears its cursor");
}

/// A `TimeseriesBatch`-typed WAL record carrying a map-shaped
/// `ColumnarWalRecord` with `kind = "columnar"`, at `lsn`.
fn columnar_wal_record(collection: &str, lsn: u64, tenant_id: u64) -> WalRecord {
    let rec = ColumnarWalRecord {
        kind: "columnar".to_string(),
        collection: collection.to_string(),
        payload: row_payload("name", "alice"),
        provenance: None,
        surrogates: Vec::new(),
        conflict_policy: crate::wal::ColumnarConflictPolicy::replace()
            .encode()
            .expect("encode policy"),
        system_from: None,
        restored_versions: Vec::new(),
    };
    let payload = zerompk::to_msgpack_vec(&rec).expect("encode columnar wal record");
    WalRecord::new(WalRecordArgs {
        record_type: RecordType::TimeseriesBatch as u32,
        lsn,
        tenant_id,
        vshard_id: 0,
        database_id: 0,
        payload,
        encryption_key: None,
        preamble_bytes: None,
    })
    .expect("wal record")
}

/// A replayed `ON CONFLICT DO NOTHING` insert keeps the row its key
/// already holds, as the live insert did.
#[test]
fn a_replayed_do_nothing_insert_keeps_the_existing_row() {
    use nodedb_types::Value;
    use nodedb_types::columnar::{ColumnDef, ColumnType, ColumnarSchema};

    let mut h = make_core();
    let schema = ColumnarSchema {
        columns: vec![
            ColumnDef::required("id", ColumnType::Int64).with_primary_key(),
            ColumnDef::required("v", ColumnType::Int64),
        ],
        version: 1,
    };
    let mut engine = nodedb_columnar::MutationEngine::new("m".to_string(), schema);
    engine
        .insert(&[Value::Integer(1), Value::Integer(10)], 1)
        .expect("seed row");
    h.core.columnar_engines.insert(
        (DatabaseId::new(0), TenantId::new(7), "m".to_string()),
        engine,
    );

    let mut row = std::collections::HashMap::new();
    row.insert("id".to_string(), Value::Integer(1));
    row.insert("v".to_string(), Value::Integer(20));
    let policy = crate::wal::ColumnarConflictPolicy {
        rls_write_check: nodedb_types::RlsWriteCheck::already_decided_elsewhere(),
        intent: nodedb_physical::physical_plan::ColumnarInsertIntent::InsertIfAbsent,
        on_conflict_updates: Vec::new(),
    };
    let rec = ColumnarWalRecord {
        kind: "columnar".to_string(),
        collection: "m".to_string(),
        payload: nodedb_types::value_to_msgpack(&Value::Array(vec![Value::Object(row)]))
            .expect("encode row"),
        provenance: None,
        surrogates: Vec::new(),
        conflict_policy: policy.encode().expect("encode policy"),
        system_from: None,
        restored_versions: Vec::new(),
    };
    let record = WalRecord::new(WalRecordArgs {
        record_type: RecordType::TimeseriesBatch as u32,
        lsn: 40,
        tenant_id: 7,
        vshard_id: 0,
        database_id: 0,
        payload: zerompk::to_msgpack_vec(&rec).expect("encode record"),
        encryption_key: None,
        preamble_bytes: None,
    })
    .expect("wal record");

    h.core.replay_timeseries_wal(
        std::slice::from_ref(&record),
        1,
        &nodedb_wal::TombstoneSet::new(),
    );

    let rows: Vec<Vec<Value>> = h
        .core
        .columnar_engines
        .get(&(DatabaseId::new(0), TenantId::new(7), "m".to_string()))
        .expect("engine")
        .scan_memtable_rows()
        .collect::<Result<_, _>>()
        .expect("read");
    assert_eq!(rows, vec![vec![Value::Integer(1), Value::Integer(10)]]);
}

/// A bitemporal columnar record replays at the ordinals its live insert
/// stamped: two cores replaying one record store the same `_ts_system`,
/// so `AS OF` after a restart reads what it read before, and the clock
/// folds past the run.
#[test]
fn a_bitemporal_columnar_record_replays_at_its_carried_ordinals() {
    use nodedb_types::Value;

    let first = 1_700_000_000_000 * nodedb_types::NANOS_PER_MS + 5;
    let rows: Vec<Value> = [10, 20]
        .iter()
        .map(|v| {
            Value::Object(std::collections::HashMap::from([
                ("id".to_string(), Value::Integer(1)),
                ("v".to_string(), Value::Integer(*v)),
            ]))
        })
        .collect();
    let rec = ColumnarWalRecord {
        kind: "columnar".to_string(),
        collection: "bt".to_string(),
        payload: nodedb_types::value_to_msgpack(&Value::Array(rows)).expect("encode rows"),
        provenance: None,
        surrogates: Vec::new(),
        conflict_policy: crate::wal::ColumnarConflictPolicy::replace()
            .encode()
            .expect("encode policy"),
        system_from: Some(first),
        restored_versions: Vec::new(),
    };
    let record = WalRecord::new(WalRecordArgs {
        record_type: RecordType::TimeseriesBatch as u32,
        lsn: 50,
        tenant_id: 7,
        vshard_id: 0,
        database_id: 0,
        payload: zerompk::to_msgpack_vec(&rec).expect("encode record"),
        encryption_key: None,
        preamble_bytes: None,
    })
    .expect("wal record");
    let key = (DatabaseId::new(0), TenantId::new(7), "bt".to_string());

    let stamps = |h: &mut CoreHarness| -> Vec<i64> {
        let mut config = crate::engine::document::store::CollectionConfig::new("bt");
        config.bitemporal = true;
        h.core.doc_configs.insert(key.clone(), config);
        h.core.replay_timeseries_wal(
            std::slice::from_ref(&record),
            1,
            &nodedb_wal::TombstoneSet::new(),
        );
        assert!(h.core.hlc.peek() > first, "the clock folds past the run");
        let engine = h.core.columnar_engines.get(&key).expect("engine");
        let ts_idx = engine.schema().ts_system_idx().expect("bitemporal schema");
        engine
            .scan_memtable_versions(nodedb_columnar::VersionCut::All)
            .map(|row| match row.expect("read").2.get(ts_idx) {
                Some(Value::Integer(ordinal)) => *ordinal,
                other => panic!("no _ts_system ordinal: {other:?}"),
            })
            .collect()
    };
    let mut before = make_core();
    let mut after_restart = make_core();
    let first_run = stamps(&mut before);
    assert_eq!(first_run, vec![first, first + 1], "row i stamps first + i");
    assert_eq!(stamps(&mut after_restart), first_run);
}

#[test]
fn conflict_policy_encoding_always_preserves_the_write_check() {
    let plain = crate::wal::ColumnarConflictPolicy::replace();
    let bytes = plain.encode().expect("encode");
    assert_eq!(
        crate::wal::ColumnarConflictPolicy::decode(&bytes).expect("decode"),
        plain
    );
    assert!(crate::wal::ColumnarConflictPolicy::decode(&[]).is_err());
    let upsert = crate::wal::ColumnarConflictPolicy {
        rls_write_check: nodedb_types::RlsWriteCheck::already_decided_elsewhere(),
        intent: nodedb_physical::physical_plan::ColumnarInsertIntent::Put,
        on_conflict_updates: vec![(
            "v".to_string(),
            nodedb_physical::physical_plan::UpdateValue::Literal(vec![0x05]),
        )],
    };
    let bytes = upsert.encode().expect("encode");
    assert_eq!(
        crate::wal::ColumnarConflictPolicy::decode(&bytes).expect("decode"),
        upsert
    );
}

/// WAL replay threads the record LSN into `replay_task` so
/// `execute_columnar_insert`'s `note_collection_write(task, ..)` call
/// (gated on `task.wal_lsn().is_some()`) fires during WAL replay too, not
/// just on live writes. The note reaches the watermark end-to-end through
/// `replay_timeseries_wal` -> `replay_columnar_payload` ->
/// `execute_columnar_insert`.
#[test]
fn columnar_insert_replay_notes_its_record() {
    let mut h = make_core();
    let record = columnar_wal_record("events_wv", 123, 7);

    h.core.replay_timeseries_wal(
        std::slice::from_ref(&record),
        1,
        &nodedb_wal::TombstoneSet::new(),
    );

    assert_eq!(
        h.core.watermark,
        Lsn::new(123),
        "columnar insert replay must note its record as applied"
    );
}

/// A columnar record already folded into a restored checkpoint must NOT be
/// replayed. Columnar replay is not idempotent, so re-applying an insert
/// re-runs the whole upsert; on a `bitemporal=true` collection it appends a
/// second version outright. The floor is what restores the "from state that
/// does not contain this record" precondition the replay depends on.
#[test]
fn columnar_records_at_or_below_the_floor_are_not_replayed() {
    let mut h = make_core();
    h.core.floors.replay_floors.columnar.set(
        crate::data::executor::applied_prefix::ReplayStamp::through(200),
    );
    let record = columnar_wal_record("events_gated", 150, 7);

    h.core.replay_timeseries_wal(
        std::slice::from_ref(&record),
        1,
        &nodedb_wal::TombstoneSet::new(),
    );

    assert!(
        h.core.columnar_engines.is_empty(),
        "a record at or below the floor is already in the restored \
         checkpoint and must not be applied a second time"
    );
}

/// The floor gates only what the checkpoint covers. A record ABOVE it is
/// absent from the restored state, so gating it would not prevent a
/// duplicate — it would drop the write.
#[test]
fn columnar_records_above_the_floor_still_replay() {
    let mut h = make_core();
    h.core.floors.replay_floors.columnar.set(
        crate::data::executor::applied_prefix::ReplayStamp::through(100),
    );
    let record = columnar_wal_record("events_ungated", 150, 7);

    h.core.replay_timeseries_wal(
        std::slice::from_ref(&record),
        1,
        &nodedb_wal::TombstoneSet::new(),
    );

    assert_eq!(
        h.core.watermark,
        Lsn::new(150),
        "a record above the floor must be applied"
    );
}

/// `TimeseriesBatch` is a shared record type: a `timeseries`-kind record
/// routes to `columnar_memtables` / `ts_registries`, which the columnar
/// checkpoint does not cover. Gating it on the COLUMNAR engines' durability
/// would not deduplicate anything — it would silently drop timeseries
/// writes whose only durable copy is the record being skipped.
#[test]
fn the_columnar_floor_does_not_gate_timeseries_records() {
    let mut h = make_core();
    // A floor far above the record's LSN: if the gate were applied by
    // record type instead of by kind, this would suppress it.
    h.core.floors.replay_floors.columnar.set(
        crate::data::executor::applied_prefix::ReplayStamp::through(10_000),
    );

    let record = ilp_wal_record(
        "metrics_ungated",
        150,
        7,
        "metrics_ungated value=42 1000000000",
    );

    h.core.replay_timeseries_wal(
        std::slice::from_ref(&record),
        1,
        &nodedb_wal::TombstoneSet::new(),
    );

    assert!(
        h.core
            .columnar_memtables
            .keys()
            .any(|(_, t, c)| { *t == TenantId::new(7) && c == "metrics_ungated" }),
        "a timeseries record must replay regardless of the columnar floor"
    );
}

/// Replaying an ingest record larger than the memtable's hard limit must
/// retain every row. The record is already committed, so dropping rows
/// past the ceiling loses durable data on restart. The record lands whole,
/// and the resident footprint overshoots the limit.
#[test]
fn oversized_timeseries_record_replays_every_row() {
    let mut h = make_core();
    // 100 rows charge 16 B each (1600 B) against a 64 B ceiling.
    h.core.ts_tuning.memtable_hard_limit_bytes = 64;
    h.core.ts_tuning.memtable_budget_bytes = 32;

    const N: u64 = 100;
    let lines: Vec<String> = (0..N)
        .map(|i| format!("metrics_big value={i} {}", 1_000_000_000 + i * 1_000_000))
        .collect();
    let record = ilp_wal_record("metrics_big", 200, 7, &lines.join("\n"));

    h.core.replay_timeseries_wal(
        std::slice::from_ref(&record),
        1,
        &nodedb_wal::TombstoneSet::new(),
    );

    let key = (
        DatabaseId::new(0),
        TenantId::new(7),
        "metrics_big".to_string(),
    );
    let memtable_rows = h
        .core
        .columnar_memtables
        .get(&key)
        .expect("memtable created by replay")
        .row_count();
    let partition_rows = h
        .core
        .ts_registries
        .get(&key)
        .map_or(0, |registry| registry.total_row_count());
    assert_eq!(
        memtable_rows + partition_rows,
        N,
        "every row of an over-limit replayed record must be retained; the \
         record lands whole, then flushes whole once it settled"
    );
}

/// A timeseries record that names an empty ingest format is not a shape
/// any writer produces. Restart replay halts on it with nothing applied.
/// It never guesses a format from the payload bytes.
#[test]
fn a_timeseries_record_without_a_format_halts_replay() {
    let mut h = make_core();
    let sample = zerompk::to_msgpack_vec(&(
        "metrics_unformatted".to_string(),
        vec![(1u64, 1_000i64, 42.0f64)],
        Option::<SyncProvenance>::None,
    ))
    .expect("encode sample payload");
    let payload = zerompk::to_msgpack_vec(&(
        "timeseries".to_string(),
        "metrics_unformatted".to_string(),
        sample,
        Option::<SyncProvenance>::None,
        String::new(),
        1_000i64,
    ))
    .expect("encode timeseries tuple");
    let record = WalRecord::new(WalRecordArgs {
        record_type: RecordType::TimeseriesBatch as u32,
        lsn: 300,
        tenant_id: 7,
        vshard_id: 0,
        database_id: 0,
        payload,
        encryption_key: None,
        preamble_bytes: None,
    })
    .expect("wal record");

    h.core.replay_timeseries_wal(
        std::slice::from_ref(&record),
        1,
        &nodedb_wal::TombstoneSet::new(),
    );

    assert!(
        h.core.replay_halted(),
        "a record with no ingest format halts replay"
    );
    assert!(
        h.core.replay_halt_error().is_some(),
        "the halt carries an error for startup to return"
    );
    assert!(
        h.core.columnar_memtables.is_empty(),
        "nothing of the refused record is applied"
    );
}

/// A timeseries record naming a format the ingest handler does not
/// accept halts replay. It is never logged and skipped.
#[test]
fn a_timeseries_record_with_an_unknown_format_halts_replay() {
    let mut h = make_core();
    let payload = crate::control::server::wal_dispatch::encode_timeseries_ingest_payload(
        crate::control::server::wal_dispatch::TimeseriesIngestRecord {
            rls_write_check: &nodedb_types::RlsWriteCheck::already_decided_elsewhere(),
            collection: "metrics_unknown",
            payload: b"metrics_unknown value=1",
            provenance: None,
            format: "samples",
            default_timestamp_ms: 1_700_000_000_000,
        },
    )
    .expect("encode timeseries ingest record");
    let record = WalRecord::new(WalRecordArgs {
        record_type: RecordType::TimeseriesBatch as u32,
        lsn: 310,
        tenant_id: 7,
        vshard_id: 0,
        database_id: 0,
        payload,
        encryption_key: None,
        preamble_bytes: None,
    })
    .expect("wal record");

    h.core.replay_timeseries_wal(
        std::slice::from_ref(&record),
        1,
        &nodedb_wal::TombstoneSet::new(),
    );

    assert!(
        h.core.replay_halted(),
        "a record with an unknown ingest format halts replay"
    );
    assert!(
        h.core.columnar_memtables.is_empty(),
        "nothing of the refused record is applied"
    );
}

#[test]
fn decodes_map_columnar_record_with_surrogates() {
    let prov = SyncProvenance {
        producer_id: 1,
        epoch: 0,
        stream_id: 5,
        seq: 42,
    };
    let rec = ColumnarWalRecord {
        kind: "columnar".to_string(),
        collection: "events".to_string(),
        payload: vec![7, 8, 9],
        provenance: Some(prov.clone()),
        surrogates: vec![Surrogate::new(100), Surrogate::new(101)],
        conflict_policy: crate::wal::ColumnarConflictPolicy::replace()
            .encode()
            .expect("encode policy"),
        system_from: None,
        restored_versions: Vec::new(),
    };
    let bytes = zerompk::to_msgpack_vec(&rec).expect("encode map record");

    let DecodedBatchRecord {
        kind,
        collection,
        payload,
        provenance,
        format,
        surrogates,
        ..
    } = decode_batch_record(&bytes).expect("decode map record");
    assert_eq!(kind, "columnar");
    assert_eq!(collection, "events");
    assert_eq!(payload, vec![7, 8, 9]);
    assert_eq!(provenance, Some(prov));
    assert_eq!(format, None);
    assert_eq!(surrogates, vec![Surrogate::new(100), Surrogate::new(101)]);
}

/// The four-element and two-element tuples are not record shapes.
#[test]
fn short_tuples_are_not_record_shapes() {
    let prov: Option<SyncProvenance> = None;
    let four = zerompk::to_msgpack_vec(&(
        "columnar".to_string(),
        "events".to_string(),
        vec![1u8, 2, 3],
        prov,
    ))
    .expect("encode four-element tuple");
    assert!(decode_batch_record(&four).is_err());

    let two = zerompk::to_msgpack_vec(&("metrics".to_string(), vec![1u8, 2]))
        .expect("encode two-element tuple");
    assert!(decode_batch_record(&two).is_err());
}

/// Every timeseries ingest record carries its statement instant. The
/// five-element tuple, which carried none, is not a record shape.
#[test]
fn a_five_element_timeseries_tuple_is_not_a_record_shape() {
    let bytes = zerompk::to_msgpack_vec(&(
        "timeseries".to_string(),
        "cpu".to_string(),
        vec![0x90u8],
        None::<SyncProvenance>,
        "ilp-msgpack".to_string(),
    ))
    .expect("encode five-element tuple");
    assert!(decode_batch_record(&bytes).is_err());
}

#[test]
fn an_autocommit_ingest_record_carries_its_default_timestamp() {
    let bytes = crate::control::server::wal_dispatch::encode_timeseries_ingest_payload(
        crate::control::server::wal_dispatch::TimeseriesIngestRecord {
            rls_write_check: &nodedb_types::RlsWriteCheck::already_decided_elsewhere(),
            collection: "cpu",
            payload: b"cpu value=1",
            provenance: None,
            format: "ilp",
            default_timestamp_ms: 1_700_000_000_123,
        },
    )
    .expect("encode ingest record");
    let DecodedBatchRecord {
        kind,
        collection,
        format,
        default_timestamp_ms,
        ..
    } = decode_batch_record(&bytes).expect("decode ingest record");
    assert_eq!(kind, "timeseries");
    assert_eq!(collection, "cpu");
    assert_eq!(format.as_deref(), Some("ilp"));
    assert_eq!(default_timestamp_ms, Some(1_700_000_000_123));
}
