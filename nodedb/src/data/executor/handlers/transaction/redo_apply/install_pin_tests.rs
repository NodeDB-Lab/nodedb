// SPDX-License-Identifier: BUSL-1.1

//! A pinned record's install derives each row body under its pins, on the
//! whole-record path, the stream path, and restart replay.
//!
//! The `tickets` collection takes rows `{status}`. A later DDL on a replica
//! adds a field the proposer never saw: a typeguard VALUE or a GENERATED
//! column `lowered = LOWER(status)`.

use nodedb_physical::physical_plan::{
    DocConfigWire, EnforcementOptions, GeneratedColumnSpec, RedoPins, RedoSumTargets, StorageMode,
    TextFieldSet,
};
use nodedb_types::columnar::{ColumnDef, ColumnType, StrictSchema};
use nodedb_types::{DatabaseId, StorageKey, Surrogate, TenantId, TypeGuardFieldDef, Value};
use nodedb_wal::{TombstoneSet, WalRecord};

use super::entry::CommittedRedo;
use super::judged_tests::{pin_of, pins};
use super::stream_validate::tests::{
    A1, A2, BALANCES, CHARGES, Hold, LSN, TID, charge, fold_targets, framed_pinned, hold,
    plain_core, record,
};
use super::test_commit::doc_put_sub_record;
use crate::bridge::envelope::{Response, Status};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::core_loop::tests::make_default_task;
use crate::data::executor::handlers::document::write::RegisterDocumentCollectionParams;
use crate::data::executor::task::ExecutionTask;
use crate::types::Lsn;
use crate::wal::RedoSubRecord;

const COLL: &str = "tickets";

/// The ways a committed record's rows install.
#[derive(Debug, Clone, Copy)]
enum Path {
    Whole,
    /// The stream path, at this spill threshold.
    Stream(usize),
}

const PATHS: [Path; 3] = [Path::Whole, Path::Stream(0), Path::Stream(usize::MAX)];

fn lowered() -> nodedb_query::expr::SqlExpr {
    nodedb_query::expr::SqlExpr::Function {
        name: "lower".into(),
        args: vec![nodedb_query::expr::SqlExpr::Column("status".into())],
    }
}

fn schemaless_wire() -> DocConfigWire {
    DocConfigWire {
        bitemporal: false,
        crdt_enabled: false,
        enforcement: EnforcementOptions::default(),
        storage_mode: StorageMode::Schemaless,
        index_paths: Vec::new(),
        declared_key: None,
        vector_primary: None,
        declared_columns: Vec::new(),
        vector_fields: Vec::new(),
    }
}

/// The schemaless wire with a later `lowered` typeguard VALUE.
fn guarded_wire() -> DocConfigWire {
    let mut wire = schemaless_wire();
    wire.enforcement.type_guards = vec![TypeGuardFieldDef {
        field: "lowered".into(),
        type_expr: "STRING".into(),
        required: false,
        check_expr: None,
        default_expr: None,
        value_expr: Some("LOWER(status)".into()),
    }];
    wire
}

/// A strict schema of `status`, at version 1.
fn strict_v1() -> StrictSchema {
    StrictSchema::new(vec![ColumnDef::nullable("status", ColumnType::String)]).expect("schema")
}

/// [`strict_v1`] with `added` appended at version 2.
fn strict_v2(added: &str) -> StrictSchema {
    let mut column = ColumnDef::nullable(added, ColumnType::String);
    column.added_at_version = 2;
    let mut schema = strict_v1();
    schema.columns.push(column);
    schema.version = 2;
    schema
}

fn strict_wire(schema: StrictSchema) -> DocConfigWire {
    DocConfigWire {
        storage_mode: StorageMode::Strict { schema },
        ..schemaless_wire()
    }
}

/// The strict wire with a later GENERATED column `lowered`.
fn generated_wire() -> DocConfigWire {
    let mut wire = strict_wire(strict_v2("lowered"));
    wire.enforcement.generated_columns = vec![GeneratedColumnSpec {
        name: "lowered".into(),
        expr: lowered(),
        depends_on: vec!["status".into()],
    }];
    wire
}

/// Register `wire` as the current config of `tickets`, with `digest`.
fn register(core: &mut CoreLoop, wire: &DocConfigWire, digest: u64) {
    register_as(core, COLL, wire, digest);
}

/// Register `wire` as the current config of `collection`, with `digest`.
fn register_as(core: &mut CoreLoop, collection: &str, wire: &DocConfigWire, digest: u64) {
    let response = core.execute_register_document_collection(
        &make_default_task(),
        RegisterDocumentCollectionParams {
            tid: TID,
            collection,
            indexes: &wire.index_paths,
            crdt_enabled: wire.crdt_enabled,
            storage_mode: &wire.storage_mode,
            enforcement: &wire.enforcement,
            bitemporal: wire.bitemporal,
            conflict_policy: None,
            timeseries: None,
            vector_primary: None,
            vector_fields: &wire.vector_fields,
            declared_columns: &wire.declared_columns,
            declared_key: wire.declared_key.as_deref(),
            text_fields: &TextFieldSet::default(),
            incarnation: nodedb_types::Hlc::ZERO,
            digest,
        },
    );
    assert_ok(&response);
}

/// A core whose current config of `tickets` is `wire`, registered with its
/// digest.
fn core_with(wire: &DocConfigWire) -> Hold {
    let mut held = hold(plain_core);
    register(&mut held.core, wire, wire.digest().expect("digest"));
    held
}

/// The pins of a record whose proposer saw `tickets` as `wire`.
fn pinned(wire: &DocConfigWire) -> RedoPins {
    pins(vec![pin_of(COLL, wire)])
}

fn put(surrogate: u32, fields: &[(&str, &str)]) -> RedoSubRecord {
    let object = fields
        .iter()
        .map(|(name, value)| (name.to_string(), Value::String(value.to_string())))
        .collect();
    let body = nodedb_types::value_to_msgpack(&Value::Object(object)).expect("encode body");
    doc_put_sub_record(COLL, &format!("t{surrogate}"), &body, surrogate)
}

fn assert_ok(response: &Response) {
    assert_eq!(response.status, Status::Ok, "{:?}", response.error_code);
}

/// The task of a committed record at `lsn`.
fn task_at(lsn: u64) -> ExecutionTask {
    let mut task = make_default_task();
    task.wal_lsn = Some(Lsn::new(lsn));
    task
}

/// What a record names beside its ops: the collections it writes, and the
/// sum targets it folds into.
struct Writes<'a> {
    collections: &'a [String],
    sum_targets: &'a [RedoSumTargets],
}

/// Install `ops` at `lsn` into `tickets`, pinned with `pins`, through
/// `path`.
fn install(
    held: &mut Hold,
    path: Path,
    lsn: u64,
    pins: &RedoPins,
    ops: Vec<RedoSubRecord>,
) -> Response {
    let collections = [COLL.to_string()];
    let writes = Writes {
        collections: &collections,
        sum_targets: &[],
    };
    install_writing(held, path, lsn, pins, ops, &writes)
}

/// [`install`] of a record that names `writes`.
fn install_writing(
    held: &mut Hold,
    path: Path,
    lsn: u64,
    pins: &RedoPins,
    ops: Vec<RedoSubRecord>,
    writes: &Writes<'_>,
) -> Response {
    let mut redo = record(ops);
    redo.sum_targets = writes.sum_targets.to_vec();
    match path {
        Path::Whole => {
            redo.pins = pins.clone();
            let bytes = redo.to_bytes().expect("encode redo");
            held.core.execute_apply_transaction_redo(
                &task_at(lsn),
                TID,
                CommittedRedo {
                    redo: &bytes,
                    collections: writes.collections,
                    sum_targets: writes.sum_targets,
                    pins,
                },
            )
        }
        Path::Stream(spill) => {
            held.core.query_tuning.max_txn_overlay_bytes = spill;
            let content = framed_pinned(&redo, writes.sum_targets, &[], pins);
            let reference = held.wal.write(&content);
            let marker = reference.marker(None, None, Vec::new()).expect("marker");
            let bytes = marker.to_bytes().expect("encode marker");
            held.core.execute_apply_transaction_redo(
                &task_at(lsn),
                TID,
                CommittedRedo {
                    redo: &bytes,
                    collections: writes.collections,
                    sum_targets: writes.sum_targets,
                    pins: &RedoPins::default(),
                },
            )
        }
    }
}

/// The stored bytes of row `surrogate`.
fn stored(core: &CoreLoop, surrogate: u32) -> Vec<u8> {
    stored_in(core, COLL, surrogate)
}

/// The stored bytes of row `surrogate` of `collection`.
fn stored_in(core: &CoreLoop, collection: &str, surrogate: u32) -> Vec<u8> {
    core.sparse
        .get(
            0,
            TID,
            collection,
            &StorageKey::for_surrogate(Surrogate::new(surrogate)),
        )
        .expect("read row")
        .expect("the row is stored")
}

/// The stored schemaless row `surrogate`, decoded.
fn document(core: &CoreLoop, surrogate: u32) -> Value {
    crate::data::executor::doc_format::decode_document_value(&stored(core, surrogate))
        .expect("decode row")
}

/// The stored strict row `surrogate`, decoded under `schema`.
fn tuple(core: &CoreLoop, surrogate: u32, schema: &StrictSchema) -> Value {
    let bytes = stored(core, surrogate);
    crate::data::executor::strict_format::binary_tuple_to_row_value(&bytes, schema)
        .expect("the row decodes under the schema")
}

/// The tuple version row `surrogate` is stored at, read under `schema`.
fn tuple_version(core: &CoreLoop, surrogate: u32, schema: &StrictSchema) -> u32 {
    nodedb_strict::TupleDecoder::new(schema)
        .schema_version(&stored(core, surrogate))
        .expect("tuple version")
}

fn field<'a>(row: &'a Value, name: &str) -> Option<&'a Value> {
    match row {
        Value::Object(fields) => fields.get(name),
        other => panic!("a row is an object, got {other:?}"),
    }
}

/// The row a core whose current config is `current` stores for one
/// record pinned with `pins`, through `path`.
fn installed_document(current: &DocConfigWire, pins: &RedoPins, path: Path) -> Value {
    let mut held = core_with(current);
    let ops = vec![put(1, &[("status", "OPEN")])];
    assert_ok(&install(&mut held, path, LSN, pins, ops));
    document(&held.core, 1)
}

/// A replica with a later typeguard VALUE stores the row a replica
/// without it stores.
#[test]
fn a_later_typeguard_value_changes_no_installed_row() {
    let proposer = schemaless_wire();
    for path in PATHS {
        let without = installed_document(&proposer, &pinned(&proposer), path);
        let with = installed_document(&guarded_wire(), &pinned(&proposer), path);
        assert_eq!(with, without, "{path:?}");
        assert_eq!(field(&with, "lowered"), None, "{path:?}");
    }
}

/// A replica with a later GENERATED column on a strict collection stores
/// the tuple a replica without it stores.
#[test]
fn a_later_generated_column_changes_no_installed_row() {
    let proposer = strict_wire(strict_v1());
    for path in PATHS {
        let mut without = core_with(&proposer);
        let mut with = core_with(&generated_wire());
        for held in [&mut without, &mut with] {
            let ops = vec![put(1, &[("status", "OPEN")])];
            assert_ok(&install(held, path, LSN, &pinned(&proposer), ops));
        }
        assert_eq!(stored(&with.core, 1), stored(&without.core, 1), "{path:?}");
        let current = strict_v2("lowered");
        assert_eq!(tuple_version(&with.core, 1, &current), 1, "{path:?}");
        let row = tuple(&with.core, 1, &current);
        assert_eq!(
            field(&row, "status"),
            Some(&Value::String("OPEN".into())),
            "{path:?}"
        );
        assert_eq!(field(&row, "lowered"), Some(&Value::Null), "{path:?}");
    }
}

/// A CONVERT to strict applied before the entry: the install stores the
/// row in the current strict mode, and the row decodes.
#[test]
fn a_convert_before_the_entry_installs_in_the_current_mode() {
    let proposer = schemaless_wire();
    for path in PATHS {
        let mut held = hold(plain_core);
        // A converted config carries digest 0.
        register(&mut held.core, &strict_wire(strict_v1()), 0);
        let ops = vec![put(1, &[("status", "OPEN")])];
        assert_ok(&install(&mut held, path, LSN, &pinned(&proposer), ops));
        let row = tuple(&held.core, 1, &strict_v1());
        assert_eq!(
            field(&row, "status"),
            Some(&Value::String("OPEN".into())),
            "{path:?}"
        );
    }
}

/// A row stored under a newer strict schema version than the pin's still
/// decodes in the install that replaces it.
#[test]
fn a_row_stored_under_a_newer_schema_decodes_in_the_install() {
    let current = strict_v2("note");
    for path in PATHS {
        let mut held = core_with(&strict_wire(current.clone()));
        let first = vec![put(1, &[("status", "OPEN"), ("note", "n")])];
        assert_ok(&install(&mut held, path, LSN, &RedoPins::default(), first));
        assert_eq!(tuple_version(&held.core, 1, &current), 2, "{path:?}");

        let pins = pinned(&strict_wire(strict_v1()));
        let replace = vec![put(1, &[("status", "CLOSED")])];
        assert_ok(&install(&mut held, path, LSN + 1, &pins, replace));
        assert_eq!(tuple_version(&held.core, 1, &current), 1, "{path:?}");
        let row = tuple(&held.core, 1, &current);
        assert_eq!(
            field(&row, "status"),
            Some(&Value::String("CLOSED".into())),
            "{path:?}"
        );
    }
}

/// A record with no pins installs under the current config.
#[test]
fn an_unpinned_record_installs_under_the_current_config() {
    for path in PATHS {
        let row = installed_document(&guarded_wire(), &RedoPins::default(), path);
        assert_eq!(
            field(&row, "lowered"),
            Some(&Value::String("open".into())),
            "{path:?}"
        );
    }
}

// ── Restart replay ───────────────────────────────────────────────────────

/// The WAL records of one committed record of `ops` pinned with `pins`,
/// as `path` appends it: a whole record, or a stream marker after its
/// chunks.
fn wal_records(
    held: &mut Hold,
    path: Path,
    pins: &RedoPins,
    ops: Vec<RedoSubRecord>,
) -> Vec<WalRecord> {
    match path {
        Path::Whole => {
            let mut redo = record(ops);
            redo.pins = pins.clone();
            let record = WalRecord::new(nodedb_wal::record::WalRecordArgs {
                record_type: nodedb_wal::record::RecordType::TransactionRedo as u32,
                lsn: LSN,
                tenant_id: TID,
                vshard_id: 0,
                database_id: 0,
                payload: redo.to_bytes().expect("encode redo"),
                encryption_key: None,
                preamble_bytes: None,
            })
            .expect("redo record");
            vec![record]
        }
        Path::Stream(_) => {
            let reference = held.wal.write(&framed_pinned(&record(ops), &[], &[], pins));
            let marker = reference.marker(None, None, Vec::new()).expect("marker");
            let wal = held.wal.manager();
            wal.appender(crate::wal::manager::NO_APPLY_KEY)
                .with_event_source(crate::event::EventSource::User)
                .append_transaction_redo(
                    TenantId::new(TID),
                    crate::types::VShardId::new(0),
                    DatabaseId::DEFAULT,
                    &marker,
                )
                .expect("marker record");
            wal.sync().expect("sync");
            wal.replay().expect("read the wal")
        }
    }
}

/// The row restart replay stores for one record pinned with `pins`, on a
/// core whose current config carries the later typeguard VALUE.
fn replayed_document(pins: &RedoPins, path: Path) -> Value {
    let mut held = core_with(&guarded_wire());
    let records = wal_records(&mut held, path, pins, vec![put(1, &[("status", "OPEN")])]);
    held.core
        .replay_engines_in_lsn_order(&records, 1, &TombstoneSet::new())
        .expect("replay");
    held.core.forget_replay_pins();
    document(&held.core, 1)
}

/// Restart replay of a pinned record stores the pinned row. With no pins
/// it stores the current config's row, so the case bites.
#[test]
fn restart_replay_of_a_pinned_record_stores_the_pinned_row() {
    let proposer = schemaless_wire();
    for path in [Path::Whole, Path::Stream(0)] {
        let row = replayed_document(&pinned(&proposer), path);
        assert_eq!(field(&row, "status"), Some(&Value::String("OPEN".into())));
        assert_eq!(field(&row, "lowered"), None, "{path:?}");
        let unpinned = replayed_document(&RedoPins::default(), path);
        assert_eq!(
            field(&unpinned, "lowered"),
            Some(&Value::String("open".into())),
            "{path:?}"
        );
    }
}

/// A put that fails in restart replay halts it: the core fail-stops and
/// boot refuses to start.
#[test]
fn restart_replay_halts_on_a_failed_put() {
    let mut failing = schemaless_wire();
    failing.enforcement.type_guards = vec![TypeGuardFieldDef {
        field: "broken".into(),
        type_expr: "INT".into(),
        required: false,
        check_expr: None,
        default_expr: None,
        value_expr: Some("1 / 0".into()),
    }];
    let mut held = core_with(&failing);
    let ops = vec![put(1, &[("status", "OPEN")])];
    let records = wal_records(&mut held, Path::Whole, &RedoPins::default(), ops);
    let replayed = held
        .core
        .replay_engines_in_lsn_order(&records, 1, &TombstoneSet::new());
    assert!(
        replayed.is_err() || held.core.replay_halted(),
        "the failed put halts replay"
    );
    assert!(held.core.is_fail_stopped(), "the core serves no state");
}

/// The `TransactionRedo` WAL record of `redo` at `lsn`.
fn redo_wal_record(lsn: u64, redo: &crate::wal::RedoRecord) -> WalRecord {
    WalRecord::new(nodedb_wal::record::WalRecordArgs {
        record_type: nodedb_wal::record::RecordType::TransactionRedo as u32,
        lsn,
        tenant_id: TID,
        vshard_id: 0,
        database_id: 0,
        payload: redo.to_bytes().expect("encode redo"),
        encryption_key: None,
        preamble_bytes: None,
    })
    .expect("redo record")
}

/// Where one continuation sits: its LSN, its origin's LSN, its index, and
/// the continuation count.
struct ContinuationAt {
    lsn: u64,
    origin: u64,
    index: u32,
    count: u32,
}

/// The continuation at `at` holding `ops`, which carries `pins`, its
/// origin's pins.
fn continuation_record(at: ContinuationAt, pins: &RedoPins, ops: Vec<RedoSubRecord>) -> WalRecord {
    let ContinuationAt {
        lsn,
        origin,
        index,
        count,
    } = at;
    let group = crate::wal::WriteGroupRecord {
        schemas: Vec::new(),
        group: crate::wal::WriteGroup::continuing_closed(origin, index, count),
        ops,
        redo: Some(crate::wal::ContinuedRedo {
            row_sources: Vec::new(),
            row_changes: Vec::new(),
            pins: pins.clone(),
        }),
    };
    WalRecord::new(nodedb_wal::record::WalRecordArgs {
        record_type: nodedb_wal::record::RecordType::WriteGroup as u32,
        lsn,
        tenant_id: TID,
        vshard_id: 0,
        database_id: 0,
        payload: group.to_bytes().expect("encode continuation"),
        encryption_key: None,
        preamble_bytes: None,
    })
    .expect("continuation record")
}

/// A split record's origin replays in one slice and its continuation in
/// the next. The continuation's row derives under the origin's pins, and
/// the pins drop once the last continuation replayed.
#[test]
fn a_continuation_in_a_later_slice_installs_under_the_origin_pins() {
    let mut held = core_with(&guarded_wire());
    let mut origin = record(vec![put(1, &[("status", "OPEN")])]);
    origin.pins = pinned(&schemaless_wire());
    let ops = vec![put(2, &[("status", "SHUT")])];
    let at = ContinuationAt {
        lsn: LSN + 1,
        origin: LSN,
        index: 1,
        count: 1,
    };
    let continuation = continuation_record(at, &origin.pins, ops);
    let tombstones = TombstoneSet::new();
    held.core
        .replay_engines_in_lsn_order(&[redo_wal_record(LSN, &origin)], 1, &tombstones)
        .expect("replay the origin");
    assert_eq!(held.core.held_replay_pins(), 1, "the origin's pins wait");
    held.core
        .replay_engines_in_lsn_order(&[continuation], 1, &tombstones)
        .expect("replay the continuation");
    assert_eq!(
        held.core.held_replay_pins(),
        0,
        "the last continuation replayed"
    );
    held.core.forget_replay_pins();
    for surrogate in [1, 2] {
        let row = document(&held.core, surrogate);
        assert_eq!(field(&row, "lowered"), None, "row {surrogate}");
    }
}

/// A continuation whose origin record the WAL no longer holds installs
/// under the pins it carries.
#[test]
fn a_continuation_with_no_origin_installs_under_its_own_pins() {
    let mut held = core_with(&guarded_wire());
    let at = ContinuationAt {
        lsn: LSN + 1,
        origin: LSN,
        index: 1,
        count: 1,
    };
    let ops = vec![put(2, &[("status", "SHUT")])];
    let continuation = continuation_record(at, &pinned(&schemaless_wire()), ops);
    held.core
        .replay_engines_in_lsn_order(&[continuation], 1, &TombstoneSet::new())
        .expect("replay the continuation");
    held.core.forget_replay_pins();
    let row = document(&held.core, 2);
    assert_eq!(field(&row, "status"), Some(&Value::String("SHUT".into())));
    assert_eq!(field(&row, "lowered"), None);
}

/// The msgpack body `{"status": "OPEN", 1: "x"}`, whose second key is an
/// integer.
fn integer_key_body() -> Vec<u8> {
    let mut body = vec![0x82, 0xa6];
    body.extend_from_slice(b"status");
    body.push(0xa4);
    body.extend_from_slice(b"OPEN");
    body.extend_from_slice(&[0x01, 0xa1, b'x']);
    body
}

/// A put whose body has an integer map key replays without halting, and
/// stores the row its live install stores.
#[test]
fn a_replayed_integer_key_put_stores_the_live_row() {
    let op = || doc_put_sub_record(COLL, "t1", &integer_key_body(), 1);
    let mut live = core_with(&schemaless_wire());
    assert_ok(&install(
        &mut live,
        Path::Whole,
        LSN,
        &RedoPins::default(),
        vec![op()],
    ));

    let mut replayed = core_with(&schemaless_wire());
    let records = wal_records(&mut replayed, Path::Whole, &RedoPins::default(), vec![op()]);
    replayed
        .core
        .replay_engines_in_lsn_order(&records, 1, &TombstoneSet::new())
        .expect("replay");
    assert!(!replayed.core.is_fail_stopped(), "replay does not halt");
    assert_eq!(document(&replayed.core, 1), document(&live.core, 1));
}

// ── KV declared columns ──────────────────────────────────────────────────

const PRICES: &str = "prices";

/// A KV collection whose `v` column is declared as `declared`.
fn kv_wire(declared: &str) -> DocConfigWire {
    let mut wire = schemaless_wire();
    wire.declared_columns = vec![
        nodedb_physical::physical_plan::DeclaredColumn::from_declared("v", declared)
            .expect("a numeric declared type"),
    ];
    wire
}

fn kv_put(key: &[u8], v: &str, surrogate: u32) -> RedoSubRecord {
    let mut body = std::collections::HashMap::new();
    body.insert("v".to_string(), Value::String(v.into()));
    let value = nodedb_types::value_to_msgpack(&Value::Object(body)).expect("encode body");
    RedoSubRecord {
        record_type: nodedb_wal::record::RecordType::Put as u32,
        payload: crate::control::server::wal_dispatch_kv::encode::encode_kv_put(
            PRICES, key, &value, 0, None, surrogate,
        )
        .expect("encode kv put"),
    }
}

/// The `v` value a core whose current `prices` config is `current` stores
/// for one KV put pinned with `pins`, through `path`.
fn installed_kv(current: &DocConfigWire, pins: &RedoPins, path: Path) -> Value {
    let mut held = hold(plain_core);
    register_as(
        &mut held.core,
        PRICES,
        current,
        current.digest().expect("digest"),
    );
    let collections = [PRICES.to_string()];
    let writes = Writes {
        collections: &collections,
        sum_targets: &[],
    };
    let ops = vec![kv_put(b"k1", "1.005", 7)];
    assert_ok(&install_writing(&mut held, path, LSN, pins, ops, &writes));
    let stored = held
        .core
        .kv_engine
        .get(0, TID, PRICES, b"k1", crate::engine::kv::current_ms())
        .expect("the value is stored");
    let row = nodedb_types::value_from_msgpack(&stored).expect("a map body");
    field(&row, "v").cloned().expect("the v column")
}

/// A replica with a later declared-column type stores the KV value a
/// replica without it stores. With no pins the later type applies.
#[test]
fn a_later_declared_column_type_changes_no_kv_value() {
    let proposer = kv_wire("DECIMAL(5,2)");
    let later = kv_wire("DECIMAL(6,3)");
    let pins = pins(vec![pin_of(PRICES, &proposer)]);
    for path in PATHS {
        let without = installed_kv(&proposer, &pins, path);
        let with = installed_kv(&later, &pins, path);
        assert_eq!(with, without, "{path:?}");
        assert_eq!(with, Value::String("1.01".into()), "{path:?}");
        let unpinned = installed_kv(&later, &RedoPins::default(), path);
        assert_eq!(unpinned, Value::String("1.005".into()), "{path:?}");
    }
}

// ── Strict sum target ────────────────────────────────────────────────────

/// `balances` as the proposer saw it: strict `{id, balance}`.
fn balances_v1() -> StrictSchema {
    StrictSchema::new(vec![
        ColumnDef::nullable("id", ColumnType::String),
        ColumnDef::nullable("balance", ColumnType::String),
    ])
    .expect("schema")
}

/// `balances` with a later required `note` column, which no folded image
/// carries.
fn balances_v2() -> StrictSchema {
    let mut note = ColumnDef::required("note", ColumnType::String);
    note.added_at_version = 2;
    let mut schema = balances_v1();
    schema.columns.push(note);
    schema.version = 2;
    schema
}

/// `charges`, which sums `amount` into `balances.balance`.
fn charges_wire() -> DocConfigWire {
    let mut wire = schemaless_wire();
    wire.enforcement.materialized_sum_sources =
        vec![nodedb_physical::physical_plan::MaterializedSumBinding {
            target_collection: BALANCES.to_string(),
            target_column: "balance".to_string(),
            join_column: "account_id".to_string(),
            value_expr: nodedb_query::expr::SqlExpr::Column("amount".to_string()),
            declared_primary_key: None,
        }];
    wire
}

fn balance_row(surrogate: Surrogate, account: &str) -> RedoSubRecord {
    let mut body = std::collections::HashMap::new();
    body.insert("id".to_string(), Value::String(account.into()));
    body.insert("balance".to_string(), Value::String("10".into()));
    let body = nodedb_types::value_to_msgpack(&Value::Object(body)).expect("encode body");
    doc_put_sub_record(BALANCES, account, &body, surrogate.as_u32())
}

/// A fold core whose `balances` rows `a1` and `a2` are stored under
/// `balances_v1`, then whose `balances` config becomes `current`.
fn strict_fold_core(current: &DocConfigWire) -> Hold {
    let mut held = hold(plain_core);
    let v1 = strict_wire(balances_v1());
    register_as(&mut held.core, BALANCES, &v1, v1.digest().expect("digest"));
    let charges = charges_wire();
    register_as(
        &mut held.core,
        CHARGES,
        &charges,
        charges.digest().expect("digest"),
    );
    let collections = [BALANCES.to_string()];
    let writes = Writes {
        collections: &collections,
        sum_targets: &[],
    };
    let seed = vec![balance_row(A1, "a1"), balance_row(A2, "a2")];
    let seeded = install_writing(
        &mut held,
        Path::Whole,
        LSN - 10,
        &RedoPins::default(),
        seed,
        &writes,
    );
    assert_ok(&seeded);
    register_as(
        &mut held.core,
        BALANCES,
        current,
        current.digest().expect("digest"),
    );
    held
}

/// A replica with a later required column on a strict sum target reaches
/// the verdict a replica without it reaches, and stores the same folded
/// image.
#[test]
fn a_later_strict_target_column_changes_no_verdict_and_no_image() {
    let proposer = strict_wire(balances_v1());
    let pins = pins(vec![
        pin_of(BALANCES, &proposer),
        pin_of(CHARGES, &charges_wire()),
    ]);
    let collections = [CHARGES.to_string()];
    let targets = fold_targets();
    let writes = Writes {
        collections: &collections,
        sum_targets: &targets,
    };
    for path in PATHS {
        let mut images = Vec::new();
        for current in [proposer.clone(), strict_wire(balances_v2())] {
            let mut held = strict_fold_core(&current);
            let ops = vec![charge(1, "a1", 5)];
            assert_ok(&install_writing(&mut held, path, LSN, &pins, ops, &writes));
            images.push(stored_in(&held.core, BALANCES, A1.as_u32()));
        }
        assert_eq!(images[0], images[1], "{path:?}");
        let row = crate::data::executor::strict_format::binary_tuple_to_row_value(
            &images[1],
            &balances_v2(),
        )
        .expect("the image decodes under the current schema");
        assert_eq!(
            field(&row, "balance"),
            Some(&Value::String("15".into())),
            "{path:?}"
        );
    }
}

/// `balances` at v2: [`balances_v1`] with a nullable `b` column.
fn balances_with_b() -> StrictSchema {
    let mut b = ColumnDef::nullable("b", ColumnType::String);
    b.added_at_version = 2;
    let mut schema = balances_v1();
    schema.columns.push(b);
    schema.version = 2;
    schema
}

/// A balances row `{id, balance: 10, b}` of `account` at `surrogate`.
fn balance_row_with_b(surrogate: Surrogate, account: &str, b: &str) -> RedoSubRecord {
    let mut body = std::collections::HashMap::new();
    body.insert("id".to_string(), Value::String(account.into()));
    body.insert("balance".to_string(), Value::String("10".into()));
    body.insert("b".to_string(), Value::String(b.into()));
    let body = nodedb_types::value_to_msgpack(&Value::Object(body)).expect("encode body");
    doc_put_sub_record(BALANCES, account, &body, surrogate.as_u32())
}

/// R2, pinned at v2, sets `b = 5` on target rows. R1, pinned at v1,
/// commits after R2 and folds into one of them. The fold keeps `b`.
#[test]
fn a_fold_pinned_at_an_older_schema_keeps_a_newer_stored_column() {
    let v1 = strict_wire(balances_v1());
    let v2 = strict_wire(balances_with_b());
    let charges = charges_wire();
    for path in PATHS {
        let mut held = hold(plain_core);
        register_as(&mut held.core, BALANCES, &v2, v2.digest().expect("digest"));
        register_as(
            &mut held.core,
            CHARGES,
            &charges,
            charges.digest().expect("digest"),
        );
        let balances = [BALANCES.to_string()];
        let r2_writes = Writes {
            collections: &balances,
            sum_targets: &[],
        };
        let r2 = vec![
            balance_row_with_b(A1, "a1", "5"),
            balance_row_with_b(A2, "a2", "5"),
        ];
        let r2_pins = pins(vec![pin_of(BALANCES, &v2)]);
        assert_ok(&install_writing(
            &mut held,
            path,
            LSN - 10,
            &r2_pins,
            r2,
            &r2_writes,
        ));

        let charges_names = [CHARGES.to_string()];
        let targets = fold_targets();
        let r1_writes = Writes {
            collections: &charges_names,
            sum_targets: &targets,
        };
        let r1_pins = pins(vec![pin_of(BALANCES, &v1), pin_of(CHARGES, &charges)]);
        let r1 = vec![charge(1, "a1", 5)];
        assert_ok(&install_writing(
            &mut held, path, LSN, &r1_pins, r1, &r1_writes,
        ));

        let image = stored_in(&held.core, BALANCES, A1.as_u32());
        let version = nodedb_strict::TupleDecoder::new(&balances_with_b())
            .schema_version(&image)
            .expect("tuple version");
        assert_eq!(version, 2, "the fold keeps the stored layout: {path:?}");
        let row = crate::data::executor::strict_format::binary_tuple_to_row_value(
            &image,
            &balances_with_b(),
        )
        .expect("the image decodes");
        assert_eq!(
            field(&row, "b"),
            Some(&Value::String("5".into())),
            "{path:?}"
        );
        assert_eq!(
            field(&row, "balance"),
            Some(&Value::String("15".into())),
            "{path:?}"
        );
    }
}
