// SPDX-License-Identifier: BUSL-1.1

//! A write's WAL records carry the system ordinal fixed before the append
//! (`RecordHeader::system_ordinal`), and the live apply carries the same one
//! (`Request::system_ordinal`). Each test applies one write live on one
//! core, replays its records on a fresh core, and checks that both stamp the
//! carried ordinal: a KV row, a timeseries batch, and an array cell.

use nodedb_array::types::ArrayId;
use nodedb_physical::physical_plan::{ArrayOp, KvOp, TimeseriesOp};
use nodedb_types::{DatabaseId, QualifiedCollection, Surrogate, SystemTimeScope, TenantId};
use nodedb_wal::{TombstoneSet, WalRecord};

use crate::bridge::envelope::{PhysicalPlan, Request, Status};
use crate::control::server::wal_dispatch::{WalAppendRequest, stamp_minted_lsn, wal_append};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::core_loop::tests::{make_core_with_dir, make_request};
use crate::data::executor::task::ExecutionTask;
use crate::engine::kv::KvVersionedScan;
use crate::types::{Lsn, VShardId};
use crate::wal::manager::{NO_APPLY_KEY, WalManager};

const TID: u64 = 1;

struct Core {
    core: CoreLoop,
    _dir: tempfile::TempDir,
}

fn core() -> Core {
    let dir = tempfile::tempdir().expect("tempdir");
    let (core, _req, _resp) = make_core_with_dir(dir.path());
    Core { core, _dir: dir }
}

/// An ordinal far above every clock, so no stored version raises it.
fn carried(core: &CoreLoop) -> i64 {
    core.hlc.peek().saturating_add(5_000_000_000_000)
}

/// Append `plan`'s records with `ordinal` in their headers, as the funnel
/// does, and stamp the minted LSN onto the plan. Returns the LSN.
fn append(wal: &WalManager, plan: &mut PhysicalPlan, ordinal: i64) -> Lsn {
    let lsn = wal_append(WalAppendRequest {
        wal: wal
            .appender(NO_APPLY_KEY)
            .with_system_ordinal(Some(ordinal)),
        event_source: crate::event::EventSource::User,
        tenant_id: TenantId::new(TID),
        vshard_id: VShardId::new(0),
        database_id: DatabaseId::DEFAULT,
        plan,
        credentials: None,
        now_override: None,
    })
    .expect("append")
    .lsn
    .expect("a write mints an LSN");
    stamp_minted_lsn(plan, lsn);
    lsn
}

fn records(wal: &WalManager) -> Vec<WalRecord> {
    wal.sync().expect("sync wal");
    wal.replay().expect("read wal")
}

/// Apply `plan` live, carrying `ordinal` as the funnel does.
fn apply(core: &mut CoreLoop, plan: PhysicalPlan, lsn: Lsn, ordinal: i64) {
    let request = Request {
        system_ordinal: Some(ordinal),
        ..make_request(plan)
    };
    let response = core.execute(&ExecutionTask::with_wal_lsn(request, Some(lsn)));
    assert_eq!(response.status, Status::Ok, "{response:?}");
}

fn wal() -> (WalManager, tempfile::TempDir) {
    let dir = tempfile::tempdir().expect("tempdir");
    let wal = WalManager::open_for_testing(&dir.path().join("wal")).expect("open wal");
    (wal, dir)
}

/// Every version of every row of KV collection `cache`, as `sys_from`.
fn kv_ordinals(core: &CoreLoop) -> Vec<i64> {
    let (rows, _) = core.kv_engine.scan_at(KvVersionedScan {
        database_id: 0,
        tenant_id: TID,
        collection: "cache",
        cursor: &[],
        count: usize::MAX,
        now_ms: crate::engine::kv::current_ms(),
        match_pattern: None,
        surrogate_ceiling: None,
        system_time: SystemTimeScope::AllVersions,
    });
    rows.into_iter().map(|row| row.sys_from).collect()
}

/// A local KV put stamps the ordinal its record carries, live and on replay.
#[test]
fn a_local_kv_put_stamps_its_record_ordinal_live_and_on_replay() {
    let (wal, _wal_dir) = wal();
    let mut live = core();
    let ordinal = carried(&live.core);
    let mut plan = PhysicalPlan::Kv(KvOp::Put {
        collection: QualifiedCollection::new(DatabaseId::DEFAULT, "cache"),
        key: b"k".to_vec(),
        value: b"v".to_vec(),
        ttl_ms: 0,
        surrogate: Surrogate::new(5),
        returning: None,
        rls_filters: Vec::new(),
        provenance: None,
    });
    let lsn = append(&wal, &mut plan, ordinal);
    apply(&mut live.core, plan, lsn, ordinal);
    assert_eq!(kv_ordinals(&live.core), vec![ordinal]);

    let mut replayed = core();
    replayed
        .core
        .replay_all_wal(&records(&wal), 1, &TombstoneSet::new())
        .expect("replay");
    assert_eq!(kv_ordinals(&replayed.core), vec![ordinal]);
}

/// The `_ts_system` ordinals of timeseries collection `metrics`'s memtable.
fn ts_ordinals(core: &CoreLoop) -> Vec<i64> {
    let key = (
        DatabaseId::DEFAULT,
        TenantId::new(TID),
        "metrics".to_string(),
    );
    let memtable = core.columnar_memtables.get(&key).expect("memtable");
    let idx = memtable.schema().ts_system_idx().expect("system column");
    match memtable.column(idx) {
        crate::engine::timeseries::columnar_memtable::ColumnData::Int64(values) => values.clone(),
        other => panic!("_ts_system is not an Int64 column: {other:?}"),
    }
}

/// A local timeseries batch stamps the ordinal its record carries, live and
/// on replay.
#[test]
fn a_local_timeseries_batch_stamps_its_record_ordinal_live_and_on_replay() {
    let (wal, _wal_dir) = wal();
    let mut live = core();
    let ordinal = carried(&live.core);
    let mut plan = PhysicalPlan::Timeseries(TimeseriesOp::Ingest {
        collection: QualifiedCollection::new(DatabaseId::DEFAULT, "metrics"),
        payload: b"metrics,host=h0 value=1i".to_vec(),
        format: "ilp".into(),
        wal_lsn: None,
        surrogates: Vec::new(),
        provenance: None,
        rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
        returning: None,
        rls_filters: Vec::new(),
    });
    let lsn = append(&wal, &mut plan, ordinal);
    apply(&mut live.core, plan, lsn, ordinal);
    assert_eq!(ts_ordinals(&live.core), vec![ordinal]);

    let mut replayed = core();
    replayed
        .core
        .replay_all_wal(&records(&wal), 1, &TombstoneSet::new())
        .expect("replay");
    assert_eq!(ts_ordinals(&replayed.core), vec![ordinal]);
}

fn array_id() -> ArrayId {
    ArrayId::new(TenantId::new(TID), "grid")
}

/// Open array `grid` on `core`, which registers it in the core's catalog.
fn open_array(core: &mut CoreLoop) {
    use nodedb_array::schema::ArraySchemaBuilder;
    use nodedb_array::schema::attr_spec::{AttrSpec, AttrType};
    use nodedb_array::schema::dim_spec::{DimSpec, DimType};
    use nodedb_array::types::domain::{Domain, DomainBound};

    let schema = ArraySchemaBuilder::new("grid")
        .dim(DimSpec::new(
            "x",
            DimType::Int64,
            Domain::new(DomainBound::Int64(0), DomainBound::Int64(100)),
        ))
        .attr(AttrSpec::new("v", AttrType::Int64, true))
        .tile_extents(vec![10])
        .build()
        .expect("schema");
    let response = core.execute(&ExecutionTask::new(make_request(PhysicalPlan::Array(
        ArrayOp::OpenArray {
            array_id: array_id(),
            schema_msgpack: zerompk::to_msgpack_vec(&schema).expect("encode schema"),
            schema_hash: 0x5,
            prefix_bits: 8,
            audit_retain_ms: None,
            minimum_audit_retain_ms: None,
        },
    ))));
    assert_eq!(response.status, Status::Ok, "{response:?}");
}

fn array_put() -> PhysicalPlan {
    use nodedb_array::types::cell_value::value::CellValue;
    use nodedb_array::types::coord::value::CoordValue;

    let cells = vec![crate::engine::array::wal::ArrayPutCell {
        coord: vec![CoordValue::Int64(3)],
        attrs: vec![CellValue::Int64(42)],
        surrogate: Surrogate::new(9),
        system_from_ms: 0,
        valid_from_ms: 0,
        valid_until_ms: i64::MAX,
    }];
    PhysicalPlan::Array(ArrayOp::Put {
        array_id: array_id(),
        cells_msgpack: zerompk::to_msgpack_vec(&cells).expect("encode cells"),
        wal_lsn: 0,
        provenance: None,
        vshard_id: 0,
    })
}

fn newest_array_stamp(core: &CoreLoop) -> Option<i64> {
    core.array_engine
        .store(&array_id())
        .expect("array open")
        .newest_version_stamp()
}

/// A local array cell write stamps the ordinal its record carries, live and
/// on replay.
#[test]
fn a_local_array_put_stamps_its_record_ordinal_live_and_on_replay() {
    let (wal, _wal_dir) = wal();
    let mut live = core();
    open_array(&mut live.core);
    let ordinal = carried(&live.core);
    let mut plan = array_put();
    let lsn = append(&wal, &mut plan, ordinal);
    apply(&mut live.core, plan, lsn, ordinal);
    assert_eq!(newest_array_stamp(&live.core), Some(ordinal));

    let mut replayed = core();
    open_array(&mut replayed.core);
    replayed
        .core
        .replay_all_wal(&records(&wal), 1, &TombstoneSet::new())
        .expect("replay");
    assert_eq!(newest_array_stamp(&replayed.core), Some(ordinal));
}

/// A replicated array cell write stamps the ordinal its entry's leader
/// assigned, on every replica.
#[test]
fn a_replicated_array_put_stamps_its_entry_ordinal() {
    let mut first = core();
    let mut second = core();
    let ordinal = carried(&first.core).max(carried(&second.core));
    for replica in [&mut first, &mut second] {
        open_array(&mut replica.core);
        let mut plan = array_put();
        stamp_minted_lsn(&mut plan, Lsn::new(7));
        apply(&mut replica.core, plan, Lsn::new(7), ordinal);
        assert_eq!(newest_array_stamp(&replica.core), Some(ordinal));
    }
}
