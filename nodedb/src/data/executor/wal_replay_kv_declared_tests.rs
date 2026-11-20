// SPDX-License-Identifier: BUSL-1.1

//! A KV write re-types under the declared columns its plan carries, live and
//! in WAL replay. A column the catalog declares after the write changes
//! nothing replay stores.

use std::collections::HashMap;

use nodedb_physical::physical_plan::{
    DeclaredColumn, KvBatchIntent, KvCounterShape, KvDeclaredColumns, KvOp, PhysicalPlan,
    TransferAmount,
};
use nodedb_types::{QualifiedCollection, RlsWriteCheck, Surrogate, Value};
use nodedb_wal::{TombstoneSet, WalRecord};

use crate::bridge::dispatch::{BridgeRequest, BridgeResponse};
use crate::bridge::envelope::Status;
use crate::control::server::wal_dispatch::wal_append_if_write;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::core_loop::tests::{make_core_with_dir, make_request};
use crate::data::executor::task::ExecutionTask;
use crate::types::{DatabaseId, TenantId, VShardId};
use crate::wal::manager::WalManager;

const TID: u64 = 1;
const COLLECTION: &str = "kv_declared";

/// A core with the guards that keep it open.
struct Harness {
    core: CoreLoop,
    _req: nodedb_bridge::buffer::Producer<BridgeRequest>,
    _resp: nodedb_bridge::buffer::Consumer<BridgeResponse>,
    _dir: tempfile::TempDir,
}

fn harness() -> Harness {
    let dir = tempfile::tempdir().expect("tempdir");
    let (core, req, resp) = make_core_with_dir(dir.path());
    Harness {
        core,
        _req: req,
        _resp: resp,
        _dir: dir,
    }
}

fn did() -> u64 {
    DatabaseId::DEFAULT.as_u64()
}

fn collection() -> QualifiedCollection {
    QualifiedCollection::new(DatabaseId::DEFAULT, COLLECTION)
}

fn columns(declared: &[(&str, &str)]) -> Vec<DeclaredColumn> {
    declared
        .iter()
        .map(|(name, ty)| DeclaredColumn::from_declared(name, ty).expect("numeric declaration"))
        .collect()
}

/// Register `declared` as the collection's columns on `core`: the catalog a
/// node boots with after a later declaration.
fn declare(core: &mut CoreLoop, declared: &[(&str, &str)]) {
    let mut config = crate::engine::document::store::CollectionConfig::new(COLLECTION);
    config.declared_columns = columns(declared);
    core.doc_configs.insert(
        (
            DatabaseId::DEFAULT,
            TenantId::new(TID),
            COLLECTION.to_string(),
        ),
        config,
    );
}

fn stored(core: &CoreLoop, key: &[u8]) -> Option<Vec<u8>> {
    core.kv_engine
        .get(did(), TID, COLLECTION, key, crate::engine::kv::current_ms())
}

/// Journal `plans` through the autocommit WAL path and read the records
/// back as a booting node does.
fn journal(plans: &[PhysicalPlan]) -> (tempfile::TempDir, Vec<WalRecord>) {
    let dir = tempfile::tempdir().expect("wal tempdir");
    let wal = WalManager::open_for_testing(&dir.path().join("wal")).expect("open wal");
    for plan in plans {
        let outcome = wal_append_if_write(
            &wal,
            TenantId::new(TID),
            VShardId::new(0),
            DatabaseId::DEFAULT,
            plan,
        )
        .expect("wal append");
        assert!(outcome.lsn.is_some(), "every KV write is journaled");
    }
    wal.sync().expect("wal sync");
    let records = wal.replay().expect("wal read");
    (dir, records)
}

/// Run `plans` live on a fresh core, as dispatch runs them.
fn run_live(plans: &[PhysicalPlan]) -> Harness {
    let mut h = harness();
    for plan in plans {
        let PhysicalPlan::Kv(op) = plan else {
            panic!("a KV plan");
        };
        let task = ExecutionTask::new(make_request(plan.clone()));
        let coerced = h.core.coerce_kv_op_bodies(op).expect("the body fits");
        let resp = h
            .core
            .execute_kv(&task, did(), TID, coerced.as_ref().unwrap_or(op));
        assert_eq!(resp.status, Status::Ok, "{:?}", resp.error_code);
    }
    h
}

/// Replay `records` on a fresh core whose catalog declares `later`.
fn replay_declaring(records: &[WalRecord], later: &[(&str, &str)]) -> Harness {
    let mut h = harness();
    declare(&mut h.core, later);
    h.core.replay_kv_wal(records, 1, &TombstoneSet::new());
    h
}

/// The stored value of each key, `None` for an absent key.
type StoredValues = Vec<Option<Vec<u8>>>;

/// The values `keys` hold live and after replay under a later declaration.
fn live_and_replayed(
    plans: &[PhysicalPlan],
    later: &[(&str, &str)],
    keys: &[&[u8]],
) -> (StoredValues, StoredValues) {
    let live = run_live(plans);
    let (_dir, records) = journal(plans);
    let replayed = replay_declaring(&records, later);
    (
        keys.iter().map(|key| stored(&live.core, key)).collect(),
        keys.iter().map(|key| stored(&replayed.core, key)).collect(),
    )
}

fn map_body(fields: &[(&str, Value)]) -> Vec<u8> {
    let map: HashMap<String, Value> = fields
        .iter()
        .map(|(name, value)| (name.to_string(), value.clone()))
        .collect();
    nodedb_types::value_to_msgpack(&Value::Object(map)).expect("encode body")
}

fn put(key: &[u8], value: Vec<u8>, surrogate: u32, declared: KvDeclaredColumns) -> PhysicalPlan {
    PhysicalPlan::Kv(KvOp::Put {
        collection: collection(),
        declared_columns: declared,
        key: key.to_vec(),
        value,
        ttl_ms: 0,
        surrogate: Surrogate::new(surrogate),
        returning: None,
        rls_filters: Vec::new(),
        provenance: None,
    })
}

#[test]
fn a_cas_replays_the_value_it_stored_live_after_a_later_int_declaration() {
    let cas = PhysicalPlan::Kv(KvOp::Cas {
        collection: collection(),
        declared_columns: KvDeclaredColumns::none(),
        key: b"k".to_vec(),
        expected: Vec::new(),
        new_value: b"abc".to_vec(),
        surrogate: Surrogate::new(1),
        rls_write_check: RlsWriteCheck::already_decided_elsewhere(),
    });
    let (live, replayed) = live_and_replayed(&[cas], &[("value", "INT")], &[b"k"]);
    assert_eq!(live, vec![Some(b"abc".to_vec())]);
    assert_eq!(replayed, live, "replay stores what the live write stored");
}

#[test]
fn an_incr_replays_the_value_it_stored_live_after_a_later_declaration() {
    let incr = PhysicalPlan::Kv(KvOp::Incr {
        collection: collection(),
        declared_columns: KvDeclaredColumns::none(),
        key: b"n".to_vec(),
        delta: 40_000,
        ttl_ms: 0,
        surrogate: Surrogate::new(1),
        rls_write_check: RlsWriteCheck::already_decided_elsewhere(),
        shape: KvCounterShape::Raw,
    });
    let (live, replayed) = live_and_replayed(&[incr], &[("value", "SMALLINT")], &[b"n"]);
    assert_eq!(live, vec![Some(b"40000".to_vec())]);
    assert_eq!(replayed, live, "replay stores what the live write stored");
}

#[test]
fn a_field_set_replays_the_row_it_stored_live_after_a_later_int_declaration() {
    let text = nodedb_types::value_to_msgpack(&Value::String("abc".into())).expect("encode");
    let field_set = PhysicalPlan::Kv(KvOp::FieldSet {
        collection: collection(),
        declared_columns: KvDeclaredColumns::none(),
        key: b"row".to_vec(),
        updates: vec![("x".to_string(), text)],
        surrogate: Surrogate::new(1),
        if_present: false,
        rls_write_check: RlsWriteCheck::already_decided_elsewhere(),
        returning: None,
        rls_filters: Vec::new(),
    });
    let (live, replayed) = live_and_replayed(&[field_set], &[("x", "INT")], &[b"row"]);
    let row = nodedb_types::value_from_msgpack(live[0].as_deref().expect("stored live"))
        .expect("a map body");
    assert_eq!(row.get("x"), Some(&Value::String("abc".into())));
    assert_eq!(replayed, live, "replay stores what the live write stored");
}

#[test]
fn a_transfer_replays_the_rows_it_stored_live_after_a_later_declaration() {
    let seed = |key: &[u8], balance: f64, surrogate: u32| {
        put(
            key,
            map_body(&[("bal", Value::Float(balance))]),
            surrogate,
            KvDeclaredColumns::none(),
        )
    };
    let transfer = PhysicalPlan::Kv(KvOp::Transfer {
        collection: collection(),
        declared_columns: KvDeclaredColumns::none(),
        source_key: b"a".to_vec(),
        dest_key: b"b".to_vec(),
        field: "bal".into(),
        amount: TransferAmount::Float(10.0),
        debit_surrogate: Surrogate::new(1),
        credit_surrogate: Surrogate::new(2),
        rls_write_check: RlsWriteCheck::already_decided_elsewhere(),
    });
    let plans = [seed(b"a", 50_000.0, 1), seed(b"b", 0.0, 2), transfer];
    let (live, replayed) = live_and_replayed(&plans, &[("bal", "SMALLINT")], &[b"a", b"b"]);
    assert!(
        live.iter().all(Option::is_some),
        "both rows are stored live"
    );
    assert_eq!(replayed, live, "replay stores what the live write stored");
}

#[test]
fn a_put_replays_under_the_columns_it_carries() {
    let carried = KvDeclaredColumns::resolved(columns(&[("value", "DECIMAL(5,2)")]));
    let plan = put(b"p", b"1.005".to_vec(), 1, carried);
    let (live, replayed) = live_and_replayed(&[plan], &[], &[b"p"]);
    assert_eq!(live, vec![Some(b"1.01".to_vec())]);
    assert_eq!(
        replayed, live,
        "replay re-types under the record's columns, not the boot catalog's"
    );
}

/// The replica's catalog declares `value INT` after the proposer resolved
/// `value DECIMAL(5,2)`. The replica stores what the proposer's columns give.
#[test]
fn a_replicated_put_applies_under_the_proposer_columns_on_a_later_declaring_replica() {
    use crate::control::wal_replication::{
        ReplicableWrite, decode_replicated_entry, to_replicated_entry,
    };

    let carried = KvDeclaredColumns::resolved(columns(&[("value", "DECIMAL(5,2)")]));
    let proposed = put(b"p", b"1.005".to_vec(), 1, carried.clone());
    let write = ReplicableWrite::decide_for_replication(&proposed).expect("replicable");
    let entry = to_replicated_entry(
        TenantId::new(TID),
        DatabaseId::DEFAULT,
        VShardId::new(0),
        &write,
    )
    .expect("encode")
    .expect("a replicated KV write");
    let (_, _, applied, _) = decode_replicated_entry(&entry.to_bytes())
        .expect("decode")
        .expect("a KV entry");
    let PhysicalPlan::Kv(op) = &applied else {
        panic!("a KV plan");
    };
    assert_eq!(
        op.declared_columns().map(|(_, columns)| columns),
        Some(&carried),
        "the entry carries the proposer's columns"
    );

    let mut replica = harness();
    declare(&mut replica.core, &[("value", "INT")]);
    let task = ExecutionTask::new(make_request(applied.clone()));
    let coerced = replica.core.coerce_kv_op_bodies(op).expect("the body fits");
    let resp = replica
        .core
        .execute_kv(&task, did(), TID, coerced.as_ref().unwrap_or(op));
    assert_eq!(resp.status, Status::Ok, "{:?}", resp.error_code);
    assert_eq!(
        stored(&replica.core, b"p"),
        Some(b"1.01".to_vec()),
        "the replica stores the value the proposer's columns give"
    );
}

#[test]
fn a_batch_put_replays_under_the_columns_it_carries() {
    let batch = PhysicalPlan::Kv(KvOp::BatchPut {
        collection: collection(),
        declared_columns: KvDeclaredColumns::resolved(columns(&[("value", "DECIMAL(5,2)")])),
        entries: vec![
            (b"p1".to_vec(), b"1.005".to_vec()),
            (b"p2".to_vec(), b"2.5".to_vec()),
        ],
        ttl_ms: 0,
        intent: KvBatchIntent::Upsert,
        surrogates: vec![Surrogate::new(1), Surrogate::new(2)],
        returning: None,
        rls_filters: Vec::new(),
    });
    let (live, replayed) = live_and_replayed(&[batch], &[], &[b"p1", b"p2"]);
    assert_eq!(live[0], Some(b"1.01".to_vec()));
    assert!(live[1].is_some(), "the second entry is stored live");
    assert_eq!(replayed, live);
}

#[test]
fn a_write_with_no_declared_columns_replays_unchanged() {
    let plan = put(b"raw", b"abc".to_vec(), 1, KvDeclaredColumns::none());
    let (live, replayed) = live_and_replayed(&[plan], &[("value", "INT")], &[b"raw"]);
    assert_eq!(live, vec![Some(b"abc".to_vec())]);
    assert_eq!(replayed, live, "an empty column set re-types nothing");
}

#[test]
fn a_write_with_pending_columns_is_refused_before_it_is_journaled() {
    let plan = put(b"p", b"v".to_vec(), 1, KvDeclaredColumns::pending());
    let dir = tempfile::tempdir().expect("wal tempdir");
    let wal = WalManager::open_for_testing(&dir.path().join("wal")).expect("open wal");
    let err = wal_append_if_write(
        &wal,
        TenantId::new(TID),
        VShardId::new(0),
        DatabaseId::DEFAULT,
        &plan,
    )
    .expect_err("pending columns never reach the WAL");
    assert!(matches!(err, crate::Error::Internal { .. }), "{err:?}");

    let h = harness();
    let PhysicalPlan::Kv(op) = &plan else {
        panic!("a KV plan");
    };
    assert!(
        h.core.coerce_kv_op_bodies(op).is_err(),
        "the core refuses a supplied body whose columns never resolved"
    );
}

#[test]
fn an_atomic_with_pending_columns_is_refused_by_the_core() {
    let cas = PhysicalPlan::Kv(KvOp::Cas {
        collection: collection(),
        declared_columns: KvDeclaredColumns::pending(),
        key: b"k".to_vec(),
        expected: Vec::new(),
        new_value: b"v".to_vec(),
        surrogate: Surrogate::new(1),
        rls_write_check: RlsWriteCheck::already_decided_elsewhere(),
    });
    let PhysicalPlan::Kv(op) = &cas else {
        panic!("a KV plan");
    };
    let mut h = harness();
    let task = ExecutionTask::new(make_request(cas.clone()));
    let resp = h.core.execute_kv(&task, did(), TID, op);
    assert_eq!(resp.status, Status::Error);
    assert_eq!(
        stored(&h.core, b"k"),
        None,
        "a refused write stores nothing"
    );
}
