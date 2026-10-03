// SPDX-License-Identifier: BUSL-1.1

//! Replay of journalled document rows and graph edges onto a base.
//!
//! Each test drives writes through the same three steps the write funnel
//! runs: the pre-dispatch append, the Data-Plane apply, and the post-apply
//! append of the response's write set. It then replays the WAL onto a core
//! that never saw the writes, or onto the live core's own directory after a
//! restart, and compares every stored byte with the live state.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use nodedb_physical::physical_plan::{DocumentOp, GraphOp, UpdateValue};
use nodedb_types::{QualifiedCollection, RlsWriteCheck, Surrogate, Value};
use nodedb_wal::{TombstoneSet, WalRecord};

use crate::bridge::envelope::{
    Admission, PhysicalPlan, Priority, Request, Response, RowEffect, Status,
};
use crate::bridge::scan_filter::{FilterOp, ScanFilter};
use crate::control::server::wal_dispatch::{
    WalAppendRequest, WriteSetTarget, append_group_origin, append_group_parts, plan_post_apply_redo,
};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::core_loop::tests::make_core_with_dir;
use crate::data::executor::task::ExecutionTask;
use crate::engine::document::store::CollectionConfig;
use crate::engine::graph::csr::Direction;
use crate::types::{DatabaseId, ReadConsistency, RequestId, TenantId, TraceId, VShardId};
use crate::wal::manager::{NO_APPLY_KEY, WalManager};

const DB: u64 = 0;
const TID: u64 = 1;
/// Schemaless, with a secondary index on `owner` and a vector field `emb`.
const ORDERS: &str = "orders";
/// `bitemporal=true`: every write lands at a version key the apply decides.
const LEDGER: &str = "ledger";
const SOCIAL: &str = "social";
const KNOWS: &str = "KNOWS";

/// A WAL the writes journal into, the way the write funnel journals them.
struct Journal {
    wal: WalManager,
    _dir: tempfile::TempDir,
}

impl Journal {
    fn open() -> Self {
        let dir = tempfile::tempdir().expect("tempdir");
        let wal = WalManager::open_for_testing(&dir.path().join("rows.wal")).expect("open wal");
        Self { wal, _dir: dir }
    }

    fn appender(&self) -> crate::wal::manager::WalAppender<'_> {
        self.wal
            .appender(NO_APPLY_KEY)
            .with_event_source(crate::event::EventSource::User)
    }

    /// Append the origin of `plan`'s group, apply the plan on `core` at the
    /// origin's LSN, and append the write set the response reports as the
    /// group's parts.
    fn write(&self, core: &mut CoreLoop, plan: PhysicalPlan) -> Response {
        let collection = plan_post_apply_redo(&plan).expect("the plan stores rows");
        let origin = append_group_origin(WalAppendRequest {
            wal: self.appender(),
            event_source: crate::event::EventSource::User,
            tenant_id: TenantId::new(TID),
            vshard_id: VShardId::new(0),
            database_id: DatabaseId::DEFAULT,
            plan: &plan,
            credentials: None,
            now_override: None,
        })
        .expect("pre-dispatch append")
        .origin;
        let response = core.execute(&ExecutionTask::new(request(plan, Some(origin.lsn))));
        append_group_parts(
            self.appender(),
            WriteSetTarget {
                tenant_id: TenantId::new(TID),
                vshard_id: VShardId::new(0),
                database_id: DatabaseId::DEFAULT,
                collection: &collection,
                origin,
            },
            &response.write_set,
        )
        .expect("post-apply append");
        assert_eq!(response.status, Status::Ok, "{response:?}");
        response
    }

    /// Every record replay reads, without the records a marker cancelled.
    fn records(&self) -> Vec<WalRecord> {
        self.wal.sync().expect("sync wal");
        self.wal.replay().expect("read wal")
    }
}

fn request(plan: PhysicalPlan, wal_lsn: Option<crate::types::Lsn>) -> Request {
    Request {
        request_id: RequestId::new(1),
        tenant_id: TenantId::new(TID),
        database_id: DatabaseId::DEFAULT,
        vshard_id: VShardId::new(0),
        plan,
        deadline: Instant::now() + Duration::from_secs(5),
        priority: Priority::Normal,
        trace_id: TraceId::ZERO,
        consistency: ReadConsistency::Strong,
        idempotency_key: None,
        event_source: crate::event::EventSource::User,
        user_roles: Vec::new(),
        user_id: None,
        statement_digest: None,
        txn_id: None,
        wal_lsn,
        resolved_now_ms: None,
        commit_hlc: None,
        admission: Admission::Admitted,
    }
}

/// Install the catalog state a booted core holds before replay.
fn register(core: &mut CoreLoop) {
    let key = |collection: &str| {
        (
            DatabaseId::DEFAULT,
            TenantId::new(TID),
            collection.to_string(),
        )
    };
    core.doc_configs.insert(
        key(ORDERS),
        CollectionConfig::new(ORDERS).with_index("owner"),
    );
    core.doc_configs.insert(
        key(LEDGER),
        CollectionConfig::new(LEDGER).with_bitemporal(true),
    );
    core.vector_params.insert(
        key(&format!("{ORDERS}:emb")),
        crate::engine::vector::hnsw::HnswParams::default(),
    );
}

fn qualified(collection: &str) -> QualifiedCollection {
    QualifiedCollection::new(DatabaseId::DEFAULT, collection)
}

fn encode(value: &Value) -> Vec<u8> {
    nodedb_types::value_to_msgpack(value).expect("encode value")
}

fn order(owner: &str, note: &str, emb: [f64; 3]) -> Vec<u8> {
    let mut obj = HashMap::new();
    obj.insert("owner".to_string(), Value::String(owner.into()));
    obj.insert("note".to_string(), Value::String(note.into()));
    obj.insert(
        "emb".to_string(),
        Value::Array(emb.iter().map(|x| Value::Float(*x)).collect()),
    );
    encode(&Value::Object(obj))
}

fn entry(amount: i64) -> Vec<u8> {
    let mut obj = HashMap::new();
    obj.insert("amount".to_string(), Value::Integer(amount));
    encode(&Value::Object(obj))
}

fn set_field(field: &str, value: Value) -> Vec<(String, UpdateValue)> {
    vec![(field.to_string(), UpdateValue::Literal(encode(&value)))]
}

fn put(collection: &str, id: &str, surrogate: u32, value: Vec<u8>) -> PhysicalPlan {
    PhysicalPlan::Document(DocumentOp::PointPut {
        collection: qualified(collection),
        document_id: id.to_string(),
        value,
        surrogate: Surrogate::new(surrogate),
        pk_bytes: id.as_bytes().to_vec(),
        returning: None,
        rls_filters: Vec::new(),
        resolved_sum_targets: Vec::new(),
    })
}

fn insert(id: &str, surrogate: u32, value: Vec<u8>, if_absent: bool) -> PhysicalPlan {
    PhysicalPlan::Document(DocumentOp::PointInsert {
        collection: qualified(ORDERS),
        document_id: id.to_string(),
        value,
        if_absent,
        surrogate: Surrogate::new(surrogate),
        returning: None,
        rls_filters: Vec::new(),
        resolved_sum_targets: Vec::new(),
        deferred_sum_targets: Vec::new(),
    })
}

fn update(
    collection: &str,
    id: &str,
    surrogate: u32,
    updates: Vec<(String, UpdateValue)>,
) -> PhysicalPlan {
    PhysicalPlan::Document(DocumentOp::PointUpdate {
        collection: qualified(collection),
        document_id: id.to_string(),
        surrogate: Some(Surrogate::new(surrogate)),
        pk_bytes: id.as_bytes().to_vec(),
        updates,
        returning: None,
        rls_filters: Vec::new(),
        rls_write_check: RlsWriteCheck::NoPolicyApplies,
        resolved_sum_targets: Vec::new(),
        declared_primary_key: None,
    })
}

fn delete(collection: &str, id: &str, surrogate: u32) -> PhysicalPlan {
    PhysicalPlan::Document(DocumentOp::PointDelete {
        collection: qualified(collection),
        document_id: id.to_string(),
        surrogate: Some(Surrogate::new(surrogate)),
        pk_bytes: id.as_bytes().to_vec(),
        returning: None,
        rls_filters: Vec::new(),
        rls_write_check: RlsWriteCheck::NoPolicyApplies,
        resolved_sum_targets: Vec::new(),
    })
}

/// `UPDATE orders SET note = <note> WHERE owner = <owner>`.
fn bulk_update(owner: &str, note: &str) -> PhysicalPlan {
    let filter = ScanFilter {
        field: "owner".into(),
        op: FilterOp::Eq,
        value: Value::String(owner.into()),
        clauses: Vec::new(),
        expr: None,
    };
    PhysicalPlan::Document(DocumentOp::BulkUpdate {
        collection: qualified(ORDERS),
        filters: zerompk::to_msgpack_vec(&vec![filter]).expect("encode filter"),
        updates: set_field("note", Value::String(note.into())),
        returning: None,
        ollp_predicted_surrogates: None,
        ollp_predicted_edges: None,
        rls_filters: Vec::new(),
        rls_write_check: RlsWriteCheck::NoPolicyApplies,
        resolved_sum_targets: Vec::new(),
        declared_primary_key: None,
    })
}

fn edge_put(src: (&str, u32), dst: (&str, u32), properties: Vec<u8>) -> PhysicalPlan {
    PhysicalPlan::Graph(GraphOp::EdgePut {
        collection: qualified(SOCIAL),
        src_id: src.0.to_string(),
        label: KNOWS.to_string(),
        dst_id: dst.0.to_string(),
        properties,
        src_surrogate: Surrogate::new(src.1),
        dst_surrogate: Surrogate::new(dst.1),
    })
}

fn edge_delete(src: (&str, u32), dst: (&str, u32)) -> PhysicalPlan {
    PhysicalPlan::Graph(GraphOp::EdgeDelete {
        collection: qualified(SOCIAL),
        src_id: src.0.to_string(),
        label: KNOWS.to_string(),
        dst_id: dst.0.to_string(),
        src_surrogate: Surrogate::new(src.1),
        dst_surrogate: Surrogate::new(dst.1),
        rls_write_check: RlsWriteCheck::NoPolicyApplies,
    })
}

/// Point insert, update and delete, a bulk update, a no-op `if_absent`
/// insert, and versioned point writes on a bitemporal collection.
fn document_workload(journal: &Journal, core: &mut CoreLoop) {
    journal.write(
        core,
        put(ORDERS, "o1", 1, order("alice", "n1", [1.0, 0.0, 0.0])),
    );
    journal.write(
        core,
        insert("o2", 2, order("bob", "n2", [0.0, 1.0, 0.0]), false),
    );
    journal.write(
        core,
        insert("o3", 3, order("alice", "n3", [0.0, 0.0, 1.0]), false),
    );
    journal.write(
        core,
        update(
            ORDERS,
            "o1",
            1,
            set_field("note", Value::String("u1".into())),
        ),
    );
    journal.write(core, bulk_update("alice", "bulk"));
    journal.write(core, delete(ORDERS, "o2", 2));
    // The row exists, so this insert writes nothing. Its pre-dispatch record
    // carries the intruder body and must never replay.
    let skipped = journal.write(
        core,
        insert("o3", 3, order("mallory", "intruder", [1.0, 1.0, 1.0]), true),
    );
    assert!(
        matches!(
            skipped.write_set.as_slice(),
            [only] if matches!(only.effect, RowEffect::CancelForward)
        ),
        "a no-op insert cancels its pre-dispatch record: {:?}",
        skipped.write_set
    );

    journal.write(core, put(LEDGER, "l1", 11, entry(10)));
    journal.write(
        core,
        update(LEDGER, "l1", 11, set_field("amount", Value::Integer(15))),
    );
    journal.write(core, put(LEDGER, "l2", 12, entry(20)));
    journal.write(core, delete(LEDGER, "l2", 12));
}

/// Two edges, a second version of one, and a delete of the other.
fn edge_workload(journal: &Journal, core: &mut CoreLoop) {
    let since = |year: i64| {
        let mut obj = HashMap::new();
        obj.insert("since".to_string(), Value::Integer(year));
        encode(&Value::Object(obj))
    };
    journal.write(core, edge_put(("a", 21), ("b", 22), since(2020)));
    journal.write(core, edge_put(("a", 21), ("c", 23), Vec::new()));
    journal.write(core, edge_put(("a", 21), ("b", 22), since(2024)));
    journal.write(core, edge_delete(("a", 21), ("c", 23)));
}

/// Every stored byte of the tenant's rows and edges, and the derived state
/// replay rebuilds in memory.
#[derive(Debug, PartialEq)]
struct StoredState {
    rows: Vec<(String, Vec<u8>)>,
    indexes: Vec<(String, Vec<u8>)>,
    versions: Vec<(String, Vec<u8>)>,
    versioned_indexes: Vec<(String, Vec<u8>)>,
    live_vectors: usize,
    edges: crate::engine::graph::edge_store::TenantEdgeScan,
    neighbors: Vec<(String, String)>,
}

fn stored_state(core: &mut CoreLoop) -> StoredState {
    let mut neighbors = core
        .csr_partition_mut(DB, TID)
        .neighbors("a", &[], Direction::Out);
    neighbors.sort();
    let sparse = &core.sparse;
    StoredState {
        rows: sparse.scan_all_for_tenant(DB, TID).expect("rows"),
        indexes: sparse.scan_indexes_for_tenant(DB, TID).expect("indexes"),
        versions: sparse
            .scan_versioned_documents_for_tenant(DB, TID)
            .expect("versions"),
        versioned_indexes: sparse
            .scan_versioned_indexes_for_tenant(DB, TID)
            .expect("versioned indexes"),
        live_vectors: core
            .vector_collections
            .values()
            .map(|collection| collection.live_count())
            .sum(),
        edges: core
            .edge_store
            .scan_edges_for_tenant(DB, TenantId::new(TID))
            .expect("edges"),
        neighbors,
    }
}

/// Replay `records` onto the core at `dir` as a booted core does.
fn replay_at(dir: &std::path::Path, records: &[WalRecord]) -> StoredState {
    let (mut core, _req, _resp) = make_core_with_dir(dir);
    register(&mut core);
    core.replay_all_wal(records, 1, &TombstoneSet::new())
        .expect("replay");
    stored_state(&mut core)
}

/// Run `workload` on a fresh live core, and return its state and journal.
fn run_live(
    dir: &std::path::Path,
    workload: impl FnOnce(&Journal, &mut CoreLoop),
) -> (StoredState, Journal) {
    let journal = Journal::open();
    let (mut core, _req, _resp) = make_core_with_dir(dir);
    register(&mut core);
    workload(&journal, &mut core);
    (stored_state(&mut core), journal)
}

#[test]
fn document_writes_replay_onto_a_base_to_the_live_state() {
    let live_dir = tempfile::tempdir().expect("tempdir");
    let (live, journal) = run_live(live_dir.path(), document_workload);
    assert!(!live.rows.is_empty(), "the workload stores current rows");
    assert!(
        !live.indexes.is_empty(),
        "the workload stores index entries"
    );
    assert!(!live.versions.is_empty(), "the workload stores versions");
    assert_eq!(live.live_vectors, 2, "o1 and o3 keep their vectors");

    let base = tempfile::tempdir().expect("tempdir");
    assert_eq!(replay_at(base.path(), &journal.records()), live);
}

#[test]
fn edge_writes_replay_onto_a_base_to_the_live_state() {
    let live_dir = tempfile::tempdir().expect("tempdir");
    let (live, journal) = run_live(live_dir.path(), edge_workload);
    assert_eq!(
        live.neighbors,
        vec![("KNOWS".to_string(), "b".to_string())],
        "only a -> b survives"
    );

    let base = tempfile::tempdir().expect("tempdir");
    assert_eq!(replay_at(base.path(), &journal.records()), live);
}

/// A document delete leaves its node's edges to the transaction's own
/// `EdgeDelete` tasks, and reports no edge tombstone itself. Each
/// `EdgeDelete` journals its tombstone, and replay writes those tombstones
/// without adding any, so every edge version and ordinal matches the live
/// state, on a base and after a restart.
#[test]
fn a_node_delete_replays_the_tombstones_its_edge_deletes_journalled() {
    let live_dir = tempfile::tempdir().expect("tempdir");
    let (live, journal) = run_live(live_dir.path(), |journal, core| {
        journal.write(
            core,
            put(ORDERS, "a", 21, order("alice", "n", [1.0, 0.0, 0.0])),
        );
        journal.write(core, edge_put(("a", 21), ("b", 22), Vec::new()));
        journal.write(core, edge_put(("a", 21), ("c", 23), Vec::new()));
        let deleted = journal.write(core, delete(ORDERS, "a", 21));
        assert!(
            !deleted
                .write_set
                .iter()
                .any(|entry| matches!(&entry.effect, RowEffect::Edge(_))),
            "the row delete tombstones no edge: {:?}",
            deleted.write_set
        );
        journal.write(core, edge_delete(("a", 21), ("b", 22)));
        journal.write(core, edge_delete(("a", 21), ("c", 23)));
    });
    assert!(
        live.neighbors.is_empty(),
        "the edge deletes removed every edge of a"
    );
    assert!(
        !live.edges.visible.is_empty(),
        "the tombstone versions stay stored"
    );

    let records = journal.records();
    let base = tempfile::tempdir().expect("tempdir");
    assert_eq!(replay_at(base.path(), &records), live, "onto a base");
    assert_eq!(
        replay_at(live_dir.path(), &records),
        live,
        "after a restart"
    );
}

/// A restart replays the journal over the rows and edges its own storage
/// already holds. Each replay converges on the live state, and a second
/// restart changes nothing: no row, index entry, vector or edge applies
/// twice.
#[test]
fn a_restart_replays_over_its_own_state_without_double_apply() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (live, journal) = run_live(dir.path(), |journal, core| {
        document_workload(journal, core);
        edge_workload(journal, core);
    });
    let records = journal.records();

    assert_eq!(replay_at(dir.path(), &records), live, "first restart");
    assert_eq!(replay_at(dir.path(), &records), live, "second restart");
}
