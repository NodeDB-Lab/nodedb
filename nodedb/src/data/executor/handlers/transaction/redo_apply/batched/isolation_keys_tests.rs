// SPDX-License-Identifier: BUSL-1.1

//! A parked install holds back the tasks whose keys only the core's
//! registries or the marker's declared keys name: a sorted index by name, a
//! sync stream, a transaction's overlay, a CRDT referent, a MATCH's node
//! collection, and the tenant's graph when the install writes it.

use nodedb_physical::physical_plan::{CrdtOp, GraphOp, GraphReadScope, KvOp, MetaOp, PhysicalPlan};
use nodedb_types::sync::wire::SyncProvenance;
use nodedb_types::{QualifiedCollection, Surrogate, SystemTimeScope};
use nodedb_wal::record::RecordType;

use crate::bridge::envelope::Request;
use crate::control::server::wal_dispatch::encode_vector_put_payload;
use crate::engine::graph::pattern::ast::MatchQuery;
use crate::engine::kv::sorted_index::manager::SortedIndexDef;
use crate::engine::kv::sorted_index::{SortKeyEncoder, WindowConfig};
use crate::types::{DatabaseId, RequestId, TenantId, TxnId};
use crate::wal::RedoSubRecord;

use super::super::stream_validate::tests::TID;
use super::equivalence_tests::{FILL, FILL_FROM, fill};
use super::isolation_tests::{
    OTHER, TASK, assert_runs_in, assert_waits_in, coll, edge_put, kv_put, off_vshard,
    parked_install_with, point_put,
};
use super::yield_tests::{INSTALL, ROWS, Ring, tick_until_answered_while_waiting};

/// The producer and stream of the install's sync frame.
const PRODUCER: u64 = 71;
const STREAM: u64 = 42;
/// The collection the install's sync frame writes.
const VECTORS: &str = "vectors";
/// The edge collection the install's edge writes.
const KNOWS: &str = "knows";

/// The filler rows after the install's leading `ops`, so it parks.
fn led_by(ops: Vec<RedoSubRecord>) -> Vec<RedoSubRecord> {
    let mut all = ops;
    all.extend(fill(FILL_FROM, ROWS));
    all
}

/// The provenance of frame `seq` of `(producer, stream)`.
fn frame(producer_id: u64, stream_id: u64, seq: u64) -> SyncProvenance {
    SyncProvenance {
        producer_id,
        epoch: 1,
        stream_id,
        seq,
    }
}

/// Queue an install whose first op is a sync frame of [`PRODUCER`]'s
/// [`STREAM`].
fn push_sync_install(ring: &mut Ring) {
    let provenance = frame(PRODUCER, STREAM, 1);
    let payload = encode_vector_put_payload(
        VECTORS,
        &[1.0, 2.0],
        2,
        "",
        Surrogate::new(31_100),
        Some(&provenance),
    )
    .expect("encode vector put");
    let vector = RedoSubRecord {
        record_type: RecordType::VectorPut as u32,
        payload,
    };
    ring.push_install_writing(
        led_by(vec![vector]),
        vec![FILL.to_string(), VECTORS.to_string()],
    );
}

/// Queue an install whose first op is an edge put.
fn push_graph_install(ring: &mut Ring) {
    let edge = RedoSubRecord {
        record_type: RecordType::Put as u32,
        payload: zerompk::to_msgpack_vec(&crate::wal::EdgePutRedo {
            collection: KNOWS.into(),
            src_id: "carol".into(),
            label: "KNOWS".into(),
            dst_id: "dave".into(),
            properties: Vec::new(),
            src_surrogate: 31_101,
            dst_surrogate: 31_102,
            system_from: Some(500),
            applied: None,
            valid_from_ms: None,
            valid_until_ms: None,
        })
        .expect("encode edge put"),
    };
    ring.push_install_writing(
        led_by(vec![edge]),
        vec![FILL.to_string(), KNOWS.to_string()],
    );
}

#[test]
fn a_sorted_index_read_waits_on_an_index_of_the_installs_collection() {
    let count = |index: &str| {
        off_vshard(PhysicalPlan::Kv(KvOp::SortedIndexCount {
            index_name: index.into(),
            system_time: SystemTimeScope::Current,
        }))
    };
    let with_indexes = |ring: &mut Ring| {
        for (index, collection) in [("on_fill", FILL), ("on_other", OTHER)] {
            let def = SortedIndexDef {
                name: index.into(),
                collection: collection.into(),
                key_column: "k".into(),
                encoder: SortKeyEncoder::new(Vec::new()),
                window: WindowConfig::none(),
            };
            ring.core.kv_engine.register_sorted_index(
                DatabaseId::DEFAULT.as_u64(),
                TID,
                collection,
                def,
            );
        }
        ring.push_fill_install();
    };
    assert_waits_in(parked_install_with(with_indexes, count("on_fill")));
    assert_runs_in(parked_install_with(with_indexes, count("on_other")));
}

#[test]
fn a_sync_frame_of_the_installs_stream_waits() {
    let task = off_vshard(kv_put(OTHER, Some(frame(PRODUCER, STREAM, 2))));
    assert_waits_in(parked_install_with(push_sync_install, task));
}

/// A stream id names no database, so a frame of the same stream into
/// another database waits too.
#[test]
fn a_sync_frame_of_the_installs_stream_waits_from_another_database() {
    let other = DatabaseId::new(7);
    let plan = PhysicalPlan::Kv(KvOp::Put {
        collection: QualifiedCollection::new(other, OTHER),
        declared_columns: nodedb_physical::physical_plan::KvDeclaredColumns::none(),
        key: b"k1".to_vec(),
        value: crate::data::executor::doc_format::encode_to_msgpack(&serde_json::json!({"n": 1})),
        ttl_ms: 0,
        surrogate: Surrogate::new(31_103),
        returning: None,
        rls_filters: Vec::new(),
        provenance: Some(frame(PRODUCER, STREAM, 2)),
    });
    let mut task = off_vshard(plan);
    task.database_id = other;
    assert_waits_in(parked_install_with(push_sync_install, task));
}

#[test]
fn a_sync_frame_of_another_stream_runs() {
    let task = off_vshard(kv_put(OTHER, Some(frame(PRODUCER, STREAM + 1, 1))));
    assert_runs_in(parked_install_with(push_sync_install, task));
}

/// A request of transaction `txn`.
fn in_txn(mut request: Request, txn: u64) -> Request {
    request.txn_id = Some(TxnId::new(txn));
    request
}

/// The request id of an overlay drop queued behind [`TASK`].
const DROP: u64 = TASK + 1;

/// The drop of transaction `txn`'s overlay, as request [`DROP`].
fn overlay_drop(txn: u64) -> Request {
    let mut release = off_vshard(PhysicalPlan::Meta(MetaOp::DropTxnOverlay {
        txn_id: TxnId::new(txn),
    }));
    release.request_id = RequestId::new(DROP);
    release
}

/// An overlay drop never runs ahead of its own transaction's waiting
/// staged write.
#[test]
fn an_overlay_drop_waits_behind_its_transactions_waiting_staged_write() {
    let stage = PhysicalPlan::Meta(MetaOp::StageWrite {
        plan: Box::new(point_put(FILL, Vec::new())),
    });
    let mut ring = parked_install_with(
        |ring| ring.push_fill_install(),
        in_txn(off_vshard(stage), 7),
    );
    ring.push(overlay_drop(7), None);
    tick_until_answered_while_waiting(&mut ring, INSTALL, &[TASK, DROP]);
    ring.tick_until_answered(TASK);
    ring.tick_until_answered(DROP);
}

#[test]
fn an_overlay_drop_of_another_transaction_runs() {
    let stage = PhysicalPlan::Meta(MetaOp::StageWrite {
        plan: Box::new(point_put(FILL, Vec::new())),
    });
    let mut ring = parked_install_with(
        |ring| ring.push_fill_install(),
        in_txn(off_vshard(stage), 7),
    );
    ring.push(overlay_drop(8), None);
    ring.tick();
    assert!(ring.answer(DROP).is_some(), "the drop ran");
    assert!(ring.answer(TASK).is_none(), "the staged write still waits");
    ring.tick_until_answered(INSTALL);
    ring.tick_until_answered(TASK);
}

#[test]
fn a_collection_size_read_keys_its_collection() {
    let size = |name: &str| {
        off_vshard(PhysicalPlan::Meta(MetaOp::QueryCollectionSize {
            tenant_id: TID,
            name: name.into(),
        }))
    };
    assert_runs_in(parked_install_with(
        |ring| ring.push_fill_install(),
        size(OTHER),
    ));
    assert_waits_in(parked_install_with(
        |ring| ring.push_fill_install(),
        size(FILL),
    ));
}

/// A CRDT apply's foreign-key check reads its referent.
#[test]
fn a_crdt_apply_waits_when_its_foreign_key_references_the_installs_collection() {
    let apply = |collection: &str| {
        off_vshard(PhysicalPlan::Crdt(CrdtOp::Apply {
            collection: coll(collection),
            document_id: "d1".into(),
            delta: Vec::new(),
            peer_id: 1,
            mutation_id: 1,
            surrogate: Surrogate::new(31_104),
            provenance: None,
            constraint_version_required: 0,
            expected_frontier_digest: None,
        }))
    };
    let with_fk = |ring: &mut Ring| {
        let mut constraints = nodedb_crdt::constraint::ConstraintSet::new();
        constraints.add_foreign_key("orders_fill_fk", "orders", "fill_id", FILL, "id");
        let engine = ring
            .core
            .get_crdt_engine(DatabaseId::DEFAULT, TenantId::new(TID))
            .expect("crdt engine");
        engine
            .validator
            .set_collection_constraints("orders", constraints.all().to_vec());
        ring.push_fill_install();
    };
    assert_waits_in(parked_install_with(with_fk, apply("orders")));
    assert_runs_in(parked_install_with(with_fk, apply("notes")));
}

#[test]
fn an_install_with_graph_writes_holds_an_edge_put_of_its_tenant() {
    assert_waits_in(parked_install_with(
        push_graph_install,
        off_vshard(edge_put("social")),
    ));
}

/// The graph fence covers the install's tenant alone.
#[test]
fn an_install_with_graph_writes_lets_an_edge_put_of_another_tenant_run() {
    let mut task = off_vshard(edge_put("social"));
    task.tenant_id = TenantId::new(TID + 1);
    assert_runs_in(parked_install_with(push_graph_install, task));
}

/// A MATCH whose `IN` collection is the installing collection reads its
/// node documents. An install with no graph writes leaves the graph free,
/// so only the node collection holds the MATCH back.
#[test]
fn a_match_waits_while_the_install_holds_its_node_collection() {
    let matching = |collection: &str| {
        let query = zerompk::to_msgpack_vec(&MatchQuery {
            clauses: Vec::new(),
            where_predicates: Vec::new(),
            return_columns: Vec::new(),
            distinct: false,
            limit: None,
            offset: None,
            order_by: Vec::new(),
            collection: Some(collection.to_string()),
        })
        .expect("encode match query");
        off_vshard(PhysicalPlan::Graph(GraphOp::Match {
            query,
            frontier_bitmap: None,
            cluster_mode: false,
            scope: GraphReadScope::default(),
        }))
    };
    assert_waits_in(parked_install_with(
        |ring| ring.push_fill_install(),
        matching(FILL),
    ));
    assert_runs_in(parked_install_with(
        |ring| ring.push_fill_install(),
        matching(OTHER),
    ));
}
