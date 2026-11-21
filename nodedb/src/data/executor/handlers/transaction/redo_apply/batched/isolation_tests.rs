// SPDX-License-Identifier: BUSL-1.1

//! A parked install holds back only the tasks that touch what it writes.
//!
//! The install parks on [`FILL`] in the default database, with its events
//! on vShard 0. It writes no graph state, so the tenant's graph stays
//! unfenced. A write of another collection of the same database runs
//! between the install's batches. The same write of [`FILL`] waits until
//! the install ends.

use nodedb_array::types::ArrayId;
use nodedb_physical::physical_plan::{
    ArrayOp, ColumnarInsertIntent, ColumnarOp, DocumentOp, EnforcementOptions, GraphOp,
    KvDeclaredColumns, KvOp, PhysicalPlan, ResolvedSumTarget, SpatialOp, StorageMode, TextFieldSet,
    TextOp, TimeseriesOp, VectorOp,
};
use nodedb_types::geometry::{Geometry, SridGeometry};
use nodedb_types::sync::wire::SyncProvenance;
use nodedb_types::{QualifiedCollection, RlsWriteCheck, Surrogate, SystemTimeScope};

use crate::bridge::envelope::Request;
use crate::types::{DatabaseId, TenantId, VShardId};

use super::super::stream_validate::tests::{TID, plain_core};
use super::equivalence_tests::FILL;
use super::yield_tests::{INSTALL, OTHER_VSHARD, Ring, request, tick_until_answered_while_waiting};

/// The collection the install never touches.
pub(super) const OTHER: &str = "b";
/// The request id of the task each case queues behind the install.
pub(super) const TASK: u64 = 20;

pub(super) fn coll(name: &str) -> QualifiedCollection {
    QualifiedCollection::new(DatabaseId::DEFAULT, name)
}

/// A request of `plan` whose events carry another vShard than the
/// install's.
pub(super) fn off_vshard(plan: PhysicalPlan) -> Request {
    let mut request = request(TASK, plan);
    request.vshard_id = OTHER_VSHARD;
    request
}

/// A core whose install of [`FILL`] parked, with `task` queued behind it.
fn parked_with(task: Request) -> Ring {
    parked_install_with(|ring| ring.push_fill_install(), task)
}

/// A core whose install, queued by `push_install`, parked with `task`
/// queued behind it.
pub(super) fn parked_install_with(push_install: impl FnOnce(&mut Ring), task: Request) -> Ring {
    let mut ring = Ring::open(plain_core);
    push_install(&mut ring);
    ring.push(task, None);
    ring.tick();
    assert!(ring.core.has_parked_install(), "the install parked");
    ring
}

/// `task` runs while the install is parked.
fn assert_runs(task: Request) {
    assert_runs_in(parked_with(task));
}

/// [`TASK`] ran while the install of `ring` is parked.
pub(super) fn assert_runs_in(mut ring: Ring) {
    assert!(
        ring.answer(TASK).is_some(),
        "the task ran between the install's batches"
    );
    assert!(
        ring.answer(INSTALL).is_none(),
        "the install is still parked"
    );
    ring.tick_until_answered(INSTALL);
}

/// `task` waits until the install ends, then runs.
fn assert_waits(task: Request) {
    assert_waits_in(parked_with(task));
}

/// [`TASK`] waits until the install of `ring` ends, then runs.
pub(super) fn assert_waits_in(mut ring: Ring) {
    tick_until_answered_while_waiting(&mut ring, INSTALL, &[TASK]);
    ring.tick_until_answered(TASK);
}

/// `plan_of(OTHER)` runs while the install is parked, and `plan_of(FILL)`
/// waits.
fn assert_keyed(plan_of: impl Fn(&str) -> PhysicalPlan) {
    assert_runs(off_vshard(plan_of(OTHER)));
    assert_waits(off_vshard(plan_of(FILL)));
}

pub(super) fn point_put(name: &str, targets: Vec<ResolvedSumTarget>) -> PhysicalPlan {
    PhysicalPlan::Document(DocumentOp::PointPut {
        collection: coll(name),
        document_id: "d1".into(),
        value: crate::data::executor::doc_format::encode_to_msgpack(&serde_json::json!({"n": 1})),
        surrogate: Surrogate::new(31_000),
        pk_bytes: Vec::new(),
        returning: None,
        rls_filters: Vec::new(),
        resolved_sum_targets: targets,
        rls_write_check: RlsWriteCheck::NoPolicyApplies,
    })
}

pub(super) fn kv_put(name: &str, provenance: Option<SyncProvenance>) -> PhysicalPlan {
    PhysicalPlan::Kv(KvOp::Put {
        collection: coll(name),
        declared_columns: KvDeclaredColumns::none(),
        key: b"k1".to_vec(),
        value: crate::data::executor::doc_format::encode_to_msgpack(&serde_json::json!({"n": 1})),
        ttl_ms: 0,
        surrogate: Surrogate::new(31_001),
        returning: None,
        rls_filters: Vec::new(),
        provenance,
    })
}

pub(super) fn edge_put(name: &str) -> PhysicalPlan {
    PhysicalPlan::Graph(GraphOp::EdgePut {
        collection: coll(name),
        src_id: "alice".into(),
        label: "follows".into(),
        dst_id: "bob".into(),
        properties: Vec::new(),
        src_surrogate: Surrogate::new(31_002),
        dst_surrogate: Surrogate::new(31_003),
        valid_from_ms: None,
        valid_until_ms: None,
    })
}

#[test]
fn a_document_point_put_of_another_collection_runs() {
    assert_keyed(|name| point_put(name, Vec::new()));
}

#[test]
fn a_document_bulk_delete_of_another_collection_runs() {
    assert_keyed(|name| {
        PhysicalPlan::Document(DocumentOp::BulkDelete {
            collection: coll(name),
            filters: Vec::new(),
            returning: None,
            ollp_predicted_surrogates: None,
            ollp_predicted_edges: None,
            rls_filters: Vec::new(),
            rls_write_check: RlsWriteCheck::NoPolicyApplies,
            resolved_sum_targets: Vec::new(),
            declared_primary_key: None,
            system_time: SystemTimeScope::Current,
        })
    });
}

#[test]
fn a_kv_put_of_another_collection_runs() {
    assert_keyed(|name| kv_put(name, None));
}

#[test]
fn a_vector_insert_of_another_collection_runs() {
    assert_keyed(|name| {
        PhysicalPlan::Vector(VectorOp::Insert {
            collection: coll(name),
            vector: vec![1.0, 0.0],
            dim: 2,
            field_name: "emb".into(),
            surrogate: Surrogate::new(31_004),
            pk_bytes: None,
            provenance: None,
        })
    });
}

#[test]
fn a_text_index_write_of_another_collection_runs() {
    assert_keyed(|name| {
        PhysicalPlan::Text(TextOp::FtsIndexDoc {
            collection: coll(name),
            surrogate: Surrogate::new(31_005),
            fields: vec![("body".into(), "hello".into())],
            provenance: None,
        })
    });
}

#[test]
fn a_columnar_insert_of_another_collection_runs() {
    assert_keyed(|name| {
        PhysicalPlan::Columnar(ColumnarOp::Insert {
            collection: coll(name),
            payload: Vec::new(),
            format: "msgpack".into(),
            intent: ColumnarInsertIntent::Insert,
            on_conflict_updates: Vec::new(),
            surrogates: Vec::new(),
            schema_bytes: Vec::new(),
            provenance: None,
            wal_lsn: None,
            system_from: None,
            restored_versions: Vec::new(),
            rls_write_check: RlsWriteCheck::NoPolicyApplies,
            returning: None,
            rls_filters: Vec::new(),
        })
    });
}

#[test]
fn a_timeseries_ingest_of_another_collection_runs() {
    assert_keyed(|name| {
        PhysicalPlan::Timeseries(TimeseriesOp::Ingest {
            collection: coll(name),
            payload: b"m v=1i 1000000000\n".to_vec(),
            format: "ilp".into(),
            wal_lsn: None,
            surrogates: Vec::new(),
            provenance: None,
            rls_write_check: RlsWriteCheck::NoPolicyApplies,
            returning: None,
            rls_filters: Vec::new(),
        })
    });
}

#[test]
fn a_spatial_insert_of_another_collection_runs() {
    assert_keyed(|name| {
        PhysicalPlan::Spatial(SpatialOp::Insert {
            collection: coll(name),
            field: "geom".into(),
            surrogate: Surrogate::new(31_006),
            geometry: SridGeometry::wgs84(Geometry::point(1.0, 2.0)),
            provenance: None,
        })
    });
}

/// An array keys by the stored name its writes go by, the name the
/// install arms for a collection of the same name.
#[test]
fn an_array_put_of_another_array_runs() {
    assert_keyed(|name| {
        PhysicalPlan::Array(ArrayOp::Put {
            array_id: ArrayId::new(TenantId::new(TID), name),
            cells_msgpack: Vec::new(),
            wal_lsn: 0,
            provenance: None,
            vshard_id: OTHER_VSHARD.as_u32(),
        })
    });
}

#[test]
fn a_truncate_of_another_collection_runs() {
    assert_keyed(|name| {
        PhysicalPlan::Document(DocumentOp::Truncate {
            collection: coll(name),
            restart_identity: false,
            resolved_sum_targets: Vec::new(),
            declared_primary_key: None,
        })
    });
}

#[test]
fn a_register_of_another_collection_runs() {
    assert_keyed(|name| {
        PhysicalPlan::Document(DocumentOp::Register {
            collection: coll(name),
            indexes: Vec::new(),
            crdt_enabled: false,
            storage_mode: StorageMode::default(),
            enforcement: Box::new(EnforcementOptions::default()),
            bitemporal: false,
            conflict_policy: None,
            timeseries: None,
            vector_primary: None,
            vector_fields: Vec::new(),
            declared_columns: Vec::new(),
            declared_key: None,
            text_fields: TextFieldSet::default(),
            incarnation: nodedb_types::Hlc::ZERO,
            digest: 0,
        })
    });
}

/// A write of another collection that folds into the installing collection
/// waits: its sum target is fenced.
#[test]
fn a_write_whose_sum_target_is_fenced_waits() {
    let target = ResolvedSumTarget::new(FILL, "a1", Surrogate::new(31_007));
    assert_waits(off_vshard(point_put(OTHER, vec![target])));
}

/// An install that writes no graph state leaves its tenant's graph
/// unfenced.
#[test]
fn an_edge_put_of_the_installs_tenant_runs_past_an_install_with_no_graph_writes() {
    assert_runs(off_vshard(edge_put("social")));
}

#[test]
fn a_label_write_of_the_installs_tenant_runs_past_an_install_with_no_graph_writes() {
    assert_runs(off_vshard(PhysicalPlan::Graph(GraphOp::SetNodeLabels {
        node_id: "alice".into(),
        labels: vec!["Person".into()],
    })));
}

#[test]
fn a_graph_read_runs_past_an_install_with_no_graph_writes() {
    assert_runs(off_vshard(PhysicalPlan::Graph(GraphOp::Stats {
        collection: None,
        as_of: None,
    })));
}

/// A write whose events carry the install's vShard waits, whatever it
/// writes. Its events advance the partition past the install's held events.
#[test]
fn a_write_of_the_installs_vshard_waits() {
    let mut task = request(TASK, point_put(OTHER, Vec::new()));
    task.vshard_id = VShardId::new(0);
    assert_waits(task);
}

/// A sync frame keys its collection and its stream. An install whose ops
/// carry no provenance fences no stream.
#[test]
fn a_sync_frame_of_another_collection_runs_past_an_install_of_no_sync_frame() {
    let provenance = SyncProvenance {
        producer_id: 9,
        epoch: 1,
        stream_id: 1,
        seq: 1,
    };
    assert_runs(off_vshard(kv_put(OTHER, Some(provenance))));
}
