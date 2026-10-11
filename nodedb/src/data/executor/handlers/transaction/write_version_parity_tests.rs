// SPDX-License-Identifier: BUSL-1.1

//! A committed Calvin slice installs through its stamped redo entry, and the
//! install records every write version the slice's plans imply.
//!
//! The oracle is `record_batch_write_versions` over the slice's plans. For
//! each engine, every per-key version and collection floor the oracle records
//! holds on the installed core at the install's LSN or above.

use nodedb_physical::physical_plan::{
    CalvinInstall, CalvinReplySpec, CalvinResolved, ColumnarInsertIntent, ColumnarOp, CrdtOp,
    CrdtWriteVerb, DocumentOp, GraphOp, KvOp, MetaOp, PhysicalPlan, RedoOrigin, SpatialOp, TextOp,
    TimeseriesOp, VectorOp,
};
use nodedb_types::geometry::Geometry;
use nodedb_types::{DatabaseId, QualifiedCollection, RlsWriteCheck, Surrogate, Value};

use crate::bridge::envelope::{Response, Status};
use crate::control::wal_replication::transaction_redo::collections::written_collections;
use crate::control::wal_replication::transaction_redo::sum_targets::redo_sum_targets;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::core_loop::tests::{make_core_with_dir, make_default_task};
use crate::data::executor::handlers::control::calvin::CalvinExecCtx;
use crate::data::executor::task::ExecutionTask;
use crate::types::Lsn;
use crate::wal::{RedoRecord, RedoSubRecord};

/// The LSN every install and every oracle record names.
const LSN: u64 = 300;

/// A core with its bridge ends and data directory kept alive.
struct Core {
    core: CoreLoop,
    _req: Box<dyn std::any::Any>,
    _resp: Box<dyn std::any::Any>,
    _dir: tempfile::TempDir,
}

fn fresh_core() -> Core {
    let dir = tempfile::tempdir().expect("tempdir");
    let (core, req, resp) = make_core_with_dir(dir.path());
    Core {
        core,
        _req: Box::new(req),
        _resp: Box::new(resp),
        _dir: dir,
    }
}

fn tid() -> u64 {
    make_default_task().request.tenant_id.as_u64()
}

fn task_at(lsn: u64) -> ExecutionTask {
    let mut task = make_default_task();
    task.wal_lsn = Some(Lsn::new(lsn));
    task
}

fn object(fields: &[(&str, Value)]) -> Vec<u8> {
    let map: std::collections::HashMap<String, Value> = fields
        .iter()
        .map(|(k, v)| ((*k).to_string(), v.clone()))
        .collect();
    nodedb_types::value_to_msgpack(&Value::Object(map)).expect("encode object")
}

/// Stage `plans` as the slice at `(1, 0)` and resolve its redo.
fn stage_and_resolve(core: &mut CoreLoop, plans: &[PhysicalPlan]) -> CalvinResolved {
    let task = make_default_task();
    let ctx = CalvinExecCtx {
        epoch: 1,
        position: 0,
        epoch_system_ms: 0,
    };
    let tenant = task.request.tenant_id;
    let staged = core.execute_calvin_execute_static(&task, ctx, &tenant, plans, &[], &[]);
    assert_eq!(staged.status, Status::Ok, "{:?}", staged.error_code);
    let resolved = core.execute_calvin_resolve(&task, 1, 0);
    assert_eq!(resolved.status, Status::Ok, "{:?}", resolved.error_code);
    zerompk::from_msgpack(resolved.payload.as_bytes()).expect("decode resolved answer")
}

/// Install `redo` as the stamped slice at `(1, 0)` at [`LSN`].
fn install(
    core: &mut CoreLoop,
    redo: Vec<u8>,
    plans: &[PhysicalPlan],
    reply: CalvinReplySpec,
) -> Response {
    let plan = PhysicalPlan::Meta(MetaOp::ApplyTransactionRedo {
        redo,
        collections: written_collections(plans),
        sum_targets: redo_sum_targets(plans),
        origin: RedoOrigin::Commit,
        calvin: Some(CalvinInstall {
            epoch: 1,
            position: 0,
            epoch_system_ms: 0,
            reply,
            user_write: true,
        }),
    });
    let installed = core.execute_plan(&task_at(LSN), &plan);
    assert_eq!(installed.status, Status::Ok, "{:?}", installed.error_code);
    installed
}

/// Check that `installed` holds every version the oracle records for
/// `plans`. The oracle must record at least one collection floor.
fn assert_parity(installed: &CoreLoop, plans: &[PhysicalPlan]) {
    let mut oracle = fresh_core();
    oracle
        .core
        .record_batch_write_versions(&task_at(LSN), tid(), plans);
    let floors: Vec<_> = oracle.core.write_index.recorded_collections().collect();
    assert!(!floors.is_empty(), "the oracle records the slice's writes");
    for (key, version) in floors {
        let held = installed.write_index.collection_version(key);
        assert!(
            held.is_some_and(|held| held >= version),
            "the install records the collection floor of {key:?}: {held:?}"
        );
    }
    for (key, version) in oracle.core.write_index.recorded_keys() {
        let held = installed.write_index.key_version(key);
        assert!(
            held.is_some_and(|held| held >= version),
            "the install records the version of {key:?}: {held:?}"
        );
    }
}

/// Stage, resolve and install `plans` as one Calvin slice on a fresh core,
/// then check parity.
fn staged_slice_parity(plans: &[PhysicalPlan]) {
    let mut core = fresh_core();
    let resolved = stage_and_resolve(&mut core.core, plans);
    install(&mut core.core, resolved.redo, plans, resolved.reply);
    assert_parity(&core.core, plans);
}

#[test]
fn a_document_slice_records_every_row_version() {
    let plans = [
        PhysicalPlan::Document(DocumentOp::PointInsert {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "notes"),
            document_id: "n1".to_string(),
            value: object(&[("title", Value::String("kept".into()))]),
            if_absent: false,
            surrogate: Surrogate::new(41),
            returning: None,
            rls_filters: Vec::new(),
            resolved_sum_targets: Vec::new(),
            deferred_sum_targets: Vec::new(),
        }),
        PhysicalPlan::Document(DocumentOp::PointPut {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "notes"),
            document_id: "n2".to_string(),
            value: object(&[("title", Value::String("also".into()))]),
            surrogate: Surrogate::new(42),
            pk_bytes: b"n2".to_vec(),
            returning: None,
            rls_filters: Vec::new(),
            resolved_sum_targets: Vec::new(),
        }),
    ];
    staged_slice_parity(&plans);
}

#[test]
fn a_kv_slice_records_every_key_version() {
    let put = |key: &[u8], surrogate: u32| {
        PhysicalPlan::Kv(KvOp::Put {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "cache"),
            key: key.to_vec(),
            value: object(&[("n", Value::Integer(1))]),
            ttl_ms: 0,
            surrogate: Surrogate::new(surrogate),
            returning: None,
            rls_filters: Vec::new(),
            provenance: None,
        })
    };
    staged_slice_parity(&[put(b"k1", 51), put(b"k2", 52)]);
}

#[test]
fn a_vector_slice_records_every_surrogate_version() {
    let plan = PhysicalPlan::Vector(VectorOp::DirectInsert {
        collection: QualifiedCollection::new(DatabaseId::DEFAULT, "vp"),
        field: "vec".into(),
        surrogate: Surrogate::new(61),
        pk_bytes: b"r".to_vec(),
        vector: vec![1.0, 0.0],
        payload: zerompk::to_msgpack_vec(&std::collections::HashMap::from([(
            "id".to_string(),
            Value::String("r".into()),
        )]))
        .expect("encode payload"),
        quantization: nodedb_types::VectorQuantization::None,
        storage_dtype: nodedb_types::VectorStorageDtype::F32,
        payload_indexes: Vec::new(),
        returning: None,
        rls_filters: Vec::new(),
    });
    staged_slice_parity(&[plan]);
}

#[test]
fn a_graph_slice_records_every_edge_version() {
    let plan = PhysicalPlan::Graph(GraphOp::EdgePut {
        collection: QualifiedCollection::new(DatabaseId::DEFAULT, "social"),
        src_id: "a".into(),
        label: "KNOWS".into(),
        dst_id: "b".into(),
        properties: Vec::new(),
        src_surrogate: Surrogate::new(1),
        dst_surrogate: Surrogate::new(2),
    });
    staged_slice_parity(&[plan]);
}

#[test]
fn a_columnar_slice_records_its_collection_floor() {
    let rows = Value::Array(vec![Value::Object(std::collections::HashMap::from([
        ("id".to_string(), Value::Integer(1)),
        ("v".to_string(), Value::Integer(7)),
    ]))]);
    let plan = PhysicalPlan::Columnar(ColumnarOp::Insert {
        collection: QualifiedCollection::new(DatabaseId::DEFAULT, "metrics_c"),
        payload: nodedb_types::value_to_msgpack(&rows).expect("encode rows"),
        format: "msgpack".into(),
        intent: ColumnarInsertIntent::Insert,
        on_conflict_updates: Vec::new(),
        surrogates: vec![Surrogate::new(71)],
        schema_bytes: Vec::new(),
        provenance: None,
        wal_lsn: None,
        rls_write_check: RlsWriteCheck::already_decided_elsewhere(),
        returning: None,
        rls_filters: Vec::new(),
    });
    staged_slice_parity(&[plan]);
}

#[test]
fn a_timeseries_slice_records_its_collection_floor() {
    let plan = PhysicalPlan::Timeseries(TimeseriesOp::Ingest {
        collection: QualifiedCollection::new(DatabaseId::DEFAULT, "metrics"),
        payload: b"metrics,host=a value=1 1000000000".to_vec(),
        format: "ilp".to_owned(),
        wal_lsn: None,
        surrogates: Vec::new(),
        provenance: None,
        rls_write_check: RlsWriteCheck::NoPolicyApplies,
        returning: None,
        rls_filters: Vec::new(),
    });
    staged_slice_parity(&[plan]);
}

#[test]
fn a_spatial_slice_records_its_collection_floor() {
    let plan = PhysicalPlan::Spatial(SpatialOp::Insert {
        collection: QualifiedCollection::new(DatabaseId::DEFAULT, "places"),
        field: "geom".to_string(),
        surrogate: Surrogate::new(81),
        geometry: Geometry::Point {
            coordinates: [1.0, 2.0],
        },
        provenance: None,
    });
    staged_slice_parity(&[plan]);
}

#[test]
fn a_crdt_slice_records_its_collection_floor() {
    let plan = PhysicalPlan::Crdt(CrdtOp::DocUpsert {
        collection: QualifiedCollection::new(DatabaseId::DEFAULT, "tasks"),
        document_id: "t1".to_string(),
        fields_json: r#"{"title":"kept"}"#.to_string(),
        surrogate: Surrogate::new(71),
        partial: false,
        verb: CrdtWriteVerb::Insert,
        returning: None,
        rls_filters: Vec::new(),
    });
    staged_slice_parity(&[plan]);
}

/// A text write stages no row. Its slice installs the sub-record its
/// resolve serializes from the plan.
#[test]
fn a_text_slice_records_its_collection_floor() {
    let op = TextOp::FtsIndexDoc {
        collection: QualifiedCollection::new(DatabaseId::DEFAULT, "docs"),
        surrogate: Surrogate::new(91),
        fields: vec![("body".to_string(), "hello world".to_string())],
        provenance: None,
    };
    let (record_type, payload) = crate::control::server::wal_dispatch::encode_text_op_record(&op)
        .expect("encode text op")
        .expect("an index op journals a record");
    let redo = RedoRecord {
        version: 1,
        ops: vec![RedoSubRecord {
            record_type: record_type as u32,
            payload,
        }],
        calvin_stamp: None,
        cross_shard_applied: None,
        row_sources: Vec::new(),
        publishes: Vec::new(),
        row_changes: Vec::new(),
    }
    .to_bytes()
    .expect("encode redo");
    let plans = [PhysicalPlan::Text(op)];

    let mut core = fresh_core();
    install(
        &mut core.core,
        redo,
        &plans,
        CalvinReplySpec::Count(Vec::new()),
    );

    assert_parity(&core.core, &plans);
}
