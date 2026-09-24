// SPDX-License-Identifier: BUSL-1.1

//! One committed transaction per engine kind, installed and then refused.
//!
//! Each kind commits once to show where its write lands, and once with a
//! sub-record after its own that fails while it installs. The refused
//! install rolls the write back, so the core holds none of it.

use nodedb_array::schema::ArraySchemaBuilder;
use nodedb_array::schema::attr_spec::{AttrSpec, AttrType};
use nodedb_array::schema::dim_spec::{DimSpec, DimType};
use nodedb_array::types::ArrayId;
use nodedb_array::types::cell_value::value::CellValue;
use nodedb_array::types::coord::value::CoordValue;
use nodedb_array::types::domain::{Domain, DomainBound};
use nodedb_physical::physical_plan::{
    ArrayOp, CrdtOp, CrdtWriteVerb, DocumentOp, KvOp, PhysicalPlan, SpatialOp, VectorOp,
};
use nodedb_types::geometry::Geometry;
use nodedb_types::{DatabaseId, QualifiedCollection, RlsWriteCheck, Surrogate, TenantId, Value};

use crate::bridge::envelope::{ErrorCode, Response, Status};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::core_loop::tests::{make_core_with_dir, make_default_task};
use crate::engine::array::wal::ArrayPutCell;

const TID: u64 = 1;

fn assert_committed(response: &Response) {
    assert_eq!(response.status, Status::Ok, "{:?}", response.error_code);
}

fn assert_refused_at_install(response: &Response) {
    assert!(
        matches!(
            response.error_code.as_deref(),
            Some(ErrorCode::RetryableRefusal { .. })
        ),
        "the install fails after the transaction's writes: {:?}",
        response.error_code
    );
    assert_eq!(response.status, Status::Error);
}

/// Commit `plans` on a fresh core, or refuse their install when `refuse`.
fn run(plans: &[PhysicalPlan], refuse: bool, setup: impl FnOnce(&mut CoreLoop)) -> CoreLoopHold {
    let dir = tempfile::tempdir().expect("tempdir");
    let (mut core, req, resp) = make_core_with_dir(dir.path());
    setup(&mut core);
    let task = make_default_task();
    let response = if refuse {
        core.commit_plans_then_refuse_for_test(&task, TID, plans, 200)
    } else {
        core.commit_plans_for_test(&task, TID, plans, 200)
    };
    if refuse {
        assert_refused_at_install(&response);
    } else {
        assert_committed(&response);
    }
    CoreLoopHold {
        core,
        _req: Box::new(req),
        _resp: Box::new(resp),
        _dir: dir,
    }
}

/// A core with its bridge ends and data directory kept alive.
struct CoreLoopHold {
    core: CoreLoop,
    _req: Box<dyn std::any::Any>,
    _resp: Box<dyn std::any::Any>,
    _dir: tempfile::TempDir,
}

fn object(fields: &[(&str, Value)]) -> Vec<u8> {
    let map: std::collections::HashMap<String, Value> = fields
        .iter()
        .map(|(k, v)| ((*k).to_string(), v.clone()))
        .collect();
    zerompk::to_msgpack_vec(&Value::Object(map)).expect("encode object")
}

// ── Document ─────────────────────────────────────────────────────────────

fn document_insert() -> PhysicalPlan {
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
    })
}

fn document_row(core: &CoreLoop) -> Option<Vec<u8>> {
    core.sparse
        .get(
            0,
            TID,
            "notes",
            &nodedb_types::StorageKey::for_surrogate(Surrogate::new(41)),
        )
        .expect("read row")
}

#[test]
fn a_document_write_lands_and_a_refused_install_removes_it() {
    assert!(document_row(&run(&[document_insert()], false, |_| {}).core).is_some());
    assert!(document_row(&run(&[document_insert()], true, |_| {}).core).is_none());
}

// ── KV predicate form ────────────────────────────────────────────────────

fn seed_kv(core: &mut CoreLoop) {
    core.kv_engine.put(crate::engine::kv::KvPutParams {
        database_id: 0,
        tenant_id: TID,
        collection: "cache",
        key: b"k",
        value: &object(&[("n", Value::Integer(1))]),
        ttl_ms: 0,
        now_ms: crate::engine::kv::current_ms(),
        surrogate: Surrogate::new(51),
    });
}

fn kv_predicate_delete() -> PhysicalPlan {
    PhysicalPlan::Kv(KvOp::PredicateDelete {
        collection: QualifiedCollection::new(DatabaseId::DEFAULT, "cache"),
        filters: Vec::new(),
        rls_write_check: RlsWriteCheck::NoPolicyApplies,
        returning: None,
        rls_filters: Vec::new(),
    })
}

fn kv_present(core: &CoreLoop) -> bool {
    core.kv_engine
        .get(0, TID, "cache", b"k", crate::engine::kv::current_ms())
        .is_some()
}

#[test]
fn a_kv_predicate_delete_lands_and_a_refused_install_restores_the_key() {
    assert!(!kv_present(
        &run(&[kv_predicate_delete()], false, seed_kv).core
    ));
    assert!(kv_present(
        &run(&[kv_predicate_delete()], true, seed_kv).core
    ));
}

// ── Vector (vector-primary direct write) ─────────────────────────────────

fn vector_direct_insert() -> PhysicalPlan {
    PhysicalPlan::Vector(VectorOp::DirectInsert {
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
    })
}

fn vector_index_present(core: &CoreLoop) -> bool {
    core.vector_collections
        .contains_key(&CoreLoop::vector_index_key(0, TID, "vp", "vec"))
}

#[test]
fn a_vector_primary_write_lands_and_a_refused_install_withdraws_it() {
    assert!(vector_index_present(
        &run(&[vector_direct_insert()], false, |_| {}).core
    ));
    assert!(!vector_index_present(
        &run(&[vector_direct_insert()], true, |_| {}).core
    ));
}

// ── CRDT ─────────────────────────────────────────────────────────────────

fn crdt_upsert() -> PhysicalPlan {
    PhysicalPlan::Crdt(CrdtOp::DocUpsert {
        collection: QualifiedCollection::new(DatabaseId::DEFAULT, "tasks"),
        document_id: "t1".to_string(),
        fields_json: r#"{"title":"kept"}"#.to_string(),
        surrogate: Surrogate::new(71),
        partial: false,
        verb: CrdtWriteVerb::Insert,
        returning: None,
        rls_filters: Vec::new(),
    })
}

fn crdt_row_present(core: &CoreLoop) -> bool {
    core.crdt_engines
        .get(&(DatabaseId::DEFAULT, TenantId::new(TID)))
        .and_then(|engine| engine.read_row("tasks", "t1"))
        .is_some()
}

#[test]
fn a_crdt_write_lands_and_a_refused_install_restores_the_document() {
    assert!(crdt_row_present(&run(&[crdt_upsert()], false, |_| {}).core));
    assert!(!crdt_row_present(&run(&[crdt_upsert()], true, |_| {}).core));
}

// ── Spatial ──────────────────────────────────────────────────────────────

fn spatial_insert() -> PhysicalPlan {
    PhysicalPlan::Spatial(SpatialOp::Insert {
        collection: QualifiedCollection::new(DatabaseId::DEFAULT, "places"),
        field: "geom".to_string(),
        surrogate: Surrogate::new(81),
        geometry: Geometry::Point {
            coordinates: [1.0, 2.0],
        },
        provenance: None,
    })
}

fn spatial_entries(core: &CoreLoop) -> usize {
    core.spatial_indexes
        .get(&(
            DatabaseId::DEFAULT,
            TenantId::new(TID),
            "places".to_string(),
            "geom".to_string(),
        ))
        .map_or(0, |rtree| rtree.len())
}

#[test]
fn a_spatial_write_lands_and_a_refused_install_removes_the_entry() {
    assert_eq!(
        spatial_entries(&run(&[spatial_insert()], false, |_| {}).core),
        1
    );
    assert_eq!(
        spatial_entries(&run(&[spatial_insert()], true, |_| {}).core),
        0
    );
}

// ── Array ────────────────────────────────────────────────────────────────

fn array_id() -> ArrayId {
    ArrayId::new(TenantId::new(TID), "grid")
}

fn open_array(core: &mut CoreLoop) {
    let schema = ArraySchemaBuilder::new("grid")
        .dim(DimSpec::new(
            "x",
            DimType::Int64,
            Domain::new(DomainBound::Int64(0), DomainBound::Int64(15)),
        ))
        .attr(AttrSpec::new("v", AttrType::Int64, true))
        .tile_extents(vec![4])
        .build()
        .expect("build schema");
    let schema_msgpack = zerompk::to_msgpack_vec(&schema).expect("encode schema");
    let response =
        core.handle_array_open(&make_default_task(), &array_id(), &schema_msgpack, 0xA11, 8);
    assert_committed(&response);
}

fn array_put() -> PhysicalPlan {
    let cells = vec![ArrayPutCell {
        coord: vec![CoordValue::Int64(3)],
        attrs: vec![CellValue::Int64(9)],
        surrogate: Surrogate::ZERO,
        system_from_ms: 1,
        valid_from_ms: 0,
        valid_until_ms: i64::MAX,
    }];
    PhysicalPlan::Array(ArrayOp::Put {
        array_id: array_id(),
        cells_msgpack: zerompk::to_msgpack_vec(&cells).expect("encode cells"),
        wal_lsn: 0,
        provenance: None,
    })
}

fn array_memtable_empty(core: &CoreLoop) -> bool {
    core.array_engine
        .store(&array_id())
        .map(|store| store.memtable.is_empty())
        .expect("array store")
}

#[test]
fn an_array_write_lands_and_a_refused_install_restores_the_tiles() {
    assert!(!array_memtable_empty(
        &run(&[array_put()], false, open_array).core
    ));
    assert!(array_memtable_empty(
        &run(&[array_put()], true, open_array).core
    ));
}

// ── Text (full-text index) ───────────────────────────────────────────────

/// A text write stages no row, so this installs its redo sub-record directly,
/// the one resolve serializes from the plan.
fn fts_documents(refuse: bool) -> u32 {
    use crate::data::executor::handlers::transaction::redo_apply::CommittedRedo;
    use crate::types::Lsn;
    use crate::wal::{RedoRecord, RedoSubRecord};

    let dir = tempfile::tempdir().expect("tempdir");
    let (mut core, _req, _resp) = make_core_with_dir(dir.path());
    let op = nodedb_physical::physical_plan::TextOp::FtsIndexDoc {
        collection: QualifiedCollection::new(DatabaseId::DEFAULT, "docs"),
        surrogate: Surrogate::new(91),
        text: "hello world".to_string(),
        provenance: None,
    };
    let (record_type, payload) = crate::control::server::wal_dispatch::encode_text_op_record(&op)
        .expect("encode text op")
        .expect("an index op journals a record");
    let mut ops = vec![RedoSubRecord {
        record_type: record_type as u32,
        payload,
    }];
    if refuse {
        let lines = zerompk::to_msgpack_vec(&vec!["other_probe,host=a value=1 1".to_string()])
            .expect("encode lines");
        ops.push(RedoSubRecord {
            record_type: nodedb_wal::record::RecordType::TimeseriesBatch as u32,
            payload:
                crate::control::server::wal_dispatch::encode_timeseries_batch_payload_with_format(
                    "refusal_probe",
                    &lines,
                    None,
                    "ilp-msgpack",
                )
                .expect("encode ingest"),
        });
    }
    let redo = RedoRecord {
        version: 1,
        ops,
        calvin_stamp: None,
    }
    .to_bytes()
    .expect("encode redo");
    let mut task = make_default_task();
    task.wal_lsn = Some(Lsn::new(210));
    let response = core.install_committed_redo(
        &task,
        TID,
        CommittedRedo {
            redo: &redo,
            collections: &["docs".to_string()],
            sum_targets: &[],
        },
    );
    if refuse {
        assert_refused_at_install(&response);
    } else {
        assert_committed(&response);
    }
    core.inverted
        .corpus_stats(0, TenantId::new(TID), "docs")
        .expect("corpus stats")
        .0
}

#[test]
fn a_text_write_lands_and_a_refused_install_removes_the_posting() {
    assert_eq!(fts_documents(false), 1);
    assert_eq!(fts_documents(true), 0);
}

// ── Index side effects of a document write ───────────────────────────────

/// A document write indexes its text and its geometry as side effects. A
/// refused install withdraws the posting and the R-tree entry with the row.
fn indexed_document_put() -> PhysicalPlan {
    let location = serde_json::json!({"type": "Point", "coordinates": [10.0, 20.0]});
    let body = nodedb_types::json_to_msgpack(&serde_json::json!({
        "title": "quantum database sentinel",
        "location": location,
    }))
    .expect("encode body");
    PhysicalPlan::Document(DocumentOp::PointPut {
        collection: QualifiedCollection::new(DatabaseId::DEFAULT, "articles"),
        document_id: "a1".to_string(),
        value: body,
        surrogate: Surrogate::new(101),
        pk_bytes: b"a1".to_vec(),
        returning: None,
        rls_filters: Vec::new(),
        resolved_sum_targets: Vec::new(),
    })
}

fn indexed_side_effects(core: &CoreLoop) -> (u32, usize) {
    let postings = core
        .inverted
        .corpus_stats(0, TenantId::new(TID), "articles")
        .expect("corpus stats")
        .0;
    let entries = core
        .spatial_indexes
        .get(&(
            DatabaseId::DEFAULT,
            TenantId::new(TID),
            "articles".to_string(),
            "location".to_string(),
        ))
        .map_or(0, |rtree| rtree.len());
    (postings, entries)
}

#[test]
fn a_refused_install_withdraws_the_index_side_effects_of_a_document_write() {
    assert_eq!(
        indexed_side_effects(&run(&[indexed_document_put()], false, |_| {}).core),
        (1, 1)
    );
    assert_eq!(
        indexed_side_effects(&run(&[indexed_document_put()], true, |_| {}).core),
        (0, 0)
    );
}

// ── Raw CRDT delta ───────────────────────────────────────────────────────

/// A committed record never carries a raw CRDT delta: a rejected delta's
/// dead-letter entry is keyed by its record's LSN, which every sub-record of
/// one committed record shares. The validate pass refuses such a record
/// before anything is written.
#[test]
fn a_committed_record_carrying_a_raw_crdt_delta_is_refused_before_any_write() {
    use crate::data::executor::handlers::transaction::redo_apply::CommittedRedo;
    use crate::types::Lsn;
    use crate::wal::{CrdtDeltaWalPayload, RedoRecord, RedoSubRecord};

    let dir = tempfile::tempdir().expect("tempdir");
    let (mut core, _req, _resp) = make_core_with_dir(dir.path());
    let delta = CrdtDeltaWalPayload::new(
        vec![0u8; 8],
        Some("tasks".to_string()),
        None,
        None,
        Some("t1".to_string()),
        Some(1),
    )
    .encode()
    .expect("encode delta");
    let redo = RedoRecord {
        version: 1,
        ops: vec![RedoSubRecord {
            record_type: nodedb_wal::record::RecordType::CrdtDelta as u32,
            payload: delta,
        }],
        calvin_stamp: None,
    }
    .to_bytes()
    .expect("encode redo");
    let mut task = make_default_task();
    task.wal_lsn = Some(Lsn::new(220));

    let response = core.install_committed_redo(
        &task,
        TID,
        CommittedRedo {
            redo: &redo,
            collections: &[],
            sum_targets: &[],
        },
    );

    assert!(
        matches!(
            response.error_code.as_deref(),
            Some(ErrorCode::RejectedPrevalidation { reason }) if reason.contains("raw CRDT delta")
        ),
        "{:?}",
        response.error_code
    );
    assert!(!crdt_row_present(&core), "nothing was written");
}

// ── Write versions and watermark ─────────────────────────────────────────

#[test]
fn a_refused_install_publishes_no_write_version_and_no_watermark() {
    use crate::types::Lsn;
    let point = nodedb_types::calvin::ReadKeyIdent::Point(crate::types::KeyRepr::Surrogate(41));
    let committed = run(&[document_insert()], false, |_| {});
    assert_eq!(committed.core.watermark, Lsn::new(200));
    assert!(!committed.core.write_index.read_is_valid(
        DatabaseId::DEFAULT,
        TenantId::new(TID),
        "notes",
        &point,
        Lsn::new(199),
    ));

    let refused = run(&[document_insert()], true, |_| {});
    assert!(
        refused.core.watermark < Lsn::new(200),
        "the rolled-back install leaves the watermark where it was"
    );
    assert!(
        refused.core.write_index.read_is_valid(
            DatabaseId::DEFAULT,
            TenantId::new(TID),
            "notes",
            &point,
            Lsn::new(199),
        ),
        "the rolled-back install publishes no write version"
    );
}
