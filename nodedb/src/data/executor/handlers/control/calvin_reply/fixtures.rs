// SPDX-License-Identifier: BUSL-1.1

//! Plan builders for the Calvin reply tests.

use nodedb_physical::physical_plan::{
    ColumnarInsertIntent, ColumnarOp, DocumentOp, KvOp, PhysicalPlan, ReturningColumns,
    ReturningItem, ReturningSpec, TimeseriesOp, UpdateValue, VectorOp,
};
use nodedb_types::columnar::{ColumnDef, ColumnType, ColumnarSchema};
use nodedb_types::{DatabaseId, QualifiedCollection, RlsWriteCheck, Surrogate, Value};

pub(super) const TID: u64 = 1;

pub(super) fn returning(columns: &[&str]) -> Option<ReturningSpec> {
    Some(ReturningSpec {
        columns: ReturningColumns::Named(
            columns
                .iter()
                .map(|name| ReturningItem::Column {
                    name: (*name).to_string(),
                    alias: None,
                })
                .collect(),
        ),
        precommit: None,
    })
}

pub(super) fn coll(name: &str) -> QualifiedCollection {
    QualifiedCollection::new(DatabaseId::DEFAULT, name)
}

/// A row body as the planner encodes one: a standard MessagePack map.
/// A KV value is stored and read back in exactly this form.
pub(super) fn object(fields: &[(&str, Value)]) -> Vec<u8> {
    let map: std::collections::HashMap<String, Value> = fields
        .iter()
        .map(|(k, v)| ((*k).to_string(), v.clone()))
        .collect();
    nodedb_types::value_to_msgpack(&Value::Object(map)).expect("encode object")
}

pub(super) fn text(value: &str) -> Value {
    Value::String(value.to_string())
}

pub(super) fn doc_insert(
    id: &str,
    surrogate: u32,
    a: &str,
    if_absent: bool,
    ret: bool,
) -> PhysicalPlan {
    PhysicalPlan::Document(DocumentOp::PointInsert {
        collection: coll("orders"),
        document_id: id.to_string(),
        value: object(&[("a", text(a))]),
        if_absent,
        surrogate: Surrogate::new(surrogate),
        returning: if ret { returning(&["a"]) } else { None },
        rls_filters: Vec::new(),
        resolved_sum_targets: Vec::new(),
        deferred_sum_targets: Vec::new(),
        rls_write_check: RlsWriteCheck::NoPolicyApplies,
    })
}

pub(super) fn doc_delete(id: &str, surrogate: u32) -> PhysicalPlan {
    PhysicalPlan::Document(DocumentOp::PointDelete {
        collection: coll("orders"),
        document_id: id.to_string(),
        surrogate: Some(Surrogate::new(surrogate)),
        pk_bytes: Vec::new(),
        returning: returning(&["a"]),
        rls_filters: Vec::new(),
        rls_write_check: RlsWriteCheck::NoPolicyApplies,
        resolved_sum_targets: Vec::new(),
    })
}

pub(super) fn kv_put(value: &str) -> PhysicalPlan {
    kv_put_at(b"k", 5, value)
}

/// A KV put of `value` under `key`, with `RETURNING v`.
pub(super) fn kv_put_at(key: &[u8], surrogate: u32, value: &str) -> PhysicalPlan {
    PhysicalPlan::Kv(KvOp::Put {
        collection: coll("cache"),
        key: key.to_vec(),
        value: object(&[("v", text(value))]),
        ttl_ms: 0,
        surrogate: Surrogate::new(surrogate),
        returning: returning(&["v"]),
        rls_filters: Vec::new(),
        provenance: None,
    })
}

/// A KV delete of the row under `k`, with `RETURNING v`.
pub(super) fn kv_delete() -> PhysicalPlan {
    PhysicalPlan::Kv(KvOp::Delete {
        collection: coll("cache"),
        keys: vec![b"k".to_vec()],
        rls_write_check: RlsWriteCheck::NoPolicyApplies,
        returning: returning(&["v"]),
        rls_filters: Vec::new(),
        provenance: None,
    })
}

/// A vector-primary insert of a row labelled `tagged`, with
/// `RETURNING label`.
pub(super) fn vector_insert() -> PhysicalPlan {
    PhysicalPlan::Vector(VectorOp::DirectInsert {
        collection: coll("vp"),
        field: "vec".into(),
        surrogate: Surrogate::new(61),
        pk_bytes: b"r".to_vec(),
        vector: vec![1.0, 0.0],
        payload: zerompk::to_msgpack_vec(&std::collections::HashMap::from([(
            "label".to_string(),
            text("tagged"),
        )]))
        .expect("encode payload"),
        quantization: nodedb_types::VectorQuantization::None,
        storage_dtype: nodedb_types::VectorStorageDtype::F32,
        payload_indexes: Vec::new(),
        returning: returning(&["label"]),
        rls_filters: Vec::new(),
    })
}

/// A columnar insert of the row `{id: c1, note: noted}`, with
/// `RETURNING note`.
pub(super) fn columnar_insert() -> PhysicalPlan {
    let schema = ColumnarSchema::new(vec![
        ColumnDef::required("id", ColumnType::String).with_primary_key(),
        ColumnDef::nullable("note", ColumnType::String),
    ])
    .expect("valid columnar schema");
    let row = Value::Object(std::collections::HashMap::from([
        ("id".to_string(), text("c1")),
        ("note".to_string(), text("noted")),
    ]));
    PhysicalPlan::Columnar(ColumnarOp::Insert {
        collection: coll("metrics_col"),
        payload: nodedb_types::value_to_msgpack(&Value::Array(vec![row]))
            .expect("encode columnar payload"),
        format: "msgpack".to_string(),
        intent: ColumnarInsertIntent::Insert,
        on_conflict_updates: Vec::new(),
        surrogates: vec![Surrogate::new(81)],
        schema_bytes: zerompk::to_msgpack_vec(&schema).expect("encode schema"),
        provenance: None,
        wal_lsn: None,
        system_from: None,
        restored_versions: Vec::new(),
        rls_write_check: RlsWriteCheck::NoPolicyApplies,
        returning: returning(&["note"]),
        rls_filters: Vec::new(),
    })
}

/// An unresolved timeseries ingest of `lines`, with `RETURNING value`. A
/// canonical ILP batch carries one row token per line, from 91 up.
pub(super) fn ts_ingest(lines: &[&str]) -> PhysicalPlan {
    PhysicalPlan::Timeseries(TimeseriesOp::Ingest {
        collection: coll("cpu"),
        payload: zerompk::to_msgpack_vec(&lines.to_vec()).expect("canonical ILP payload"),
        format: "ilp-msgpack".to_owned(),
        wal_lsn: None,
        surrogates: (91..)
            .zip(lines)
            .map(|(token, _)| Surrogate::new(token))
            .collect(),
        provenance: None,
        rls_write_check: RlsWriteCheck::NoPolicyApplies,
        returning: returning(&["value"]),
        rls_filters: Vec::new(),
    })
}

pub(super) fn doc_put(id: &str, surrogate: u32, a: &str) -> PhysicalPlan {
    PhysicalPlan::Document(DocumentOp::PointPut {
        collection: coll("orders"),
        document_id: id.to_string(),
        value: object(&[("a", text(a))]),
        surrogate: Surrogate::new(surrogate),
        pk_bytes: Vec::new(),
        returning: None,
        rls_filters: Vec::new(),
        resolved_sum_targets: Vec::new(),
        rls_write_check: RlsWriteCheck::NoPolicyApplies,
    })
}

pub(super) fn doc_update(id: &str, surrogate: u32, a: &str) -> PhysicalPlan {
    PhysicalPlan::Document(DocumentOp::PointUpdate {
        collection: coll("orders"),
        document_id: id.to_string(),
        surrogate: Some(Surrogate::new(surrogate)),
        pk_bytes: Vec::new(),
        updates: vec![(
            "a".to_string(),
            UpdateValue::Literal(nodedb_types::value_to_msgpack(&text(a)).expect("literal")),
        )],
        returning: returning(&["a"]),
        rls_filters: Vec::new(),
        rls_write_check: RlsWriteCheck::NoPolicyApplies,
        resolved_sum_targets: Vec::new(),
        declared_primary_key: None,
    })
}

/// A resolved ingest with `RETURNING`, as a submitting node resolves it.
pub(super) fn resolved_ingest(line: &str) -> PhysicalPlan {
    use crate::data::executor::core_loop::tests::{make_core_with_dir, make_default_task};
    use crate::data::executor::handlers::timeseries::TsResolveInput;
    use crate::engine::timeseries::resolved_ingest::{RESOLVED_INGEST_FORMAT, TsDriftPolicy};
    let dir = tempfile::tempdir().expect("tempdir");
    let (core, _tx, _rx) = make_core_with_dir(dir.path());
    let lines = vec![line.to_string()];
    let batch = core
        .resolve_ts_batch(
            &make_default_task(),
            TsResolveInput {
                tid: crate::types::TenantId::new(TID),
                collection: "cpu",
                lines: &lines,
                now_ms: 1_000,
                drift: TsDriftPolicy::ApplyByName,
                needs_images: true,
                base: None,
            },
        )
        .expect("resolve");
    PhysicalPlan::Timeseries(TimeseriesOp::Ingest {
        collection: coll("cpu"),
        payload: batch.to_bytes().expect("encode batch"),
        format: RESOLVED_INGEST_FORMAT.to_owned(),
        wal_lsn: None,
        surrogates: Vec::new(),
        provenance: None,
        rls_write_check: RlsWriteCheck::NoPolicyApplies,
        returning: returning(&["value"]),
        rls_filters: Vec::new(),
    })
}
