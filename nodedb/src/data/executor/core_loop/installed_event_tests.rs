// SPDX-License-Identifier: BUSL-1.1

//! Committed document events carry the installed public row image.

use super::{
    CoreLoop,
    tests::{make_core_with_dir, make_request},
};
use crate::bridge::dispatch::JournalGroup;
use crate::bridge::envelope::{Response, RowEffect, Status};
use crate::data::executor::{
    doc_format, enforcement::typeguard::CompiledTypeGuard, task::ExecutionTask,
};
use crate::engine::document::store::CollectionConfig;
use crate::event::{WriteEvent, WriteOp};
use crate::types::{Lsn, RequestId};
use crate::wal::WriteSetCapture;
use nodedb_physical::physical_plan::{DocumentOp, GeneratedColumnSpec, PhysicalPlan, StorageMode};
use nodedb_types::columnar::{ColumnDef, ColumnType, StrictSchema};
use nodedb_types::{
    DatabaseId, QualifiedCollection, StorageKey, Surrogate, TenantId, TypeGuardFieldDef,
};

const COLLECTION: &str = "event_rows";

fn collection() -> QualifiedCollection {
    QualifiedCollection::new(DatabaseId::DEFAULT, COLLECTION)
}

fn configure(core: &mut CoreLoop) -> StrictSchema {
    let schema = StrictSchema::new(vec![
        ColumnDef::required("_rowid", ColumnType::Int64),
        ColumnDef::nullable("amount", ColumnType::Int64),
        ColumnDef::nullable("coerced", ColumnType::Int64),
        ColumnDef::nullable("status", ColumnType::String),
        ColumnDef::nullable("doubled", ColumnType::Int64),
    ])
    .expect("strict schema");
    let mut config = CollectionConfig::new(COLLECTION).with_storage_mode(StorageMode::Strict {
        schema: schema.clone(),
    });
    config.type_guards = vec![CompiledTypeGuard::compile(
        COLLECTION,
        &TypeGuardFieldDef {
            field: "status".into(),
            type_expr: "STRING".into(),
            required: false,
            check_expr: None,
            default_expr: Some("'ready'".into()),
            value_expr: None,
        },
    )];
    let (expr, depends_on) =
        nodedb_query::expr_parse::parse_generated_expr("amount * 2").expect("generated expression");
    config.enforcement.generated_columns = vec![GeneratedColumnSpec {
        name: "doubled".into(),
        expr,
        depends_on,
    }];
    core.doc_configs.insert(
        (DatabaseId::DEFAULT, TenantId::new(1), COLLECTION.into()),
        config,
    );
    schema
}

fn events(core: &mut CoreLoop) -> impl FnMut() -> Vec<WriteEvent> + use<> {
    let (mut producers, mut consumers) = crate::event::bus::create_event_bus_with_capacity(1, 64);
    core.set_event_producer(producers.pop().expect("event producer"));
    let mut consumer = consumers.pop().expect("event consumer");
    move || {
        let mut events = Vec::new();
        while let Some(event) = consumer.try_recv() {
            events.push(event);
        }
        events
    }
}

fn execute(core: &mut CoreLoop, id: u64, op: DocumentOp) -> Response {
    let mut request = make_request(PhysicalPlan::Document(op));
    request.request_id = RequestId::new(id);
    request.wal_lsn = Some(Lsn::new(id));
    let task = ExecutionTask::new(request);
    let permit = core.begin_task_journal(
        &task,
        JournalGroup {
            origin: Lsn::new(id),
            collection: COLLECTION.into(),
            apply_key: 0,
            commit_hlc: None,
            change_position: None,
        },
    );
    let mut response = core.execute_with_journal(&task, permit);
    assert_eq!(response.status, Status::Ok, "{response:?}");
    core.finish_journalled_task(&task, &mut response)
        .expect("durable capture");
    response
}

fn body(amount: i64) -> Vec<u8> {
    nodedb_types::json_to_msgpack(&serde_json::json!({"amount": amount, "coerced": "7"}))
        .expect("input document")
}

fn point(amount: i64) -> DocumentOp {
    DocumentOp::PointPut {
        collection: collection(),
        document_id: "row-1".into(),
        value: body(amount),
        surrogate: Surrogate::new(1),
        pk_bytes: Vec::new(),
        returning: None,
        rls_filters: Vec::new(),
        resolved_sum_targets: Vec::new(),
        rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
    }
}

fn batch(amount: i64, if_absent: bool) -> DocumentOp {
    DocumentOp::BatchInsert {
        collection: collection(),
        documents: vec![("row-1".into(), body(amount))],
        if_absent,
        surrogates: vec![Surrogate::new(1)],
        returning: None,
        rls_filters: Vec::new(),
        resolved_sum_targets: Vec::new(),
        deferred_sum_targets: Vec::new(),
        rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
    }
}

fn upsert(amount: i64) -> DocumentOp {
    DocumentOp::Upsert {
        collection: collection(),
        document_id: "row-1".into(),
        value: body(amount),
        on_conflict_updates: Vec::new(),
        surrogate: Surrogate::new(1),
        rls_write_check: nodedb_types::RlsWriteCheck::already_decided_elsewhere(),
        returning: None,
        rls_filters: Vec::new(),
        resolved_sum_targets: Vec::new(),
    }
}

fn batch_upsert(amount: i64) -> DocumentOp {
    DocumentOp::BatchUpsert {
        collection: collection(),
        documents: vec![("row-1".into(), body(amount))],
        surrogates: vec![Surrogate::new(1)],
        on_conflict_updates: Vec::new(),
        rls_write_check: nodedb_types::RlsWriteCheck::already_decided_elsewhere(),
        returning: None,
        rls_filters: Vec::new(),
        resolved_sum_targets: Vec::new(),
    }
}

fn capture(core: &CoreLoop, id: u64) -> WriteSetCapture {
    core.sparse
        .load_write_set_captures()
        .expect("load captures")
        .into_iter()
        .map(|(_, bytes)| WriteSetCapture::from_bytes(&bytes).expect("decode capture"))
        .find(|capture| capture.origin == id)
        .expect("statement capture")
}

fn check_event(
    core: &CoreLoop,
    schema: &StrictSchema,
    id: u64,
    event: &WriteEvent,
    op: WriteOp,
    amount: i64,
) {
    assert_eq!(event.op, op);
    let bytes = event.new_value.as_deref().expect("new event image");
    assert!(matches!(bytes.first(), Some(0x80..=0x8f | 0xde | 0xdf)));
    let public = doc_format::decode_document(bytes).expect("public native map");
    assert_eq!(public["amount"], amount);
    assert_eq!(public["coerced"], 7);
    assert_eq!(public["status"], "ready");
    assert_eq!(public["doubled"], amount * 2);
    assert_eq!(public["_rowid"], 1);
    let stored = core
        .sparse
        .get(
            0,
            1,
            COLLECTION,
            &StorageKey::for_surrogate(Surrogate::new(1)),
        )
        .expect("read installed row")
        .expect("installed row");
    assert_eq!(
        public,
        crate::data::executor::strict_format::binary_tuple_to_json(&stored, schema)
            .expect("decode installed tuple")
    );
    let entries = capture(core, id).write_set().expect("capture entries");
    let installed: Vec<_> = entries
        .iter()
        .filter_map(|entry| match &entry.effect {
            RowEffect::InstalledPut { image, event } => Some((image, event)),
            _ => None,
        })
        .collect();
    assert_eq!(installed.len(), 1);
    let captured_event = installed[0].1;
    assert_eq!(bytes, captured_event.new_value.as_slice());
    assert_eq!(
        event.old_value.as_deref(),
        captured_event.old_value.as_deref()
    );
    assert_eq!(
        event.op,
        if captured_event.old_value.is_some() {
            WriteOp::Update
        } else {
            WriteOp::Insert
        }
    );
    assert_eq!(stored, installed[0].0.stored);
}

#[test]
fn installed_point_batch_and_upsert_events_include_coercion_defaults_and_generated_values() {
    for op in [point(21), batch(21, false), upsert(21), batch_upsert(21)] {
        let directory = tempfile::tempdir().expect("directory");
        let (mut core, _requests, _responses) = make_core_with_dir(directory.path());
        let schema = configure(&mut core);
        let mut events = events(&mut core);
        execute(&mut core, 1, op);
        let emitted = events();
        assert_eq!(emitted.len(), 1);
        check_event(&core, &schema, 1, &emitted[0], WriteOp::Insert, 21);
        assert!(emitted[0].old_value.is_none());
    }
}

#[test]
fn installed_point_and_upsert_events_classify_existing_rows_as_updates() {
    for op in [point(30), upsert(30), batch_upsert(30)] {
        let directory = tempfile::tempdir().expect("directory");
        let (mut core, _requests, _responses) = make_core_with_dir(directory.path());
        let schema = configure(&mut core);
        let mut events = events(&mut core);
        execute(&mut core, 1, point(21));
        assert_eq!(events().len(), 1);
        execute(&mut core, 2, op);
        let emitted = events();
        assert_eq!(emitted.len(), 1);
        check_event(&core, &schema, 2, &emitted[0], WriteOp::Update, 30);
        let prior = doc_format::decode_document(
            emitted[0].old_value.as_deref().expect("prior event image"),
        )
        .expect("decode prior event");
        assert_eq!(prior["amount"], 21);
        assert_eq!(prior["doubled"], 42);
    }
}

#[test]
fn skipped_batch_rows_publish_no_event_or_installed_image() {
    let directory = tempfile::tempdir().expect("directory");
    let (mut core, _requests, _responses) = make_core_with_dir(directory.path());
    configure(&mut core);
    let mut events = events(&mut core);
    execute(&mut core, 1, point(21));
    assert_eq!(events().len(), 1);
    let response = execute(&mut core, 2, batch(99, true));
    assert!(events().is_empty());
    assert!(
        response
            .write_set
            .iter()
            .all(|entry| !matches!(entry.effect, RowEffect::InstalledPut { .. }))
    );
    assert!(
        capture(&core, 2)
            .write_set()
            .expect("capture entries")
            .iter()
            .all(|entry| !matches!(entry.effect, RowEffect::InstalledPut { .. }))
    );
}
