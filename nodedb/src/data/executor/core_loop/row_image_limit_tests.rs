// SPDX-License-Identifier: BUSL-1.1

//! A document write whose row image is over the row image limit is refused
//! before it installs: no row, no write set and no event.

use std::collections::HashMap;

use nodedb_physical::physical_plan::{DocumentOp, PhysicalPlan, StorageMode, UpdateValue};
use nodedb_types::columnar::{ColumnDef, ColumnType, StrictSchema};
use nodedb_types::{
    DatabaseId, MAX_ROW_IMAGE_BYTES, QualifiedCollection, RlsWriteCheck, StorageKey, Surrogate,
    TenantId, Value,
};

use super::CoreLoop;
use super::tests::{make_core_with_dir, make_request};
use crate::bridge::dispatch::JournalGroup;
use crate::bridge::envelope::{ErrorCode, Response, RowEffect, Status};
use crate::control::server::shared::ddl::sqlstate::error_code_to_sqlstate;
use crate::data::executor::task::ExecutionTask;
use crate::engine::document::store::CollectionConfig;
use crate::event::WriteEvent;
use crate::types::{Lsn, RequestId};
use crate::wal::WriteSetCapture;

const COLLECTION: &str = "large_rows";

/// A column name long enough that a strict row's event image outgrows its
/// stored Binary Tuple.
const LONG_COLUMN: &str = "a_column_whose_name_is_long_enough_that_every_rendered_image_spends_\
                           many_bytes_on_it_beyond_the_tuple";

fn collection() -> QualifiedCollection {
    QualifiedCollection::new(DatabaseId::DEFAULT, COLLECTION)
}

fn configure(core: &mut CoreLoop, storage_mode: StorageMode) {
    core.doc_configs.insert(
        (DatabaseId::DEFAULT, TenantId::new(1), COLLECTION.into()),
        CollectionConfig::new(COLLECTION).with_storage_mode(storage_mode),
    );
}

fn strict_mode() -> StorageMode {
    StorageMode::Strict {
        schema: StrictSchema::new(vec![
            ColumnDef::required("_rowid", ColumnType::Int64),
            ColumnDef::nullable(LONG_COLUMN, ColumnType::String),
        ])
        .expect("strict schema"),
    }
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

/// Run `op` journalled as statement `id`, as the funnel does.
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
    core.finish_journalled_task(&task, &mut response)
        .expect("journal settles");
    response
}

fn doc(fields: &[(&str, String)]) -> Vec<u8> {
    let object: HashMap<String, Value> = fields
        .iter()
        .map(|(name, value)| (name.to_string(), Value::String(value.clone())))
        .collect();
    nodedb_types::value_to_msgpack(&Value::Object(object)).expect("encode document")
}

fn filler(len: usize) -> String {
    "x".repeat(len)
}

fn insert(value: Vec<u8>) -> DocumentOp {
    DocumentOp::PointInsert {
        collection: collection(),
        document_id: "row-1".into(),
        value,
        if_absent: false,
        surrogate: Surrogate::new(1),
        returning: None,
        rls_filters: Vec::new(),
        resolved_sum_targets: Vec::new(),
        deferred_sum_targets: Vec::new(),
        rls_write_check: RlsWriteCheck::NoPolicyApplies,
    }
}

fn put(value: Vec<u8>) -> DocumentOp {
    DocumentOp::PointPut {
        collection: collection(),
        document_id: "row-1".into(),
        value,
        surrogate: Surrogate::new(1),
        pk_bytes: Vec::new(),
        returning: None,
        rls_filters: Vec::new(),
        resolved_sum_targets: Vec::new(),
        rls_write_check: RlsWriteCheck::NoPolicyApplies,
    }
}

fn update(field: &str, value: String) -> DocumentOp {
    let literal = nodedb_types::value_to_msgpack(&Value::String(value)).expect("encode literal");
    DocumentOp::PointUpdate {
        collection: collection(),
        document_id: "row-1".into(),
        surrogate: Some(Surrogate::new(1)),
        pk_bytes: Vec::new(),
        updates: vec![(field.to_string(), UpdateValue::Literal(literal))],
        returning: None,
        rls_filters: Vec::new(),
        rls_write_check: RlsWriteCheck::NoPolicyApplies,
        resolved_sum_targets: Vec::new(),
        declared_primary_key: None,
    }
}

fn upsert(value: Vec<u8>) -> DocumentOp {
    DocumentOp::Upsert {
        collection: collection(),
        document_id: "row-1".into(),
        value,
        on_conflict_updates: Vec::new(),
        surrogate: Surrogate::new(1),
        rls_write_check: RlsWriteCheck::already_decided_elsewhere(),
        returning: None,
        rls_filters: Vec::new(),
        resolved_sum_targets: Vec::new(),
    }
}

fn stored_row(core: &CoreLoop) -> Option<Vec<u8>> {
    core.sparse
        .get(
            0,
            1,
            COLLECTION,
            &StorageKey::for_surrogate(Surrogate::new(1)),
        )
        .expect("read row")
}

/// Every installed row the write sets stored for statement `id` name.
fn installed_rows(core: &CoreLoop, id: u64) -> usize {
    core.sparse
        .load_write_set_captures()
        .expect("load captures")
        .into_iter()
        .map(|(_, bytes)| WriteSetCapture::from_bytes(&bytes).expect("decode capture"))
        .filter(|capture| capture.origin == id)
        .flat_map(|capture| capture.write_set().expect("capture entries"))
        .filter(|entry| matches!(entry.effect, RowEffect::InstalledPut { .. }))
        .count()
}

/// `response` refuses at the row image limit with SQLSTATE 54000, and
/// statement `id` journals no installed row.
fn assert_refused(core: &CoreLoop, id: u64, response: &Response) {
    assert_eq!(response.status, Status::Error, "{response:?}");
    let code = response.error_code.as_deref().expect("error code");
    assert!(
        matches!(code, ErrorCode::ProgramLimitExceeded { .. }),
        "{code:?}"
    );
    assert_eq!(error_code_to_sqlstate(code).1, "54000");
    assert!(
        response
            .write_set
            .iter()
            .all(|entry| !matches!(entry.effect, RowEffect::InstalledPut { .. })),
        "a refused write reports no row"
    );
    assert_eq!(installed_rows(core, id), 0, "a refused write stores no row");
}

/// The bytes a row built by `fields` spends beyond its filler, in the
/// collection `mode` configures: `(stored body, larger of the accepted body
/// and the event image)`. Past 64 KiB of filler both grow byte for byte.
fn image_overhead(mode: StorageMode, fields: impl Fn(String) -> Vec<u8>) -> (usize, usize) {
    const FILL: usize = 100_000;
    let dir = tempfile::tempdir().expect("tempdir");
    let (mut core, _requests, _responses) = make_core_with_dir(dir.path());
    configure(&mut core, mode);
    let response = execute(&mut core, 1, put(fields(filler(FILL))));
    assert_eq!(response.status, Status::Ok, "{response:?}");
    let entry = response
        .write_set
        .iter()
        .find_map(|entry| match &entry.effect {
            RowEffect::InstalledPut { image, event } => Some((image.clone(), event.clone())),
            _ => None,
        })
        .expect("installed row");
    let (image, event) = entry;
    (
        image.stored.len() - FILL,
        image.canonical.len().max(event.new_value.len()) - FILL,
    )
}

#[test]
fn an_insert_over_the_limit_refuses_and_leaves_nothing() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (mut core, _requests, _responses) = make_core_with_dir(dir.path());
    configure(&mut core, StorageMode::Schemaless);
    let mut events = events(&mut core);
    let response = execute(
        &mut core,
        1,
        insert(doc(&[("note", filler(MAX_ROW_IMAGE_BYTES + 1))])),
    );
    assert_refused(&core, 1, &response);
    assert!(
        stored_row(&core).is_none(),
        "a refused insert stores no row"
    );
    assert!(events().is_empty(), "a refused insert emits no event");
}

#[test]
fn an_update_that_grows_past_the_limit_keeps_the_prior_row() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (mut core, _requests, _responses) = make_core_with_dir(dir.path());
    configure(&mut core, StorageMode::Schemaless);
    let mut events = events(&mut core);
    let inserted = execute(&mut core, 1, insert(doc(&[("note", "small".into())])));
    assert_eq!(inserted.status, Status::Ok, "{inserted:?}");
    let prior = stored_row(&core).expect("inserted row");
    assert_eq!(events().len(), 1);

    let response = execute(
        &mut core,
        2,
        update("note", filler(MAX_ROW_IMAGE_BYTES + 1)),
    );
    assert_refused(&core, 2, &response);
    assert_eq!(stored_row(&core), Some(prior), "the prior row is unchanged");
    assert!(events().is_empty(), "a refused update emits no event");
}

#[test]
fn an_upsert_merge_past_the_limit_refuses() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (mut core, _requests, _responses) = make_core_with_dir(dir.path());
    configure(&mut core, StorageMode::Schemaless);
    let mut events = events(&mut core);
    // Each body alone is within the limit. Their merge is not.
    let half = MAX_ROW_IMAGE_BYTES / 2 + 1024;
    let inserted = execute(&mut core, 1, insert(doc(&[("a", filler(half))])));
    assert_eq!(inserted.status, Status::Ok, "{inserted:?}");
    let prior = stored_row(&core).expect("inserted row");
    assert_eq!(events().len(), 1);

    let response = execute(&mut core, 2, upsert(doc(&[("b", filler(half))])));
    assert_refused(&core, 2, &response);
    assert_eq!(stored_row(&core), Some(prior), "the prior row is unchanged");
    assert!(events().is_empty(), "a refused upsert emits no event");
}

#[test]
fn a_strict_row_whose_event_image_is_over_the_limit_refuses() {
    let body = |note: String| doc(&[(LONG_COLUMN, note)]);
    let (stored_overhead, image_overhead) = image_overhead(strict_mode(), body);
    assert!(
        image_overhead > stored_overhead,
        "the rendered image outgrows the tuple: {image_overhead} vs {stored_overhead}"
    );
    // The stored tuple is within the limit, its rendered image is not.
    let fill = MAX_ROW_IMAGE_BYTES + 1 - image_overhead;
    assert!(fill + stored_overhead <= MAX_ROW_IMAGE_BYTES);

    let dir = tempfile::tempdir().expect("tempdir");
    let (mut core, _requests, _responses) = make_core_with_dir(dir.path());
    configure(&mut core, strict_mode());
    let mut events = events(&mut core);
    let response = execute(&mut core, 1, insert(body(filler(fill))));
    assert_refused(&core, 1, &response);
    assert!(
        stored_row(&core).is_none(),
        "a refused insert stores no row"
    );
    assert!(events().is_empty(), "a refused insert emits no event");
}

/// `strict_mode` after `ALTER ADD COLUMN` of a column with a long default.
fn strict_mode_with_added_column() -> StorageMode {
    let StorageMode::Strict { mut schema } = strict_mode() else {
        panic!("strict fixture must be strict");
    };
    let mut added = ColumnDef::nullable("added", ColumnType::String);
    added.default = Some(format!("'{}'", "d".repeat(1000)));
    added.added_at_version = 2;
    schema.columns.push(added);
    schema.version = 2;
    StorageMode::Strict { schema }
}

/// A row stored at the limit renders past it once a column is added. An
/// update of it still installs: its prior image stays within the prior
/// image limit the catalog bounds.
#[test]
fn a_row_at_the_limit_updates_after_a_column_is_added() {
    let body = |note: String| doc(&[(LONG_COLUMN, note)]);
    let (_, image_overhead) = image_overhead(strict_mode(), body);
    let dir = tempfile::tempdir().expect("tempdir");
    let (mut core, _requests, _responses) = make_core_with_dir(dir.path());
    configure(&mut core, strict_mode());
    let inserted = execute(
        &mut core,
        1,
        insert(body(filler(MAX_ROW_IMAGE_BYTES - image_overhead))),
    );
    assert_eq!(inserted.status, Status::Ok, "{inserted:?}");

    configure(&mut core, strict_mode_with_added_column());
    let updated = execute(&mut core, 2, update(LONG_COLUMN, "small".into()));
    assert_eq!(updated.status, Status::Ok, "{updated:?}");
    let prior = updated
        .write_set
        .iter()
        .find_map(|entry| match &entry.effect {
            RowEffect::InstalledPut { event, .. } => event.old_value.as_ref().map(Vec::len),
            _ => None,
        })
        .expect("prior image");
    assert!(
        prior > MAX_ROW_IMAGE_BYTES,
        "the prior image renders the added column: {prior} bytes"
    );
}
