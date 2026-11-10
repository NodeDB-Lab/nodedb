// SPDX-License-Identifier: BUSL-1.1

use nodedb_physical::physical_plan::StorageMode;
use nodedb_types::columnar::{ColumnDef, ColumnType, StrictSchema};
use nodedb_types::value::Value;

use super::*;
use crate::data::executor::core_loop::redo_image::WriteSetRow;
use crate::data::executor::core_loop::tests::{make_core_with_dir, make_default_task};
use crate::engine::document::store::{CollectionConfig, RowIdentity};
use crate::event::bus::{EventConsumerRx, create_event_bus_with_capacity};
use crate::event::image_fault::ImageFault;
use crate::event::{WriteEvent, WriteOp};
use crate::types::{DatabaseId, TenantId};

const TID: u64 = 1;
const COLL: &str = "strict_images";
/// Bytes no strict row is stored as: neither a Binary Tuple nor a map.
const CORRUPT: &[u8] = b"corrupt row bytes";

fn schema() -> StrictSchema {
    StrictSchema::new(vec![
        ColumnDef::required("id", ColumnType::String),
        ColumnDef::nullable("name", ColumnType::String),
    ])
    .expect("valid schema")
}

fn strict_core(dir: &std::path::Path) -> (CoreLoop, EventConsumerRx) {
    let (mut core, _tx, _rx) = make_core_with_dir(dir);
    core.seed_doc_configs(&[(
        (DatabaseId::DEFAULT, TenantId::new(TID), COLL.to_string()),
        CollectionConfig::new(COLL).with_storage_mode(StorageMode::Strict { schema: schema() }),
    )]);
    let (mut producers, mut consumers) = create_event_bus_with_capacity(1, 16);
    core.set_event_producer(producers.pop().expect("producer"));
    (core, consumers.pop().expect("consumer"))
}

fn tuple(name: &str) -> Vec<u8> {
    let row = Value::Object(
        [
            ("id".to_string(), Value::String("r1".into())),
            ("name".to_string(), Value::String(name.into())),
        ]
        .into_iter()
        .collect(),
    );
    crate::data::executor::strict_format::value_to_binary_tuple(&row, &schema(), COLL)
        .expect("row encodes")
}

fn drain(events: &mut EventConsumerRx) -> Vec<WriteEvent> {
    std::iter::from_fn(|| events.try_recv()).collect()
}

fn name_of(image: &[u8]) -> String {
    nodedb_types::value_from_msgpack(image)
        .expect("image is MessagePack")
        .as_object()
        .and_then(|o| o.get("name"))
        .and_then(|v| v.as_str())
        .expect("name field")
        .to_owned()
}

#[test]
fn a_tuple_converts_and_corrupt_bytes_are_undecodable() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (core, _events) = strict_core(dir.path());
    match core.resolve_event_payload(0, TID, COLL, &tuple("alice")) {
        StoredImage::Converted(image) => assert_eq!(name_of(&image), "alice"),
        other => panic!("a tuple converts, got {other:?}"),
    }
    assert_eq!(
        core.resolve_event_payload(0, TID, COLL, CORRUPT),
        StoredImage::Undecodable
    );
    // A collection with no strict schema reads its bytes as stored.
    assert_eq!(
        core.resolve_event_payload(0, TID, "schemaless", CORRUPT),
        StoredImage::AsStored
    );
}

#[test]
fn a_corrupt_prior_row_withholds_the_old_image_and_names_the_fault() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (mut core, mut events) = strict_core(dir.path());
    let task = make_default_task();
    core.emit_put_event(
        &task,
        TID,
        COLL,
        RowIdentity::from_user_key("r1"),
        &tuple("bob"),
        Some(CORRUPT),
    );
    let emitted = drain(&mut events);
    assert_eq!(emitted.len(), 1);
    let event = &emitted[0];
    assert_eq!(event.op, WriteOp::Update);
    assert_eq!(event.image_fault, Some(ImageFault::Old));
    assert!(
        event.old_value.is_none(),
        "raw bytes never reach the Event Plane"
    );
    assert_eq!(
        name_of(event.new_value.as_deref().expect("new image")),
        "bob"
    );
}

#[test]
fn a_corrupt_deleted_row_emits_no_raw_bytes() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (mut core, mut events) = strict_core(dir.path());
    let task = make_default_task();
    core.emit_document_delete_event(
        &task,
        TID,
        COLL,
        RowIdentity::from_user_key("r1"),
        Some(CORRUPT),
    );
    let emitted = drain(&mut events);
    assert_eq!(emitted.len(), 1);
    assert_eq!(emitted[0].op, WriteOp::Delete);
    assert_eq!(emitted[0].image_fault, Some(ImageFault::Old));
    assert!(emitted[0].old_value.is_none());

    // A decodable prior converts and names no fault.
    core.emit_document_delete_event(
        &task,
        TID,
        COLL,
        RowIdentity::from_user_key("r1"),
        Some(&tuple("carol")),
    );
    let emitted = drain(&mut events);
    assert_eq!(emitted[0].image_fault, None);
    assert_eq!(
        name_of(emitted[0].old_value.as_deref().expect("old image")),
        "carol"
    );
}

#[test]
fn an_older_tuple_event_uses_defaults_from_its_acceptance_schema() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (mut core, _events) = strict_core(dir.path());
    let key = (DatabaseId::DEFAULT, TenantId::new(TID), COLL.to_owned());
    let config = core.doc_configs.get_mut(&key).expect("config");
    let StorageMode::Strict { schema } = &mut config.storage_mode else {
        panic!("strict config");
    };
    let mut added = ColumnDef::nullable("status", ColumnType::String).with_default("'ready'");
    added.added_at_version = 2;
    schema.columns.push(added);
    schema.version = 2;
    let row = WriteSetRow {
        database_id: 0,
        tid: TID,
        collection: COLL,
        identity: RowIdentity::from_user_key("r1"),
    };
    let image = core
        .capture_stored_document(
            &key,
            &tuple("dave"),
            crate::bridge::envelope::RowVersion::open(1),
        )
        .expect("capture older tuple");
    let event = core
        .prepare_installed_put_event(&row, &image, None)
        .expect("event");
    let value = nodedb_types::value_from_msgpack(&event.new_value).expect("public image");
    assert_eq!(
        value.as_object().expect("row").get("status"),
        Some(&Value::String("ready".into()))
    );
    let canonical = zerompk::from_msgpack::<Value>(&image.canonical).expect("canonical image");
    assert!(
        !canonical
            .as_object()
            .expect("canonical row")
            .contains_key("status")
    );
}

#[test]
fn prepared_put_event_captures_prior_image_and_refuses_corrupt_prior() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (core, _events) = strict_core(dir.path());
    let key = (DatabaseId::DEFAULT, TenantId::new(TID), COLL.to_owned());
    let row = WriteSetRow {
        database_id: 0,
        tid: TID,
        collection: COLL,
        identity: RowIdentity::from_user_key("r1"),
    };
    let image = core
        .capture_stored_document(
            &key,
            &tuple("dave"),
            crate::bridge::envelope::RowVersion::open(1),
        )
        .expect("capture tuple");
    let event = core
        .prepare_installed_put_event(&row, &image, Some(&tuple("carol")))
        .expect("event");
    assert_eq!(name_of(&event.new_value), "dave");
    assert_eq!(name_of(event.old_value.as_deref().expect("prior")), "carol");
    assert!(
        core.prepare_installed_put_event(&row, &image, Some(CORRUPT))
            .is_err()
    );
    assert!(!core.is_fail_stopped());
}

#[test]
fn a_corrupt_post_image_refuses_preparation_without_stopping_the_core() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (core, _events) = strict_core(dir.path());
    let row = WriteSetRow {
        database_id: 0,
        tid: TID,
        collection: COLL,
        identity: RowIdentity::from_user_key("r1"),
    };
    let key = (DatabaseId::DEFAULT, TenantId::new(TID), COLL.to_owned());
    let image = core
        .capture_stored_document(
            &key,
            &tuple("dave"),
            crate::bridge::envelope::RowVersion::open(1),
        )
        .expect("captured tuple");
    let event = core
        .prepare_installed_event_image(&row, &image)
        .expect("prepared event");
    assert_eq!(name_of(&event), "dave");
    assert!(
        core.capture_stored_document(&key, CORRUPT, crate::bridge::envelope::RowVersion::open(1))
            .is_err()
    );
    assert!(!core.is_fail_stopped());
}

#[test]
fn native_prepared_images_render_public_identity_and_keep_canonical_storage() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
    let key = (DatabaseId::DEFAULT, TenantId::new(TID), COLL.to_owned());
    let mut config = CollectionConfig::new(COLL);
    config.declared_key = Some("account_key".into());
    core.doc_configs.insert(key.clone(), config);
    let native = |name: &str| {
        Value::Object(
            [
                ("name".into(), Value::String(name.into())),
                (
                    "decimal".into(),
                    Value::Decimal("123456789.123456789".parse().expect("decimal")),
                ),
                (
                    "uuid".into(),
                    Value::Uuid("550e8400-e29b-41d4-a716-446655440000".into()),
                ),
                (
                    "nested".into(),
                    Value::Array(vec![Value::Bytes(vec![0, 255])]),
                ),
            ]
            .into_iter()
            .collect(),
        )
    };
    let new_native = native("new");
    let stored = zerompk::to_msgpack_vec(&new_native).expect("native stored");
    let old_stored = zerompk::to_msgpack_vec(&native("old")).expect("prior native stored");
    let image = core
        .capture_stored_document(&key, &stored, crate::bridge::envelope::RowVersion::open(1))
        .expect("captured image");
    let row = WriteSetRow {
        database_id: 0,
        tid: TID,
        collection: COLL,
        identity: RowIdentity::from_user_key("authoritative-key"),
    };
    // The accepted storage mode controls rendering after catalog drift.
    core.doc_configs.get_mut(&key).expect("config").storage_mode =
        StorageMode::Strict { schema: schema() };
    let event = core
        .prepare_installed_put_event(&row, &image, Some(&old_stored))
        .expect("public event");
    for (bytes, name) in [
        (event.new_value.as_slice(), "new"),
        (event.old_value.as_deref().expect("old"), "old"),
    ] {
        let public = nodedb_types::value_from_msgpack(bytes).expect("public map");
        let object = public.as_object().expect("public object");
        assert_eq!(
            object.get("account_key"),
            Some(&Value::String("authoritative-key".into()))
        );
        assert!(!object.contains_key("id"));
        assert_eq!(object.get("name"), Some(&Value::String(name.into())));
        assert_eq!(
            object.get("uuid"),
            Some(&Value::String(
                "550e8400-e29b-41d4-a716-446655440000".into()
            ))
        );
        let json = nodedb_types::json_from_msgpack(bytes).expect("public JSON image");
        assert!(json.is_object());
        assert_eq!(json["name"], name);
    }
    assert_eq!(
        zerompk::from_msgpack::<Value>(&image.canonical).expect("canonical"),
        new_native
    );
    assert_eq!(image.stored, stored);
    assert_eq!(image.canonical, stored);
}
