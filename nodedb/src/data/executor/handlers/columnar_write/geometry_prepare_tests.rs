// SPDX-License-Identifier: BUSL-1.1

use nodedb_physical::physical_plan::document::UpdateValue;
use nodedb_physical::physical_plan::{ColumnarInsertIntent, ColumnarOp};
use nodedb_types::columnar::{ColumnDef, ColumnType, ColumnarSchema};
use nodedb_types::{DatabaseId, QualifiedCollection, RlsWriteCheck, TenantId, Value};

use crate::bridge::envelope::{Response, Status};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::core_loop::tests::make_core_with_dir;
use crate::data::executor::task::ExecutionTask;
use crate::types::VShardId;

use super::ColumnarInsertParams;

const COLLECTION: &str = "geometry_rows";

fn task() -> ExecutionTask {
    CoreLoop::replay_task(
        TenantId::new(1),
        DatabaseId::DEFAULT,
        VShardId::new(0),
        crate::bridge::envelope::PhysicalPlan::Columnar(ColumnarOp::Truncate {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, COLLECTION),
            restart_identity: false,
            system_from: None,
            erase_history: false,
        }),
        None,
    )
}

fn schema() -> ColumnarSchema {
    ColumnarSchema::new(vec![
        ColumnDef::required("id", ColumnType::String).with_primary_key(),
        ColumnDef::nullable("geom", ColumnType::Geometry),
        ColumnDef::required("v", ColumnType::Int64),
    ])
    .expect("schema")
}

fn row(id: &str, geometry: &str) -> Value {
    Value::Object(std::collections::HashMap::from([
        ("id".to_string(), Value::String(id.to_string())),
        ("geom".to_string(), Value::String(geometry.to_string())),
        ("v".to_string(), Value::Integer(1)),
    ]))
}

fn insert(
    core: &mut CoreLoop,
    rows: Vec<Value>,
    intent: ColumnarInsertIntent,
    updates: &[(String, UpdateValue)],
    stamp: i64,
) -> Response {
    let payload = nodedb_types::value_to_msgpack(&Value::Array(rows)).expect("payload");
    let schema_bytes = zerompk::to_msgpack_vec(&schema()).expect("schema bytes");
    core.execute_columnar_insert(
        &task(),
        ColumnarInsertParams {
            collection: COLLECTION,
            payload: &payload,
            format: "msgpack",
            intent,
            on_conflict_updates: updates,
            surrogates: &[],
            schema_bytes: &schema_bytes,
            provenance: None,
            system_from: Some(stamp),
            restored_versions: &[],
            rls_write_check: &RlsWriteCheck::already_decided_elsewhere(),
            returning: None,
            rls_filters: &[],
            spatial_undo: None,
        },
    )
}

fn tree(core: &CoreLoop) -> &crate::engine::spatial::RTree {
    core.spatial_indexes
        .get(&(
            DatabaseId::DEFAULT,
            TenantId::new(1),
            COLLECTION.to_string(),
            "geom".to_string(),
        ))
        .expect("geometry tree")
}

#[test]
fn a_skipped_row_leaves_geometry_and_srid_unchanged() {
    let directory = tempfile::tempdir().expect("directory");
    let (mut core, _, _) = make_core_with_dir(directory.path());
    let first = insert(
        &mut core,
        vec![row("a", "POINT (1 2)")],
        ColumnarInsertIntent::Insert,
        &[],
        10,
    );
    assert_eq!(first.status, Status::Ok, "{:?}", first.error_code);
    let before = tree(&core).history_rows();
    let skipped = insert(
        &mut core,
        vec![row("a", "SRID=3857;POINT (3 4)")],
        ColumnarInsertIntent::InsertIfAbsent,
        &[],
        20,
    );
    assert_eq!(skipped.status, Status::Ok, "{:?}", skipped.error_code);
    assert_eq!(tree(&core).history_rows(), before);
    assert_eq!(tree(&core).entries().len(), 1);
    assert_eq!(tree(&core).entries()[0].bbox.min_lng, 1.0);
    assert_eq!(tree(&core).srid(), nodedb_types::geometry::Srid::WGS84);
}

#[test]
fn repeated_skip_keys_admit_only_the_first_geometry() {
    let directory = tempfile::tempdir().expect("directory");
    let (mut core, _, _) = make_core_with_dir(directory.path());
    let response = insert(
        &mut core,
        vec![row("a", "POINT (1 2)"), row("a", "SRID=3857;POINT (3 4)")],
        ColumnarInsertIntent::InsertIfAbsent,
        &[],
        10,
    );
    assert_eq!(response.status, Status::Ok, "{:?}", response.error_code);
    assert_eq!(tree(&core).entries().len(), 1);
    assert_eq!(tree(&core).entries()[0].bbox.min_lng, 1.0);
    assert!(tree(&core).history_rows().ended.is_empty());
}

#[test]
fn conflict_indexing_uses_the_merged_postimage() {
    let directory = tempfile::tempdir().expect("directory");
    let (mut core, _, _) = make_core_with_dir(directory.path());
    let first = insert(
        &mut core,
        vec![row("a", "POINT (1 2)")],
        ColumnarInsertIntent::Insert,
        &[],
        10,
    );
    assert_eq!(first.status, Status::Ok, "{:?}", first.error_code);
    let updates = vec![(
        "v".to_string(),
        UpdateValue::Literal(nodedb_types::value_to_msgpack(&Value::Integer(2)).expect("literal")),
    )];
    let response = insert(
        &mut core,
        vec![row("a", "SRID=3857;POINT (3 4)")],
        ColumnarInsertIntent::Put,
        &updates,
        20,
    );
    assert_eq!(response.status, Status::Ok, "{:?}", response.error_code);
    assert_eq!(tree(&core).entries().len(), 1);
    assert_eq!(tree(&core).entries()[0].bbox.min_lng, 1.0);
    assert_eq!(tree(&core).history_rows().live[0].sys_from, 20);
}

#[test]
fn repeated_keys_keep_each_actual_raised_ordinal() {
    let directory = tempfile::tempdir().expect("directory");
    let (mut core, _, _) = make_core_with_dir(directory.path());
    let first = insert(
        &mut core,
        vec![row("a", "POINT (1 2)")],
        ColumnarInsertIntent::Insert,
        &[],
        1000,
    );
    assert_eq!(first.status, Status::Ok, "{:?}", first.error_code);
    let response = insert(
        &mut core,
        vec![row("a", "POINT (3 4)"), row("a", "POINT (5 6)")],
        ColumnarInsertIntent::Put,
        &[],
        10,
    );
    assert_eq!(response.status, Status::Ok, "{:?}", response.error_code);
    let history = tree(&core).history_rows();
    assert_eq!(history.live[0].sys_from, 1002);
    assert_eq!(history.ended.len(), 2);
    assert_eq!(
        (history.ended[0].sys_from, history.ended[0].sys_to),
        (1000, 1001)
    );
    assert_eq!(
        (history.ended[1].sys_from, history.ended[1].sys_to),
        (1001, 1002)
    );
    assert_eq!(tree(&core).entries()[0].bbox.min_lng, 5.0);
}

#[test]
fn invalid_geometry_leaves_all_source_rows_and_indexes_untouched() {
    let directory = tempfile::tempdir().expect("directory");
    let (mut core, _, _) = make_core_with_dir(directory.path());
    let response = insert(
        &mut core,
        vec![row("a", "POINT (1 2)"), row("b", "POINT (broken)")],
        ColumnarInsertIntent::Insert,
        &[],
        10,
    );
    assert_ne!(response.status, Status::Ok);
    let engine = core
        .columnar_engines
        .get(&(
            DatabaseId::DEFAULT,
            TenantId::new(1),
            COLLECTION.to_string(),
        ))
        .expect("engine");
    assert_eq!(engine.live_row_count(), 0);
    assert!(core.spatial_indexes.is_empty());
    assert!(core.spatial_doc_map.is_empty());
}

#[test]
fn accepted_null_geometry_ends_the_prior_index_entry() {
    let directory = tempfile::tempdir().expect("directory");
    let (mut core, _, _) = make_core_with_dir(directory.path());
    let first = insert(
        &mut core,
        vec![row("a", "POINT (1 2)")],
        ColumnarInsertIntent::Insert,
        &[],
        10,
    );
    assert_eq!(first.status, Status::Ok, "{:?}", first.error_code);
    let replacement = Value::Object(std::collections::HashMap::from([
        ("id".to_string(), Value::String("a".to_string())),
        ("geom".to_string(), Value::Null),
        ("v".to_string(), Value::Integer(2)),
    ]));
    let response = insert(
        &mut core,
        vec![replacement],
        ColumnarInsertIntent::Put,
        &[],
        20,
    );
    assert_eq!(response.status, Status::Ok, "{:?}", response.error_code);
    assert!(tree(&core).is_empty());
    assert!(core.spatial_doc_map.is_empty());
    let history = tree(&core).history_rows();
    assert_eq!(history.ended.len(), 1);
    assert_eq!(
        (history.ended[0].sys_from, history.ended[0].sys_to),
        (10, 20)
    );
}

#[test]
fn restore_projection_preserves_stamps_and_excludes_clone_copy_skips() {
    use super::row_ingest::RowIngestParams;
    use nodedb_types::columnar::RestoredVersion;

    let directory = tempfile::tempdir().expect("directory");
    let (mut core, _, _) = make_core_with_dir(directory.path());
    let first = insert(
        &mut core,
        vec![row("a", "POINT (1 2)")],
        ColumnarInsertIntent::Insert,
        &[],
        10,
    );
    assert_eq!(first.status, Status::Ok, "{:?}", first.error_code);
    let key = (
        DatabaseId::DEFAULT,
        TenantId::new(1),
        COLLECTION.to_string(),
    );
    let rows = [row("a", "SRID=3857;POINT (3 4)"), row("b", "POINT (5 6)")];
    let versions = [
        RestoredVersion {
            sys_from: 30,
            sys_to: None,
            clone_copy: true,
        },
        RestoredVersion {
            sys_from: 40,
            sys_to: None,
            clone_copy: false,
        },
    ];
    let schema = schema();
    let outcome = core
        .insert_columnar_rows(
            &task(),
            RowIngestParams {
                engine_key: &key,
                schema: &schema,
                bitemporal: false,
                system_from: None,
                intent: ColumnarInsertIntent::Restore,
                on_conflict_updates: &[],
                surrogates: &[],
                ndb_rows: &rows,
                restored_versions: &versions,
                rls_write_check: &RlsWriteCheck::already_decided_elsewhere(),
                provenance: None,
                collect_stored_rows: true,
            },
        )
        .unwrap_or_else(|response| panic!("restore: {:?}", response.error_code));
    assert_eq!(outcome.accepted, 1);
    assert_eq!(outcome.stored_rows.len(), 1);
    assert_eq!(outcome.geometry_rows.len(), 1);
    assert_eq!(outcome.geometry_rows[0].1, 40);
    core.publish_columnar_geometry(&task(), COLLECTION, outcome.geometry_rows);
    let history = tree(&core).history_rows();
    assert_eq!(history.live.len(), 2);
    assert!(history.ended.is_empty());
    let mut starts: Vec<_> = history.live.iter().map(|entry| entry.sys_from).collect();
    starts.sort_unstable();
    assert_eq!(starts, [10, 40]);
}

fn repeated_key_rollback(existing: bool) {
    use crate::data::executor::handlers::transaction::undo::UndoEntry;

    let directory = tempfile::tempdir().expect("directory");
    let (mut core, _, _) = make_core_with_dir(directory.path());
    let seeded = insert(
        &mut core,
        vec![row("kept", "POINT (8 9)")],
        ColumnarInsertIntent::Insert,
        &[],
        1,
    );
    assert_eq!(seeded.status, Status::Ok, "{:?}", seeded.error_code);
    if existing {
        for (geometry, stamp) in [("POINT (1 2)", 10), ("POINT (3 4)", 20)] {
            let response = insert(
                &mut core,
                vec![row("a", geometry)],
                ColumnarInsertIntent::Put,
                &[],
                stamp,
            );
            assert_eq!(response.status, Status::Ok, "{:?}", response.error_code);
        }
    }
    let before_history = tree(&core).history_rows();
    let before_entries: Vec<_> = tree(&core)
        .entries()
        .iter()
        .map(|entry| (entry.id, entry.bbox))
        .collect();
    let before_mapping = core.spatial_doc_map.clone();
    let key = (
        DatabaseId::DEFAULT,
        TenantId::new(1),
        COLLECTION.to_string(),
    );
    let engine = core.columnar_engines.get(&key).expect("engine");
    let before_source = engine
        .stored_versions(&[], &[], None)
        .expect("source versions");
    let row_count_before = engine.memtable().row_count();
    let rows = vec![row("a", "POINT (5 6)"), row("a", "POINT (6 7)")];
    let schema = schema();
    let values: Vec<_> = rows
        .iter()
        .map(|row| {
            let Value::Object(object) = row else {
                panic!("row object")
            };
            super::restore_ingest::schema_row_values(&schema, false, object).expect("row cells")
        })
        .collect();
    let (inserted_pks, displaced) =
        core.columnar_insert_undo_state(&key, &values, ColumnarInsertIntent::Put);
    let mut undo = vec![UndoEntry::ColumnarInsert {
        collection_key: key.clone(),
        row_count_before,
        inserted_pks,
        displaced,
    }];
    let payload = nodedb_types::value_to_msgpack(&Value::Array(rows)).expect("payload");
    let schema_bytes = zerompk::to_msgpack_vec(&schema).expect("schema bytes");
    let response = core.execute_columnar_insert(
        &task(),
        ColumnarInsertParams {
            collection: COLLECTION,
            payload: &payload,
            format: "msgpack",
            intent: ColumnarInsertIntent::Put,
            on_conflict_updates: &[],
            surrogates: &[],
            schema_bytes: &schema_bytes,
            provenance: None,
            system_from: Some(30),
            restored_versions: &[],
            rls_write_check: &RlsWriteCheck::already_decided_elsewhere(),
            returning: None,
            rls_filters: &[],
            spatial_undo: Some(&mut undo),
        },
    );
    assert_eq!(response.status, Status::Ok, "{:?}", response.error_code);
    core.rollback_undo_log(DatabaseId::DEFAULT.as_u64(), 1, undo)
        .expect("rollback");
    assert_eq!(
        core.columnar_engines
            .get(&key)
            .expect("engine")
            .stored_versions(&[], &[], None)
            .expect("source versions"),
        before_source
    );
    assert_eq!(tree(&core).history_rows(), before_history);
    let after_entries: Vec<_> = tree(&core)
        .entries()
        .iter()
        .map(|entry| (entry.id, entry.bbox))
        .collect();
    assert_eq!(after_entries, before_entries);
    assert_eq!(core.spatial_doc_map, before_mapping);
}

#[test]
fn repeated_fresh_key_rollback_removes_all_spatial_versions() {
    repeated_key_rollback(false);
}

#[test]
fn repeated_existing_key_rollback_restores_exact_spatial_history() {
    repeated_key_rollback(true);
}
