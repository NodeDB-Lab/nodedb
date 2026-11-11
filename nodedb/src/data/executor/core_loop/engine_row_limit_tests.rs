// SPDX-License-Identifier: BUSL-1.1

//! A columnar or vector-primary write whose row image is over the row image
//! limit refuses with SQLSTATE 54000 and changes nothing.

use std::collections::HashMap;

use nodedb_physical::physical_plan::{ColumnarInsertIntent, UpdateValue, VectorDirectWriteIntent};
use nodedb_query::expr::{BinaryOp, SqlExpr};
use nodedb_types::{MAX_ROW_IMAGE_BYTES, RlsWriteCheck, Surrogate, Value};

use super::CoreLoop;
use super::tests::{make_core_with_dir, make_default_task};
use crate::bridge::envelope::{ErrorCode, Response, Status};
use crate::control::server::shared::ddl::sqlstate::error_code_to_sqlstate;
use crate::data::executor::handlers::columnar_mutation::ColumnarDml;
use crate::data::executor::handlers::columnar_write::ColumnarInsertParams;
use crate::data::executor::handlers::vector_direct_test_rows::{
    COLLECTION as VECTOR_COLLECTION, FIELD, TID, has_node, seed,
};
use crate::data::executor::handlers::vector_upsert::VectorDirectUpsertParams;
use crate::data::executor::task::ExecutionTask;
use crate::types::DatabaseId;

const COLUMNAR: &str = "wide_rows";

fn assert_refused_at_the_limit(response: &Response) {
    assert_eq!(response.status, Status::Error, "{response:?}");
    let code = response.error_code.as_deref().expect("error code");
    assert!(
        matches!(code, ErrorCode::ProgramLimitExceeded { .. }),
        "{code:?}"
    );
    assert_eq!(error_code_to_sqlstate(code).1, "54000");
}

fn columnar_insert(core: &mut CoreLoop, task: &ExecutionTask, note: String) -> Response {
    let row = Value::Object(HashMap::from([
        ("id".to_string(), Value::Integer(1)),
        ("s".to_string(), Value::String(note)),
    ]));
    let payload = nodedb_types::value_to_msgpack(&Value::Array(vec![row])).expect("encode");
    core.execute_columnar_insert(
        task,
        ColumnarInsertParams {
            collection: COLUMNAR,
            payload: &payload,
            format: "msgpack",
            intent: ColumnarInsertIntent::Insert,
            on_conflict_updates: &[],
            surrogates: &[],
            schema_bytes: &[],
            provenance: None,
            system_from: None,
            restored_versions: &[],
            rls_write_check: &RlsWriteCheck::already_decided_elsewhere(),
            returning: None,
            rls_filters: &[],
            spatial_undo: None,
        },
    )
}

/// The length of `s` in every stored row of the columnar collection.
fn stored_note_lengths(core: &CoreLoop, task: &ExecutionTask) -> Vec<usize> {
    let key = (
        task.request.database_id,
        task.request.tenant_id,
        COLUMNAR.to_string(),
    );
    let Some(engine) = core.columnar_engines.get(&key) else {
        return Vec::new();
    };
    let s_idx = engine
        .schema()
        .columns
        .iter()
        .position(|column| column.name == "s")
        .expect("s column");
    engine
        .scan_memtable_rows()
        .map(|row| match &row.expect("read")[s_idx] {
            Value::String(text) => text.len(),
            other => panic!("expected a string, got {other:?}"),
        })
        .collect()
}

#[test]
fn a_columnar_insert_over_the_limit_refuses_and_stores_nothing() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
    let task = make_default_task();
    let response = columnar_insert(&mut core, &task, "x".repeat(MAX_ROW_IMAGE_BYTES));
    assert_refused_at_the_limit(&response);
    assert!(stored_note_lengths(&core, &task).is_empty());
}

/// `UPDATE ... SET s = s || s` doubles the row until it passes the limit.
/// That update refuses, and the row keeps its value.
#[test]
fn a_columnar_update_doubling_a_row_past_the_limit_refuses() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
    let task = make_default_task();
    let half = MAX_ROW_IMAGE_BYTES / 2 + 16;
    let seeded = columnar_insert(&mut core, &task, "x".repeat(half));
    assert_eq!(seeded.status, Status::Ok, "{:?}", seeded.error_code);

    let doubled = UpdateValue::Expr(SqlExpr::BinaryOp {
        left: Box::new(SqlExpr::Column("s".into())),
        op: BinaryOp::Concat,
        right: Box::new(SqlExpr::Column("s".into())),
    });
    let check = RlsWriteCheck::already_decided_elsewhere();
    let response = core.execute_columnar_update(
        &task,
        ColumnarDml {
            collection: COLUMNAR,
            filter_bytes: &[],
            system_from: None,
            rls_write_check: &check,
        },
        &[("s".to_string(), doubled)],
        None,
    );
    assert_refused_at_the_limit(&response);
    assert_eq!(stored_note_lengths(&core, &task), vec![half]);
}

fn vector_write(
    core: &mut CoreLoop,
    surrogate: Surrogate,
    fields: &HashMap<String, Value>,
    intent: VectorDirectWriteIntent,
    on_conflict_updates: &[(String, UpdateValue)],
) -> Response {
    let payload = zerompk::to_msgpack_vec(fields).expect("encode payload");
    let task = make_default_task();
    core.execute_vector_direct_upsert(VectorDirectUpsertParams {
        task: &task,
        tid: TID,
        collection: VECTOR_COLLECTION,
        field: FIELD,
        surrogate,
        vector: &[1.0, 0.0, 0.0],
        payload: &payload,
        quantization: nodedb_types::VectorQuantization::None,
        storage_dtype: nodedb_types::VectorStorageDtype::F32,
        payload_indexes: &[],
        intent,
        on_conflict_updates,
        rls_write_check: &RlsWriteCheck::decided_earlier_in_request(),
        returning: None,
        rls_filters: &[],
    })
}

/// An autocommit vector-primary write whose payload is over the limit
/// refuses before its node or sidecar lands.
#[test]
fn an_autocommit_vector_primary_write_over_the_limit_refuses() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
    let fields = HashMap::from([(
        "note".to_string(),
        Value::String("n".repeat(MAX_ROW_IMAGE_BYTES)),
    )]);
    let surrogate = Surrogate::new(3);
    let response = vector_write(
        &mut core,
        surrogate,
        &fields,
        VectorDirectWriteIntent::Insert,
        &[],
    );
    assert_refused_at_the_limit(&response);
    assert!(!has_node(&core, surrogate));
}

/// An `ON CONFLICT DO UPDATE` patch that grows the stored row past the
/// limit refuses, and the stored row stays.
#[test]
fn a_vector_upsert_patch_past_the_limit_refuses() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
    let surrogate = Surrogate::new(4);
    seed(&mut core, surrogate);
    let before = core
        .vector_sidecar_row(
            DatabaseId::DEFAULT.as_u64(),
            TID,
            VECTOR_COLLECTION,
            surrogate,
        )
        .expect("read sidecar")
        .expect("seeded sidecar")
        .bytes;

    let large = nodedb_types::value_to_msgpack(&Value::String("n".repeat(MAX_ROW_IMAGE_BYTES)))
        .expect("encode");
    let response = vector_write(
        &mut core,
        surrogate,
        &HashMap::new(),
        VectorDirectWriteIntent::Upsert,
        &[("note".to_string(), UpdateValue::Literal(large))],
    );
    assert_refused_at_the_limit(&response);
    assert!(has_node(&core, surrogate));
    let after = core
        .vector_sidecar_row(
            DatabaseId::DEFAULT.as_u64(),
            TID,
            VECTOR_COLLECTION,
            surrogate,
        )
        .expect("read sidecar")
        .expect("the stored sidecar stays")
        .bytes;
    assert_eq!(after, before);
}
