// SPDX-License-Identifier: BUSL-1.1

//! Columnar scan rows keep their declared column types when another operator
//! reads them.
//!
//! A columnar scan's rows travel to the next operator as native objects. A
//! `DECIMAL` or `UUID` column read as a hash join input then matches a native
//! schemaless document field of the same value.

use super::helpers::{doc_surrogate, make_ctx, rows_json, send_ok};
use nodedb::data::executor::response_codec::{self, RowContext};
use nodedb_physical::physical_plan::{
    ColumnarInsertIntent, ColumnarOp, DocumentOp, PhysicalPlan, QueryOp,
};
use nodedb_types::Value;
use nodedb_types::columnar::{ColumnDef, ColumnType, ColumnarSchema};

const UID: &str = "550e8400-e29b-41d4-a716-446655440000";
const OTHER_UID: &str = "6fa459ea-ee8a-3ca4-894e-db77e160355e";

fn coll(name: &str) -> nodedb_types::QualifiedCollection {
    nodedb_types::QualifiedCollection::new(nodedb_types::DatabaseId::DEFAULT, name)
}

fn decimal(text: &str) -> Value {
    Value::Decimal(text.parse().expect("decimal literal"))
}

/// Insert one row into the columnar collection `typed_col`, declared as
/// `id BIGINT PRIMARY KEY, amount DECIMAL, uid UUID`.
fn insert_columnar(ctx: &mut super::helpers::TestCtx) {
    let schema = ColumnarSchema::new(vec![
        ColumnDef::required("id", ColumnType::Int64).with_primary_key(),
        ColumnDef::required("amount", ColumnType::Decimal(None)),
        ColumnDef::required("uid", ColumnType::Uuid),
    ])
    .expect("valid schema");
    let row = Value::Object(
        [
            ("id".to_string(), Value::Integer(1)),
            ("amount".to_string(), Value::String("1.50".into())),
            ("uid".to_string(), Value::String(UID.into())),
        ]
        .into(),
    );
    send_ok(
        &mut ctx.core,
        &mut ctx.tx,
        &mut ctx.rx,
        PhysicalPlan::Columnar(ColumnarOp::Insert {
            collection: coll("typed_col"),
            payload: nodedb_types::value_to_msgpack(&Value::Array(vec![row]))
                .expect("encode insert rows"),
            format: "msgpack".into(),
            intent: ColumnarInsertIntent::Insert,
            on_conflict_updates: Vec::new(),
            surrogates: Vec::new(),
            schema_bytes: zerompk::to_msgpack_vec(&schema).expect("encode schema"),
            provenance: None,
            wal_lsn: None,
            system_from: None,
            restored_versions: Vec::new(),
            rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
            returning: None,
            rls_filters: Vec::new(),
        }),
    );
}

fn columnar_scan(ctx: &mut super::helpers::TestCtx) -> Vec<u8> {
    send_ok(
        &mut ctx.core,
        &mut ctx.tx,
        &mut ctx.rx,
        PhysicalPlan::Columnar(ColumnarOp::Scan {
            collection: coll("typed_col"),
            filters: Vec::new(),
            rls_filters: Vec::new(),
            sort_keys: Vec::new(),
            limit: 100,
            projection: Vec::new(),
            system_time: nodedb_types::SystemTimeScope::Current,
            valid_at_ms: None,
            prefilter: None,
            computed_columns: Vec::new(),
        }),
    )
}

/// Store `fields` as a native document body under `id` in `typed_docs`.
fn put_native(ctx: &mut super::helpers::TestCtx, id: &str, fields: Vec<(&str, Value)>) {
    let body = Value::Object(
        fields
            .into_iter()
            .map(|(k, v)| (k.to_string(), v))
            .chain([("id".to_string(), Value::String(id.into()))])
            .collect(),
    );
    send_ok(
        &mut ctx.core,
        &mut ctx.tx,
        &mut ctx.rx,
        PhysicalPlan::Document(DocumentOp::PointPut {
            collection: coll("typed_docs"),
            document_id: id.into(),
            value: zerompk::to_msgpack_vec(&body).expect("native body"),
            surrogate: doc_surrogate(id),
            pk_bytes: Vec::new(),
            returning: None,
            rls_filters: Vec::new(),
            resolved_sum_targets: Vec::new(),
            rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
        }),
    );
}

fn provider_scan(rows: Vec<u8>) -> Box<PhysicalPlan> {
    Box::new(PhysicalPlan::Query(QueryOp::ProviderScan {
        provider: None,
        rows,
        sources: Vec::new(),
        filters: Vec::new(),
        projection: Vec::new(),
        computed_columns: Vec::new(),
        window_functions: Vec::new(),
        sort_keys: Vec::new(),
        limit: None,
        offset: 0,
        distinct: false,
        system_time: nodedb_types::SystemTimeScope::Current,
    }))
}

fn document_scan(ctx: &mut super::helpers::TestCtx) -> Box<PhysicalPlan> {
    let payload = send_ok(
        &mut ctx.core,
        &mut ctx.tx,
        &mut ctx.rx,
        PhysicalPlan::Document(DocumentOp::Scan {
            collection: coll("typed_docs"),
            filters: Vec::new(),
            limit: 100,
            offset: 0,
            sort_keys: Vec::new(),
            distinct: false,
            projection: Vec::new(),
            computed_columns: Vec::new(),
            window_functions: Vec::new(),
            system_time: nodedb_types::SystemTimeScope::Current,
            valid_at_ms: None,
            prefilter: None,
        }),
    );
    provider_scan(
        response_codec::flatten_rows_in_context(&payload, RowContext::ScanEnvelope)
            .expect("document scan rows split"),
    )
}

fn hash_join(left: Box<PhysicalPlan>, right: Box<PhysicalPlan>, key: &str) -> PhysicalPlan {
    PhysicalPlan::Query(QueryOp::HashJoin {
        left_collection: coll("typed_col"),
        right_collection: coll("typed_docs"),
        left_alias: None,
        right_alias: None,
        on: vec![(key.into(), key.into())],
        join_type: "inner".into(),
        limit: 100,
        projection: Vec::new(),
        computed_projection: Vec::new(),
        join_filters: Vec::new(),
        post_filters: Vec::new(),
        left_input: Some(left),
        right_input: Some(right),
        left_bitmap: None,
        right_bitmap: None,
        left_rls_filters: Vec::new(),
        right_rls_filters: Vec::new(),
        left_scan_filters: Vec::new(),
        right_scan_filters: Vec::new(),
    })
}

fn seed(ctx: &mut super::helpers::TestCtx) {
    insert_columnar(ctx);
    put_native(
        ctx,
        "d1",
        vec![
            ("amount", decimal("1.50")),
            ("uid", Value::Uuid(UID.into())),
            ("name", Value::String("match".into())),
        ],
    );
    put_native(
        ctx,
        "d2",
        vec![
            ("amount", decimal("2.25")),
            ("uid", Value::Uuid(OTHER_UID.into())),
            ("name", Value::String("other".into())),
        ],
    );
}

/// The columnar scan's rows carry the declared DECIMAL and UUID types.
#[test]
fn columnar_scan_rows_keep_decimal_and_uuid_types() {
    let mut ctx = make_ctx();
    seed(&mut ctx);
    let payload = columnar_scan(&mut ctx);
    let Value::Array(rows) =
        response_codec::decode_row_payload_value(&payload, RowContext::Flat).expect("rows decode")
    else {
        panic!("columnar payload is a row array");
    };
    assert_eq!(rows.len(), 1);
    let Value::Object(row) = &rows[0] else {
        panic!("columnar row is an object");
    };
    assert_eq!(row.get("amount"), Some(&decimal("1.50")));
    assert_eq!(row.get("uid"), Some(&Value::Uuid(UID.into())));
}

/// A columnar scan feeding a hash join matches a native schemaless field on
/// its DECIMAL column and on its UUID column, and only the matching document
/// joins.
#[test]
fn columnar_scan_joins_native_field_on_typed_column() {
    let mut ctx = make_ctx();
    seed(&mut ctx);
    for key in ["amount", "uid"] {
        let scanned = columnar_scan(&mut ctx);
        let rows = response_codec::flatten_rows_in_context(&scanned, RowContext::Flat)
            .expect("columnar rows split");
        let right = document_scan(&mut ctx);
        let payload = send_ok(
            &mut ctx.core,
            &mut ctx.tx,
            &mut ctx.rx,
            hash_join(provider_scan(rows), right, key),
        );
        let json = rows_json(&payload);
        let parsed: Vec<serde_json::Value> = serde_json::from_str(&json)
            .unwrap_or_else(|e| panic!("invalid JSON: {e}\nraw: {json}"));
        assert_eq!(
            parsed.len(),
            1,
            "join on {key} matches one document: {json}"
        );
        let names: Vec<&serde_json::Value> = parsed[0]
            .as_object()
            .expect("join row is an object")
            .iter()
            .filter(|(column, _)| column.ends_with("name"))
            .map(|(_, value)| value)
            .collect();
        assert_eq!(
            names,
            [&serde_json::json!("match")],
            "join on {key}: {json}"
        );
    }
}
