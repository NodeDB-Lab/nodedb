// SPDX-License-Identifier: BUSL-1.1

//! Aggregate rows keep their key types when another operator reads them.
//!
//! An aggregate's rows travel to the next operator as native objects. A
//! derived-table aggregate grouped on a DECIMAL or UUID key then joins with a
//! native schemaless document field of the same value: the key is never
//! turned into its text on the way.

use super::helpers::{doc_surrogate, make_ctx, rows_json, send_ok};
use nodedb::data::executor::response_codec::{self, RowContext};
use nodedb_physical::physical_plan::{
    AggregateSpec, DocumentOp, GroupKeySpec, PhysicalPlan, QueryOp,
};
use nodedb_types::Value;

const UID: &str = "550e8400-e29b-41d4-a716-446655440000";
const OTHER_UID: &str = "6fa459ea-ee8a-3ca4-894e-db77e160355e";

fn coll(name: &str) -> nodedb_types::QualifiedCollection {
    nodedb_types::QualifiedCollection::new(nodedb_types::DatabaseId::DEFAULT, name)
}

fn decimal(text: &str) -> Value {
    Value::Decimal(text.parse().expect("decimal literal"))
}

/// Store `fields` as a native document body under `id`.
fn put_native(
    ctx: &mut super::helpers::TestCtx,
    collection: &str,
    id: &str,
    fields: Vec<(&str, Value)>,
) {
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
            collection: coll(collection),
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

/// `SELECT amount, uid, count(*) AS n FROM typed_src GROUP BY amount, uid`.
fn grouped(ctx: &mut super::helpers::TestCtx) -> Vec<u8> {
    send_ok(
        &mut ctx.core,
        &mut ctx.tx,
        &mut ctx.rx,
        PhysicalPlan::Query(QueryOp::Aggregate {
            collection: coll("typed_src"),
            input: None,
            group_by: vec![GroupKeySpec::column("amount"), GroupKeySpec::column("uid")],
            aggregates: vec![AggregateSpec {
                function: "count".into(),
                alias: "count(*)".into(),
                user_alias: Some("n".into()),
                field: "*".into(),
                expr: None,
                param: None,
            }],
            filters: Vec::new(),
            system_time: nodedb_types::SystemTimeScope::Current,
            having: Vec::new(),
            limit: 10,
            sub_group_by: Vec::new(),
            sub_aggregates: Vec::new(),
            grouping_sets: Vec::new(),
            group_key_aliases: Vec::new(),
            sort_keys: Vec::new(),
        }),
    )
}

/// The documents of `typed_docs` as a coordinator-resolved join input.
fn docs(ctx: &mut super::helpers::TestCtx) -> Box<PhysicalPlan> {
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
        left_collection: coll("grouped"),
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
    for id in ["s1", "s2"] {
        put_native(
            ctx,
            "typed_src",
            id,
            vec![
                ("amount", decimal("1.50")),
                ("uid", Value::Uuid(UID.into())),
            ],
        );
    }
    put_native(
        ctx,
        "typed_docs",
        "d1",
        vec![
            ("amount", decimal("1.50")),
            ("uid", Value::Uuid(UID.into())),
            ("name", Value::String("match".into())),
        ],
    );
    put_native(
        ctx,
        "typed_docs",
        "d2",
        vec![
            ("amount", decimal("2.25")),
            ("uid", Value::Uuid(OTHER_UID.into())),
            ("name", Value::String("other".into())),
        ],
    );
}

/// The aggregate's rows carry the typed key values, not their text.
#[test]
fn aggregate_rows_keep_decimal_and_uuid_keys() {
    let mut ctx = make_ctx();
    seed(&mut ctx);
    let payload = grouped(&mut ctx);
    let Value::Array(rows) =
        response_codec::decode_row_payload_value(&payload, RowContext::Flat).expect("rows decode")
    else {
        panic!("aggregate payload is a row array");
    };
    assert_eq!(rows.len(), 1, "one group");
    let Value::Object(row) = &rows[0] else {
        panic!("aggregate row is an object");
    };
    assert_eq!(row.get("amount"), Some(&decimal("1.50")));
    assert_eq!(row.get("uid"), Some(&Value::Uuid(UID.into())));
    assert_eq!(row.get("n"), Some(&Value::Integer(2)));
}

/// A derived-table aggregate joins a native schemaless field on its DECIMAL
/// key and on its UUID key, and only the matching document joins.
#[test]
fn aggregate_subquery_joins_native_field_on_typed_key() {
    let mut ctx = make_ctx();
    seed(&mut ctx);
    for key in ["amount", "uid"] {
        let rows = response_codec::flatten_rows_in_context(&grouped(&mut ctx), RowContext::Flat)
            .expect("aggregate rows split");
        let right = docs(&mut ctx);
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
