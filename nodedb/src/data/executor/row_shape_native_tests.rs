// SPDX-License-Identifier: BUSL-1.1

use super::*;
use crate::data::executor::{
    core_loop::tests::{make_core_with_dir, make_default_task},
    doc_format,
    handlers::document::{
        read::{DocFetchParams, DocScanMode},
        sort::sort_rows,
    },
};
use crate::engine::document::store::CollectionConfig;
use crate::types::{DatabaseId, TenantId};
use nodedb_physical::physical_plan::SortKeySpec;
use nodedb_types::{StorageKey, Surrogate, Value};

#[test]
fn full_scan_source_keeps_native_fields_through_sorting() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
    let collection = "native_source";
    let mut config = CollectionConfig::new(collection);
    config.declared_key = Some("account_key".into());
    core.doc_configs.insert(
        (DatabaseId::DEFAULT, TenantId::new(1), collection.into()),
        config,
    );
    let uuid = Value::Uuid("550e8400-e29b-41d4-a716-446655440000".into());
    let nested = Value::Array(vec![Value::Bytes(vec![0, 255])]);
    for (id, decimal) in [(1, "9007199254740993.0002"), (2, "9007199254740993.0001")] {
        let doc = Value::Object(
            [
                (
                    "decimal".into(),
                    Value::Decimal(decimal.parse().expect("decimal")),
                ),
                ("uuid".into(), uuid.clone()),
                ("nested".into(), nested.clone()),
                (
                    nodedb_types::ROWID_COLUMN.into(),
                    Value::Integer(i64::from(id + 10)),
                ),
            ]
            .into_iter()
            .collect(),
        );
        let stored = zerompk::to_msgpack_vec(&doc).expect("native stored body");
        core.sparse
            .put(
                0,
                1,
                collection,
                &StorageKey::for_surrogate(Surrogate::new(id)),
                &stored,
                1,
            )
            .expect("stored version");
    }
    let fetched = core
        .document_scan_fetch(
            &make_default_task(),
            1,
            DocFetchParams {
                collection,
                mode: &DocScanMode::Current,
                limit: usize::MAX,
                offset: 0,
                filter_predicates: &[],
                strict_schema: None,
                full_fetch: true,
            },
        )
        .expect("full source scan");
    let mut rows = fetched
        .rows
        .into_iter()
        .map(|(key, body)| {
            sparse_row_to_native_doc(&key, &body, SparseBodyFormatRef::Document, "account_key")
        })
        .collect::<crate::Result<Vec<_>>>()
        .expect("native source rows");
    sort_rows(
        &mut rows,
        &[SortKeySpec::column("decimal", true)],
        &[],
        &nodedb_crs::CrsContext::builtin_only(),
    )
    .expect("sort source rows");
    let first = doc_format::decode_document_value(&rows[0].1).expect("native sorted row");
    let first = first.as_object().expect("object");
    assert_eq!(first.get("account_key"), Some(&Value::String("12".into())));
    assert!(!first.contains_key("id"));
    assert_eq!(
        first.get("decimal"),
        Some(&Value::Decimal(
            "9007199254740993.0001".parse().expect("decimal")
        ))
    );
    assert_eq!(first.get("uuid"), Some(&uuid));
    assert_eq!(first.get("nested"), Some(&nested));
}

#[test]
fn native_identity_injection_keeps_explicit_keys_and_rejects_malformed_source() {
    let key = StorageKey::for_surrogate(Surrogate::new(7));
    let doc = Value::Object(
        [("sku".into(), Value::String("declared".into()))]
            .into_iter()
            .collect(),
    );
    let body = zerompk::to_msgpack_vec(&doc).expect("native body");
    let (_, rendered) = sparse_row_to_native_doc(&key, &body, SparseBodyFormatRef::Document, "sku")
        .expect("native row");
    assert_eq!(
        doc_format::decode_document_value(&rendered).expect("decoded"),
        doc
    );
    let malformed = [0x92, 0x01];
    assert!(
        sparse_row_to_native_doc(&key, &malformed, SparseBodyFormatRef::Document, "sku").is_err()
    );
    assert_eq!(
        inject_row_identity(&malformed, &key, "sku").as_ref(),
        malformed
    );
}

#[test]
fn historical_strict_sources_keep_native_fields_through_audit_and_sort() {
    use nodedb_physical::physical_plan::StorageMode;
    use nodedb_types::columnar::{ColumnDef, ColumnType, StrictSchema};
    let dir = tempfile::tempdir().expect("tempdir");
    let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
    let collection = "historical_native_source";
    let schema = StrictSchema::new(vec![
        ColumnDef::required("id", ColumnType::String).with_primary_key(),
        ColumnDef::required("decimal", ColumnType::Decimal(None)),
        ColumnDef::nullable("uuid", ColumnType::Uuid),
        ColumnDef::nullable("bytes", ColumnType::Bytes),
    ])
    .expect("schema");
    core.doc_configs.insert(
        (DatabaseId::DEFAULT, TenantId::new(1), collection.into()),
        CollectionConfig::new(collection).with_storage_mode(StorageMode::Strict {
            schema: schema.clone(),
        }),
    );
    let key = StorageKey::for_surrogate(Surrogate::new(7));
    let uuid = Value::Uuid("550e8400-e29b-41d4-a716-446655440000".into());
    let bytes = Value::Bytes(vec![0, 255]);
    for (millisecond, decimal) in [
        (1000, "9007199254740993.0001"),
        (2000, "9007199254740993.0002"),
    ] {
        let doc = Value::Object(
            [
                ("id".into(), Value::String("stored-key".into())),
                (
                    "decimal".into(),
                    Value::Decimal(decimal.parse().expect("decimal")),
                ),
                ("uuid".into(), uuid.clone()),
                ("bytes".into(), bytes.clone()),
            ]
            .into_iter()
            .collect(),
        );
        let stored =
            crate::data::executor::strict_format::value_to_binary_tuple(&doc, &schema, collection)
                .expect("strict body");
        core.sparse
            .put(
                0,
                1,
                collection,
                &key,
                &stored,
                millisecond * nodedb_types::NANOS_PER_MS + 1,
            )
            .expect("version");
    }
    for (mode, expected_count) in [
        (
            DocScanMode::AsOf {
                system_as_of: Some(1500 * nodedb_types::NANOS_PER_MS),
                valid_at_ms: None,
                merge_overlay: false,
            },
            1,
        ),
        (DocScanMode::AllVersions { valid_at_ms: None }, 2),
    ] {
        let fetched = core
            .document_scan_fetch(
                &make_default_task(),
                1,
                DocFetchParams {
                    collection,
                    mode: &mode,
                    limit: usize::MAX,
                    offset: 0,
                    filter_predicates: &[],
                    strict_schema: Some(&schema),
                    full_fetch: true,
                },
            )
            .expect("historical source scan");
        assert!(fetched.effective_schema.is_none());
        let mut rows = fetched
            .rows
            .into_iter()
            .map(|(key, body)| {
                sparse_row_to_native_doc(&key, &body, SparseBodyFormatRef::Document, "id")
            })
            .collect::<crate::Result<Vec<_>>>()
            .expect("native historical rows");
        sort_rows(
            &mut rows,
            &[SortKeySpec::column("decimal", true)],
            &[],
            &nodedb_crs::CrsContext::builtin_only(),
        )
        .expect("sort historical rows");
        assert_eq!(rows.len(), expected_count);
        for (index, (_, body)) in rows.iter().enumerate() {
            let row = doc_format::decode_document_value(body).expect("native historical row");
            assert_eq!(row.get("uuid"), Some(&uuid));
            assert_eq!(row.get("bytes"), Some(&bytes));
            assert_eq!(row.get("id"), Some(&Value::String("stored-key".into())));
            assert!(matches!(row.get("decimal"), Some(Value::Decimal(_))));
            if matches!(mode, DocScanMode::AllVersions { .. }) {
                assert_eq!(
                    row.get(nodedb_types::columnar::schema::TS_SYSTEM),
                    Some(&Value::Integer((index as i64 + 1) * 1000))
                );
                assert_eq!(
                    row.get(nodedb_types::columnar::schema::TS_VALID_FROM),
                    Some(&Value::Integer(i64::MIN))
                );
                assert_eq!(
                    row.get(nodedb_types::columnar::schema::TS_VALID_UNTIL),
                    Some(&Value::Integer(i64::MAX))
                );
            }
        }
    }
}
