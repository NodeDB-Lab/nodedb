// SPDX-License-Identifier: BUSL-1.1

//! Historical layout and exact tuple correspondence checks.

use super::installed_schema::{canonical_stored, check_image, compatible};
use crate::{bridge::envelope::RowVersion, types::InstalledDocument};
use nodedb_physical::physical_plan::StorageMode;
use nodedb_types::{
    Value,
    columnar::{ColumnDef, ColumnType, DroppedColumn, StrictSchema},
};
use std::sync::Arc;

fn schema() -> StrictSchema {
    StrictSchema::new(vec![
        ColumnDef::required("n", ColumnType::Int64),
        ColumnDef::nullable("old", ColumnType::String),
    ])
    .unwrap()
}
fn stored(schema: &StrictSchema) -> Vec<u8> {
    nodedb_strict::TupleEncoder::new(schema)
        .encode(&[Value::Integer(7), Value::String("source".into())])
        .unwrap()
}
#[test]
fn historical_layout_allows_later_add_drop_and_default_changes_without_filling_columns() {
    let captured = schema();
    let bytes = stored(&captured);
    let mut current = captured.clone();
    current.version = 2;
    let mut added = ColumnDef::nullable("later", ColumnType::String);
    added.added_at_version = 2;
    added.default = Some("'destination'".into());
    current.columns.push(added);
    current.columns[0].nullable = true;
    current.columns[0].default = Some("99".into());
    let captured_mode = StorageMode::Strict {
        schema: captured.clone(),
    };
    assert!(
        compatible(
            &captured_mode,
            &StorageMode::Strict {
                schema: current.clone()
            },
            &bytes
        )
        .is_ok()
    );
    current.version = 3;
    let dropped = current.columns.remove(1);
    current.dropped_columns.push(DroppedColumn {
        def: dropped,
        position: 1,
        dropped_at_version: 3,
    });
    let current_mode = StorageMode::Strict { schema: current };
    assert!(compatible(&captured_mode, &current_mode, &bytes).is_ok());
    let canonical = canonical_stored(&bytes, &current_mode, Some(RowVersion::open(11))).unwrap();
    let Value::Object(fields) =
        crate::data::executor::doc_format::decode_document_value(&canonical).unwrap()
    else {
        panic!("Object");
    };
    assert_eq!(fields.get("old"), Some(&Value::String("source".into())));
    assert!(!fields.contains_key("later"));
    // A witness's later version does not require the destination to reach that version.
    assert!(compatible(&current_mode, &captured_mode, &bytes).is_ok());
}
#[test]
fn conflicting_historical_layouts_and_future_tuple_versions_are_rejected() {
    let captured = schema();
    let bytes = stored(&captured);
    let mode = StorageMode::Strict {
        schema: captured.clone(),
    };
    for case in 0..4 {
        let mut current = captured.clone();
        match case {
            0 => current.columns.swap(0, 1),
            1 => current.columns[0].name = "renamed".into(),
            2 => current.columns[0].column_type = ColumnType::Float64,
            3 => {
                current.version = 2;
                current.columns[0].added_at_version = 2;
            }
            _ => unreachable!(),
        }
        assert!(compatible(&mode, &StorageMode::Strict { schema: current }, &bytes).is_err());
    }
    assert!(compatible(&mode, &StorageMode::Schemaless, &bytes).is_err());
    let mut future = captured.clone();
    future.version = 2;
    assert!(
        compatible(
            &StorageMode::Strict {
                schema: future.clone()
            },
            &mode,
            &stored(&future)
        )
        .is_err()
    );
    assert!(
        compatible(
            &mode,
            &StorageMode::Strict {
                schema: future.clone()
            },
            &stored(&future)
        )
        .is_err()
    );
}
#[test]
fn tuple_temporal_slots_must_match_every_selected_version_bound() {
    let schema =
        StrictSchema::new_bitemporal(vec![ColumnDef::required("n", ColumnType::Int64)]).unwrap();
    let version = RowVersion {
        sys_from: 11,
        valid_from_ms: 20,
        valid_until_ms: 30,
    };
    let bytes = nodedb_strict::TupleEncoder::new(&schema)
        .encode_bitemporal(11, 20, 30, &[Value::Integer(7)])
        .unwrap();
    let mode = Arc::new(StorageMode::Strict { schema });
    let canonical = canonical_stored(&bytes, &mode, Some(version)).unwrap();
    let image = InstalledDocument {
        canonical,
        stored: bytes,
        version,
        schema: mode,
        chain_head: None,
    };
    assert!(check_image(&image).is_ok());
    for case in 0..3 {
        let mut changed = image.clone();
        match case {
            0 => changed.version.sys_from += 1,
            1 => changed.version.valid_from_ms += 1,
            2 => changed.version.valid_until_ms += 1,
            _ => unreachable!(),
        }
        assert!(check_image(&changed).is_err());
    }
    let Value::Object(fields) =
        crate::data::executor::doc_format::decode_document_value(&image.canonical).unwrap()
    else {
        panic!("Object");
    };
    assert_eq!(fields.len(), 1);
    assert_eq!(fields.get("n"), Some(&Value::Integer(7)));
}
#[test]
fn typed_native_strict_image_preserves_non_json_values_and_rejects_changed_contents() {
    let schema = StrictSchema::new(vec![ColumnDef::required("span", ColumnType::Range)]).unwrap();
    let span = Value::Range {
        start: Some(Box::new(Value::Integer(1))),
        end: Some(Box::new(Value::Integer(9))),
        inclusive: true,
    };
    let bytes = nodedb_strict::TupleEncoder::new(&schema)
        .encode(std::slice::from_ref(&span))
        .unwrap();
    let mode = Arc::new(StorageMode::Strict { schema });
    let canonical = canonical_stored(&bytes, &mode, Some(RowVersion::open(11))).unwrap();
    let Value::Object(fields) =
        crate::data::executor::doc_format::decode_document_value(&canonical).unwrap()
    else {
        panic!("Object");
    };
    assert_eq!(fields.get("span"), Some(&span));
    let mut image = InstalledDocument {
        canonical,
        stored: bytes,
        version: RowVersion::open(11),
        schema: mode,
        chain_head: None,
    };
    assert!(check_image(&image).is_ok());
    image.canonical = zerompk::to_msgpack_vec(&Value::Object(std::collections::HashMap::from([(
        "span".into(),
        Value::Null,
    )])))
    .unwrap();
    assert!(check_image(&image).is_err());
}
