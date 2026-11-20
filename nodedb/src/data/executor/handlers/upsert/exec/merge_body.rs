// SPDX-License-Identifier: BUSL-1.1

//! The body an upsert stores over an existing row, shared by the point
//! overwrite branch and the page handler.

use nodedb_physical::physical_plan::UpdateValue;
use nodedb_types::Value;
use nodedb_types::columnar::StrictSchema;

use crate::bridge::envelope::ErrorCode;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::core_loop::doc_config_key::DocConfigKey;
use crate::data::executor::handlers::transaction::redo_apply::fit_stored_rewrite;
use crate::data::executor::handlers::upsert::merge::{apply_on_conflict_updates, merge_values};

/// The merged row an upsert stores, as the MessagePack body
/// `apply_point_put` takes for both storage modes, and the fields the merge
/// wrote.
pub(in crate::data::executor) struct MergedUpsert {
    pub body: Vec<u8>,
    /// The `ON CONFLICT DO UPDATE SET` targets, else every field of the
    /// incoming body. Typeguards judge them.
    pub assigned: Vec<String>,
}

/// Decode a stored row: a strict Binary Tuple through `strict_schema`, else
/// MessagePack. A strict row stored as MessagePack decodes too.
pub(in crate::data::executor) fn decode_stored_row(
    current_bytes: &[u8],
    strict_schema: Option<&StrictSchema>,
) -> Result<Value, ErrorCode> {
    if let Some(schema) = strict_schema
        && let Some(value) =
            crate::data::executor::strict_format::binary_tuple_to_value(current_bytes, schema)
    {
        return Ok(value);
    }
    crate::data::executor::doc_format::decode_document_value(current_bytes).map_err(|_| {
        ErrorCode::Internal {
            detail: "failed to decode document for upsert".into(),
        }
    })
}

/// Merge the incoming `value` into `existing`: the `ON CONFLICT DO UPDATE
/// SET` assignments evaluated against `existing` when there are any, else
/// the plain field merge `UPSERT INTO` uses.
pub(in crate::data::executor) fn merge_upsert_body(
    existing: Value,
    value: &[u8],
    on_conflict_updates: &[(String, UpdateValue)],
    crs: &nodedb_crs::CrsContext<'_>,
) -> Result<MergedUpsert, ErrorCode> {
    let new_val =
        crate::data::executor::doc_format::decode_document_value(value).map_err(|_| {
            ErrorCode::Internal {
                detail: "failed to decode upsert value from msgpack".into(),
            }
        })?;
    let assigned: Vec<String> = if on_conflict_updates.is_empty() {
        match &new_val {
            Value::Object(fields) => fields.keys().cloned().collect(),
            _ => Vec::new(),
        }
    } else {
        crate::data::executor::handlers::write_body::assigned_names(on_conflict_updates)
    };
    let merged = if on_conflict_updates.is_empty() {
        merge_values(existing, new_val)
    } else {
        apply_on_conflict_updates(existing, &new_val, on_conflict_updates, crs)?
    };
    // The merged row as a MessagePack body: `apply_point_put` encodes the
    // strict Binary Tuple, and stamps a bitemporal version, itself.
    let body = zerompk::to_msgpack_vec(&merged).map_err(|_| ErrorCode::Internal {
        detail: "failed to encode merged upsert value".into(),
    })?;
    Ok(MergedUpsert { body, assigned })
}

impl CoreLoop {
    /// Fit `merged`, an upsert's body over the stored row `stored` of
    /// `key`, to the schema its rewrite encodes under, as an update's
    /// rewrite does. The upsert's assigned fields stay. A schemaless row
    /// passes unchanged. `Err` refuses an upsert whose pin is older than the
    /// stored layout (`StalePinnedSchema`).
    pub(in crate::data::executor) fn fit_merged_upsert(
        &self,
        key: &DocConfigKey,
        stored: &[u8],
        merged: &mut MergedUpsert,
    ) -> Result<(), ErrorCode> {
        let Some(schema) = self.write_rewrite_schema(key, stored)? else {
            return Ok(());
        };
        let mut doc = crate::data::executor::doc_format::decode_document_value(&merged.body)
            .map_err(ErrorCode::from)?;
        let Value::Object(fields) = &mut doc else {
            return Ok(());
        };
        fit_stored_rewrite(schema, fields, &merged.assigned);
        merged.body = zerompk::to_msgpack_vec(&doc).map_err(|error| ErrorCode::Internal {
            detail: format!("failed to encode merged upsert value: {error}"),
        })?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::executor::{
        core_loop::tests::make_core_with_dir,
        enforcement::typeguard::{CompiledTypeGuard, GuardScope},
        handlers::point::apply_put::stored_body::StoredBodyInput,
    };
    use crate::{
        engine::document::store::CollectionConfig,
        types::{DatabaseId, TenantId},
    };
    use nodedb_types::{Surrogate, TypeGuardFieldDef};

    #[test]
    fn merging_materialized_storage_retains_native_fields_and_defaults() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        let key = (DatabaseId::DEFAULT, TenantId::new(1), "native_merge".into());
        let mut config = CollectionConfig::new("native_merge");
        config.type_guards = vec![CompiledTypeGuard::compile(
            "native_merge",
            &TypeGuardFieldDef {
                field: "status".into(),
                type_expr: "STRING".into(),
                required: false,
                check_expr: None,
                default_expr: Some("'ready'".into()),
                value_expr: None,
            },
        )];
        core.doc_configs.insert(key.clone(), config);
        let original = Value::Object(
            [
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
        );
        let input = zerompk::to_msgpack_vec(&original).expect("native input");
        let stored = core
            .build_stored_body(StoredBodyInput {
                config_key: &key,
                surrogate: Surrogate::new(1),
                value: &input,
                bitemporal: false,
                sys_from: 1,
                valid_from_ms: i64::MIN,
                valid_until_ms: i64::MAX,
                guards: GuardScope::Document,
            })
            .expect("materialized stored body")
            .stored;
        let incoming = Value::Object([("count".into(), Value::Integer(2))].into_iter().collect());
        let incoming = zerompk::to_msgpack_vec(&incoming).expect("incoming");
        let merged = merge_upsert_body(
            decode_stored_row(&stored, None).expect("stored native row"),
            &incoming,
            &[],
            &core.crs_context(),
        )
        .expect("merged row");
        let decoded = crate::data::executor::doc_format::decode_document_value(&merged.body)
            .expect("merged native body");
        let fields = decoded.as_object().expect("object");
        for (name, value) in original.as_object().expect("original object") {
            assert_eq!(fields.get(name), Some(value));
        }
        assert_eq!(fields.get("status"), Some(&Value::String("ready".into())));
        assert_eq!(fields.get("count"), Some(&Value::Integer(2)));
        let duplicate = merge_upsert_body(decoded, &incoming, &[], &core.crs_context())
            .expect("duplicate merge");
        assert_eq!(
            crate::data::executor::doc_format::decode_document_value(&duplicate.body)
                .expect("duplicate native"),
            crate::data::executor::doc_format::decode_document_value(&merged.body)
                .expect("merged native")
        );
    }
}
