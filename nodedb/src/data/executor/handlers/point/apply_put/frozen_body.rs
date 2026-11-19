// SPDX-License-Identifier: BUSL-1.1

//! Owned user fields, materialized before temporal and chain completion.

use super::config_witness::BodyConfigWitness;
use crate::{
    data::executor::{
        core_loop::CoreLoop,
        doc_format,
        enforcement::typeguard::{GuardScope, check_type_guards, inject_defaults},
        strict_format,
    },
    types::{DatabaseId, TenantId},
};
use nodedb_types::{Surrogate, Value};

/// Inputs that determine user fields without reserving a version or chain head.
pub(in crate::data::executor) struct FreezeDocumentBodyInput<'a> {
    pub config_key: &'a (DatabaseId, TenantId, String),
    pub surrogate: Surrogate,
    pub value: &'a [u8],
    pub guards: GuardScope<'a>,
}

/// Materialized user fields and their exact runtime config witness.
#[derive(Clone)]
pub(in crate::data::executor) struct FrozenDocumentBody {
    pub(super) config_key: (DatabaseId, TenantId, String),
    pub(super) surrogate: Surrogate,
    pub(super) value: Vec<u8>,
    pub(super) witness: BodyConfigWitness,
}

impl FrozenDocumentBody {
    pub(super) fn check(
        &self,
        core: &CoreLoop,
        key: &(DatabaseId, TenantId, String),
        surrogate: Surrogate,
    ) -> crate::Result<()> {
        if self.config_key != *key
            || self.surrogate != surrogate
            || !self.witness.matches(core.judged_doc_config(key))
        {
            return Err(crate::Error::DataPlane(
                crate::bridge::envelope::ErrorCode::ConflictRetry,
            ));
        }
        Ok(())
    }
}

impl CoreLoop {
    /// Evaluate user-body expressions once without selecting durable ordering.
    pub(in crate::data::executor) fn freeze_document_body(
        &self,
        input: FreezeDocumentBodyInput<'_>,
    ) -> crate::Result<FrozenDocumentBody> {
        // A pinned record's validate freezes the body under its pin.
        let config = self.judged_doc_config(input.config_key);
        // `input.value` is the body a write submits, so a body that is not a
        // document object is the request's fault, never a storage fault.
        let mut doc = doc_format::decode_document_value(input.value).map_err(|error| {
            crate::Error::BadRequest {
                detail: format!(
                    "document body for '{}' is not a document object: {error}",
                    input.config_key.2
                ),
            }
        })?;
        let Value::Object(ref mut fields) = doc else {
            return Err(crate::Error::BadRequest {
                detail: format!(
                    "document body for '{}' must be an object, got {}",
                    input.config_key.2,
                    doc.type_name()
                ),
            });
        };
        if let Some(config) = config {
            let crs = self.crs_context();
            if let nodedb_physical::physical_plan::StorageMode::Strict { schema } =
                &config.storage_mode
            {
                super::frozen_defaults::materialize_strict_defaults(
                    super::frozen_defaults::StrictDefaults {
                        collection: &input.config_key.2,
                        schema,
                        logged: self.apply_scope.applies_logged_record(),
                    },
                    fields,
                    &crs,
                )?;
            }
            materialize_native_write_body(config, &mut doc, input.guards, &crs)
                .map_err(crate::Error::DataPlane)?;
            if let nodedb_physical::physical_plan::StorageMode::Strict { schema } =
                &config.storage_mode
                && schema
                    .columns
                    .iter()
                    .find(|column| {
                        !nodedb_types::columnar::schema::is_reserved_bitemporal_column(&column.name)
                    })
                    .is_some_and(|column| column.name == "_rowid" && !column.nullable)
                && let Value::Object(fields) = &mut doc
                && !fields.contains_key("_rowid")
            {
                fields.insert(
                    "_rowid".into(),
                    Value::Integer(i64::from(input.surrogate.as_u32())),
                );
            }
            if let nodedb_physical::physical_plan::StorageMode::Strict { schema } =
                &config.storage_mode
                && let Value::Object(fields) = &mut doc
            {
                for column in &schema.columns {
                    if nodedb_types::columnar::schema::is_reserved_bitemporal_column(&column.name) {
                        continue;
                    }
                    if let Some(value) = fields.get_mut(&column.name)
                        && !matches!(value, Value::Null)
                    {
                        *value = strict_format::coerce_value(value, column)?;
                    }
                }
            }
        }
        if let Value::Object(fields) = &mut doc {
            strict_format::coerce_declared_row(fields, self.declared_columns(input.config_key))?;
            if let Some(config) = config
                && matches!(
                    config.storage_mode,
                    nodedb_physical::physical_plan::StorageMode::Schemaless
                )
                && config.enforcement.hash_chain
            {
                fields
                    .entry(
                        config
                            .declared_key
                            .as_deref()
                            .unwrap_or(nodedb_types::DEFAULT_IDENTITY_COLUMN)
                            .to_string(),
                    )
                    .or_insert_with(|| Value::String(input.surrogate.as_u32().to_string()));
            }
        }
        let value = zerompk::to_msgpack_vec(&doc).map_err(|error| crate::Error::Serialization {
            format: "msgpack".into(),
            detail: error.to_string(),
        })?;
        Ok(FrozenDocumentBody {
            config_key: input.config_key.clone(),
            surrogate: input.surrogate,
            value,
            witness: BodyConfigWitness::capture(config),
        })
    }
}

/// Apply typeguards and generated expressions without converting native fields.
pub(in crate::data::executor) fn materialize_native_write_body(
    config: &crate::engine::document::store::CollectionConfig,
    doc: &mut Value,
    scope: GuardScope<'_>,
    crs: &nodedb_crs::CrsContext<'_>,
) -> Result<(), crate::bridge::envelope::ErrorCode> {
    if !config.crdt_enabled
        && !config.type_guards.is_empty()
        && let Value::Object(fields) = doc
    {
        let injected = inject_defaults(&config.type_guards, fields, crs)?;
        let in_scope = match scope {
            GuardScope::Document => None,
            GuardScope::Assigned(assigned) => Some(
                assigned
                    .iter()
                    .chain(&injected)
                    .cloned()
                    .collect::<Vec<_>>(),
            ),
            GuardScope::InjectOnly => Some(Vec::new()),
        };
        if !matches!(scope, GuardScope::InjectOnly) {
            check_type_guards(&config.type_guards, doc, in_scope.as_deref(), crs)?;
        }
    }
    let specs = &config.enforcement.generated_columns;
    for index in crate::data::executor::handlers::generated::topological_sort(specs)? {
        let spec = &specs[index];
        let value = spec
            .expr
            .eval(doc, crs)
            .map_err(crate::bridge::envelope::ErrorCode::from)?;
        if let Value::Object(fields) = doc {
            fields.insert(spec.name.clone(), value);
        }
    }
    Ok(())
}
