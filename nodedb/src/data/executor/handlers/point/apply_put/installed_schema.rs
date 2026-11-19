// SPDX-License-Identifier: BUSL-1.1

//! Exact historical tuple layouts and installed-image correspondence.

use crate::{
    bridge::envelope::RowVersion,
    data::executor::{core_loop::CoreLoop, doc_format},
    types::{DatabaseId, SharedInstalledDocument, TenantId},
};
use nodedb_physical::physical_plan::StorageMode;
use nodedb_types::{
    columnar::{StrictSchema, schema::is_reserved_bitemporal_column},
    value::Value,
};
use std::{borrow::Cow, collections::HashMap, sync::Arc};

pub(super) fn invalid(detail: impl Into<String>) -> crate::Error {
    crate::Error::Storage {
        engine: "document".into(),
        detail: format!("installed document {}", detail.into()),
    }
}
pub(super) fn layout<'a>(
    schema: &'a StrictSchema,
    stored: &[u8],
) -> crate::Result<Cow<'a, StrictSchema>> {
    let version = nodedb_strict::TupleDecoder::new(schema)
        .schema_version(stored)
        .map_err(|error| invalid(format!("tuple version is unreadable: {error}")))?;
    if version == 0 || version > schema.version {
        return Err(invalid("tuple version exceeds its captured schema"));
    }
    Ok(if version == schema.version {
        Cow::Borrowed(schema)
    } else {
        Cow::Owned(schema.schema_for_version(version))
    })
}
pub(super) fn canonical_stored(
    stored: &[u8],
    mode: &StorageMode,
    version: Option<RowVersion>,
) -> crate::Result<Vec<u8>> {
    let StorageMode::Strict { schema } = mode else {
        return Ok(stored.to_vec());
    };
    let schema = layout(schema, stored)?;
    if schema.bitemporal {
        let reserved = nodedb_types::columnar::BITEMPORAL_RESERVED_COLUMNS;
        if schema.columns.len() < reserved.len()
            || schema.columns.iter().zip(reserved).any(|(column, name)| {
                column.name != name
                    || column.column_type != nodedb_types::columnar::ColumnType::Int64
            })
        {
            return Err(invalid(
                "bitemporal tuple lacks its reserved physical columns",
            ));
        }
    }
    let values = nodedb_strict::TupleDecoder::new(&schema)
        .extract_all(stored)
        .map_err(|error| {
            invalid(format!(
                "tuple is unreadable under its captured layout: {error}"
            ))
        })?;
    if values.len() != schema.columns.len() {
        return Err(invalid(
            "tuple column count contradicts its captured layout",
        ));
    }
    let mut fields = HashMap::with_capacity(values.len());
    for (column, value) in schema.columns.iter().zip(values) {
        if is_reserved_bitemporal_column(&column.name) {
            if let Some(version) = version {
                let expected = match column.name.as_str() {
                    nodedb_types::columnar::BITEMPORAL_SYSTEM_FROM => version.sys_from,
                    nodedb_types::columnar::BITEMPORAL_VALID_FROM => version.valid_from_ms,
                    nodedb_types::columnar::BITEMPORAL_VALID_UNTIL => version.valid_until_ms,
                    _ => return Err(invalid("tuple has an unknown temporal column")),
                };
                if value != Value::Integer(expected) {
                    return Err(invalid(
                        "tuple temporal bounds contradict its selected version",
                    ));
                }
            }
        } else {
            fields.insert(column.name.clone(), value);
        }
    }
    zerompk::to_msgpack_vec(&Value::Object(fields))
        .map_err(|error| invalid(format!("canonical tuple image cannot encode: {error}")))
}
pub(super) fn compatible(
    captured: &StorageMode,
    current: &StorageMode,
    stored: &[u8],
) -> crate::Result<()> {
    match (captured, current) {
        (StorageMode::Schemaless, StorageMode::Schemaless) => Ok(()),
        (StorageMode::Strict { schema: captured }, StorageMode::Strict { schema: current }) => {
            let captured = layout(captured, stored)?;
            let current = layout(current, stored)?;
            if captured.bitemporal != current.bitemporal
                || captured.columns.len() != current.columns.len()
                || captured
                    .columns
                    .iter()
                    .zip(&current.columns)
                    .any(|(left, right)| {
                        left.name != right.name
                            || left.column_type != right.column_type
                            || left.added_at_version != right.added_at_version
                    })
            {
                return Err(invalid(
                    "captured layout conflicts with the current catalog",
                ));
            }
            Ok(())
        }
        _ => Err(invalid("storage mode conflicts with the current catalog")),
    }
}
pub(super) fn check_image(image: &SharedInstalledDocument) -> crate::Result<()> {
    let canonical = canonical_stored(&image.stored, &image.schema, Some(image.version))?;
    if matches!(&*image.schema, StorageMode::Schemaless) {
        if image.canonical != image.stored {
            return Err(invalid("schemaless canonical and stored bytes differ"));
        }
        return Ok(());
    }
    let expected = doc_format::decode_document_value(&canonical)?;
    let actual = doc_format::decode_document_value(&image.canonical)?;
    let encode = |value: &Value| {
        zerompk::to_msgpack_vec(value)
            .map_err(|error| invalid(format!("canonical correspondence cannot encode: {error}")))
    };
    if encode(&expected)? != encode(&actual)? {
        return Err(invalid("canonical contents contradict the captured tuple"));
    }
    Ok(())
}
pub(super) struct InstalledReadProjection {
    pub native: Vec<u8>,
    pub document: serde_json::Value,
}

impl CoreLoop {
    /// Derive current read views without changing the accepted stored image.
    pub(super) fn installed_read_projection(
        &self,
        key: &(DatabaseId, TenantId, String),
        image: &SharedInstalledDocument,
    ) -> crate::Result<InstalledReadProjection> {
        let projection = match self.judged_doc_config(key) {
            Some(config) => {
                compatible(&image.schema, &config.storage_mode, &image.stored)?;
                let native = match &config.storage_mode {
                    StorageMode::Strict { schema } => {
                        let value =
                            crate::data::executor::strict_format::binary_tuple_to_row_value(
                                &image.stored,
                                schema,
                            )
                            .ok_or_else(|| {
                                invalid("stored tuple has no current native read projection")
                            })?;
                        zerompk::to_msgpack_vec(&value).map_err(|error| {
                            invalid(format!("native read projection cannot encode: {error}"))
                        })?
                    }
                    StorageMode::Schemaless => image.canonical.clone(),
                };
                InstalledReadProjection {
                    native,
                    document: self.decode_stored_document(config, &image.stored)?,
                }
            }
            None => InstalledReadProjection {
                native: image.canonical.clone(),
                document: doc_format::decode_document(&image.canonical)?,
            },
        };
        if !projection.document.is_object() {
            return Err(invalid("read projection is not a document object"));
        }
        Ok(projection)
    }

    /// Capture the exact image of a write that already selected its stored bytes.
    pub(in crate::data::executor) fn capture_stored_document(
        &self,
        key: &(DatabaseId, TenantId, String),
        stored: &[u8],
        version: RowVersion,
    ) -> crate::Result<SharedInstalledDocument> {
        let schema = self.capture_storage_schema(key)?;
        let canonical = canonical_stored(stored, &schema, Some(version))?;
        Ok(crate::types::InstalledDocument {
            canonical,
            stored: stored.to_vec(),
            version,
            schema,
            chain_head: None,
        })
    }
    pub(in crate::data::executor) fn capture_storage_schema(
        &self,
        key: &(DatabaseId, TenantId, String),
    ) -> crate::Result<Arc<StorageMode>> {
        let schemaless = StorageMode::Schemaless;
        let mode = self
            .judged_doc_config(key)
            .map(|config| &config.storage_mode)
            .unwrap_or(&schemaless);
        let mut schemas = self
            .apply_scope
            .storage_schemas
            .try_borrow_mut()
            .map_err(|_| invalid("storage witness cache is already borrowed"))?;
        if let Some(schema) = schemas.get(key)
            && &**schema == mode
        {
            return Ok(Arc::clone(schema));
        }
        let schema = Arc::new(mode.clone());
        schemas.insert(key.clone(), Arc::clone(&schema));
        Ok(schema)
    }
}
