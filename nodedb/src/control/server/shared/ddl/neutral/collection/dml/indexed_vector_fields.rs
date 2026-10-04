// SPDX-License-Identifier: BUSL-1.1

//! The fields of a `{ ... }` INSERT that the document write already indexes
//! into a vector index.
//!
//! The Data Plane indexes a document's vectors when it stores the document:
//!
//! - A strict collection indexes every `VECTOR(n)` column.
//! - Any other collection indexes each field that has its own vector index.
//!   When no field has one, a default-field vector index covers `embedding`.
//! - A schemaless collection also indexes every declared `VECTOR(n)` column.
//!
//! The `{ ... }` handler also sends a vector insert for numeric-array
//! fields, so a field no index covers is still searchable. It skips the
//! fields listed here. A second insert of such a field appends a second
//! HNSW node for the same row.

use std::collections::HashSet;

use nodedb_types::{CollectionType, ColumnType, DatabaseId, DocumentMode};

use crate::control::server::shared::ddl::result::DdlError;
use crate::control::state::SharedState;

/// The default vector field a vector index without a field name covers.
const DEFAULT_VECTOR_FIELD: &str = "embedding";

/// The fields of `collection` the document write indexes into a vector
/// index. Fails when the catalog cannot be read: guessing would either
/// index a field twice or not at all.
pub(super) fn indexed_vector_fields(
    state: &SharedState,
    database_id: DatabaseId,
    tenant_id: u64,
    collection: &str,
    collection_type: Option<&CollectionType>,
) -> Result<HashSet<String>, DdlError> {
    if let Some(CollectionType::Document(DocumentMode::Strict(schema))) = collection_type {
        let strict: HashSet<String> = schema
            .columns
            .iter()
            .filter(|c| matches!(c.column_type, ColumnType::Vector(_)))
            .map(|c| c.name.clone())
            .collect();
        if !strict.is_empty() {
            return Ok(strict);
        }
    }

    let params = state
        .credentials
        .catalog()
        .list_vector_index_params_in_database(database_id.as_u64())
        .map_err(|e| {
            DdlError::from_error_in_context(
                &format!("read vector indexes of \"{collection}\" for INSERT"),
                &e,
            )
        })?;
    let mut named = HashSet::new();
    let mut has_default = false;
    for p in params
        .iter()
        .filter(|p| p.tenant_id == tenant_id && p.collection == collection)
    {
        if p.field_name.is_empty() {
            has_default = true;
        } else {
            named.insert(p.field_name.clone());
        }
    }
    if named.is_empty() && has_default {
        named.insert(DEFAULT_VECTOR_FIELD.to_string());
    }
    if collection_type.is_some_and(CollectionType::is_schemaless) {
        let stored = state
            .credentials
            .catalog()
            .get_collection(database_id, tenant_id, collection)
            .map_err(|e| {
                DdlError::from_error_in_context(
                    &format!("read declared columns of \"{collection}\" for INSERT"),
                    &e,
                )
            })?;
        if let Some(stored) = stored
            && stored.vector_primary.is_none()
        {
            named.extend(
                crate::control::server::shared::ddl::schema_validation::extract_vector_fields(
                    &stored.fields,
                )
                .into_iter()
                .map(|(field, _dim, _metric)| field),
            );
        }
    }
    Ok(named)
}
