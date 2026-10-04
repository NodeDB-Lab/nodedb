// SPDX-License-Identifier: BUSL-1.1

//! Which fields of a collection carry vectors: strict-schema `Vector(dim)`
//! columns, schemaless fields registered via `vector_params`, and the
//! declared `VECTOR(n)` columns of a schemaless collection.

use crate::data::executor::core_loop::CoreLoop;

/// The field a collection-level vector index (no field name) covers.
pub(super) const DEFAULT_VECTOR_FIELD: &str = "embedding";

impl CoreLoop {
    /// Strict-schema `Vector(dim)` column names + dims declared on
    /// `collection`, or empty if the collection has no strict schema / no
    /// vector columns. Shared by `apply_point_put_vector_indexes` (which
    /// needs `dim` to validate extracted float arrays) and
    /// `apply_point_delete`'s vector cleanup (which only needs the field
    /// names to construct exact `vector_doc_map` keys without a full-map
    /// scan).
    pub(in crate::data::executor) fn strict_vector_fields(
        &self,
        database_id: u64,
        tid: u64,
        collection: &str,
    ) -> Vec<(String, u32)> {
        let config_key = (
            crate::types::DatabaseId::new(database_id),
            crate::types::TenantId::new(tid),
            collection.to_string(),
        );
        self.doc_configs
            .get(&config_key)
            .and_then(|config| {
                if let nodedb_physical::physical_plan::StorageMode::Strict { ref schema } =
                    config.storage_mode
                {
                    let fields: Vec<_> = schema
                        .columns
                        .iter()
                        .filter_map(|col| {
                            if let nodedb_types::columnar::ColumnType::Vector(dim) = col.column_type
                            {
                                Some((col.name.clone(), dim))
                            } else {
                                None
                            }
                        })
                        .collect();
                    if fields.is_empty() {
                        None
                    } else {
                        Some(fields)
                    }
                } else {
                    None
                }
            })
            .unwrap_or_default()
    }

    /// Schemaless vector field names of `collection`: the named-field
    /// `vector_params` entries `"{collection}:{field}"`, the bare
    /// `"{collection}"` key defaulting to `"embedding"`, and the collection's
    /// declared `VECTOR(n)` columns. Shared by the put path's schemaless
    /// indexing branch and the delete cleanup's exact key construction.
    pub(in crate::data::executor) fn schemaless_vector_field_names(
        &self,
        database_id: u64,
        tid: u64,
        collection: &str,
    ) -> Vec<String> {
        let db_key = nodedb_types::DatabaseId::new(database_id);
        let tid_key = crate::types::TenantId::new(tid);
        let field_prefix = format!("{collection}:");
        let bare_key = (db_key, tid_key, collection.to_string());

        let mut names: Vec<String> = self
            .vector_params
            .keys()
            .filter(|(d, t, coll_key)| {
                *d == bare_key.0 && *t == bare_key.1 && coll_key.starts_with(&field_prefix)
            })
            .map(|k| k.2[field_prefix.len()..].to_string())
            .collect();
        if names.is_empty() && self.vector_params.contains_key(&bare_key) {
            names.push(DEFAULT_VECTOR_FIELD.to_string());
        }
        if let Some(config) = self.doc_configs.get(&bare_key) {
            for (field, _dim) in &config.vector_fields {
                if !names.contains(field) {
                    names.push(field.clone());
                }
            }
        }
        names
    }

    /// The width of `field` when `collection` declares it a `VECTOR(n)`
    /// column of a schemaless collection, else `None`.
    pub(in crate::data::executor) fn declared_schemaless_vector_dim(
        &self,
        database_id: u64,
        tid: u64,
        collection: &str,
        field: &str,
    ) -> Option<usize> {
        let config_key = (
            nodedb_types::DatabaseId::new(database_id),
            crate::types::TenantId::new(tid),
            collection.to_string(),
        );
        self.doc_configs
            .get(&config_key)?
            .vector_fields
            .iter()
            .find(|(name, _dim)| name == field)
            .map(|(_name, dim)| *dim)
    }

    /// Whether `collection` has any vector fields — strict-schema `Vector(dim)`
    /// columns OR schemaless fields registered via `vector_params` or declared
    /// as `VECTOR(n)` columns. Combines
    /// `strict_vector_fields` + `schemaless_vector_field_names` into the single
    /// gate check callers need before deciding whether to pay for HNSW
    /// maintenance at all. Callers that loop over many rows (bulk update/
    /// delete, merge, update-from-join) must call this ONCE before the loop
    /// and thread the resulting bool through, rather than recomputing it per
    /// row — the schemaless half is an unindexed scan of `vector_params`.
    pub(in crate::data::executor) fn collection_has_vectors(
        &self,
        database_id: u64,
        tid: u64,
        collection: &str,
    ) -> bool {
        !self
            .strict_vector_fields(database_id, tid, collection)
            .is_empty()
            || !self
                .schemaless_vector_field_names(database_id, tid, collection)
                .is_empty()
    }
}
