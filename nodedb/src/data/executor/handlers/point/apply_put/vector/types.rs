// SPDX-License-Identifier: BUSL-1.1

//! Parameter and delta types for the vector side-effects of `apply_point_put`.

/// Inputs to `apply_point_put_vector_indexes` for one document write.
///
/// `wal_lsn` is the WAL LSN of the document write driving this indexing (`0`
/// when unassigned); it advances each touched collection's checkpoint watermark
/// and, on replay, gates a record the collection's checkpoint already absorbed.
pub(in crate::data::executor) struct VectorIndexPutParams<'a> {
    pub database_id: u64,
    pub tid: u64,
    pub collection: &'a str,
    pub storage_key: crate::engine::document::store::StorageKey,
    pub value: &'a [u8],
    pub wal_lsn: u64,
}

/// Capture of a single HNSW vector index mutation (insert or soft-delete),
/// carrying everything needed to both key the `VectorCollection` (`index_key`,
/// `vector_id`) AND reverse the paired `vector_doc_map` entry on rollback
/// (`collection`, `field`, `doc_id`). Replaces a raw `(index_key, vector_id)`
/// tuple so undo can restore/remove the reverse-lookup map symmetrically with
/// the R-tree's `SpatialInsert`/`SpatialDelete` undo pattern.
pub(in crate::data::executor) struct VectorIndexDelta {
    pub index_key: (nodedb_types::DatabaseId, crate::types::TenantId, String),
    pub vector_id: u32,
    pub collection: String,
    pub field: String,
    pub doc_id: crate::engine::document::store::StorageKey,
}

/// Inputs to `remove_then_insert_vector_field`, the shared per-field
/// remove-before-insert tail of `apply_point_put_vector_indexes`'s strict and
/// schemaless arms, once each has resolved its own `index_key` and extracted
/// `floats` for `field_name`. `storage_key` carries the row's surrogate: call
/// `storage_key.surrogate()` rather than threading a second surrogate field.
pub(super) struct VectorFieldInsert<'a> {
    pub(super) database_id: u64,
    pub(super) tid: u64,
    pub(super) index_key: (nodedb_types::DatabaseId, crate::types::TenantId, String),
    pub(super) collection: &'a str,
    pub(super) field_name: &'a str,
    pub(super) storage_key: crate::engine::document::store::StorageKey,
    pub(super) floats: Vec<f32>,
    pub(super) wal_lsn: u64,
}
