// SPDX-License-Identifier: BUSL-1.1

//! R-tree removal for a columnar row that is deleted, rewritten, or
//! truncated, so spatial predicates stop finding rows the collection no
//! longer holds. The insert-side twin is `geometry_index.rs`.

use nodedb_types::columnar::{ColumnType, ColumnarSchema};
use nodedb_types::value::Value;

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::point::apply_put::SpatialEntryId;
use crate::data::executor::handlers::transaction::undo::UndoEntry;
use crate::data::executor::spatial_key::SpatialIndexKey;

/// One R-tree entry a write removed: enough to put it back on undo
/// (`UndoEntry::SpatialDelete`).
pub(in crate::data::executor) struct RemovedSpatialEntry {
    pub key: SpatialIndexKey,
    pub entry_id: u64,
    pub bbox: nodedb_types::BoundingBox,
    /// The reverse-map document id.
    pub document_id: String,
    /// The SRID of the tree the entry left.
    pub srid: crate::engine::spatial::TreeSrid,
    /// The system-time ordinal the entry ended at.
    pub sys_to: i64,
}

impl RemovedSpatialEntry {
    /// The undo entry that puts this entry back.
    pub(in crate::data::executor) fn into_undo(self) -> UndoEntry {
        UndoEntry::SpatialDelete {
            key: self.key,
            entry_id: self.entry_id,
            bbox: self.bbox,
            document_id: self.document_id,
            srid: self.srid,
            sys_to: self.sys_to,
        }
    }
}

/// Whether `schema` declares at least one `Geometry` column, i.e. whether
/// the collection's rows can carry R-tree entries at all.
pub(in crate::data::executor) fn schema_has_geometry(schema: &ColumnarSchema) -> bool {
    schema
        .columns
        .iter()
        .any(|c| c.column_type == ColumnType::Geometry)
}

/// The R-tree entry id of a schema-ordered columnar row, derived exactly as
/// `index_columnar_geometry_columns` derives it: the row's `id` column
/// rendered as a string. `None` when the row carries no string `id`, in
/// which case the insert path indexed nothing for it either.
pub(in crate::data::executor) fn columnar_row_spatial_entry_id(
    schema: &ColumnarSchema,
    row: &[Value],
) -> Option<SpatialEntryId> {
    let id_idx = schema.columns.iter().position(|c| c.name == "id")?;
    let id = row.get(id_idx)?.as_str()?;
    if id.is_empty() {
        return None;
    }
    Some(SpatialEntryId::from_user_id(id))
}

impl CoreLoop {
    /// Remove every per-field R-tree entry and reverse-map record of the
    /// columnar row `row` (schema order), ending each at `sys_to`, the
    /// system-time ordinal the write ended the row at. A no-op for a schema
    /// with no geometry column. Returns the removed entries for a
    /// transactional caller to reverse.
    pub(in crate::data::executor) fn remove_columnar_row_spatial_entries(
        &mut self,
        db_id: nodedb_types::DatabaseId,
        tid: crate::types::TenantId,
        collection: &str,
        schema: &ColumnarSchema,
        removed: RemovedColumnarRow<'_>,
    ) -> Vec<RemovedSpatialEntry> {
        if !schema_has_geometry(schema) {
            return Vec::new();
        }
        let Some(entry_id) = columnar_row_spatial_entry_id(schema, removed.row) else {
            return Vec::new();
        };
        self.remove_document_spatial_indexes(
            db_id.as_u64(),
            tid.as_u64(),
            collection,
            entry_id,
            removed.sys_to,
        )
    }

    /// The system-time ordinal the write ended the row at `location` of
    /// columnar engine `key` at. [`nodedb_spatial::rtree::UNVERSIONED`] when
    /// the row keeps no end: its R-tree entry then leaves without history.
    pub(in crate::data::executor) fn columnar_row_end(
        &self,
        key: &crate::data::executor::handlers::columnar_mutation_apply::ColumnarEngineKey,
        location: Option<nodedb_columnar::pk_index::RowLocation>,
    ) -> i64 {
        location
            .and_then(|loc| self.columnar_engines.get(key)?.sys_to_at(loc))
            .unwrap_or(nodedb_spatial::rtree::UNVERSIONED)
    }
}

/// A columnar row a write ended: its pre-image in schema order and the
/// ordinal the write ended it at.
pub(in crate::data::executor) struct RemovedColumnarRow<'a> {
    pub row: &'a [Value],
    pub sys_to: i64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_types::columnar::ColumnDef;

    fn schema() -> ColumnarSchema {
        ColumnarSchema::new(vec![
            ColumnDef::required("id", ColumnType::String).with_primary_key(),
            ColumnDef::nullable("loc", ColumnType::Geometry),
        ])
        .expect("valid")
    }

    #[test]
    fn entry_id_matches_the_insert_side_derivation() {
        let row = vec![Value::String("a".into()), Value::Null];
        assert_eq!(
            columnar_row_spatial_entry_id(&schema(), &row),
            Some(SpatialEntryId::from_user_id("a"))
        );
    }

    #[test]
    fn a_row_without_a_string_id_has_no_entry() {
        let row = vec![Value::Integer(7), Value::Null];
        assert_eq!(columnar_row_spatial_entry_id(&schema(), &row), None);
        let row = vec![Value::String(String::new()), Value::Null];
        assert_eq!(columnar_row_spatial_entry_id(&schema(), &row), None);
    }

    #[test]
    fn geometry_detection_reads_the_schema() {
        assert!(schema_has_geometry(&schema()));
        let plain = ColumnarSchema::new(vec![
            ColumnDef::required("id", ColumnType::String).with_primary_key(),
        ])
        .expect("valid");
        assert!(!schema_has_geometry(&plain));
    }
}
