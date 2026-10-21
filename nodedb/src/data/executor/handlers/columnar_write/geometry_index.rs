// SPDX-License-Identifier: BUSL-1.1

//! Post-insert R-tree maintenance for `Geometry`-typed columnar columns, so
//! spatial predicates can find newly-inserted rows.

use nodedb_types::columnar::{ColumnType, ColumnarSchema};
use nodedb_types::geometry::{Srid, SridGeometry};
use nodedb_types::value::Value;

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::point::apply_put::SpatialEntryId;
use crate::data::executor::spatial_key::SpatialIndexKey;
use crate::data::executor::task::ExecutionTask;

use super::geometry_remove::RemovedSpatialEntry;

/// What one geometry-index pass changed, for a transactional caller to
/// reverse: the entries it removed (a re-indexed `id`'s prior geometry) and
/// the entries it inserted.
#[derive(Default)]
pub(in crate::data::executor) struct GeometryIndexDelta {
    pub removed: Vec<RemovedSpatialEntry>,
    pub inserted: Vec<(SpatialIndexKey, u64)>,
}

/// The indexable geometry a columnar cell holds, in every form the
/// memtable stores: a native geometry, its text (GeoJSON, EWKT, WKT, hex
/// EWKB), its WKB bytes, or a GeoJSON object. An EMPTY geometry and a value
/// of another type index nothing: the memtable refuses that type. A form
/// that does not parse, or an SRID `crs` does not define, fails, so an
/// index never misses a stored geometry.
fn cell_geometry(
    value: &Value,
    crs: &nodedb_crs::CrsContext<'_>,
) -> crate::Result<Option<SridGeometry>> {
    match nodedb_spatial::geometry_from_value(value, nodedb_spatial::SridCheck::Resolve(crs)) {
        Ok(geom) => Ok((!geom.is_empty()).then_some(geom)),
        Err(nodedb_spatial::SpatialError::NotGeometryValue { .. }) => Ok(None),
        Err(e) => Err(e.into()),
    }
}

impl CoreLoop {
    /// Check that every geometry cell of `ndb_rows` can join its column's
    /// R-tree. Changes nothing.
    ///
    /// A non-empty index admits only its own SRID. An empty or absent index
    /// takes the SRID of the batch's first geometry, and every later
    /// geometry of the column must match it. The first refusal returns
    /// `Error::MixedSrid`.
    pub(in crate::data::executor) fn check_columnar_geometry_srids(
        &self,
        task: &ExecutionTask,
        schema: &ColumnarSchema,
        collection: &str,
        ndb_rows: &[nodedb_types::Value],
    ) -> crate::Result<()> {
        let crs = self.crs_context();
        let db_id = task.request.database_id;
        let tid = task.request.tenant_id;
        for col_def in schema
            .columns
            .iter()
            .filter(|c| c.column_type == ColumnType::Geometry)
        {
            let index_key = (db_id, tid, collection.to_string(), col_def.name.clone());
            let mut fixed: Option<Srid> = self
                .spatial_indexes
                .get(&index_key)
                .filter(|rtree| !rtree.is_empty())
                .map(|rtree| rtree.srid());
            for row in ndb_rows {
                let Value::Object(obj) = row else {
                    continue;
                };
                let Some(cell) = obj.get(&col_def.name) else {
                    continue;
                };
                let Some(geom) = cell_geometry(cell, &crs)? else {
                    continue;
                };
                match fixed {
                    Some(srid) if srid != geom.srid => {
                        return Err(crate::Error::MixedSrid {
                            collection: collection.to_string(),
                            field: col_def.name.clone(),
                            index_srid: srid.code(),
                            row_srid: geom.srid.code(),
                        });
                    }
                    Some(_) => {}
                    None => fixed = Some(geom.srid),
                }
            }
        }
        Ok(())
    }

    /// Populate the R-tree for any `Geometry` columns on `schema`, from the
    /// rows just inserted (`ndb_rows`). Returns what it changed.
    ///
    /// Runs [`Self::check_columnar_geometry_srids`] first, so a mixed-SRID
    /// batch fails with `Error::MixedSrid` before any index changes.
    ///
    /// `schema` is threaded in from the caller rather than re-read from
    /// `columnar_engines` here: the caller already resolved it, and taking it
    /// as a parameter means there is no "engine missing" case to invent an
    /// answer for. Re-looking it up would force a silent early return on a
    /// state the caller has already ruled out, and a geometry index that
    /// silently skips maintenance drifts out of sync with its rows.
    pub(in crate::data::executor) fn index_columnar_geometry_columns(
        &mut self,
        task: &ExecutionTask,
        schema: &ColumnarSchema,
        collection: &str,
        ndb_rows: &[nodedb_types::Value],
    ) -> crate::Result<GeometryIndexDelta> {
        let mut delta = GeometryIndexDelta::default();
        let db_id = task.request.database_id;
        let tid = task.request.tenant_id;
        let geom_cols: Vec<usize> = schema
            .columns
            .iter()
            .enumerate()
            .filter(|(_, c)| c.column_type == ColumnType::Geometry)
            .map(|(i, _)| i)
            .collect();

        if geom_cols.is_empty() {
            return Ok(delta);
        }
        self.check_columnar_geometry_srids(task, schema, collection, ndb_rows)?;
        let crs = self.crs_context();
        for row in ndb_rows {
            let obj = match row {
                nodedb_types::Value::Object(m) => m,
                _ => continue,
            };
            let doc_id = obj
                .get("id")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            if doc_id.is_empty() {
                continue;
            }
            // Re-indexing this doc_id (e.g. a live geometry UPDATE) must
            // REPLACE, not append: `RTree::insert` blindly pushes a new
            // entry even when one with this id already exists, so
            // without clearing first the tree would carry a stale bbox
            // alongside the fresh one and a scan would match/emit the
            // document twice. Mirrors `apply_point_put_spatial`'s use of
            // the same helper. The removed tuples are reported so a
            // transactional caller can put them back on rollback.
            let spatial_entry_id = SpatialEntryId::from_user_id(&doc_id);
            delta.removed.extend(self.remove_document_spatial_indexes(
                db_id.as_u64(),
                tid.as_u64(),
                collection,
                spatial_entry_id,
            ));
            for &col_idx in &geom_cols {
                let col_def = &schema.columns[col_idx];
                let field_val = match obj.get(&col_def.name) {
                    Some(v) => v,
                    None => continue,
                };
                let Some(geom) = cell_geometry(field_val, &crs)? else {
                    continue;
                };
                let tree_srid = crate::engine::spatial::TreeSrid::resolve(geom.srid, &crs)?;
                let bbox = geom.bbox(&crs)?;
                let index_key = (db_id, tid, collection.to_string(), col_def.name.clone());
                let entry_id = spatial_entry_id.as_u64();
                let memory = nodedb_mem::ScopedMemory::new(
                    self.governor.clone(),
                    db_id,
                    tid,
                    nodedb_mem::EngineId::Spatial,
                );
                let rtree = self
                    .spatial_indexes
                    .entry(index_key.clone())
                    .or_insert_with(|| crate::engine::spatial::RTree::new(memory, tree_srid));
                // The batch check above admitted every SRID here; removing a
                // re-indexed row's prior entry can only empty the tree.
                rtree
                    .bind_srid(tree_srid)
                    .map_err(|m| crate::Error::MixedSrid {
                        collection: collection.to_string(),
                        field: col_def.name.clone(),
                        index_srid: m.index.code(),
                        row_srid: m.entry.code(),
                    })?;
                rtree.insert(crate::engine::spatial::RTreeEntry { id: entry_id, bbox });
                self.spatial_doc_map.insert(
                    (
                        db_id,
                        tid,
                        collection.to_string(),
                        col_def.name.clone(),
                        entry_id,
                    ),
                    doc_id.clone(),
                );
                delta.inserted.push((index_key, entry_id));
            }
        }
        Ok(delta)
    }

    /// Record a geometry-index pass on `undo_log` so a rollback reverses
    /// it: the removed entries re-insert, the inserted entries delete. The
    /// entries are pushed removed-first, so the reverse-order rollback
    /// deletes the fresh entries before it re-inserts the prior ones.
    pub(in crate::data::executor) fn push_geometry_index_undo(
        undo_log: &mut Vec<crate::data::executor::handlers::transaction::undo::UndoEntry>,
        delta: GeometryIndexDelta,
    ) {
        use crate::data::executor::handlers::transaction::undo::UndoEntry;
        for (key, entry_id, bbox, document_id, srid) in delta.removed {
            undo_log.push(UndoEntry::SpatialDelete {
                key,
                entry_id,
                bbox,
                document_id,
                srid,
            });
        }
        for (key, entry_id) in delta.inserted {
            undo_log.push(UndoEntry::SpatialInsert { key, entry_id });
        }
    }
}
