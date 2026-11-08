// SPDX-License-Identifier: BUSL-1.1

//! Geometry preparation before columnar source mutation.

use std::collections::HashMap;

use nodedb_types::columnar::{ColumnType, ColumnarSchema};
use nodedb_types::{BoundingBox, Value};

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::point::apply_put::SpatialEntryId;
use crate::data::executor::task::ExecutionTask;
use crate::engine::spatial::{RTree, RTreeEntry, TreeSrid};

use super::geometry_index::{GeometryIndexDelta, cell_geometry};

pub(super) struct PreparedGeometryRow {
    document_id: String,
    cells: Vec<PreparedGeometryCell>,
}

struct PreparedGeometryCell {
    field: String,
    srid: TreeSrid,
    bbox: BoundingBox,
}

impl CoreLoop {
    /// Resolve geometry and check batch SRIDs before changing source rows.
    pub(super) fn prepare_columnar_geometry<'a>(
        &self,
        task: &ExecutionTask,
        schema: &ColumnarSchema,
        collection: &str,
        rows: impl IntoIterator<Item = Option<&'a [Value]>>,
    ) -> crate::Result<Vec<Option<PreparedGeometryRow>>> {
        let geometry_columns: Vec<_> = schema
            .columns
            .iter()
            .enumerate()
            .filter(|(_, column)| column.column_type == ColumnType::Geometry)
            .collect();
        let id_column = schema.columns.iter().position(|column| column.name == "id");
        let crs = self.crs_context();
        let mut fixed = HashMap::new();
        for (_, column) in &geometry_columns {
            let key = (
                task.request.database_id,
                task.request.tenant_id,
                collection.to_string(),
                column.name.clone(),
            );
            if let Some(tree) = self
                .spatial_indexes
                .get(&key)
                .filter(|tree| !tree.holds_nothing())
            {
                fixed.insert(column.name.as_str(), tree.srid());
            }
        }
        rows.into_iter()
            .map(|row| {
                let Some(values) = row else { return Ok(None) };
                if geometry_columns.is_empty() {
                    return Ok(None);
                }
                let mut cells = Vec::with_capacity(geometry_columns.len());
                for (index, column) in &geometry_columns {
                    let Some(value) = values.get(*index) else {
                        continue;
                    };
                    let Some(geometry) = cell_geometry(value, &crs)? else {
                        continue;
                    };
                    if let Some(srid) = fixed.get(column.name.as_str()) {
                        if *srid != geometry.srid {
                            return Err(crate::Error::MixedSrid {
                                collection: collection.to_string(),
                                field: column.name.clone(),
                                index_srid: srid.code(),
                                row_srid: geometry.srid.code(),
                            });
                        }
                    } else {
                        fixed.insert(column.name.as_str(), geometry.srid);
                    }
                    cells.push(PreparedGeometryCell {
                        field: column.name.clone(),
                        srid: TreeSrid::resolve(geometry.srid, &crs)?,
                        bbox: geometry.bbox(&crs)?,
                    });
                }
                let Some(document_id) = id_column
                    .and_then(|index| values.get(index))
                    .and_then(Value::as_str)
                    .filter(|id| !id.is_empty())
                else {
                    return Ok(None);
                };
                Ok(Some(PreparedGeometryRow {
                    document_id: document_id.to_string(),
                    cells,
                }))
            })
            .collect()
    }

    /// Publish checked projections, retaining row order for reverse-order undo.
    pub(super) fn publish_columnar_geometry(
        &mut self,
        task: &ExecutionTask,
        collection: &str,
        rows: Vec<(PreparedGeometryRow, i64)>,
    ) -> Vec<GeometryIndexDelta> {
        let db = task.request.database_id;
        let tenant = task.request.tenant_id;
        let mut deltas = Vec::with_capacity(rows.len());
        for (row, sys_from) in rows {
            let mut delta = GeometryIndexDelta::default();
            let entry_id = SpatialEntryId::from_user_id(&row.document_id);
            delta.removed.extend(self.remove_document_spatial_indexes(
                db.as_u64(),
                tenant.as_u64(),
                collection,
                entry_id,
                sys_from,
            ));
            for cell in row.cells {
                let key = (db, tenant, collection.to_string(), cell.field.clone());
                let memory = nodedb_mem::ScopedMemory::new(
                    self.governor.clone(),
                    db,
                    tenant,
                    nodedb_mem::EngineId::Spatial,
                );
                let tree = self
                    .spatial_indexes
                    .entry(key.clone())
                    .or_insert_with(|| RTree::new(memory.clone(), cell.srid));
                // Preparation checks every nonempty tree and every batch cell.
                // Empty trees adopt the prepared SRID without another fallible bind.
                if tree.holds_nothing() && tree.srid() != cell.srid.srid {
                    *tree = RTree::new(memory, cell.srid);
                }
                tree.insert_at(
                    RTreeEntry {
                        id: entry_id.as_u64(),
                        bbox: cell.bbox,
                    },
                    sys_from,
                );
                self.spatial_doc_map.insert(
                    (
                        db,
                        tenant,
                        collection.to_string(),
                        cell.field,
                        entry_id.as_u64(),
                    ),
                    row.document_id.clone(),
                );
                delta.inserted.push((key, entry_id.as_u64()));
            }
            deltas.push(delta);
        }
        deltas
    }
}
