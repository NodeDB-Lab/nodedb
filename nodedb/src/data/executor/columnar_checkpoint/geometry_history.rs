// SPDX-License-Identifier: BUSL-1.1

//! Rebuilding the system-time history of a restored columnar collection's
//! R-trees from its stored row versions.
//!
//! The rebuild in `geometry_restore.rs` indexes each row's open version.
//! The history comes from the same stored versions: each live entry starts
//! where its row's open version starts, and the geometry of each ended
//! version stays as an ended entry over the version's interval. A read at a
//! past cut then finds the entries of the versions visible there.

use std::collections::HashMap;

use nodedb_spatial::rtree::{EndedEntry, HistoryRows, LiveStart, TreeSrid};
use nodedb_types::columnar::ColumnType;
use nodedb_types::value::Value;

use super::super::core_loop::CoreLoop;
use crate::data::executor::handlers::columnar_write::geometry_index::cell_geometry;
use crate::data::executor::handlers::point::apply_put::SpatialEntryId;
use crate::types::{DatabaseId, TenantId};

/// The history one field's R-tree takes, and the SRID of its first geometry.
#[derive(Default)]
struct FieldHistory {
    rows: HistoryRows,
    srid: Option<TreeSrid>,
}

impl CoreLoop {
    /// Install the system-time history of the R-trees of restored columnar
    /// collection `key`, read from `engine`'s stored versions. `segments`
    /// holds the blob of every flushed segment, index `i` being segment id
    /// `i + 1`.
    ///
    /// `Err` when a stored version does not read or holds a geometry that
    /// does not parse.
    pub(super) fn restore_columnar_geometry_history(
        &mut self,
        key: &(DatabaseId, TenantId, String),
        engine: &nodedb_columnar::MutationEngine,
        segments: &[Vec<u8>],
    ) -> crate::Result<()> {
        let (db_id, tenant_id, collection) = key;
        let schema = engine.schema();
        let geometry_columns: Vec<(usize, &str)> = schema
            .columns
            .iter()
            .enumerate()
            .filter(|(_, c)| c.column_type == ColumnType::Geometry)
            .map(|(i, c)| (i, c.name.as_str()))
            .collect();
        let Some(id_idx) = schema.columns.iter().position(|c| c.name == "id") else {
            return Ok(());
        };
        if geometry_columns.is_empty() {
            return Ok(());
        }
        let versions = engine.stored_versions(segments, &[], None)?;
        let crs = self.crs_context();
        let mut fields: HashMap<String, FieldHistory> = HashMap::new();
        for version in &versions {
            let Some(id) = version
                .values
                .get(id_idx)
                .and_then(Value::as_str)
                .filter(|id| !id.is_empty())
            else {
                continue;
            };
            let entry_id = SpatialEntryId::from_user_id(id).as_u64();
            for (col_idx, field) in &geometry_columns {
                let Some(cell) = version.values.get(*col_idx) else {
                    continue;
                };
                let Some(geom) = cell_geometry(cell, &crs)? else {
                    continue;
                };
                let history = fields.entry((*field).to_string()).or_default();
                if history.srid.is_none() {
                    history.srid = Some(TreeSrid::resolve(geom.srid, &crs)?);
                }
                match version.sys_to {
                    None => history.rows.live.push(LiveStart {
                        entry_id,
                        sys_from: version.sys_from,
                    }),
                    Some(sys_to) => history.rows.ended.push(EndedEntry {
                        entry_id,
                        document_id: id.to_string(),
                        bbox: geom.bbox(&crs)?,
                        sys_from: version.sys_from,
                        sys_to,
                    }),
                }
            }
        }
        for (field, history) in fields {
            let Some(srid) = history.srid else {
                continue;
            };
            let memory = nodedb_mem::ScopedMemory::new(
                self.governor.clone(),
                *db_id,
                *tenant_id,
                nodedb_mem::EngineId::Spatial,
            );
            self.spatial_indexes
                .entry((*db_id, *tenant_id, collection.clone(), field))
                .or_insert_with(|| crate::engine::spatial::RTree::new(memory, srid))
                .restore_history(history.rows);
        }
        Ok(())
    }
}
