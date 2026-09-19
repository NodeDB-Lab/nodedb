// SPDX-License-Identifier: BUSL-1.1

//! Per-row apply for a columnar UPDATE / DELETE once the row set is known:
//! the `MutationEngine` mutation, its undo capture, and the R-tree cascade
//! that keeps spatial predicates from finding rows the collection no
//! longer holds.
//!
//! Shared by the predicate handlers (`columnar_mutation.rs`) and the
//! resolved-row-set handlers (`columnar_resolved_mutation.rs`), so the two
//! cannot drift in what a mutation leaves behind.

use nodedb_columnar::pk_index::{RowLocation, encode_pk};
use nodedb_types::Value;
use nodedb_types::columnar::ColumnarSchema;
use tracing::warn;

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::columnar_write::{
    GeometryIndexDelta, RemovedSpatialEntry, row_values_to_object, schema_has_geometry,
};
use crate::data::executor::handlers::transaction::undo::UndoEntry;
use crate::data::executor::task::ExecutionTask;

/// Engine map key of one columnar collection.
pub(in crate::data::executor) type ColumnarEngineKey =
    (nodedb_types::DatabaseId, crate::types::TenantId, String);

/// What [`CoreLoop::apply_columnar_update_rows`] changed, for the caller's
/// `UndoEntry::ColumnarUpdate`.
pub(in crate::data::executor) struct ColumnarUpdateOutcome {
    pub affected: u64,
    pub inserted_pks: Vec<Vec<u8>>,
    pub displaced: Vec<(Vec<u8>, RowLocation)>,
    pub restored: Vec<(Vec<u8>, RowLocation)>,
}

/// What [`CoreLoop::apply_columnar_delete_pks`] changed, for the caller's
/// `UndoEntry::ColumnarDelete`.
pub(in crate::data::executor) struct ColumnarDeleteOutcome {
    pub affected: u64,
    pub restored: Vec<(Vec<u8>, RowLocation)>,
}

impl CoreLoop {
    /// Apply `(old_pk, post_image)` rows through `MutationEngine::update`
    /// (delete-old-PK + insert-new-row). For a collection with geometry
    /// columns the old row's R-tree entries go and the post-image is
    /// indexed. When `undo_log` is `Some`, every in-memory change this
    /// makes is recorded on it.
    pub(in crate::data::executor) fn apply_columnar_update_rows(
        &mut self,
        task: &ExecutionTask,
        key: &ColumnarEngineKey,
        schema: &ColumnarSchema,
        rows: &[(Value, Vec<Value>)],
        undo_log: Option<&mut Vec<UndoEntry>>,
    ) -> ColumnarUpdateOutcome {
        let track = undo_log.is_some();
        let has_geometry = schema_has_geometry(schema);
        let collection = key.2.as_str();
        let mut outcome = ColumnarUpdateOutcome {
            affected: 0,
            inserted_pks: Vec::new(),
            displaced: Vec::new(),
            restored: Vec::new(),
        };
        let mut spatial_removed: Vec<RemovedSpatialEntry> = Vec::new();
        let mut geometry_delta = GeometryIndexDelta::default();

        for (old_pk, new_row) in rows {
            let old_pk_bytes = encode_pk(old_pk);
            // The pre-image is read BEFORE the update unbinds the PK: it is
            // what names the R-tree entries the old row owns.
            let pre_image = if has_geometry {
                self.read_columnar_row_by_pk(key, &old_pk_bytes)
            } else {
                None
            };
            let Some(engine) = self.columnar_engines.get_mut(key) else {
                break;
            };
            // Capture the pre-image BEFORE mutating (the update removes the
            // old PK binding and appends a new row): the tombstoned
            // original's location, the appended replacement's PK, and — for
            // a PK-changing update — the memtable row its insert half
            // displaces.
            let capture = if track {
                let old_location = engine.pk_index().get(&old_pk_bytes).copied();
                let new_pk_bytes = engine.encode_pk_from_row(new_row).ok();
                let displaced_entry = match &new_pk_bytes {
                    Some(nb) if *nb != old_pk_bytes => engine
                        .pk_index()
                        .get(nb)
                        .copied()
                        .filter(|loc| loc.segment_id == engine.memtable_segment_id())
                        .map(|loc| (nb.clone(), loc)),
                    _ => None,
                };
                Some((old_location, new_pk_bytes, displaced_entry))
            } else {
                None
            };
            match engine.update(old_pk, new_row) {
                Ok(_) => {}
                Err(e) => {
                    warn!(core = self.core_id, %collection, error = %e, "columnar update row failed");
                    continue;
                }
            }
            outcome.affected += 1;
            if let Some((old_location, new_pk_bytes, displaced_entry)) = capture {
                if let Some(nb) = new_pk_bytes {
                    outcome.inserted_pks.push(nb);
                }
                if let Some(loc) = old_location {
                    outcome.restored.push((old_pk_bytes, loc));
                }
                if let Some(d) = displaced_entry {
                    outcome.displaced.push(d);
                }
            }
            if has_geometry {
                if let Some(row) = &pre_image {
                    spatial_removed.extend(self.remove_columnar_row_spatial_entries(
                        key.0, key.1, collection, schema, row,
                    ));
                }
                let delta = self.index_columnar_geometry_columns(
                    task,
                    schema,
                    collection,
                    &[row_values_to_object(schema, new_row)],
                );
                geometry_delta.removed.extend(delta.removed);
                geometry_delta.inserted.extend(delta.inserted);
            }
        }

        if let Some(log) = undo_log {
            push_removed_spatial_undo(log, spatial_removed);
            Self::push_geometry_index_undo(log, geometry_delta);
        }
        outcome
    }

    /// Remove the rows bound to `pks` through `MutationEngine::delete`. For
    /// a collection with geometry columns each row's R-tree entries go too.
    /// When `undo_log` is `Some`, every in-memory change this makes is
    /// recorded on it.
    pub(in crate::data::executor) fn apply_columnar_delete_pks(
        &mut self,
        key: &ColumnarEngineKey,
        schema: &ColumnarSchema,
        pks: &[Value],
        undo_log: Option<&mut Vec<UndoEntry>>,
    ) -> ColumnarDeleteOutcome {
        let track = undo_log.is_some();
        let has_geometry = schema_has_geometry(schema);
        let collection = key.2.as_str();
        let mut outcome = ColumnarDeleteOutcome {
            affected: 0,
            restored: Vec::new(),
        };
        let mut spatial_removed: Vec<RemovedSpatialEntry> = Vec::new();

        for pk in pks {
            let pk_bytes = encode_pk(pk);
            let pre_image = if has_geometry {
                self.read_columnar_row_by_pk(key, &pk_bytes)
            } else {
                None
            };
            let Some(engine) = self.columnar_engines.get_mut(key) else {
                break;
            };
            // Read the location BEFORE the delete removes the PK binding.
            let captured = if track {
                engine
                    .pk_index()
                    .get(&pk_bytes)
                    .copied()
                    .map(|loc| (pk_bytes.clone(), loc))
            } else {
                None
            };
            match engine.delete(pk) {
                Ok(_) => {}
                Err(e) => {
                    warn!(core = self.core_id, %collection, error = %e, "columnar delete row failed");
                    continue;
                }
            }
            outcome.affected += 1;
            if let Some(entry) = captured {
                outcome.restored.push(entry);
            }
            if let Some(row) = &pre_image {
                spatial_removed.extend(
                    self.remove_columnar_row_spatial_entries(key.0, key.1, collection, schema, row),
                );
            }
        }

        if let Some(log) = undo_log {
            push_removed_spatial_undo(log, spatial_removed);
        }
        outcome
    }
}

/// Record removed R-tree entries so a rollback re-inserts them. Shares
/// `CoreLoop::push_geometry_index_undo`'s removed-entry push with an empty
/// inserted half.
fn push_removed_spatial_undo(undo_log: &mut Vec<UndoEntry>, removed: Vec<RemovedSpatialEntry>) {
    CoreLoop::push_geometry_index_undo(
        undo_log,
        GeometryIndexDelta {
            removed,
            inserted: Vec::new(),
        },
    );
}
