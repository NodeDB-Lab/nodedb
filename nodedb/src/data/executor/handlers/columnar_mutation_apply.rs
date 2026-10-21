// SPDX-License-Identifier: BUSL-1.1

//! Per-row apply for a columnar UPDATE / DELETE once the row set is known:
//! the `MutationEngine` mutation, its undo capture, and the R-tree cascade
//! that keeps spatial predicates from finding rows the collection no
//! longer holds.
//!
//! Shared by the predicate handlers (`columnar_mutation.rs`) and the
//! resolved-row-set handlers (`columnar_resolved_mutation.rs`), so the two
//! cannot drift in what a mutation leaves behind. The close of a bitemporal
//! key lives in `columnar_close_apply.rs`.

use nodedb_columnar::pk_index::{RowLocation, encode_pk};
use nodedb_types::Value;
use nodedb_types::columnar::ColumnarSchema;

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::columnar_write::{
    GeometryIndexDelta, RemovedSpatialEntry, row_values_to_object, schema_has_geometry,
};
use crate::data::executor::handlers::transaction::undo::UndoEntry;
use crate::data::executor::handlers::transaction::undo::memory::abort_error;
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

/// How [`CoreLoop::apply_columnar_delete_pks`] removes a key.
#[derive(Clone, Copy)]
pub(in crate::data::executor) enum ColumnarDeleteMode {
    /// A bitemporal collection closes each key with a tombstone version
    /// stamped as an UPDATE stamps: the Calvin transaction's ordinal, else
    /// `system_from`, else a fresh one, raised above the key's newest stored
    /// version. The closed version stays as history. Any other collection
    /// marks the row deleted.
    Close { system_from: Option<i64> },
    /// Mark the bound row deleted in every collection. A transaction's
    /// row-image install removes the base rows it displaced this way.
    Erase,
}

/// What [`CoreLoop::apply_columnar_delete_pks`] changed, for the caller's
/// undo entry ([`Self::undo_entry`]).
pub(in crate::data::executor) struct ColumnarDeleteOutcome {
    pub affected: u64,
    /// Each removed key and the row it was bound to.
    pub restored: Vec<(Vec<u8>, RowLocation)>,
    /// The memtable row count before the first tombstone version was
    /// appended. `None` when no key was closed: every row was marked deleted.
    pub closed_from: Option<usize>,
}

impl ColumnarDeleteOutcome {
    /// The undo entry that reverses this outcome: a close cuts its
    /// tombstones and re-binds each key, a delete clears each mark and
    /// re-binds each key.
    pub(in crate::data::executor) fn undo_entry(
        self,
        collection_key: ColumnarEngineKey,
    ) -> UndoEntry {
        match self.closed_from {
            Some(row_count_before) => UndoEntry::ColumnarUpdate {
                collection_key,
                row_count_before,
                inserted_pks: Vec::new(),
                displaced: Vec::new(),
                restored: self.restored,
            },
            None => UndoEntry::ColumnarDelete {
                collection_key,
                restored: self.restored,
            },
        }
    }
}

/// The surrogate the segment sidecar records for the flushed row at `loc`.
/// Segment ids are 1-based: segment `n` is sidecar index `n - 1`.
pub(in crate::data::executor) fn flushed_row_surrogate(
    sidecars: &std::collections::HashMap<
        ColumnarEngineKey,
        nodedb_columnar::mutation::snapshot::FlushedSurrogateTable,
    >,
    key: &ColumnarEngineKey,
    loc: RowLocation,
) -> Option<nodedb_types::Surrogate> {
    let segment_index = usize::try_from(loc.segment_id.checked_sub(1)?).ok()?;
    sidecars
        .get(key)?
        .get(segment_index)?
        .get(loc.row_index as usize)
        .copied()
        .flatten()
}

impl CoreLoop {
    /// Apply `(old_pk, post_image)` rows through `MutationEngine::update`
    /// (delete-old-PK + insert-new-row). For a collection with geometry
    /// columns the old row's R-tree entries go and the post-image is
    /// indexed. When `undo_log` is `Some`, every in-memory change this
    /// makes is recorded on it.
    ///
    /// The statement applies whole or not at all. Every pre-image is read
    /// before the first row is mutated, so a pre-image that does not read
    /// changes nothing. A row the engine refuses reverses the rows this
    /// statement already changed and returns the engine's error. A reversal
    /// that fails returns `RollbackFailed`, which fail-stops the core.
    pub(in crate::data::executor) fn apply_columnar_update_rows(
        &mut self,
        task: &ExecutionTask,
        key: &ColumnarEngineKey,
        schema: &ColumnarSchema,
        rows: &[(Value, Vec<Value>)],
        system_from: Option<i64>,
        undo_log: Option<&mut Vec<UndoEntry>>,
    ) -> crate::Result<ColumnarUpdateOutcome> {
        // Every new version of a bitemporal collection takes the statement's
        // system time, raised above its key's bound row, before any row
        // changes.
        let stamped;
        let rows = if schema.is_bitemporal() {
            let mut owned = rows.to_vec();
            self.stamp_columnar_update_rows(key, schema, &mut owned, system_from)?;
            stamped = owned;
            stamped.as_slice()
        } else {
            rows
        };
        // A key-changing update of a bitemporal collection closes the old
        // key with a tombstone at the new version's stamp.
        let closings = if schema.is_bitemporal() {
            self.columnar_update_closings(key, schema, rows)?
        } else {
            vec![None; rows.len()]
        };
        let has_geometry = schema_has_geometry(schema);
        let collection = key.2.as_str();
        let row_count_before = self
            .columnar_engines
            .get(key)
            .map(|engine| engine.memtable().row_count())
            .ok_or_else(|| engine_missing(collection))?;
        let mut outcome = ColumnarUpdateOutcome {
            affected: 0,
            inserted_pks: Vec::new(),
            displaced: Vec::new(),
            restored: Vec::new(),
        };
        let mut spatial_removed: Vec<RemovedSpatialEntry> = Vec::new();
        let mut geometry_delta = GeometryIndexDelta::default();
        // Every pre-image is read before the first update unbinds a PK: it
        // names the R-tree entries the old row owns.
        let old_pks = rows.iter().map(|r| &r.0);
        let pre_images = self.read_geometry_pre_images(key, has_geometry, old_pks)?;
        // Every post-image geometry's SRID is checked before the first row
        // changes, so a mixed-SRID statement changes nothing.
        if has_geometry {
            let post_images: Vec<Value> = rows
                .iter()
                .map(|(_, new_row)| row_values_to_object(schema, new_row))
                .collect();
            self.check_columnar_geometry_srids(task, schema, collection, &post_images)?;
        }

        for (((old_pk, new_row), pre_image), closing) in rows.iter().zip(pre_images).zip(closings) {
            let old_pk_bytes = encode_pk(old_pk);
            let applied = match self.columnar_engines.get_mut(key) {
                Some(engine) => {
                    // Capture before mutating (the update removes the old PK
                    // binding and appends a new row): the tombstoned
                    // original's location, the appended replacement's PK,
                    // and, for a PK-changing update, the row its insert half
                    // displaces. That row is in the memtable or a flushed
                    // segment, and the undo puts back either one.
                    let old_location = engine.pk_index().get(&old_pk_bytes).copied();
                    let new_pk_bytes = engine.encode_pk_from_row(new_row).ok();
                    let displaced_entry = match &new_pk_bytes {
                        Some(nb) if *nb != old_pk_bytes => engine
                            .pk_index()
                            .get(nb)
                            .copied()
                            .map(|loc| (nb.clone(), loc)),
                        _ => None,
                    };
                    // A flushed row's surrogate lives in its segment's
                    // sidecar, which the engine does not hold. The
                    // replacement row keeps it.
                    let flushed_surrogate = old_location
                        .filter(|loc| loc.segment_id != engine.memtable_segment_id())
                        .and_then(|loc| {
                            flushed_row_surrogate(&self.columnar_flushed_surrogates, key, loc)
                        });
                    let updated = match &closing {
                        Some(tombstone) => {
                            engine.update_closing(old_pk, new_row, tombstone, flushed_surrogate)
                        }
                        None => engine.update(old_pk, new_row, flushed_surrogate),
                    };
                    updated
                        .map(|_| (old_location, new_pk_bytes, displaced_entry))
                        .map_err(crate::Error::from)
                }
                None => Err(engine_missing(collection)),
            };
            let (old_location, new_pk_bytes, displaced_entry) = match applied {
                Ok(capture) => capture,
                Err(error) => {
                    let mut undo = Vec::new();
                    push_removed_spatial_undo(&mut undo, spatial_removed);
                    Self::push_geometry_index_undo(&mut undo, geometry_delta);
                    undo.push(UndoEntry::ColumnarUpdate {
                        collection_key: key.clone(),
                        row_count_before,
                        inserted_pks: outcome.inserted_pks,
                        displaced: outcome.displaced,
                        restored: outcome.restored,
                    });
                    return Err(self.reverse_columnar_statement(key, undo, error));
                }
            };
            outcome.affected += 1;
            if let Some(nb) = new_pk_bytes {
                outcome.inserted_pks.push(nb);
            }
            if let Some(loc) = old_location {
                outcome.restored.push((old_pk_bytes, loc));
            }
            if let Some(d) = displaced_entry {
                outcome.displaced.push(d);
            }
            if has_geometry {
                if let Some(row) = &pre_image {
                    spatial_removed.extend(self.remove_columnar_row_spatial_entries(
                        key.0, key.1, collection, schema, row,
                    ));
                }
                let delta = match self.index_columnar_geometry_columns(
                    task,
                    schema,
                    collection,
                    &[row_values_to_object(schema, new_row)],
                ) {
                    Ok(delta) => delta,
                    Err(error) => {
                        let mut undo = Vec::new();
                        push_removed_spatial_undo(&mut undo, spatial_removed);
                        Self::push_geometry_index_undo(&mut undo, geometry_delta);
                        undo.push(UndoEntry::ColumnarUpdate {
                            collection_key: key.clone(),
                            row_count_before,
                            inserted_pks: outcome.inserted_pks,
                            displaced: outcome.displaced,
                            restored: outcome.restored,
                        });
                        return Err(self.reverse_columnar_statement(key, undo, error));
                    }
                };
                geometry_delta.removed.extend(delta.removed);
                geometry_delta.inserted.extend(delta.inserted);
            }
        }

        if let Some(log) = undo_log {
            push_removed_spatial_undo(log, spatial_removed);
            Self::push_geometry_index_undo(log, geometry_delta);
        }
        Ok(outcome)
    }

    /// Remove the keys `pks` binds, as `mode` says: `MutationEngine::close`
    /// or `MutationEngine::delete`. For a collection with geometry columns
    /// each row's R-tree entries go too. When `undo_log` is `Some`, every
    /// in-memory change this makes is recorded on it.
    ///
    /// The statement applies whole or not at all. Every pre-image is read
    /// before the first row is removed, so a pre-image that does not read
    /// changes nothing. A row the engine refuses restores the rows this
    /// statement already removed and returns the engine's error. A reversal
    /// that fails returns `RollbackFailed`, which fail-stops the core.
    pub(in crate::data::executor) fn apply_columnar_delete_pks(
        &mut self,
        key: &ColumnarEngineKey,
        schema: &ColumnarSchema,
        pks: &[Value],
        mode: ColumnarDeleteMode,
        undo_log: Option<&mut Vec<UndoEntry>>,
    ) -> crate::Result<ColumnarDeleteOutcome> {
        if let ColumnarDeleteMode::Close { system_from } = mode
            && schema.is_bitemporal()
        {
            return self.apply_columnar_close_pks(key, schema, pks, system_from, undo_log);
        }
        let has_geometry = schema_has_geometry(schema);
        let collection = key.2.as_str();
        let mut outcome = ColumnarDeleteOutcome {
            affected: 0,
            restored: Vec::new(),
            closed_from: None,
        };
        let mut spatial_removed: Vec<RemovedSpatialEntry> = Vec::new();
        let pre_images = self.read_geometry_pre_images(key, has_geometry, pks.iter())?;

        for (pk, pre_image) in pks.iter().zip(pre_images) {
            let pk_bytes = encode_pk(pk);
            let applied = match self.columnar_engines.get_mut(key) {
                Some(engine) => {
                    // Read the location before the delete removes the PK
                    // binding.
                    let location = engine.pk_index().get(&pk_bytes).copied();
                    engine
                        .delete(pk)
                        .map(|_| location)
                        .map_err(crate::Error::from)
                }
                None => Err(engine_missing(collection)),
            };
            let location = match applied {
                Ok(location) => location,
                Err(error) => {
                    let mut undo = Vec::new();
                    push_removed_spatial_undo(&mut undo, spatial_removed);
                    undo.push(outcome.undo_entry(key.clone()));
                    return Err(self.reverse_columnar_statement(key, undo, error));
                }
            };
            outcome.affected += 1;
            if let Some(loc) = location {
                outcome.restored.push((pk_bytes, loc));
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
        Ok(outcome)
    }

    /// The pre-image of each row `pks` binds, in order, for a collection
    /// with geometry columns. One `None` per PK when `has_geometry` is false,
    /// so the caller reads nothing it does not need.
    ///
    /// `Err` on the first pre-image that does not read: a corrupt row would
    /// otherwise keep its R-tree entries after it is gone.
    fn read_geometry_pre_images<'v>(
        &self,
        key: &ColumnarEngineKey,
        has_geometry: bool,
        pks: impl Iterator<Item = &'v Value>,
    ) -> crate::Result<Vec<Option<Vec<Value>>>> {
        pks.map(|pk| {
            if has_geometry {
                self.read_columnar_row_by_pk(key, &encode_pk(pk))
            } else {
                Ok(None)
            }
        })
        .collect()
    }

    /// Reverse the in-memory changes `undo` records for a statement that
    /// stopped at `error`, last change first. The answer is `error`, or
    /// `RollbackFailed` when a change does not reverse: the core's state is
    /// then unknown, and the core fail-stops when that error leaves it.
    pub(super) fn reverse_columnar_statement(
        &mut self,
        key: &ColumnarEngineKey,
        undo: Vec<UndoEntry>,
        error: crate::Error,
    ) -> crate::Error {
        let reversal = self.undo_memory_effects(key.0.as_u64(), key.1.as_u64(), undo);
        abort_error(error, reversal)
    }
}

/// The error for a columnar engine that is absent mid-statement.
pub(super) fn engine_missing(collection: &str) -> crate::Error {
    crate::Error::Internal {
        detail: format!("columnar engine not found for collection '{collection}'"),
    }
}

/// Record removed R-tree entries so a rollback re-inserts them. Shares
/// `CoreLoop::push_geometry_index_undo`'s removed-entry push with an empty
/// inserted half.
pub(super) fn push_removed_spatial_undo(
    undo_log: &mut Vec<UndoEntry>,
    removed: Vec<RemovedSpatialEntry>,
) {
    CoreLoop::push_geometry_index_undo(
        undo_log,
        GeometryIndexDelta {
            removed,
            inserted: Vec::new(),
        },
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::executor::core_loop::tests::{make_core_with_dir, make_default_task};
    use nodedb_types::columnar::{ColumnDef, ColumnType};
    use nodedb_types::{DatabaseId, Surrogate};

    fn schema() -> ColumnarSchema {
        ColumnarSchema {
            columns: vec![
                ColumnDef::required("id", ColumnType::Int64).with_primary_key(),
                ColumnDef::required("v", ColumnType::Int64),
            ],
            version: 1,
        }
    }

    #[test]
    fn an_update_of_a_flushed_row_keeps_the_surrogate_its_segment_records() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        let key: ColumnarEngineKey = (
            DatabaseId::DEFAULT,
            crate::types::TenantId::new(1),
            "m".to_string(),
        );
        let mut engine = nodedb_columnar::MutationEngine::new("m".to_string(), schema());
        engine
            .insert_with_surrogate(&[Value::Integer(1), Value::Integer(10)], Surrogate::new(42))
            .expect("insert");
        let segment_id = engine.next_segment_id();
        let sidecar = engine.memtable_surrogates().to_vec();
        let _drained = engine.memtable_mut().drain_optimized();
        engine.on_memtable_flushed(segment_id).expect("flush");
        core.columnar_engines.insert(key.clone(), engine);
        core.columnar_flushed_surrogates
            .insert(key.clone(), vec![sidecar]);

        let outcome = core
            .apply_columnar_update_rows(
                &make_default_task(),
                &key,
                &schema(),
                &[(
                    Value::Integer(1),
                    vec![Value::Integer(1), Value::Integer(99)],
                )],
                None,
                None,
            )
            .expect("apply");

        assert_eq!(outcome.affected, 1);
        let live: Vec<(Option<Surrogate>, Vec<Value>)> = core
            .columnar_engines
            .get(&key)
            .expect("engine")
            .scan_memtable_rows_with_surrogates()
            .collect::<Result<_, _>>()
            .expect("read");
        assert_eq!(
            live,
            vec![(
                Some(Surrogate::new(42)),
                vec![Value::Integer(1), Value::Integer(99)]
            )]
        );
    }

    fn two_row_key(core: &mut CoreLoop) -> ColumnarEngineKey {
        let key: ColumnarEngineKey = (
            DatabaseId::DEFAULT,
            crate::types::TenantId::new(1),
            "m".to_string(),
        );
        let mut engine = nodedb_columnar::MutationEngine::new("m".to_string(), schema());
        engine
            .insert(&[Value::Integer(1), Value::Integer(10)])
            .expect("insert");
        engine
            .insert(&[Value::Integer(2), Value::Integer(20)])
            .expect("insert");
        core.columnar_engines.insert(key.clone(), engine);
        key
    }

    fn live_rows(core: &CoreLoop, key: &ColumnarEngineKey) -> Vec<Vec<Value>> {
        core.columnar_engines
            .get(key)
            .expect("engine")
            .scan_memtable_rows()
            .collect::<Result<_, _>>()
            .expect("read")
    }

    fn bound_row(core: &CoreLoop, key: &ColumnarEngineKey, pk: i64) -> Option<RowLocation> {
        core.columnar_engines
            .get(key)
            .expect("engine")
            .pk_index()
            .get(&encode_pk(&Value::Integer(pk)))
            .copied()
    }

    #[test]
    fn an_update_refused_partway_reverses_the_rows_it_already_changed() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        let key = two_row_key(&mut core);

        // Row 2's post-image breaks NOT NULL, after row 1 is already updated.
        let result = core.apply_columnar_update_rows(
            &make_default_task(),
            &key,
            &schema(),
            &[
                (
                    Value::Integer(1),
                    vec![Value::Integer(1), Value::Integer(11)],
                ),
                (Value::Integer(2), vec![Value::Integer(2), Value::Null]),
            ],
            None,
            None,
        );

        match result {
            Err(crate::Error::RejectedConstraint { .. }) => {}
            Err(other) => panic!("expected the NOT NULL refusal, got {other:?}"),
            Ok(outcome) => panic!("expected a refusal, {} rows applied", outcome.affected),
        }
        assert_eq!(
            live_rows(&core, &key),
            vec![
                vec![Value::Integer(1), Value::Integer(10)],
                vec![Value::Integer(2), Value::Integer(20)],
            ]
        );
        assert_eq!(
            core.columnar_engines
                .get(&key)
                .expect("engine")
                .memtable()
                .row_count(),
            2
        );
        assert_eq!(bound_row(&core, &key, 1).map(|loc| loc.row_index), Some(0));
    }

    #[test]
    fn a_delete_refused_partway_restores_the_rows_it_already_removed() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        let key = two_row_key(&mut core);

        // PK 99 is unbound, after row 1 is already removed.
        let result = core.apply_columnar_delete_pks(
            &key,
            &schema(),
            &[Value::Integer(1), Value::Integer(99)],
            ColumnarDeleteMode::Close { system_from: None },
            None,
        );

        match result {
            Err(crate::Error::Storage { .. }) => {}
            Err(other) => panic!("expected the unbound-key refusal, got {other:?}"),
            Ok(outcome) => panic!("expected a refusal, {} rows applied", outcome.affected),
        }
        assert_eq!(
            live_rows(&core, &key),
            vec![
                vec![Value::Integer(1), Value::Integer(10)],
                vec![Value::Integer(2), Value::Integer(20)],
            ]
        );
        assert_eq!(bound_row(&core, &key, 1).map(|loc| loc.row_index), Some(0));
    }
}
