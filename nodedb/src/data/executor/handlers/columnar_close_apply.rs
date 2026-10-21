// SPDX-License-Identifier: BUSL-1.1

//! The close of a bitemporal columnar key: a DELETE appends a tombstone
//! version, a copy of the bound row with an empty valid interval, and keeps
//! the closed version as history for `AS OF` reads.

use nodedb_columnar::pk_index::encode_pk;
use nodedb_types::Value;
use nodedb_types::columnar::ColumnarSchema;
use nodedb_types::columnar::schema::{TS_VALID_FROM, TS_VALID_UNTIL};

use super::columnar_mutation_apply::{
    ColumnarDeleteOutcome, ColumnarEngineKey, engine_missing, flushed_row_surrogate,
    push_removed_spatial_undo,
};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::columnar_write::{RemovedSpatialEntry, schema_has_geometry};
use crate::data::executor::handlers::transaction::undo::UndoEntry;

/// The tombstone that closes `bound`, the row a key is bound to: a copy of
/// it with an empty valid interval. Its `_ts_system` cell is the caller's to
/// stamp.
///
/// `Err` when the schema has no valid-time columns.
pub(in crate::data::executor) fn tombstone_of(
    schema: &ColumnarSchema,
    collection: &str,
    bound: &[Value],
) -> crate::Result<Vec<Value>> {
    let required = |idx: Option<usize>, name: &str| {
        idx.ok_or_else(|| crate::Error::Internal {
            detail: format!("bitemporal columnar '{collection}' has no {name} column"),
        })
    };
    let valid_from = required(schema.ts_valid_from_idx(), TS_VALID_FROM)?;
    let valid_until = required(schema.ts_valid_until_idx(), TS_VALID_UNTIL)?;
    let mut tombstone = bound.to_vec();
    for idx in [valid_from, valid_until] {
        if let Some(cell) = tombstone.get_mut(idx) {
            *cell = Value::Integer(i64::MIN);
        }
    }
    Ok(tombstone)
}

/// The keys a close removes, each with its tombstone, and the pre-image
/// the geometry cascade reads.
struct Tombstones {
    rows: Vec<(Value, Vec<Value>)>,
    pre_images: Vec<Option<Vec<Value>>>,
}

impl CoreLoop {
    /// Close each key `pks` binds in a bitemporal collection: append a
    /// tombstone version (see [`tombstone_of`]) and unbind the key. The
    /// closed version stays as history. Every tombstone is stamped before
    /// the first key closes, as `ColumnarDeleteMode::Close` says. The R-tree
    /// cascade and the all-or-nothing contract are those of
    /// [`CoreLoop::apply_columnar_delete_pks`].
    ///
    /// `Err` (`PrimaryKeyNotFound`) before any change when a key is unbound.
    pub(super) fn apply_columnar_close_pks(
        &mut self,
        key: &ColumnarEngineKey,
        schema: &ColumnarSchema,
        pks: &[Value],
        system_from: Option<i64>,
        undo_log: Option<&mut Vec<UndoEntry>>,
    ) -> crate::Result<ColumnarDeleteOutcome> {
        let mut tombstones = self.columnar_tombstones(key, schema, pks.iter())?;
        self.stamp_columnar_update_rows(key, schema, &mut tombstones.rows, system_from)?;
        self.install_columnar_tombstones(key, schema, tombstones, undo_log)
    }

    /// Close each key of `closes` at the system-time ordinal it carries, as
    /// [`Self::apply_columnar_close_pks`] closes a key. The ordinals are
    /// installed as given and fold into the clock. A transaction's row-image
    /// install closes its keys this way, at the stamps its resolve fixed.
    ///
    /// `Err` (`PrimaryKeyNotFound`) before any change when a key is unbound.
    pub(in crate::data::executor) fn apply_columnar_close_pks_at(
        &mut self,
        key: &ColumnarEngineKey,
        schema: &ColumnarSchema,
        closes: &[(Value, i64)],
        undo_log: Option<&mut Vec<UndoEntry>>,
    ) -> crate::Result<ColumnarDeleteOutcome> {
        let mut tombstones = self.columnar_tombstones(key, schema, closes.iter().map(|c| &c.0))?;
        if let Some(ts_idx) = schema.ts_system_idx() {
            for ((_, tombstone), (_, stamp)) in tombstones.rows.iter_mut().zip(closes) {
                if let Some(cell) = tombstone.get_mut(ts_idx) {
                    *cell = Value::Integer(*stamp);
                }
                self.observe_bitemporal_ordinal(*stamp);
            }
        }
        self.install_columnar_tombstones(key, schema, tombstones, undo_log)
    }

    /// For each `(old primary key, post-image)` row of a bitemporal UPDATE,
    /// the tombstone that closes the old key when the post-image changes the
    /// key: the old row's [`tombstone_of`], stamped with the post-image's
    /// `_ts_system`. `None` for a row that keeps its key. Every old row is
    /// read before any row changes.
    ///
    /// `Err` (`PrimaryKeyNotFound`) when an old key is unbound.
    pub(super) fn columnar_update_closings(
        &self,
        key: &ColumnarEngineKey,
        schema: &ColumnarSchema,
        rows: &[(Value, Vec<Value>)],
    ) -> crate::Result<Vec<Option<Vec<Value>>>> {
        let collection = key.2.as_str();
        let engine = self
            .columnar_engines
            .get(key)
            .ok_or_else(|| engine_missing(collection))?;
        let ts_idx = schema.ts_system_idx();
        rows.iter()
            .map(|(old_pk, new_row)| {
                let old_bytes = encode_pk(old_pk);
                if engine.encode_pk_from_row(new_row)? == old_bytes {
                    return Ok(None);
                }
                let bound = self
                    .read_columnar_row_by_pk(key, &old_bytes)?
                    .ok_or(nodedb_columnar::ColumnarError::PrimaryKeyNotFound)?;
                let mut tombstone = tombstone_of(schema, collection, &bound)?;
                if let Some(idx) = ts_idx
                    && let (Some(cell), Some(stamp)) = (tombstone.get_mut(idx), new_row.get(idx))
                {
                    *cell = stamp.clone();
                }
                Ok(Some(tombstone))
            })
            .collect()
    }

    /// The tombstone of the row each of `pks` binds, read before any key
    /// closes.
    ///
    /// `Err` (`PrimaryKeyNotFound`) when a key is unbound.
    fn columnar_tombstones<'v>(
        &self,
        key: &ColumnarEngineKey,
        schema: &ColumnarSchema,
        pks: impl Iterator<Item = &'v Value>,
    ) -> crate::Result<Tombstones> {
        let has_geometry = schema_has_geometry(schema);
        let collection = key.2.as_str();
        let mut out = Tombstones {
            rows: Vec::new(),
            pre_images: Vec::new(),
        };
        for pk in pks {
            let bound = self
                .read_columnar_row_by_pk(key, &encode_pk(pk))?
                .ok_or(nodedb_columnar::ColumnarError::PrimaryKeyNotFound)?;
            out.rows
                .push((pk.clone(), tombstone_of(schema, collection, &bound)?));
            out.pre_images.push(has_geometry.then_some(bound));
        }
        Ok(out)
    }

    /// Close each key of `tombstones` through `MutationEngine::close`, with
    /// the R-tree cascade and the all-or-nothing contract of
    /// [`CoreLoop::apply_columnar_delete_pks`].
    fn install_columnar_tombstones(
        &mut self,
        key: &ColumnarEngineKey,
        schema: &ColumnarSchema,
        tombstones: Tombstones,
        undo_log: Option<&mut Vec<UndoEntry>>,
    ) -> crate::Result<ColumnarDeleteOutcome> {
        let collection = key.2.as_str();
        let row_count_before = self
            .columnar_engines
            .get(key)
            .map(|engine| engine.memtable().row_count())
            .ok_or_else(|| engine_missing(collection))?;
        let mut outcome = ColumnarDeleteOutcome {
            affected: 0,
            restored: Vec::new(),
            closed_from: Some(row_count_before),
        };
        let mut spatial_removed: Vec<RemovedSpatialEntry> = Vec::new();
        for ((pk, tombstone), pre_image) in tombstones.rows.iter().zip(tombstones.pre_images) {
            let pk_bytes = encode_pk(pk);
            let applied = match self.columnar_engines.get_mut(key) {
                Some(engine) => {
                    // Read the location before the close unbinds the key.
                    let location = engine.pk_index().get(&pk_bytes).copied();
                    let flushed_surrogate = location
                        .filter(|loc| loc.segment_id != engine.memtable_segment_id())
                        .and_then(|loc| {
                            flushed_row_surrogate(&self.columnar_flushed_surrogates, key, loc)
                        });
                    engine
                        .close(tombstone, flushed_surrogate)
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
}

#[cfg(test)]
mod tests {
    use nodedb_types::columnar::{ColumnDef, ColumnType};
    use nodedb_types::{DatabaseId, TenantId};

    use super::*;
    use crate::data::executor::core_loop::tests::{make_core_with_dir, make_default_task};
    use crate::data::executor::handlers::columnar_mutation_apply::ColumnarDeleteMode;

    fn schema() -> ColumnarSchema {
        ColumnarSchema::new(vec![
            ColumnDef::required("_ts_system", ColumnType::Int64),
            ColumnDef::required("_ts_valid_from", ColumnType::Int64),
            ColumnDef::required("_ts_valid_until", ColumnType::Int64),
            ColumnDef::required("id", ColumnType::Int64).with_primary_key(),
            ColumnDef::required("v", ColumnType::Int64),
        ])
        .expect("valid")
    }

    fn version(ts: i64, id: i64, v: i64) -> Vec<Value> {
        vec![
            Value::Integer(ts),
            Value::Integer(0),
            Value::Integer(i64::MAX),
            Value::Integer(id),
            Value::Integer(v),
        ]
    }

    /// A close appends a tombstone with an empty valid interval, keeps the
    /// closed version and unbinds the key. Its undo entry cuts the
    /// tombstone, drops the closed-key entry and re-binds the key.
    #[test]
    fn a_close_appends_a_tombstone_and_its_undo_reverses_it() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        let key: ColumnarEngineKey = (DatabaseId::DEFAULT, TenantId::new(1), "bt".to_string());
        let mut engine = nodedb_columnar::MutationEngine::new("bt".to_string(), schema());
        engine.insert(&version(100, 1, 10)).expect("insert");
        core.columnar_engines.insert(key.clone(), engine);

        let mut undo = Vec::new();
        let outcome = core
            .apply_columnar_delete_pks(
                &key,
                &schema(),
                &[Value::Integer(1)],
                ColumnarDeleteMode::Close {
                    system_from: Some(50),
                },
                Some(&mut undo),
            )
            .expect("close");
        assert_eq!(outcome.affected, 1);
        assert_eq!(outcome.closed_from, Some(1));

        let pk = encode_pk(&Value::Integer(1));
        let engine = core.columnar_engines.get(&key).expect("engine");
        let rows: Vec<Vec<Value>> = engine
            .scan_memtable_rows()
            .collect::<Result<_, _>>()
            .expect("read");
        let tombstone = vec![
            Value::Integer(101),
            Value::Integer(i64::MIN),
            Value::Integer(i64::MIN),
            Value::Integer(1),
            Value::Integer(10),
        ];
        assert_eq!(
            rows,
            vec![version(100, 1, 10), tombstone],
            "the tombstone rises above the version it closes"
        );
        assert!(!engine.pk_index().contains(&pk));
        assert!(engine.closed_location(&pk).is_some());

        undo.push(outcome.undo_entry(key.clone()));
        core.undo_memory_effects(key.0.as_u64(), key.1.as_u64(), undo)
            .expect("undo");
        let engine = core.columnar_engines.get(&key).expect("engine");
        assert_eq!(engine.memtable().row_count(), 1);
        assert_eq!(engine.pk_index().get(&pk).map(|loc| loc.row_index), Some(0));
        assert_eq!(engine.closed_location(&pk), None);
    }

    /// A key-changing update of a bitemporal key closes the old key with a
    /// tombstone at the new version's stamp and keeps the old version.
    #[test]
    fn a_key_changing_update_closes_the_old_key() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        let key: ColumnarEngineKey = (DatabaseId::DEFAULT, TenantId::new(1), "bt".to_string());
        let mut engine = nodedb_columnar::MutationEngine::new("bt".to_string(), schema());
        engine.insert(&version(100, 1, 10)).expect("insert");
        core.columnar_engines.insert(key.clone(), engine);

        core.apply_columnar_update_rows(
            &make_default_task(),
            &key,
            &schema(),
            &[(Value::Integer(1), version(0, 2, 10))],
            Some(50),
            None,
        )
        .expect("update");

        let engine = core.columnar_engines.get(&key).expect("engine");
        let rows: Vec<Vec<Value>> = engine
            .scan_memtable_rows()
            .collect::<Result<_, _>>()
            .expect("read");
        let tombstone = vec![
            Value::Integer(101),
            Value::Integer(i64::MIN),
            Value::Integer(i64::MIN),
            Value::Integer(1),
            Value::Integer(10),
        ];
        assert_eq!(
            rows,
            vec![version(100, 1, 10), tombstone, version(101, 2, 10)]
        );
        let old = encode_pk(&Value::Integer(1));
        assert!(!engine.pk_index().contains(&old));
        assert!(engine.closed_location(&old).is_some());
        assert!(engine.pk_index().contains(&encode_pk(&Value::Integer(2))));
    }
}
