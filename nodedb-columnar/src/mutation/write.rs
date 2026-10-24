// SPDX-License-Identifier: Apache-2.0

//! Write-path mutations: insert, insert_if_absent, delete, close, update.

use nodedb_types::surrogate::Surrogate;
use nodedb_types::value::Value;

use crate::error::ColumnarError;
use crate::pk_index::{RowLocation, encode_pk};
use crate::wal_record::{ColumnarWalRecord, encode_row_for_wal};

use super::engine::{MutationEngine, MutationResult};

impl MutationEngine {
    /// Insert a row with upsert-on-duplicate semantics. Returns WAL
    /// records to persist.
    ///
    /// Validates schema. If the PK already exists, the prior row is
    /// tombstoned via the segment's delete bitmap (a single positional
    /// delete) after the new row is appended to the memtable. A row that
    /// fails validation returns an error and leaves the prior row live. The PK
    /// index is rebound to the new row location. This matches the
    /// ClickHouse / Iceberg "sparse PK + positional delete" model and
    /// keeps `SELECT WHERE pk = X` linearizable on one row without a
    /// read-time merge pass.
    ///
    /// Callers that want strict INSERT (error on duplicate) should check
    /// `pk_index().contains()` themselves before calling; callers that
    /// want `ON CONFLICT DO NOTHING` semantics should use
    /// [`Self::insert_if_absent`].
    pub fn insert(&mut self, values: &[Value]) -> Result<MutationResult, ColumnarError> {
        self.upsert_row(values, None)
    }

    /// Insert with a stable cross-engine surrogate identity.
    ///
    /// Identical to [`Self::insert`] but also records `surrogate` in the
    /// per-row side-table so scan prefilters can perform bitmap membership
    /// checks without a separate lookup pass.
    pub fn insert_with_surrogate(
        &mut self,
        values: &[Value],
        surrogate: Surrogate,
    ) -> Result<MutationResult, ColumnarError> {
        self.upsert_row(values, Some(surrogate))
    }

    /// Shared body of [`Self::insert`] and [`Self::insert_with_surrogate`].
    ///
    /// A non-bitemporal write tombstones the prior row for the PK. A
    /// bitemporal write keeps every version of a PK: the prior row stays
    /// visible to `AS OF` queries, and the PK index still rebinds so
    /// current-state reads see the latest version.
    fn upsert_row(
        &mut self,
        values: &[Value],
        surrogate: Option<Surrogate>,
    ) -> Result<MutationResult, ColumnarError> {
        let pk_bytes = self.extract_pk_bytes(values)?;
        let prior = if self.schema.is_bitemporal() {
            None
        } else {
            self.pk_index.get(&pk_bytes).copied()
        };
        let mut wal_records = Vec::with_capacity(2);
        self.commit_row(
            values,
            pk_bytes,
            surrogate,
            prior.as_slice(),
            &mut wal_records,
        )?;
        Ok(MutationResult { wal_records })
    }

    /// Append `values` as a memtable row bound to `pk_bytes`, then
    /// tombstone each row in `replaced`.
    ///
    /// Every fallible step runs before any state changes:
    /// `encode_row_for_wal` runs first, and `append_row` is all-or-nothing.
    /// An error leaves the engine and `wal_records` unchanged. Tombstone
    /// records go into `wal_records` before the insert record, so replay
    /// applies them in the same order.
    pub(super) fn commit_row(
        &mut self,
        values: &[Value],
        pk_bytes: Vec<u8>,
        surrogate: Option<Surrogate>,
        replaced: &[RowLocation],
        wal_records: &mut Vec<ColumnarWalRecord>,
    ) -> Result<(), ColumnarError> {
        let row_data = encode_row_for_wal(values)?;
        self.memtable.append_row(values)?;

        for prior in replaced {
            self.delete_bitmaps
                .entry(prior.segment_id)
                .or_default()
                .mark_deleted(prior.row_index);
            wal_records.push(ColumnarWalRecord::DeleteRows {
                collection: self.collection.clone(),
                segment_id: prior.segment_id,
                row_indices: vec![prior.row_index],
            });
        }
        wal_records.push(ColumnarWalRecord::InsertRow {
            collection: self.collection.clone(),
            row_data,
        });

        let location = RowLocation {
            segment_id: self.memtable_segment_id,
            row_index: self.memtable_row_counter,
        };
        self.pk_index.upsert(pk_bytes, location);
        self.memtable_surrogates.push(surrogate);
        self.memtable_row_counter += 1;
        Ok(())
    }

    /// `INSERT ... ON CONFLICT DO NOTHING` semantics: append only if the
    /// PK is absent; silently skip on duplicate.
    ///
    /// Returns `Ok(MutationResult { wal_records })` with an empty vector
    /// when the row was skipped, so callers that batch WAL appends can
    /// detect no-ops by checking `wal_records.is_empty()`.
    pub fn insert_if_absent(&mut self, values: &[Value]) -> Result<MutationResult, ColumnarError> {
        let pk_bytes = self.extract_pk_bytes(values)?;
        if self.pk_index.contains(&pk_bytes) {
            return Ok(MutationResult {
                wal_records: Vec::new(),
            });
        }
        let mut wal_records = Vec::with_capacity(1);
        self.commit_row(values, pk_bytes, None, &[], &mut wal_records)?;
        Ok(MutationResult { wal_records })
    }

    /// Look up the current row for a PK in the memtable, if present.
    ///
    /// Returns `None` if the PK is not in the index, or if the PK points
    /// to a flushed segment (callers needing cross-segment lookup must
    /// go through a segment reader separately). This is the fast path
    /// used by `ON CONFLICT DO UPDATE` to read the would-be-merged row
    /// when the duplicate hits the memtable — the common case under
    /// back-to-back inserts.
    ///
    /// `Err` when a cell of the bound row is corrupt: a corrupt row is not an
    /// absent row.
    pub fn lookup_memtable_row_by_pk(
        &self,
        pk_bytes: &[u8],
    ) -> Result<Option<Vec<Value>>, ColumnarError> {
        let Some(loc) = self.pk_index.get(pk_bytes).copied() else {
            return Ok(None);
        };
        if loc.segment_id != self.memtable_segment_id {
            return Ok(None);
        }
        self.memtable
            .get_row(loc.row_index as usize)
            .map_err(|e| self.memtable_read_fault(e))
    }

    /// Delete a row by PK value. Returns WAL record to persist.
    ///
    /// Looks up PK in the index to find the segment + row, then marks
    /// the row in the segment's delete bitmap.
    pub fn delete(&mut self, pk_value: &Value) -> Result<MutationResult, ColumnarError> {
        let pk_bytes = encode_pk(pk_value);

        let location = self
            .pk_index
            .get(&pk_bytes)
            .copied()
            .ok_or(ColumnarError::PrimaryKeyNotFound)?;
        Ok(self.delete_bound(pk_bytes, location))
    }

    /// Mark the row `pk_bytes` binds at `location` deleted and unbind it.
    fn delete_bound(&mut self, pk_bytes: Vec<u8>, location: RowLocation) -> MutationResult {
        let wal = ColumnarWalRecord::DeleteRows {
            collection: self.collection.clone(),
            segment_id: location.segment_id,
            row_indices: vec![location.row_index],
        };
        self.delete_bitmaps
            .entry(location.segment_id)
            .or_default()
            .mark_deleted(location.row_index);
        self.pk_index.remove(&pk_bytes);
        MutationResult {
            wal_records: vec![wal],
        }
    }

    /// Close a key of a bitemporal collection: append `tombstone`, the
    /// version that ends the key, and unbind the key. The version the close
    /// supersedes stays as history for `AS OF` reads. The tombstone is bound
    /// in the closed-key index, so a later write of the key can read its
    /// system time.
    ///
    /// The tombstone keeps the cross-engine surrogate of the row it closes,
    /// read as [`Self::update`] reads it. A collection that is not
    /// bitemporal keeps no history: the row is deleted as [`Self::delete`]
    /// deletes it, and `tombstone` is not written.
    ///
    /// `PrimaryKeyNotFound` when the tombstone's key is unbound. An invalid
    /// tombstone returns an error and leaves the key bound.
    pub fn close(
        &mut self,
        tombstone: &[Value],
        flushed_surrogate: Option<Surrogate>,
    ) -> Result<MutationResult, ColumnarError> {
        let pk_bytes = self.extract_pk_bytes(tombstone)?;
        let location = self
            .pk_index
            .get(&pk_bytes)
            .copied()
            .ok_or(ColumnarError::PrimaryKeyNotFound)?;
        if !self.schema.is_bitemporal() {
            return Ok(self.delete_bound(pk_bytes, location));
        }
        let surrogate = if location.segment_id == self.memtable_segment_id {
            self.memtable_surrogates
                .get(location.row_index as usize)
                .copied()
                .flatten()
        } else {
            flushed_surrogate
        };
        let mut wal_records = Vec::with_capacity(1);
        self.commit_row(
            tombstone,
            pk_bytes.clone(),
            surrogate,
            &[],
            &mut wal_records,
        )?;
        if let Some(tombstone_location) = self.pk_index.remove(&pk_bytes) {
            self.closed_index.upsert(pk_bytes, tombstone_location);
        }
        Ok(MutationResult { wal_records })
    }

    /// Update a row by PK: DELETE old + INSERT new.
    ///
    /// `updates` maps column names to new values. Columns not in the map
    /// retain their existing values from the old row.
    ///
    /// Returns WAL records for both the delete and the insert. The old row
    /// is tombstoned only after the new row is appended, so an invalid new
    /// row returns an error and leaves the old row live. A bitemporal
    /// collection tombstones nothing: the old row stays as the version the
    /// update superseded, visible to `AS OF` reads, and the PK index binds
    /// the new row.
    ///
    /// NOTE: The caller must provide the full old row values for the re-insert.
    /// This method takes the complete new row (already merged with old values).
    ///
    /// The new row keeps the cross-engine surrogate the old row carried. A
    /// memtable row's surrogate is in this engine's side table. A flushed
    /// row's surrogate is in its segment's sidecar, outside this engine, so
    /// the caller passes it as `flushed_surrogate`. It is read only when the
    /// old row is flushed.
    pub fn update(
        &mut self,
        old_pk: &Value,
        new_values: &[Value],
        flushed_surrogate: Option<Surrogate>,
    ) -> Result<MutationResult, ColumnarError> {
        self.update_row(old_pk, new_values, None, flushed_surrogate)
    }

    /// [`Self::update`] for a key-changing update of a bitemporal
    /// collection: the old key closes with `tombstone` as [`Self::close`]
    /// closes a key, so a current read finds no row under it. The tombstone
    /// is appended before the new row. When the new row is refused, the
    /// tombstone is cut and the old key is bound again, so an error leaves
    /// the engine unchanged.
    ///
    /// An update that keeps its key, or a collection that is not
    /// bitemporal, writes no tombstone and runs as [`Self::update`].
    pub fn update_closing(
        &mut self,
        old_pk: &Value,
        new_values: &[Value],
        tombstone: &[Value],
        flushed_surrogate: Option<Surrogate>,
    ) -> Result<MutationResult, ColumnarError> {
        self.update_row(old_pk, new_values, Some(tombstone), flushed_surrogate)
    }

    /// Shared body of [`Self::update`] and [`Self::update_closing`].
    fn update_row(
        &mut self,
        old_pk: &Value,
        new_values: &[Value],
        tombstone: Option<&[Value]>,
        flushed_surrogate: Option<Surrogate>,
    ) -> Result<MutationResult, ColumnarError> {
        let old_pk_bytes = encode_pk(old_pk);
        let old_location = self
            .pk_index
            .get(&old_pk_bytes)
            .copied()
            .ok_or(ColumnarError::PrimaryKeyNotFound)?;
        let surrogate = if old_location.segment_id == self.memtable_segment_id {
            self.memtable_surrogates
                .get(old_location.row_index as usize)
                .copied()
                .flatten()
        } else {
            flushed_surrogate
        };

        let new_pk_bytes = self.extract_pk_bytes(new_values)?;
        let pk_changed = new_pk_bytes != old_pk_bytes;

        // A non-bitemporal old row is tombstoned. When the PK changes, a
        // non-bitemporal row already bound to the new PK is replaced too,
        // as an upsert replaces it. A bitemporal collection keeps both as
        // history.
        let mut replaced = Vec::with_capacity(2);
        if !self.schema.is_bitemporal() {
            replaced.push(old_location);
        }
        if pk_changed
            && !self.schema.is_bitemporal()
            && let Some(prior) = self.pk_index.get(&new_pk_bytes).copied()
        {
            replaced.push(prior);
        }

        let mut wal_records = Vec::with_capacity(replaced.len() + 2);
        let closing = tombstone.filter(|_| pk_changed && self.schema.is_bitemporal());
        if let Some(tombstone) = closing {
            let row_count_before = self.memtable.row_count();
            self.commit_row(
                tombstone,
                old_pk_bytes.clone(),
                surrogate,
                &[],
                &mut wal_records,
            )?;
            if let Err(error) = self.commit_row(
                new_values,
                new_pk_bytes,
                surrogate,
                &replaced,
                &mut wal_records,
            ) {
                self.rollback_memtable_inserts(
                    row_count_before,
                    &[],
                    &[(old_pk_bytes, old_location)],
                );
                return Err(error);
            }
            if let Some(tombstone_location) = self.pk_index.remove(&old_pk_bytes) {
                self.closed_index.upsert(old_pk_bytes, tombstone_location);
            }
            return Ok(MutationResult { wal_records });
        }
        self.commit_row(
            new_values,
            new_pk_bytes,
            surrogate,
            &replaced,
            &mut wal_records,
        )?;
        if pk_changed {
            self.pk_index.remove(&old_pk_bytes);
        }

        Ok(MutationResult { wal_records })
    }
}

#[cfg(test)]
mod tests {
    use nodedb_types::columnar::{ColumnDef, ColumnType, ColumnarSchema};

    use super::*;

    fn schema() -> ColumnarSchema {
        ColumnarSchema::new(vec![
            ColumnDef::required("id", ColumnType::Int64).with_primary_key(),
            ColumnDef::required("name", ColumnType::String),
            ColumnDef::nullable("score", ColumnType::Float64),
        ])
        .expect("valid")
    }

    fn row(id: i64, name: &str, score: f64) -> Vec<Value> {
        vec![
            Value::Integer(id),
            Value::String(name.into()),
            Value::Float(score),
        ]
    }

    fn live_rows(engine: &MutationEngine) -> Vec<Vec<Value>> {
        engine
            .scan_memtable_rows()
            .collect::<Result<_, _>>()
            .expect("read")
    }

    #[test]
    fn rejected_upsert_keeps_prior_row() {
        let mut engine = MutationEngine::new("t".into(), schema());
        engine.insert(&row(1, "a", 0.5)).expect("insert");

        let err = engine
            .insert(&[Value::Integer(1), Value::Null, Value::Null])
            .unwrap_err();
        assert!(matches!(err, ColumnarError::NullViolation(_)));

        assert_eq!(live_rows(&engine), vec![row(1, "a", 0.5)]);
        assert_eq!(engine.memtable().row_count(), 1);
        let loc = engine
            .pk_index()
            .get(&encode_pk(&Value::Integer(1)))
            .copied();
        assert_eq!(loc.map(|l| l.row_index), Some(0));
        assert!(
            engine
                .delete_bitmap(engine.memtable_segment_id())
                .is_none_or(|bm| !bm.is_deleted(0))
        );
    }

    #[test]
    fn rejected_upsert_with_surrogate_keeps_prior_row() {
        let mut engine = MutationEngine::new("t".into(), schema());
        engine
            .insert_with_surrogate(&row(1, "a", 0.5), Surrogate(7))
            .expect("insert");

        let err = engine
            .insert_with_surrogate(
                &[
                    Value::Integer(1),
                    Value::String("b".into()),
                    Value::Bool(true),
                ],
                Surrogate(8),
            )
            .unwrap_err();
        assert!(matches!(err, ColumnarError::TypeMismatch { .. }));

        assert_eq!(live_rows(&engine), vec![row(1, "a", 0.5)]);
        assert_eq!(engine.memtable_surrogates(), &[Some(Surrogate(7))]);
    }

    #[test]
    fn rejected_insert_if_absent_leaves_engine_unchanged() {
        let mut engine = MutationEngine::new("t".into(), schema());
        let err = engine
            .insert_if_absent(&[Value::Integer(1), Value::Null, Value::Null])
            .unwrap_err();
        assert!(matches!(err, ColumnarError::NullViolation(_)));
        assert!(engine.pk_index().is_empty());
        assert_eq!(engine.memtable().row_count(), 0);

        engine
            .insert_if_absent(&row(1, "a", 0.5))
            .expect("insert after rejected row");
        assert_eq!(live_rows(&engine), vec![row(1, "a", 0.5)]);
    }

    #[test]
    fn rejected_update_keeps_old_row() {
        let mut engine = MutationEngine::new("t".into(), schema());
        engine.insert(&row(1, "a", 0.5)).expect("insert");

        let err = engine
            .update(
                &Value::Integer(1),
                &[Value::Integer(1), Value::Null, Value::Null],
                None,
            )
            .unwrap_err();
        assert!(matches!(err, ColumnarError::NullViolation(_)));

        assert_eq!(live_rows(&engine), vec![row(1, "a", 0.5)]);
        assert!(engine.pk_index().contains(&encode_pk(&Value::Integer(1))));
    }

    #[test]
    fn update_to_new_pk_rebinds_index() {
        let mut engine = MutationEngine::new("t".into(), schema());
        engine.insert(&row(1, "a", 0.5)).expect("insert");

        let result = engine
            .update(&Value::Integer(1), &row(2, "b", 0.25), None)
            .expect("update");
        assert_eq!(result.wal_records.len(), 2);

        assert_eq!(live_rows(&engine), vec![row(2, "b", 0.25)]);
        assert!(!engine.pk_index().contains(&encode_pk(&Value::Integer(1))));
        assert!(engine.pk_index().contains(&encode_pk(&Value::Integer(2))));
    }

    /// A bitemporal update keeps the version it supersedes: no tombstone, no
    /// delete record, and the PK index binds the new version.
    #[test]
    fn a_bitemporal_update_keeps_the_superseded_version() {
        let schema = ColumnarSchema::new(vec![
            ColumnDef::required("_ts_system", ColumnType::Int64),
            ColumnDef::required("id", ColumnType::Int64).with_primary_key(),
            ColumnDef::required("v", ColumnType::Int64),
        ])
        .expect("valid");
        let mut engine = MutationEngine::new("t".into(), schema);
        let version =
            |ts: i64, v: i64| vec![Value::Integer(ts), Value::Integer(1), Value::Integer(v)];
        engine.insert(&version(100, 10)).expect("insert");

        let result = engine
            .update(&Value::Integer(1), &version(200, 20), None)
            .expect("update");
        assert_eq!(
            result.wal_records.len(),
            1,
            "an insert record, no delete record"
        );

        assert_eq!(live_rows(&engine), vec![version(100, 10), version(200, 20)]);
        let bound = engine
            .pk_index()
            .get(&encode_pk(&Value::Integer(1)))
            .copied()
            .expect("bound");
        assert_eq!(bound.row_index, 1, "the PK binds the new version");
    }

    /// A bitemporal close appends the tombstone, keeps the closed version,
    /// unbinds the key and binds the tombstone in the closed-key index. A
    /// flush carries the closed-key entry to the new segment.
    #[test]
    fn a_bitemporal_close_keeps_the_closed_version() {
        let schema = ColumnarSchema::new(vec![
            ColumnDef::required("_ts_system", ColumnType::Int64),
            ColumnDef::required("id", ColumnType::Int64).with_primary_key(),
            ColumnDef::required("v", ColumnType::Int64),
        ])
        .expect("valid");
        let mut engine = MutationEngine::new("t".into(), schema);
        let version =
            |ts: i64, v: i64| vec![Value::Integer(ts), Value::Integer(1), Value::Integer(v)];
        engine.insert(&version(100, 10)).expect("insert");

        let result = engine.close(&version(200, 10), None).expect("close");
        assert_eq!(
            result.wal_records.len(),
            1,
            "an insert record, no delete record"
        );

        assert_eq!(live_rows(&engine), vec![version(100, 10), version(200, 10)]);
        let pk = encode_pk(&Value::Integer(1));
        assert!(
            !engine.pk_index().contains(&pk),
            "the closed key is unbound"
        );
        assert_eq!(engine.closed_location(&pk).map(|l| l.row_index), Some(1));
        assert!(matches!(
            engine.close(&version(300, 10), None),
            Err(ColumnarError::PrimaryKeyNotFound)
        ));

        let segment_id = engine.next_segment_id();
        engine.on_memtable_flushed(segment_id).expect("flush");
        assert_eq!(
            engine.closed_location(&pk),
            Some(RowLocation {
                segment_id,
                row_index: 1
            })
        );
    }

    /// A key-changing bitemporal update closes the old key with its
    /// tombstone, keeps the old version, and binds the new key. A refused
    /// new row leaves the engine unchanged.
    #[test]
    fn a_key_changing_bitemporal_update_closes_the_old_key() {
        let schema = ColumnarSchema::new(vec![
            ColumnDef::required("_ts_system", ColumnType::Int64),
            ColumnDef::required("id", ColumnType::Int64).with_primary_key(),
            ColumnDef::required("v", ColumnType::Int64),
        ])
        .expect("valid");
        let mut engine = MutationEngine::new("t".into(), schema);
        let row = |ts: i64, id: i64, v: i64| {
            vec![Value::Integer(ts), Value::Integer(id), Value::Integer(v)]
        };
        engine.insert(&row(100, 1, 10)).expect("insert");

        let refused = engine.update_closing(
            &Value::Integer(1),
            &[Value::Integer(200), Value::Integer(2), Value::Null],
            &row(200, 1, 10),
            None,
        );
        assert!(refused.is_err());
        assert_eq!(live_rows(&engine), vec![row(100, 1, 10)]);
        let old = encode_pk(&Value::Integer(1));
        assert_eq!(engine.pk_index().get(&old).map(|l| l.row_index), Some(0));
        assert_eq!(engine.closed_location(&old), None);

        engine
            .update_closing(&Value::Integer(1), &row(200, 2, 10), &row(200, 1, 10), None)
            .expect("update");
        assert_eq!(
            live_rows(&engine),
            vec![row(100, 1, 10), row(200, 1, 10), row(200, 2, 10)]
        );
        assert!(!engine.pk_index().contains(&old));
        assert_eq!(engine.closed_location(&old).map(|l| l.row_index), Some(1));
        let new = encode_pk(&Value::Integer(2));
        assert_eq!(engine.pk_index().get(&new).map(|l| l.row_index), Some(2));
    }

    /// A close on a collection that is not bitemporal deletes the row.
    #[test]
    fn a_plain_close_deletes_the_row() {
        let mut engine = MutationEngine::new("t".into(), schema());
        engine.insert(&row(1, "a", 0.5)).expect("insert");
        engine.close(&row(1, "a", 0.5), None).expect("close");
        assert!(live_rows(&engine).is_empty());
        assert!(engine.pk_index().is_empty());
        assert_eq!(engine.closed_location(&encode_pk(&Value::Integer(1))), None);
    }
}
