// SPDX-License-Identifier: Apache-2.0

//! Write-path mutations: insert, insert_if_absent, delete, close, update.
//!
//! Every write takes `stamp`, the system-time ordinal of the statement. A new
//! row's version starts at `sys_from` ([`MutationEngine::version_stamp`]).
//! A row a write overwrites or deletes ends at the write's ordinal: it is
//! marked in the delete bitmap, records `sys_to`, and joins its key's chain.
//! It stays as history until a purge removes it.

use nodedb_types::surrogate::Surrogate;
use nodedb_types::value::Value;

use crate::error::ColumnarError;
use crate::pk_index::{RowLocation, encode_pk};
use crate::wal_record::{ColumnarWalRecord, encode_row_for_wal};

use super::engine::{MutationEngine, MutationResult};

/// A row a write ends, with the primary key whose chain it joins.
pub(super) type Replaced = (Vec<u8>, RowLocation);

impl MutationEngine {
    /// Insert a row with upsert-on-duplicate semantics at system-time
    /// ordinal `stamp`. Returns WAL records to persist.
    ///
    /// Validates schema. If the PK already exists, the prior row ends at
    /// the new row's `sys_from` after the new row is appended to the
    /// memtable. A row that fails validation returns an error and leaves
    /// the prior row open. The PK index is rebound to the new row location,
    /// so `SELECT WHERE pk = X` reads one row without a read-time merge.
    ///
    /// Callers that want strict INSERT (error on duplicate) should check
    /// `pk_index().contains()` themselves before calling; callers that
    /// want `ON CONFLICT DO NOTHING` semantics should use
    /// [`Self::insert_if_absent`].
    pub fn insert(
        &mut self,
        values: &[Value],
        stamp: i64,
    ) -> Result<MutationResult, ColumnarError> {
        self.upsert_row(values, None, stamp)
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
        stamp: i64,
    ) -> Result<MutationResult, ColumnarError> {
        self.upsert_row(values, Some(surrogate), stamp)
    }

    /// Shared body of [`Self::insert`] and [`Self::insert_with_surrogate`].
    fn upsert_row(
        &mut self,
        values: &[Value],
        surrogate: Option<Surrogate>,
        stamp: i64,
    ) -> Result<MutationResult, ColumnarError> {
        let pk_bytes = self.extract_pk_bytes(values)?;
        let sys_from = self.version_stamp(values, stamp, &[pk_bytes.as_slice()]);
        let prior = self
            .pk_index
            .get(&pk_bytes)
            .copied()
            .map(|loc| (pk_bytes.clone(), loc));
        let mut wal_records = Vec::with_capacity(2);
        self.commit_row(
            values,
            pk_bytes,
            surrogate,
            prior.as_slice(),
            sys_from,
            &mut wal_records,
        )?;
        Ok(MutationResult { wal_records })
    }

    /// Append `values` as a memtable row bound to `pk_bytes` and stamped
    /// `sys_from`, then end each row in `replaced` at `sys_from`.
    ///
    /// Every fallible step runs before any state changes:
    /// `encode_row_for_wal` runs first, and `append_row` is all-or-nothing.
    /// An error leaves the engine and `wal_records` unchanged. End records
    /// go into `wal_records` before the insert record, so replay applies
    /// them in the same order. Returns the new row's location.
    pub(super) fn commit_row(
        &mut self,
        values: &[Value],
        pk_bytes: Vec<u8>,
        surrogate: Option<Surrogate>,
        replaced: &[Replaced],
        sys_from: i64,
        wal_records: &mut Vec<ColumnarWalRecord>,
    ) -> Result<RowLocation, ColumnarError> {
        let row_data = encode_row_for_wal(values)?;
        self.memtable.append_row(values)?;
        self.stamp_memtable_row(sys_from);

        for (prior_pk, prior) in replaced {
            let record = self.end_row(prior_pk.clone(), *prior, sys_from);
            wal_records.push(record);
        }
        wal_records.push(ColumnarWalRecord::InsertRow {
            collection: self.collection.clone(),
            row_data,
            sys_from,
        });

        let location = RowLocation {
            segment_id: self.memtable_segment_id,
            row_index: self.memtable_row_counter,
        };
        self.pk_index.upsert(pk_bytes, location);
        self.memtable_surrogates.push(surrogate);
        self.memtable_row_counter += 1;
        Ok(location)
    }

    /// `INSERT ... ON CONFLICT DO NOTHING` semantics: append only if the
    /// PK is absent; silently skip on duplicate.
    ///
    /// Returns `Ok(MutationResult { wal_records })` with an empty vector
    /// when the row was skipped, so callers that batch WAL appends can
    /// detect no-ops by checking `wal_records.is_empty()`.
    pub fn insert_if_absent(
        &mut self,
        values: &[Value],
        stamp: i64,
    ) -> Result<MutationResult, ColumnarError> {
        let pk_bytes = self.extract_pk_bytes(values)?;
        if self.pk_index.contains(&pk_bytes) {
            return Ok(MutationResult {
                wal_records: Vec::new(),
            });
        }
        let sys_from = self.version_stamp(values, stamp, &[pk_bytes.as_slice()]);
        let mut wal_records = Vec::with_capacity(1);
        self.commit_row(values, pk_bytes, None, &[], sys_from, &mut wal_records)?;
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

    /// Delete a row by PK value at system-time ordinal `stamp`. Returns the
    /// WAL record to persist.
    ///
    /// Looks up PK in the index to find the segment + row, ends the row at
    /// `stamp` raised above its `sys_from`, and unbinds the PK.
    pub fn delete(
        &mut self,
        pk_value: &Value,
        stamp: i64,
    ) -> Result<MutationResult, ColumnarError> {
        let pk_bytes = encode_pk(pk_value);

        let location = self
            .pk_index
            .get(&pk_bytes)
            .copied()
            .ok_or(ColumnarError::PrimaryKeyNotFound)?;
        Ok(self.delete_bound(pk_bytes, location, stamp))
    }

    /// End the row `pk_bytes` binds at `location` and unbind it.
    fn delete_bound(
        &mut self,
        pk_bytes: Vec<u8>,
        location: RowLocation,
        stamp: i64,
    ) -> MutationResult {
        let sys_to = self.end_stamp(location, stamp);
        let wal = self.end_row(pk_bytes.clone(), location, sys_to);
        self.pk_index.remove(&pk_bytes);
        MutationResult {
            wal_records: vec![wal],
        }
    }

    /// Close a key of a bitemporal collection: append `tombstone`, the
    /// version that ends the key, and unbind the key. The closed version
    /// ends at the tombstone's `_ts_system` and stays as history for `AS OF`
    /// reads. The tombstone ends at its own stamp: no read at a cut sees it,
    /// and an all-versions read does. It is bound in the closed-key index,
    /// so a later write of the key can read its system time.
    ///
    /// The tombstone keeps the cross-engine surrogate of the row it closes,
    /// read as [`Self::update`] reads it. A collection that is not
    /// bitemporal writes no tombstone: the row ends at `stamp` as
    /// [`Self::delete`] ends it.
    ///
    /// `PrimaryKeyNotFound` when the tombstone's key is unbound. An invalid
    /// tombstone returns an error and leaves the key bound.
    pub fn close(
        &mut self,
        tombstone: &[Value],
        flushed_surrogate: Option<Surrogate>,
        stamp: i64,
    ) -> Result<MutationResult, ColumnarError> {
        let pk_bytes = self.extract_pk_bytes(tombstone)?;
        let location = self
            .pk_index
            .get(&pk_bytes)
            .copied()
            .ok_or(ColumnarError::PrimaryKeyNotFound)?;
        if !self.schema.is_bitemporal() {
            return Ok(self.delete_bound(pk_bytes, location, stamp));
        }
        let surrogate = self.surrogate_at(location, flushed_surrogate);
        let sys_from = self.version_stamp(tombstone, stamp, &[pk_bytes.as_slice()]);
        let mut wal_records = Vec::with_capacity(3);
        self.commit_row(
            tombstone,
            pk_bytes.clone(),
            surrogate,
            &[(pk_bytes.clone(), location)],
            sys_from,
            &mut wal_records,
        )?;
        self.unbind_closed(pk_bytes, sys_from, &mut wal_records);
        Ok(MutationResult { wal_records })
    }

    /// Unbind `pk_bytes` from the tombstone it binds, end the tombstone at
    /// its own `sys_from`, and bind it in the closed-key index.
    fn unbind_closed(
        &mut self,
        pk_bytes: Vec<u8>,
        sys_from: i64,
        wal_records: &mut Vec<ColumnarWalRecord>,
    ) {
        if let Some(tombstone_location) = self.pk_index.remove(&pk_bytes) {
            wal_records.push(self.end_row(pk_bytes.clone(), tombstone_location, sys_from));
            self.closed_index.upsert(pk_bytes, tombstone_location);
        }
    }

    /// The surrogate the row at `location` carries: the memtable side table
    /// for a memtable row, else `flushed_surrogate`.
    fn surrogate_at(
        &self,
        location: RowLocation,
        flushed_surrogate: Option<Surrogate>,
    ) -> Option<Surrogate> {
        if location.segment_id == self.memtable_segment_id {
            self.memtable_surrogates
                .get(location.row_index as usize)
                .copied()
                .flatten()
        } else {
            flushed_surrogate
        }
    }

    /// Update a row by PK at system-time ordinal `stamp`: end the old row,
    /// append the new one.
    ///
    /// Returns WAL records for the end and the insert. The old row ends
    /// only after the new row is appended, so an invalid new row returns an
    /// error and leaves the old row open. The old row ends at the new row's
    /// `sys_from` and stays as history for `AS OF` reads. The PK index
    /// binds the new row.
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
        stamp: i64,
    ) -> Result<MutationResult, ColumnarError> {
        self.update_row(old_pk, new_values, None, flushed_surrogate, stamp)
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
        stamp: i64,
    ) -> Result<MutationResult, ColumnarError> {
        self.update_row(
            old_pk,
            new_values,
            Some(tombstone),
            flushed_surrogate,
            stamp,
        )
    }

    /// Shared body of [`Self::update`] and [`Self::update_closing`].
    fn update_row(
        &mut self,
        old_pk: &Value,
        new_values: &[Value],
        tombstone: Option<&[Value]>,
        flushed_surrogate: Option<Surrogate>,
        stamp: i64,
    ) -> Result<MutationResult, ColumnarError> {
        let old_pk_bytes = encode_pk(old_pk);
        let old_location = self
            .pk_index
            .get(&old_pk_bytes)
            .copied()
            .ok_or(ColumnarError::PrimaryKeyNotFound)?;
        let surrogate = self.surrogate_at(old_location, flushed_surrogate);

        let new_pk_bytes = self.extract_pk_bytes(new_values)?;
        let pk_changed = new_pk_bytes != old_pk_bytes;
        let sys_from = self.version_stamp(
            new_values,
            stamp,
            &[old_pk_bytes.as_slice(), new_pk_bytes.as_slice()],
        );

        // When the PK changes, a row already bound to the new PK ends too,
        // as an upsert ends it.
        let displaced = if pk_changed {
            self.pk_index
                .get(&new_pk_bytes)
                .copied()
                .map(|prior| (new_pk_bytes.clone(), prior))
        } else {
            None
        };

        let mut wal_records = Vec::with_capacity(4);
        let closing = tombstone.filter(|_| pk_changed && self.schema.is_bitemporal());
        if let Some(tombstone) = closing {
            let row_count_before = self.memtable.row_count();
            self.commit_row(
                tombstone,
                old_pk_bytes.clone(),
                surrogate,
                &[(old_pk_bytes.clone(), old_location)],
                sys_from,
                &mut wal_records,
            )?;
            if let Err(error) = self.commit_row(
                new_values,
                new_pk_bytes,
                surrogate,
                displaced.as_slice(),
                sys_from,
                &mut wal_records,
            ) {
                self.rollback_memtable_inserts(
                    row_count_before,
                    &[],
                    &[(old_pk_bytes, old_location)],
                );
                return Err(error);
            }
            self.unbind_closed(old_pk_bytes, sys_from, &mut wal_records);
            return Ok(MutationResult { wal_records });
        }

        let mut replaced = Vec::with_capacity(2);
        replaced.push((old_pk_bytes.clone(), old_location));
        replaced.extend(displaced);
        self.commit_row(
            new_values,
            new_pk_bytes,
            surrogate,
            &replaced,
            sys_from,
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
    use crate::versions::VersionCut;

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
        rows_at(engine, VersionCut::Current)
    }

    fn rows_at(engine: &MutationEngine, cut: VersionCut) -> Vec<Vec<Value>> {
        engine
            .scan_memtable_versions(cut)
            .map(|r| r.map(|(_, _, values)| values))
            .collect::<Result<_, _>>()
            .expect("read")
    }

    #[test]
    fn rejected_upsert_keeps_prior_row() {
        let mut engine = MutationEngine::new("t".into(), schema());
        engine.insert(&row(1, "a", 0.5), 10).expect("insert");

        let err = engine
            .insert(&[Value::Integer(1), Value::Null, Value::Null], 20)
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
        assert_eq!(
            engine.segment_versions(0).map(|v| v.len()),
            Some(1),
            "the refused row takes no stamp"
        );
    }

    #[test]
    fn rejected_upsert_with_surrogate_keeps_prior_row() {
        let mut engine = MutationEngine::new("t".into(), schema());
        engine
            .insert_with_surrogate(&row(1, "a", 0.5), Surrogate(7), 10)
            .expect("insert");

        let err = engine
            .insert_with_surrogate(
                &[
                    Value::Integer(1),
                    Value::String("b".into()),
                    Value::Bool(true),
                ],
                Surrogate(8),
                20,
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
            .insert_if_absent(&[Value::Integer(1), Value::Null, Value::Null], 10)
            .unwrap_err();
        assert!(matches!(err, ColumnarError::NullViolation(_)));
        assert!(engine.pk_index().is_empty());
        assert_eq!(engine.memtable().row_count(), 0);

        engine
            .insert_if_absent(&row(1, "a", 0.5), 10)
            .expect("insert after rejected row");
        assert_eq!(live_rows(&engine), vec![row(1, "a", 0.5)]);
    }

    #[test]
    fn rejected_update_keeps_old_row() {
        let mut engine = MutationEngine::new("t".into(), schema());
        engine.insert(&row(1, "a", 0.5), 10).expect("insert");

        let err = engine
            .update(
                &Value::Integer(1),
                &[Value::Integer(1), Value::Null, Value::Null],
                None,
                20,
            )
            .unwrap_err();
        assert!(matches!(err, ColumnarError::NullViolation(_)));

        assert_eq!(live_rows(&engine), vec![row(1, "a", 0.5)]);
        assert!(engine.pk_index().contains(&encode_pk(&Value::Integer(1))));
        assert!(engine.version_chains().is_empty());
    }

    #[test]
    fn update_to_new_pk_rebinds_index() {
        let mut engine = MutationEngine::new("t".into(), schema());
        engine.insert(&row(1, "a", 0.5), 10).expect("insert");

        let result = engine
            .update(&Value::Integer(1), &row(2, "b", 0.25), None, 20)
            .expect("update");
        assert_eq!(result.wal_records.len(), 2);

        assert_eq!(live_rows(&engine), vec![row(2, "b", 0.25)]);
        assert!(!engine.pk_index().contains(&encode_pk(&Value::Integer(1))));
        assert!(engine.pk_index().contains(&encode_pk(&Value::Integer(2))));
        assert_eq!(rows_at(&engine, VersionCut::At(15)), vec![row(1, "a", 0.5)]);
    }

    /// Put, overwrite, delete, and re-insert of one key: a read at each
    /// ordinal sees the version that was current then, and a current read
    /// sees only the newest.
    #[test]
    fn a_read_at_an_ordinal_sees_the_version_current_then() {
        let mut engine = MutationEngine::new("t".into(), schema());
        engine.insert(&row(1, "a", 0.1), 10).expect("put");
        engine.insert(&row(1, "b", 0.2), 20).expect("overwrite");
        engine.delete(&Value::Integer(1), 30).expect("delete");
        engine.insert(&row(1, "c", 0.3), 40).expect("re-insert");

        assert!(rows_at(&engine, VersionCut::At(9)).is_empty());
        assert_eq!(rows_at(&engine, VersionCut::At(10)), vec![row(1, "a", 0.1)]);
        assert_eq!(rows_at(&engine, VersionCut::At(19)), vec![row(1, "a", 0.1)]);
        assert_eq!(rows_at(&engine, VersionCut::At(25)), vec![row(1, "b", 0.2)]);
        assert!(rows_at(&engine, VersionCut::At(30)).is_empty());
        assert_eq!(rows_at(&engine, VersionCut::At(45)), vec![row(1, "c", 0.3)]);
        assert_eq!(live_rows(&engine), vec![row(1, "c", 0.3)]);
        assert_eq!(rows_at(&engine, VersionCut::All).len(), 3);

        let pk = encode_pk(&Value::Integer(1));
        let at = |s| {
            engine
                .version_at(&pk, VersionCut::At(s))
                .map(|l| l.row_index)
        };
        assert_eq!(at(15), Some(0));
        assert_eq!(at(25), Some(1));
        assert_eq!(at(35), None);
        assert_eq!(at(45), Some(2));
    }

    /// A stamp at or below the key's newest version rises above it, so a
    /// key's versions always sort in write order.
    #[test]
    fn a_stale_stamp_rises_above_the_keys_newest_version() {
        let mut engine = MutationEngine::new("t".into(), schema());
        engine.insert(&row(1, "a", 0.1), 50).expect("put");
        engine
            .insert(&row(1, "b", 0.2), 40)
            .expect("stale overwrite");
        let pk = encode_pk(&Value::Integer(1));
        let bound = engine.pk_index().get(&pk).copied().expect("bound");
        assert_eq!(engine.sys_from_at(bound), Some(51));
        engine.delete(&Value::Integer(1), 10).expect("stale delete");
        assert_eq!(
            engine.sys_to_at(bound),
            Some(52),
            "the delete ends above sys_from"
        );
        engine
            .insert(&row(1, "c", 0.3), 5)
            .expect("stale re-insert");
        let rebound = engine.pk_index().get(&pk).copied().expect("bound");
        assert_eq!(engine.sys_from_at(rebound), Some(52));
    }

    fn bitemporal_schema() -> ColumnarSchema {
        ColumnarSchema::new(vec![
            ColumnDef::required("_ts_system", ColumnType::Int64),
            ColumnDef::required("id", ColumnType::Int64).with_primary_key(),
            ColumnDef::required("v", ColumnType::Int64),
        ])
        .expect("valid")
    }

    /// A bitemporal update ends the version it supersedes at the new
    /// version's `_ts_system`: a current read sees the new version, a read
    /// at the old stamp sees the old one, and the PK index binds the new.
    #[test]
    fn a_bitemporal_update_keeps_the_superseded_version() {
        let mut engine = MutationEngine::new("t".into(), bitemporal_schema());
        let version =
            |ts: i64, v: i64| vec![Value::Integer(ts), Value::Integer(1), Value::Integer(v)];
        engine.insert(&version(100, 10), 0).expect("insert");

        let result = engine
            .update(&Value::Integer(1), &version(200, 20), None, 0)
            .expect("update");
        assert_eq!(result.wal_records.len(), 2, "an end and an insert");

        assert_eq!(live_rows(&engine), vec![version(200, 20)]);
        assert_eq!(
            rows_at(&engine, VersionCut::At(150)),
            vec![version(100, 10)]
        );
        assert_eq!(
            rows_at(&engine, VersionCut::All),
            vec![version(100, 10), version(200, 20)]
        );
        let bound = engine
            .pk_index()
            .get(&encode_pk(&Value::Integer(1)))
            .copied()
            .expect("bound");
        assert_eq!(bound.row_index, 1, "the PK binds the new version");
    }

    /// A bitemporal close appends the tombstone, ends the closed version at
    /// the tombstone's stamp, unbinds the key and binds the tombstone in the
    /// closed-key index. No read at a cut sees the tombstone. A flush
    /// carries the closed-key entry to the new segment.
    #[test]
    fn a_bitemporal_close_keeps_the_closed_version() {
        let mut engine = MutationEngine::new("t".into(), bitemporal_schema());
        let version =
            |ts: i64, v: i64| vec![Value::Integer(ts), Value::Integer(1), Value::Integer(v)];
        engine.insert(&version(100, 10), 0).expect("insert");

        engine.close(&version(200, 10), None, 0).expect("close");

        assert!(live_rows(&engine).is_empty());
        assert_eq!(
            rows_at(&engine, VersionCut::At(150)),
            vec![version(100, 10)]
        );
        assert!(rows_at(&engine, VersionCut::At(200)).is_empty());
        assert_eq!(
            rows_at(&engine, VersionCut::All),
            vec![version(100, 10), version(200, 10)],
            "the audit log holds the tombstone"
        );
        let pk = encode_pk(&Value::Integer(1));
        assert!(
            !engine.pk_index().contains(&pk),
            "the closed key is unbound"
        );
        assert_eq!(engine.closed_location(&pk).map(|l| l.row_index), Some(1));
        assert!(matches!(
            engine.close(&version(300, 10), None, 0),
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
        engine.insert(&row(100, 1, 10), 0).expect("insert");

        let refused = engine.update_closing(
            &Value::Integer(1),
            &[Value::Integer(200), Value::Integer(2), Value::Null],
            &row(200, 1, 10),
            None,
            0,
        );
        assert!(refused.is_err());
        assert_eq!(live_rows(&engine), vec![row(100, 1, 10)]);
        assert_eq!(rows_at(&engine, VersionCut::All), vec![row(100, 1, 10)]);
        let old = encode_pk(&Value::Integer(1));
        assert_eq!(engine.pk_index().get(&old).map(|l| l.row_index), Some(0));
        assert_eq!(engine.closed_location(&old), None);
        assert!(engine.version_chains().is_empty());

        engine
            .update_closing(
                &Value::Integer(1),
                &row(200, 2, 10),
                &row(200, 1, 10),
                None,
                0,
            )
            .expect("update");
        assert_eq!(live_rows(&engine), vec![row(200, 2, 10)]);
        assert_eq!(
            rows_at(&engine, VersionCut::All),
            vec![row(100, 1, 10), row(200, 1, 10), row(200, 2, 10)]
        );
        assert!(!engine.pk_index().contains(&old));
        assert_eq!(engine.closed_location(&old).map(|l| l.row_index), Some(1));
        let new = encode_pk(&Value::Integer(2));
        assert_eq!(engine.pk_index().get(&new).map(|l| l.row_index), Some(2));
    }

    /// A close on a collection that is not bitemporal ends the row.
    #[test]
    fn a_plain_close_deletes_the_row() {
        let mut engine = MutationEngine::new("t".into(), schema());
        engine.insert(&row(1, "a", 0.5), 10).expect("insert");
        engine.close(&row(1, "a", 0.5), None, 20).expect("close");
        assert!(live_rows(&engine).is_empty());
        assert!(engine.pk_index().is_empty());
        assert_eq!(engine.closed_location(&encode_pk(&Value::Integer(1))), None);
        assert_eq!(rows_at(&engine, VersionCut::At(15)), vec![row(1, "a", 0.5)]);
    }
}
