// SPDX-License-Identifier: Apache-2.0

//! Write-path mutations: insert, insert_if_absent, delete, update.

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
    /// delete) before the new row is appended to the memtable. The PK
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
        let pk_bytes = self.extract_pk_bytes(values)?;
        let mut wal_records = Vec::with_capacity(2);

        // Bitemporal collections preserve every version of a PK: each
        // write appends a new row with a distinct `_ts_system` stamp and
        // the prior row stays visible to `AS OF` queries. Skipping the
        // upsert-tombstone here keeps compaction lossless without
        // needing a separate "version-aware" delete bitmap. The PK
        // index is still rebound below so current-state reads see the
        // latest version.
        let bitemporal = self.schema.is_bitemporal();

        // If a prior row exists for this PK, tombstone it in place so
        // subsequent scans skip the stale row. The PK index is rebound
        // below to the freshly-appended row.
        if !bitemporal && let Some(prior) = self.pk_index.get(&pk_bytes).copied() {
            let bitmap = self.delete_bitmaps.entry(prior.segment_id).or_default();
            bitmap.mark_deleted(prior.row_index);
            wal_records.push(ColumnarWalRecord::DeleteRows {
                collection: self.collection.clone(),
                segment_id: prior.segment_id,
                row_indices: vec![prior.row_index],
            });
        }

        let row_data = encode_row_for_wal(values)?;
        wal_records.push(ColumnarWalRecord::InsertRow {
            collection: self.collection.clone(),
            row_data,
        });

        self.memtable.append_row(values)?;
        let location = RowLocation {
            segment_id: self.memtable_segment_id,
            row_index: self.memtable_row_counter,
        };
        self.pk_index.upsert(pk_bytes, location);
        self.memtable_surrogates.push(None);
        self.memtable_row_counter += 1;

        Ok(MutationResult { wal_records })
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
        let pk_bytes = self.extract_pk_bytes(values)?;
        let mut wal_records = Vec::with_capacity(2);

        let bitemporal = self.schema.is_bitemporal();
        if !bitemporal && let Some(prior) = self.pk_index.get(&pk_bytes).copied() {
            let bitmap = self.delete_bitmaps.entry(prior.segment_id).or_default();
            bitmap.mark_deleted(prior.row_index);
            wal_records.push(ColumnarWalRecord::DeleteRows {
                collection: self.collection.clone(),
                segment_id: prior.segment_id,
                row_indices: vec![prior.row_index],
            });
        }

        let row_data = encode_row_for_wal(values)?;
        wal_records.push(ColumnarWalRecord::InsertRow {
            collection: self.collection.clone(),
            row_data,
        });

        self.memtable.append_row(values)?;
        let location = RowLocation {
            segment_id: self.memtable_segment_id,
            row_index: self.memtable_row_counter,
        };
        self.pk_index.upsert(pk_bytes, location);
        self.memtable_surrogates.push(Some(surrogate));
        self.memtable_row_counter += 1;

        Ok(MutationResult { wal_records })
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

        let row_data = encode_row_for_wal(values)?;
        let wal = ColumnarWalRecord::InsertRow {
            collection: self.collection.clone(),
            row_data,
        };
        self.memtable.append_row(values)?;
        let location = RowLocation {
            segment_id: self.memtable_segment_id,
            row_index: self.memtable_row_counter,
        };
        self.pk_index.upsert(pk_bytes, location);
        self.memtable_surrogates.push(None);
        self.memtable_row_counter += 1;

        Ok(MutationResult {
            wal_records: vec![wal],
        })
    }

    /// Look up the current row for a PK in the memtable, if present.
    ///
    /// Returns `None` if the PK is not in the index, or if the PK points
    /// to a flushed segment (callers needing cross-segment lookup must
    /// go through a segment reader separately). This is the fast path
    /// used by `ON CONFLICT DO UPDATE` to read the would-be-merged row
    /// when the duplicate hits the memtable — the common case under
    /// back-to-back inserts.
    pub fn lookup_memtable_row_by_pk(&self, pk_bytes: &[u8]) -> Option<Vec<Value>> {
        let loc = self.pk_index.get(pk_bytes).copied()?;
        if loc.segment_id != self.memtable_segment_id {
            return None;
        }
        self.memtable.get_row(loc.row_index as usize)
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

        // Generate WAL record BEFORE applying.
        let wal = ColumnarWalRecord::DeleteRows {
            collection: self.collection.clone(),
            segment_id: location.segment_id,
            row_indices: vec![location.row_index],
        };

        // Mark in delete bitmap.
        let bitmap = self.delete_bitmaps.entry(location.segment_id).or_default();
        bitmap.mark_deleted(location.row_index);

        // Remove from PK index.
        self.pk_index.remove(&pk_bytes);

        Ok(MutationResult {
            wal_records: vec![wal],
        })
    }

    /// Update a row by PK: DELETE old + INSERT new.
    ///
    /// `updates` maps column names to new values. Columns not in the map
    /// retain their existing values from the old row.
    ///
    /// Returns WAL records for both the delete and the insert.
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
        let surrogate = match self.pk_index.get(&encode_pk(old_pk)) {
            Some(loc) if loc.segment_id == self.memtable_segment_id => self
                .memtable_surrogates
                .get(loc.row_index as usize)
                .copied()
                .flatten(),
            Some(_) => flushed_surrogate,
            None => None,
        };

        // Delete the old row.
        let delete_result = self.delete(old_pk)?;

        // Insert the new row under the old row's surrogate.
        let insert_result = match surrogate {
            Some(surrogate) => self.insert_with_surrogate(new_values, surrogate)?,
            None => self.insert(new_values)?,
        };

        // Combine WAL records.
        let mut wal_records = delete_result.wal_records;
        wal_records.extend(insert_result.wal_records);

        Ok(MutationResult { wal_records })
    }
}
