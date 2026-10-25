// SPDX-License-Identifier: BUSL-1.1

//! Flushed-segment PK lookup for ON CONFLICT DO UPDATE prior-row reads,
//! and the tombstone read of a closed bitemporal key.

use nodedb_columnar::pk_index::RowLocation;
use nodedb_types::value::Value;

use crate::data::executor::core_loop::CoreLoop;

impl CoreLoop {
    /// Read the live row bound to `pk_bytes`, wherever it lives: the memtable
    /// first, then a flushed segment. `Ok(None)` when the PK is unbound.
    ///
    /// `Err` when the bound row does not read. A corrupt row is never
    /// reported as an absent one: an upsert would then insert a duplicate
    /// key, and a write policy would decide against no prior row.
    pub(in crate::data::executor) fn read_columnar_row_by_pk(
        &self,
        engine_key: &(nodedb_types::DatabaseId, crate::types::TenantId, String),
        pk_bytes: &[u8],
    ) -> crate::Result<Option<Vec<Value>>> {
        let Some(engine) = self.columnar_engines.get(engine_key) else {
            return Ok(None);
        };
        if let Some(row) = engine.lookup_memtable_row_by_pk(pk_bytes)? {
            return Ok(Some(row));
        }
        self.read_flushed_row_by_pk(engine_key, pk_bytes)
    }

    /// Read a single row from a flushed columnar segment by PK, if the PK
    /// index points to one. `Ok(None)` when the PK is unbound, lives in the
    /// memtable, or was tombstoned. Used by the `ON CONFLICT DO UPDATE` and
    /// write-policy paths to read a prior row already flushed out of the
    /// memtable.
    ///
    /// `Err` when the PK index points at a segment or row this core does not
    /// hold, or when the segment does not decode. The shared segment reader
    /// files the corruption report.
    pub(in crate::data::executor) fn read_flushed_row_by_pk(
        &self,
        engine_key: &(nodedb_types::DatabaseId, crate::types::TenantId, String),
        pk_bytes: &[u8],
    ) -> crate::Result<Option<Vec<Value>>> {
        let Some(engine) = self.columnar_engines.get(engine_key) else {
            return Ok(None);
        };
        let Some(loc) = engine.pk_index().get(pk_bytes).copied() else {
            return Ok(None);
        };
        // Memtable case is already covered by the engine-side lookup.
        if loc.segment_id == engine.memtable_segment_id() {
            return Ok(None);
        }
        self.read_flushed_row_at(engine_key, loc)
    }

    /// Read the tombstone the latest bitemporal close of `pk_bytes` wrote.
    /// `Ok(None)` for a key never closed, or one whose tombstone a retention
    /// purge removed.
    ///
    /// `Err` when the tombstone does not read.
    pub(in crate::data::executor) fn read_columnar_closed_row(
        &self,
        engine_key: &(nodedb_types::DatabaseId, crate::types::TenantId, String),
        pk_bytes: &[u8],
    ) -> crate::Result<Option<Vec<Value>>> {
        let Some(engine) = self.columnar_engines.get(engine_key) else {
            return Ok(None);
        };
        let Some(loc) = engine.closed_location(pk_bytes) else {
            return Ok(None);
        };
        if loc.segment_id == engine.memtable_segment_id() {
            return Ok(engine.get_memtable_row(loc.row_index as usize)?);
        }
        self.read_flushed_row_at(engine_key, loc)
    }

    /// Read the flushed row at `loc`. `Ok(None)` when its segment's delete
    /// bitmap marks it.
    ///
    /// `Err` when `loc` names a segment or row this core does not hold, or
    /// when the segment does not decode.
    fn read_flushed_row_at(
        &self,
        engine_key: &(nodedb_types::DatabaseId, crate::types::TenantId, String),
        loc: RowLocation,
    ) -> crate::Result<Option<Vec<Value>>> {
        let Some(engine) = self.columnar_engines.get(engine_key) else {
            return Ok(None);
        };
        // Tombstoned — prior row no longer logically present.
        if engine
            .delete_bitmap(loc.segment_id)
            .is_some_and(|bm| bm.is_deleted(loc.row_index))
        {
            return Ok(None);
        }
        let collection = engine_key.2.as_str();
        let unheld = || crate::Error::Internal {
            detail: format!(
                "columnar '{collection}': an index binds a row to flushed segment {} row {}, \
                 which this core does not hold",
                loc.segment_id, loc.row_index
            ),
        };
        // Segments are pushed in order starting at segment_id=1.
        let seg_bytes = usize::try_from(loc.segment_id)
            .ok()
            .and_then(|id| id.checked_sub(1))
            .and_then(|seg_idx| self.columnar_flushed_segments.get(engine_key)?.get(seg_idx))
            .ok_or_else(unheld)?;
        let schema = engine.schema();
        let segment = self.decode_flushed_segment(
            collection,
            loc.segment_id,
            seg_bytes,
            schema.columns.len(),
            "columnar_prior_row",
        )?;
        let row_idx = loc.row_index as usize;
        if row_idx >= segment.row_count() {
            return Err(unheld());
        }
        segment.row(schema, row_idx).map(Some)
    }
}
