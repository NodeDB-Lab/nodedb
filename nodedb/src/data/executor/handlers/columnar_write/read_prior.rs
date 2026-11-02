// SPDX-License-Identifier: BUSL-1.1

//! PK lookup of a row version: the current row for ON CONFLICT DO UPDATE
//! prior-row reads, the version a read at an ordinal sees, and the
//! tombstone read of a closed bitemporal key.

use nodedb_columnar::VersionCut;
use nodedb_columnar::pk_index::RowLocation;
use nodedb_types::value::Value;

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::columnar_read::flushed_segment::SegmentBlocks;

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
        self.read_columnar_version_by_pk(engine_key, pk_bytes, VersionCut::Current)
    }

    /// Read the version of `pk_bytes` a read at `cut` sees. The engine finds
    /// it through the PK index and the key's version chain
    /// (`MutationEngine::version_at`), so one row is decoded, never the
    /// key's whole history. `Ok(None)` when the cut sees no version of the
    /// key.
    ///
    /// `Err` when the located row does not read.
    pub(in crate::data::executor) fn read_columnar_version_by_pk(
        &self,
        engine_key: &(nodedb_types::DatabaseId, crate::types::TenantId, String),
        pk_bytes: &[u8],
        cut: VersionCut,
    ) -> crate::Result<Option<Vec<Value>>> {
        let Some(engine) = self.columnar_engines.get(engine_key) else {
            return Ok(None);
        };
        let Some(loc) = engine.version_at(pk_bytes, cut) else {
            return Ok(None);
        };
        if loc.segment_id == engine.memtable_segment_id() {
            return Ok(engine.get_memtable_version(loc.row_index as usize, cut)?);
        }
        self.read_flushed_row_at(engine_key, loc, cut)
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
        // The tombstone is an ended version: an all-versions read sees it
        // until a purge removes it.
        if loc.segment_id == engine.memtable_segment_id() {
            return Ok(engine.get_memtable_version(loc.row_index as usize, VersionCut::All)?);
        }
        self.read_flushed_row_at(engine_key, loc, VersionCut::All)
    }

    /// Read the flushed row at `loc`. `Ok(None)` when a read at `cut` does
    /// not see it.
    ///
    /// `Err` when `loc` names a segment or row this core does not hold, or
    /// when the segment does not decode.
    fn read_flushed_row_at(
        &self,
        engine_key: &(nodedb_types::DatabaseId, crate::types::TenantId, String),
        loc: RowLocation,
        cut: VersionCut,
    ) -> crate::Result<Option<Vec<Value>>> {
        let Some(engine) = self.columnar_engines.get(engine_key) else {
            return Ok(None);
        };
        if !engine.row_visible(loc.segment_id, loc.row_index, cut) {
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
            engine_key,
            loc.segment_id,
            seg_bytes,
            schema.columns.len(),
            SegmentBlocks::Row(loc.row_index as usize),
            "columnar_prior_row",
        )?;
        let row_idx = loc.row_index as usize;
        if row_idx >= segment.row_count() {
            return Err(unheld());
        }
        segment.row(schema, row_idx).map(Some)
    }
}
