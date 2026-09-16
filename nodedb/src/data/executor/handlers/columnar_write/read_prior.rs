// SPDX-License-Identifier: BUSL-1.1

//! Flushed-segment PK lookup for ON CONFLICT DO UPDATE prior-row reads.

use std::mem::size_of;

use nodedb_types::decode_bounds::checked_decode_capacity;
use nodedb_types::value::Value;

use crate::data::executor::core_loop::CoreLoop;

impl CoreLoop {
    /// Read a single row from a flushed columnar segment by PK, if the PK
    /// index points to one. Returns `None` when the PK lives in the
    /// memtable, when the segment is not in memory, or when the row was
    /// tombstoned. Used by the `ON CONFLICT DO UPDATE` path to locate a
    /// prior row that has already been flushed out of the memtable.
    pub(in crate::data::executor) fn read_flushed_row_by_pk(
        &self,
        engine_key: &(nodedb_types::DatabaseId, crate::types::TenantId, String),
        pk_bytes: &[u8],
    ) -> Option<Vec<Value>> {
        let engine = self.columnar_engines.get(engine_key)?;
        let loc = engine.pk_index().get(pk_bytes).copied()?;
        // Memtable case is already covered by the engine-side lookup.
        if loc.segment_id == 0 {
            return None;
        }
        // Tombstoned — prior row no longer logically present.
        if engine
            .delete_bitmap(loc.segment_id)
            .is_some_and(|bm| bm.is_deleted(loc.row_index))
        {
            return None;
        }
        let segs = self.columnar_flushed_segments.get(engine_key)?;
        // Segments are pushed in order starting at segment_id=1.
        let seg_idx = (loc.segment_id as usize).checked_sub(1)?;
        let seg_bytes = segs.get(seg_idx)?;
        let seg_id_str = loc.segment_id.to_string();
        let reader = if let Some(reg) = &self.quarantine_registry {
            crate::storage::quarantine::engines::open_segment_with_quarantine(
                reg,
                seg_bytes,
                &engine_key.2,
                &seg_id_str,
            )
            .ok()?
        } else {
            nodedb_columnar::SegmentReader::open(seg_bytes).ok()?
        };
        let schema = engine.schema();
        // Schema cardinality is catalog-owned rather than byte-decoded; bind
        // it once so the allocation's direct trusted bound is explicit.
        let column_count = schema.columns.len();
        let row_capacity = checked_decode_capacity(
            column_count,
            size_of::<Value>(),
            column_count,
            1,
            column_count,
            usize::MAX,
        )?;
        let mut row = Vec::with_capacity(row_capacity);
        for (col_idx, col_def) in schema.columns.iter().enumerate() {
            let decoded = reader.read_column(col_idx).ok()?;
            row.push(crate::data::executor::scan_normalize::decoded_col_to_value(
                &decoded,
                loc.row_index as usize,
                &col_def.column_type,
            ));
        }
        Some(row)
    }
}
