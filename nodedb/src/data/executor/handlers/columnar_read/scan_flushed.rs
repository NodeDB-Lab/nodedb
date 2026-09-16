// SPDX-License-Identifier: BUSL-1.1

//! The flushed-segment pass of the columnar base scan.

use crate::bridge::expr_eval::ComputedColumn;
use crate::bridge::scan_filter::ScanFilter;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::transaction::overlay::ColumnarMatchedRow;
use crate::data::executor::scan_normalize::decoded_col_to_value;

use super::bitemporal::bitemporal_row_visible;
use super::convert::row_to_projected_value;
use super::filter::row_matches_filters_and_policy;

/// Read-only context for the flushed-segment scan phase. All fields are
/// borrowed from locals already computed in `execute_columnar_scan`.
pub(in crate::data::executor) struct FlushedScanCtx<'a> {
    pub collection: &'a str,
    pub engine_key: &'a (crate::types::DatabaseId, crate::types::TenantId, String),
    pub schema: &'a nodedb_types::columnar::ColumnarSchema,
    pub projection: &'a [String],
    pub limit: usize,
    pub sort_keys: &'a [nodedb_physical::physical_plan::SortKeySpec],
    pub filter_predicates: &'a [ScanFilter],
    /// The caller's decoded read policy; empty admits every row.
    pub rls_predicates: &'a [ScanFilter],
    pub prefilter: Option<&'a nodedb_types::surrogate_bitmap::SurrogateBitmap>,
    pub computed_cols: &'a [ComputedColumn],
    pub all_versions: bool,
    pub system_as_of_ms: Option<i64>,
    pub valid_at_ms: Option<i64>,
    pub ts_system_idx: Option<usize>,
    pub ts_valid_from_idx: Option<usize>,
    pub ts_valid_until_idx: Option<usize>,
}

impl CoreLoop {
    /// Scan all flushed columnar segments for `engine_key`, appending matching
    /// rows to `matched`. This is Phase 1 of `execute_columnar_scan`; Phase 2
    /// (live memtable) continues in the caller with the same `matched` Vec.
    ///
    /// Each cell is decoded by `decoded_col_to_value` with its declared column
    /// type, and each row is projected by `row_to_projected_value` — the same
    /// two steps the live-memtable phase applies — so a row reads identically
    /// from a segment and from the memtable.
    pub(in crate::data::executor) fn scan_flushed_columnar_segments(
        &self,
        ctx: FlushedScanCtx<'_>,
        matched: &mut Vec<ColumnarMatchedRow>,
    ) -> crate::Result<()> {
        let FlushedScanCtx {
            collection,
            engine_key,
            schema,
            projection,
            limit,
            sort_keys,
            filter_predicates,
            rls_predicates,
            prefilter,
            computed_cols,
            all_versions,
            system_as_of_ms,
            valid_at_ms,
            ts_system_idx,
            ts_valid_from_idx,
            ts_valid_until_idx,
        } = ctx;

        if let Some(segments) = self.columnar_flushed_segments.get(engine_key) {
            for (seg_idx, seg_bytes) in segments.iter().enumerate() {
                if sort_keys.is_empty() && matched.len() >= limit {
                    break;
                }
                // Segment ids are 1-based (segment_id 0 is reserved for the
                // active memtable virtual segment). Mirror: materialize_scan.rs.
                let seg_id = seg_idx as u64 + 1;

                let reader = if let Some(ref reg) = self.quarantine_registry {
                    match crate::storage::quarantine::engines::open_segment_with_quarantine(
                        reg,
                        seg_bytes,
                        collection,
                        &seg_id.to_string(),
                    ) {
                        Ok(r) => r,
                        Err(e) => {
                            tracing::warn!(
                                collection,
                                seg_id,
                                error = %e,
                                "execute_columnar_scan: failed to open flushed segment (quarantine); skipping"
                            );
                            continue;
                        }
                    }
                } else {
                    match nodedb_columnar::SegmentReader::open(seg_bytes) {
                        Ok(r) => r,
                        Err(e) => {
                            tracing::warn!(
                                collection,
                                seg_id,
                                error = %e,
                                "execute_columnar_scan: failed to open flushed segment; skipping"
                            );
                            continue;
                        }
                    }
                };

                let row_count = reader.row_count() as usize;
                let col_count = schema.columns.len();

                // Decode all columns for this segment up front.
                let mut decoded_cols = Vec::with_capacity(col_count);
                let mut decode_ok = true;
                for col_idx in 0..col_count {
                    match reader.read_column(col_idx) {
                        Ok(dc) => decoded_cols.push(dc),
                        Err(e) => {
                            tracing::warn!(
                                collection,
                                seg_id,
                                col_idx,
                                error = %e,
                                "execute_columnar_scan: column decode failed; skipping segment"
                            );
                            decode_ok = false;
                            break;
                        }
                    }
                }
                if !decode_ok {
                    continue;
                }

                // Fetch the delete bitmap for this segment once per segment.
                let delete_bm = self
                    .columnar_engines
                    .get(engine_key)
                    .and_then(|e| e.delete_bitmap(seg_id));

                // Resolve this segment's per-row surrogate slice once. Held in
                // lockstep with the segment-bytes Vec, so index `seg_idx` here
                // matches `segments[seg_idx]`. `None` when absent (e.g. segments
                // restored from backup before the sidecar carry-through lands).
                let seg_surrogates: Option<&Vec<Option<nodedb_types::Surrogate>>> = self
                    .columnar_flushed_surrogates
                    .get(engine_key)
                    .and_then(|segs| segs.get(seg_idx));

                for row_idx in 0..row_count {
                    // Skip tombstoned rows.
                    if delete_bm.is_some_and(|bm| bm.is_deleted(row_idx as u32)) {
                        continue;
                    }

                    let row_surrogate: Option<nodedb_types::Surrogate> = seg_surrogates
                        .and_then(|s| s.get(row_idx))
                        .copied()
                        .flatten();

                    // Row-boundary cross-engine prefilter, mirroring the live
                    // memtable phase below (which uses `bitmap.contains(s)` on
                    // `Option<Surrogate>`). A row passes only when its recorded
                    // surrogate is `Some(s)` and present in the bitmap. A row
                    // with no recorded surrogate (`None`, or no sidecar entry)
                    // cannot satisfy a cross-engine prefilter, so it is skipped.
                    // When no prefilter is active the check is bypassed entirely.
                    if let Some(bitmap) = prefilter {
                        match row_surrogate {
                            Some(s) if bitmap.contains(s) => {}
                            _ => continue,
                        }
                    }

                    // Build the row as Vec<Value> using the shared decoder,
                    // typing each cell by its declared column type.
                    let row: Vec<nodedb_types::value::Value> = decoded_cols
                        .iter()
                        .zip(&schema.columns)
                        .map(|(dc, col_def)| {
                            decoded_col_to_value(dc, row_idx, &col_def.column_type)
                        })
                        .collect();

                    if !bitemporal_row_visible(
                        &row,
                        ts_system_idx,
                        ts_valid_from_idx,
                        ts_valid_until_idx,
                        system_as_of_ms,
                        valid_at_ms,
                    ) {
                        continue;
                    }
                    // The query's WHERE predicates, then the caller's read
                    // policy, before projection so a limit counts admitted
                    // rows only.
                    if !row_matches_filters_and_policy(
                        &row,
                        schema,
                        filter_predicates,
                        rls_predicates,
                    )? {
                        continue;
                    }

                    // Computed-column projection is projection-shaped: a
                    // division/modulo-by-zero fails the whole scan.
                    let obj = row_to_projected_value(
                        &row,
                        schema,
                        projection,
                        computed_cols,
                        all_versions,
                    )?;
                    matched.push((row_surrogate, row, obj));
                    if sort_keys.is_empty() && matched.len() >= limit {
                        break;
                    }
                }
            }
        }
        Ok(())
    }
}
