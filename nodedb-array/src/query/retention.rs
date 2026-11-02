// SPDX-License-Identifier: Apache-2.0

//! Cell-level retention merger for bitemporal tile compaction.
//!
//! [`merge_for_retention`] replaces the tile-granularity
//! `partition_by_retention` for compaction paths that must honour the sparse
//! tile model: because tile-versions are SPARSE (each version only stores the
//! cells that were written at that `system_from_ms`), the retention ceiling
//! must be computed per cell coordinate, not per tile.

use std::collections::{HashMap, HashSet};

use crate::error::{ArrayError, ArrayResult};
use crate::schema::ArraySchema;
use crate::segment::TileEntry;
use crate::segment::reader::{SegmentReader, TilePayload};
use crate::tile::cell_payload::CellPayload;
use crate::tile::sparse_tile::{RowKind, SparseRow, SparseTile, SparseTileBuilder};
use crate::types::TileId;
use crate::types::coord::value::CoordValue;

// ── Result type ─────────────────────────────────────────────────────────────

/// Result of [`merge_for_retention`].
pub struct RetentionMergeResult {
    /// Synthetic ceiling tile materialising all cells live AS OF the horizon.
    ///
    /// `None` when there are zero cells to carry forward (everything erased
    /// or no out-of-horizon versions existed).
    pub ceiling_tile: Option<SparseTile>,
    /// TileIds of in-horizon tile-versions that pass through unchanged.
    pub keep_inhorizon: Vec<TileId>,
    /// TileIds of out-of-horizon tile-versions whose cells were merged into
    /// the ceiling tile (or dropped via GDPR) and should be removed from the
    /// segment.
    pub dropped_tile_ids: Vec<TileId>,
    /// Telemetry: number of distinct cells materialised into the ceiling.
    pub cells_carried_forward: usize,
}

// ── Coord key helpers ────────────────────────────────────────────────────────

/// Encode a coordinate vector into a stable byte key for HashMap / HashSet use.
///
/// Uses the same zerompk encoding as the write path so keys are comparable
/// across tile-versions.
pub fn encode_coord_key(coord: &[CoordValue]) -> ArrayResult<Vec<u8>> {
    let owned = coord.to_vec();
    zerompk::to_msgpack_vec(&owned).map_err(|e| ArrayError::SegmentCorruption {
        detail: format!("encode_coord_key: {e}"),
    })
}

// ── Row iteration helper ─────────────────────────────────────────────────────

/// Decoded row from a [`SparseTile`].
pub struct DecodedRow {
    pub coord_key: Vec<u8>,
    pub coord: Vec<CoordValue>,
    pub kind: RowKind,
    /// `Some` for `Live` rows; `None` for `Tombstone` / `GdprErased`.
    pub payload: Option<CellPayload>,
}

/// Iterate every row in a [`SparseTile`], decoding coords and payload.
///
/// Rows are returned in storage order (same as `row_count()` iteration).
/// Attribute columns in `SparseTile` are indexed by *live-row index*, not by
/// iteration index, so a separate live counter advances only for Live rows.
pub fn decode_sparse_rows(tile: &SparseTile) -> ArrayResult<Vec<DecodedRow>> {
    let n = tile.row_count();
    let arity = tile.dim_dicts.len();
    let mut rows = Vec::with_capacity(n);
    let mut live_idx: usize = 0;

    for row in 0..n {
        // Decode coordinate for this row.
        let mut coord = Vec::with_capacity(arity);
        for dim_idx in 0..arity {
            let dict =
                tile.dim_dicts
                    .get(dim_idx)
                    .ok_or_else(|| ArrayError::SegmentCorruption {
                        detail: format!("decode_sparse_rows: dim {dim_idx} missing"),
                    })?;
            let entry_idx = *dict
                .indices
                .get(row)
                .ok_or_else(|| ArrayError::SegmentCorruption {
                    detail: format!("decode_sparse_rows: row {row} index out of range"),
                })? as usize;
            let val = dict
                .values
                .get(entry_idx)
                .ok_or_else(|| ArrayError::SegmentCorruption {
                    detail: format!("decode_sparse_rows: dict entry {entry_idx} out of range"),
                })?;
            coord.push(val.clone());
        }

        let coord_key = encode_coord_key(&coord)?;
        let kind = tile.row_kind(row)?;

        let payload = match kind {
            RowKind::Live => {
                // Build attrs from each attr column using live_idx.
                let attrs: Vec<_> = tile
                    .attr_cols
                    .iter()
                    .map(|col| {
                        col.get(live_idx)
                            .cloned()
                            .ok_or_else(|| ArrayError::SegmentCorruption {
                                detail: format!(
                                    "decode_sparse_rows: attr col live_idx {live_idx} out of range"
                                ),
                            })
                    })
                    .collect::<ArrayResult<Vec<_>>>()?;

                let surrogate = tile.live_surrogate(row)?;
                let valid_from_ms = tile.valid_from_ms.get(row).copied().ok_or_else(|| {
                    ArrayError::SegmentCorruption {
                        detail: format!("decode_sparse_rows: valid_from_ms row {row} out of range"),
                    }
                })?;
                let valid_until_ms = tile.valid_until_ms.get(row).copied().ok_or_else(|| {
                    ArrayError::SegmentCorruption {
                        detail: format!(
                            "decode_sparse_rows: valid_until_ms row {row} out of range"
                        ),
                    }
                })?;

                live_idx += 1;
                Some(CellPayload {
                    valid_from_ms,
                    valid_until_ms,
                    attrs,
                    surrogate,
                })
            }
            RowKind::Tombstone | RowKind::GdprErased => None,
        };

        rows.push(DecodedRow {
            coord_key,
            coord,
            kind,
            payload,
        });
    }

    Ok(rows)
}

// ── Public API ───────────────────────────────────────────────────────────────

/// Merge tile-versions for one `hilbert_prefix` according to the retention
/// policy, operating at cell granularity.
///
/// `versions` contains all [`TileEntry`] records for a single `hilbert_prefix`
/// (the caller is responsible for grouping). They may arrive in any order; this
/// function sorts internally.
///
/// `horizon_ms` is the retention boundary expressed as an absolute
/// `system_from_ms` timestamp: versions with `system_from_ms < horizon_ms`
/// are outside the retention window.
pub fn merge_for_retention(
    versions: &[TileEntry],
    reader: &SegmentReader<'_>,
    schema: &ArraySchema,
    horizon_ms: i64,
) -> ArrayResult<RetentionMergeResult> {
    if versions.is_empty() {
        return Ok(RetentionMergeResult {
            ceiling_tile: None,
            keep_inhorizon: Vec::new(),
            dropped_tile_ids: Vec::new(),
            cells_carried_forward: 0,
        });
    }

    // Partition into inside / outside by system_from_ms.
    let mut inside: Vec<&TileEntry> = Vec::new();
    let mut outside: Vec<&TileEntry> = Vec::new();
    for entry in versions {
        if entry.tile_id.system_from_ms >= horizon_ms {
            inside.push(entry);
        } else {
            outside.push(entry);
        }
    }

    // Collect TileIds for in-horizon pass-through.
    let keep_inhorizon: Vec<TileId> = inside.iter().map(|e| e.tile_id).collect();

    // If nothing is outside the horizon, there is nothing to merge.
    if outside.is_empty() {
        return Ok(RetentionMergeResult {
            ceiling_tile: None,
            keep_inhorizon,
            dropped_tile_ids: Vec::new(),
            cells_carried_forward: 0,
        });
    }

    // Build the set of coord keys already covered by inside-horizon versions.
    // Any coord present in any inside tile (regardless of RowKind) supersedes
    // what the ceiling would contribute for that coord.
    let mut inhorizon_coords: HashSet<Vec<u8>> = HashSet::new();
    for entry in &inside {
        let tile_idx = find_tile_index(reader, entry.tile_id)?;
        let payload = reader.read_tile(tile_idx)?;
        if let TilePayload::Sparse(ref tile) = payload {
            for trow in decode_sparse_rows(tile)? {
                inhorizon_coords.insert(trow.coord_key);
            }
        }
        // Dense tiles inside horizon contribute no coords to the exclusion set
        // for the ceiling (dense tiles don't participate in sparse retention
        // merge; their coords are not relevant here).
    }

    // Walk outside-horizon versions newest → oldest, accumulating the ceiling.
    // For each coord not yet seen, the first (newest) occurrence wins.
    outside.sort_by_key(|e| std::cmp::Reverse(e.tile_id.system_from_ms));

    // Maps coord_key → (coord, RowKind, Option<CellPayload>).
    let mut ceiling: HashMap<Vec<u8>, (Vec<CoordValue>, RowKind, Option<CellPayload>)> =
        HashMap::new();

    for entry in &outside {
        let tile_idx = find_tile_index(reader, entry.tile_id)?;
        let payload = reader.read_tile(tile_idx)?;
        if let TilePayload::Sparse(ref tile) = payload {
            for trow in decode_sparse_rows(tile)? {
                // Skip coords already covered by an inside-horizon version.
                if inhorizon_coords.contains(&trow.coord_key) {
                    continue;
                }
                // First occurrence (newest) wins for each coord.
                ceiling
                    .entry(trow.coord_key)
                    .or_insert((trow.coord, trow.kind, trow.payload));
            }
        }
    }

    // Collect dropped TileIds — all outside tile-versions are superseded by
    // the ceiling or dropped via GDPR.
    let dropped_tile_ids: Vec<TileId> = outside.iter().map(|e| e.tile_id).collect();

    // Build the synthetic ceiling SparseTile.
    // GdprErased coords are excluded from the ceiling entirely.
    // Tombstone and Live coords are included with their respective RowKind.
    let mut builder = SparseTileBuilder::new(schema);
    let mut cells_carried_forward: usize = 0;

    // Deterministic output order: sort by coord_key for stable tests.
    type CeilingRow = (Vec<u8>, Vec<CoordValue>, RowKind, Option<CellPayload>);
    let mut ceiling_rows: Vec<CeilingRow> = ceiling
        .into_iter()
        .map(|(k, (coord, kind, payload))| (k, coord, kind, payload))
        .collect();
    ceiling_rows.sort_by(|a, b| a.0.cmp(&b.0));

    for (_key, coord, kind, payload) in ceiling_rows {
        match kind {
            RowKind::GdprErased => {
                // Drop entirely — GDPR erasure removes the cell from the ceiling.
            }
            RowKind::Tombstone => {
                builder.push_row(SparseRow::sentinel(&coord, RowKind::Tombstone))?;
                cells_carried_forward += 1;
            }
            RowKind::Live => {
                let p = payload.ok_or_else(|| ArrayError::SegmentCorruption {
                    detail: "Live row in ceiling has no CellPayload".into(),
                })?;
                builder.push_row(SparseRow::live(
                    &coord,
                    &p.attrs,
                    p.surrogate,
                    p.valid_from_ms,
                    p.valid_until_ms,
                ))?;
                cells_carried_forward += 1;
            }
        }
    }

    let ceiling_tile = if cells_carried_forward == 0 {
        None
    } else {
        Some(builder.build())
    };

    Ok(RetentionMergeResult {
        ceiling_tile,
        keep_inhorizon,
        dropped_tile_ids,
        cells_carried_forward,
    })
}

// ── Internal helpers ─────────────────────────────────────────────────────────

/// Locate the position of a tile with `tile_id` in the reader's footer.
fn find_tile_index(reader: &SegmentReader<'_>, tile_id: TileId) -> ArrayResult<usize> {
    reader
        .tiles()
        .iter()
        .position(|e| e.tile_id == tile_id)
        .ok_or_else(|| ArrayError::SegmentCorruption {
            detail: format!("find_tile_index: TileId {tile_id:?} not found in segment"),
        })
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::ArraySchemaBuilder;
    use crate::schema::attr_spec::{AttrSpec, AttrType};
    use crate::schema::dim_spec::{DimSpec, DimType};
    use crate::segment::writer::SegmentWriter;
    use crate::types::cell_value::value::CellValue;
    use crate::types::coord::value::CoordValue;
    use crate::types::domain::{Domain, DomainBound};
    use nodedb_types::{OPEN_UPPER, Surrogate};

    fn schema() -> ArraySchema {
        ArraySchemaBuilder::new("t")
            .dim(DimSpec::new(
                "x",
                DimType::Int64,
                Domain::new(DomainBound::Int64(0), DomainBound::Int64(1000)),
            ))
            .attr(AttrSpec::new("v", AttrType::Int64, true))
            .tile_extents(vec![100])
            .build()
            .unwrap()
    }

    /// Write a segment with the given (TileId, SparseTile) pairs (must be
    /// strictly ascending by TileId) and return the in-memory bytes.
    fn build_segment(pairs: Vec<(TileId, SparseTile)>) -> Vec<u8> {
        let mut w = SegmentWriter::new(0xBEEF);
        for (id, tile) in pairs {
            w.append_sparse(id, 0, &tile).unwrap();
        }
        w.finish(None).unwrap()
    }

    /// Build a single-row Live SparseTile at coord `x` with value `v`.
    fn live_tile(schema: &ArraySchema, x: i64, v: i64) -> SparseTile {
        let mut b = SparseTileBuilder::new(schema);
        b.push_row(SparseRow {
            coord: &[CoordValue::Int64(x)],
            attrs: &[CellValue::Int64(v)],
            surrogate: Some(Surrogate::new(x as u32 + 1)),
            valid_from_ms: 0,
            valid_until_ms: OPEN_UPPER,
            kind: RowKind::Live,
        })
        .unwrap();
        b.build()
    }

    /// Build a single-row Tombstone SparseTile at coord `x`.
    fn tombstone_tile(schema: &ArraySchema, x: i64) -> SparseTile {
        let mut b = SparseTileBuilder::new(schema);
        b.push_row(SparseRow {
            coord: &[CoordValue::Int64(x)],
            attrs: &[],
            surrogate: None,
            valid_from_ms: 0,
            valid_until_ms: OPEN_UPPER,
            kind: RowKind::Tombstone,
        })
        .unwrap();
        b.build()
    }

    /// Build a single-row GdprErased SparseTile at coord `x`.
    fn gdpr_tile(schema: &ArraySchema, x: i64) -> SparseTile {
        let mut b = SparseTileBuilder::new(schema);
        b.push_row(SparseRow {
            coord: &[CoordValue::Int64(x)],
            attrs: &[],
            surrogate: None,
            valid_from_ms: 0,
            valid_until_ms: OPEN_UPPER,
            kind: RowKind::GdprErased,
        })
        .unwrap();
        b.build()
    }

    /// Collect coord x-values from a SparseTile (single-dim schema).
    fn ceiling_x_values(tile: &SparseTile) -> Vec<i64> {
        let rows = decode_sparse_rows(tile).unwrap();
        let mut xs: Vec<i64> = rows
            .iter()
            .map(|r| match r.coord[0] {
                CoordValue::Int64(x) => x,
                _ => panic!("unexpected coord type"),
            })
            .collect();
        xs.sort_unstable();
        xs
    }

    /// Collect RowKinds from ceiling tile rows, keyed by coord.
    fn ceiling_kinds(tile: &SparseTile) -> HashMap<i64, RowKind> {
        decode_sparse_rows(tile)
            .unwrap()
            .into_iter()
            .map(|r| {
                let x = match r.coord[0] {
                    CoordValue::Int64(x) => x,
                    _ => panic!("unexpected coord type"),
                };
                (x, r.kind)
            })
            .collect()
    }

    // ── Test 1 ──────────────────────────────────────────────────────────────

    /// Two coords written at different system times, both outside horizon.
    /// Both must appear in the ceiling (the classic cell-loss regression).
    #[test]
    fn cell_preservation_across_sparse_writes() {
        let s = schema();
        // T=100: writes coord x=1. T=200: writes coord x=2. Horizon=300.
        let t100 = TileId::new(0, 100);
        let t200 = TileId::new(0, 200);
        let bytes = build_segment(vec![
            (t100, live_tile(&s, 1, 10)),
            (t200, live_tile(&s, 2, 20)),
        ]);
        let reader = SegmentReader::open(&bytes).unwrap();
        let versions: Vec<TileEntry> = reader.tiles().to_vec();
        let result = merge_for_retention(&versions, &reader, &s, 300).unwrap();
        let ceiling = result.ceiling_tile.expect("ceiling must be non-None");
        let xs = ceiling_x_values(&ceiling);
        assert_eq!(xs, vec![1, 2], "both cells must survive in ceiling");
        assert_eq!(result.cells_carried_forward, 2);
        assert!(result.keep_inhorizon.is_empty());
        assert_eq!(result.dropped_tile_ids.len(), 2);
    }

    // ── Test 2 ──────────────────────────────────────────────────────────────

    /// All versions inside horizon → pass through unchanged, no ceiling.
    #[test]
    fn inhorizon_versions_pass_through_unchanged() {
        let s = schema();
        let t400 = TileId::new(0, 400);
        let t500 = TileId::new(0, 500);
        let bytes = build_segment(vec![
            (t400, live_tile(&s, 1, 10)),
            (t500, live_tile(&s, 1, 20)),
        ]);
        let reader = SegmentReader::open(&bytes).unwrap();
        let versions: Vec<TileEntry> = reader.tiles().to_vec();
        let result = merge_for_retention(&versions, &reader, &s, 300).unwrap();
        assert!(result.ceiling_tile.is_none());
        let mut keep: Vec<i64> = result
            .keep_inhorizon
            .iter()
            .map(|id| id.system_from_ms)
            .collect();
        keep.sort_unstable();
        assert_eq!(keep, vec![400, 500]);
        assert!(result.dropped_tile_ids.is_empty());
    }

    // ── Test 3 ──────────────────────────────────────────────────────────────

    /// Mixed: T=100 (cell A), T=200 (cell B), T=400 (cell C). Horizon=300.
    /// Ceiling contains A+B; T=400 is in keep_inhorizon.
    #[test]
    fn mixed_inhorizon_and_outhorizon() {
        let s = schema();
        let bytes = build_segment(vec![
            (TileId::new(0, 100), live_tile(&s, 1, 10)),
            (TileId::new(0, 200), live_tile(&s, 2, 20)),
            (TileId::new(0, 400), live_tile(&s, 3, 30)),
        ]);
        let reader = SegmentReader::open(&bytes).unwrap();
        let versions: Vec<TileEntry> = reader.tiles().to_vec();
        let result = merge_for_retention(&versions, &reader, &s, 300).unwrap();
        let ceiling = result.ceiling_tile.expect("ceiling must have A and B");
        let xs = ceiling_x_values(&ceiling);
        assert_eq!(xs, vec![1, 2]);
        assert_eq!(result.cells_carried_forward, 2);
        assert_eq!(result.keep_inhorizon, vec![TileId::new(0, 400)]);
        let mut dropped: Vec<i64> = result
            .dropped_tile_ids
            .iter()
            .map(|id| id.system_from_ms)
            .collect();
        dropped.sort_unstable();
        assert_eq!(dropped, vec![100, 200]);
    }

    // ── Test 4 ──────────────────────────────────────────────────────────────

    /// T=100 Live(A), T=200 Tombstone(A). Horizon=300.
    /// Ceiling contains Tombstone(A) — newest outside-horizon wins.
    #[test]
    fn tombstone_collapses_into_ceiling() {
        let s = schema();
        let bytes = build_segment(vec![
            (TileId::new(0, 100), live_tile(&s, 1, 10)),
            (TileId::new(0, 200), tombstone_tile(&s, 1)),
        ]);
        let reader = SegmentReader::open(&bytes).unwrap();
        let versions: Vec<TileEntry> = reader.tiles().to_vec();
        let result = merge_for_retention(&versions, &reader, &s, 300).unwrap();
        let ceiling = result.ceiling_tile.expect("ceiling must contain tombstone");
        let kinds = ceiling_kinds(&ceiling);
        assert_eq!(kinds.get(&1), Some(&RowKind::Tombstone));
        assert_eq!(result.cells_carried_forward, 1);
    }

    // ── Test 5 ──────────────────────────────────────────────────────────────

    /// T=100 Tombstone(A), T=400 Live(A). Horizon=300.
    /// Inside version T=400 has Live(A) — ceiling must NOT include A.
    #[test]
    fn tombstone_below_with_inhorizon_live_succeeds() {
        let s = schema();
        // TileIds must be strictly ascending for SegmentWriter.
        let bytes = build_segment(vec![
            (TileId::new(0, 100), tombstone_tile(&s, 1)),
            (TileId::new(0, 400), live_tile(&s, 1, 99)),
        ]);
        let reader = SegmentReader::open(&bytes).unwrap();
        let versions: Vec<TileEntry> = reader.tiles().to_vec();
        let result = merge_for_retention(&versions, &reader, &s, 300).unwrap();
        // Ceiling should be None: coord x=1 is covered by the inside version.
        assert!(
            result.ceiling_tile.is_none(),
            "inside version covers x=1; ceiling must be empty"
        );
        assert_eq!(result.cells_carried_forward, 0);
        assert_eq!(result.keep_inhorizon, vec![TileId::new(0, 400)]);
    }

    // ── Test 6 ──────────────────────────────────────────────────────────────

    /// T=100 Live(A), T=200 GdprErased(A). Horizon=300.
    /// GDPR erasure takes precedence: ceiling does NOT contain A.
    #[test]
    fn gdpr_erasure_drops_cell_outright() {
        let s = schema();
        let bytes = build_segment(vec![
            (TileId::new(0, 100), live_tile(&s, 1, 10)),
            (TileId::new(0, 200), gdpr_tile(&s, 1)),
        ]);
        let reader = SegmentReader::open(&bytes).unwrap();
        let versions: Vec<TileEntry> = reader.tiles().to_vec();
        let result = merge_for_retention(&versions, &reader, &s, 300).unwrap();
        assert!(
            result.ceiling_tile.is_none(),
            "GdprErased newest version must suppress cell from ceiling"
        );
        assert_eq!(result.cells_carried_forward, 0);
        assert_eq!(result.dropped_tile_ids.len(), 2);
    }

    // ── Test 7 ──────────────────────────────────────────────────────────────

    /// Empty input → all result fields empty.
    #[test]
    fn empty_input_returns_empty() {
        let s = schema();
        let bytes = build_segment(vec![]);
        let reader = SegmentReader::open(&bytes).unwrap();
        let result = merge_for_retention(&[], &reader, &s, 300).unwrap();
        assert!(result.ceiling_tile.is_none());
        assert!(result.keep_inhorizon.is_empty());
        assert!(result.dropped_tile_ids.is_empty());
        assert_eq!(result.cells_carried_forward, 0);
    }
}
