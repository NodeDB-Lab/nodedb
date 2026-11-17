// SPDX-License-Identifier: BUSL-1.1

//! Read-your-own-writes merge of a transaction's [`ArrayTxnOverlay`] into
//! the tile set an array read is about to reduce.
//!
//! Every array read (Slice, Aggregate, Elementwise) resolves its
//! base state into sparse tiles, then filters and projects those tiles. This
//! module rewrites that tile set in place, BEFORE the read's own filters run:
//!
//! - base rows whose coordinate this transaction tombstoned OR re-put are
//!   dropped (a staged put shadows the base cell, last-writer-wins);
//! - staged puts are appended as synthetic tiles, one per tile key, so
//!   a `shard_bucket` filter, the slice window, the attribute
//!   projection, the surrogate `cell_filter`, and the row cap all apply to
//!   staged cells exactly as they apply to base cells.
//!
//! Only a live (`Current`) read merges. `AS OF SYSTEM TIME` and the
//! all-versions audit read are snapshots of committed history and never see
//! uncommitted state; the callers gate on that before calling in.

use std::collections::{HashMap, HashSet};

use nodedb_array::ArrayError;
use nodedb_array::schema::ArraySchema;
use nodedb_array::tile::sparse_tile::{RowKind, SparseRow, SparseTile, SparseTileBuilder};
use nodedb_array::tile::tile_id_for_cell;
use nodedb_array::types::ArrayId;
use nodedb_array::types::coord::value::CoordValue;

use crate::data::executor::core_loop::CoreLoop;
use crate::types::TxnId;

use super::array_staged::ArrayTxnOverlay;

/// An error of the array overlay merge: a tile rewrite, or a spilled
/// overlay tier that cannot be read.
#[derive(Debug, thiserror::Error)]
pub enum ArrayOverlayMergeError {
    #[error(transparent)]
    Array(#[from] ArrayError),
    #[error(transparent)]
    Overlay(#[from] crate::Error),
}

/// Inputs for [`CoreLoop::merge_array_overlay_tiles`].
pub(in crate::data::executor) struct ArrayOverlayMergeParams<'a> {
    /// The read's transaction, from `task.request.txn_id`. `None` (autocommit)
    /// merges nothing.
    pub txn_id: Option<TxnId>,
    pub array_id: &'a ArrayId,
    pub schema: &'a ArraySchema,
    /// Valid-time point filter the read applies to base cells; applied to
    /// staged puts the same way. `None` = no valid-time filter.
    pub valid_at_ms: Option<i64>,
}

impl CoreLoop {
    /// Merge the transaction's staged cells for `p.array_id` into `tiles`
    /// (`(hilbert_prefix, tile)` pairs, the `scan_tiles_at` shape). A no-op
    /// when the task carries no transaction or the transaction never staged
    /// on this array. Refreshes the overlay lease on every merge so a long
    /// read-only transaction never ages out.
    pub(in crate::data::executor) fn merge_array_overlay_tiles(
        &self,
        p: ArrayOverlayMergeParams<'_>,
        tiles: &mut Vec<(u64, SparseTile)>,
    ) -> Result<(), ArrayOverlayMergeError> {
        let Some(txn_id) = p.txn_id else {
            return Ok(());
        };
        let Some(overlay) = self.array_txn_overlays.get(&txn_id) else {
            return Ok(());
        };
        if !overlay.touches(p.array_id) {
            return Ok(());
        }
        self.touch_overlay(txn_id);
        merge_overlay(overlay, p.array_id, p.schema, p.valid_at_ms, tiles)
    }
}

fn merge_overlay(
    overlay: &ArrayTxnOverlay,
    array_id: &ArrayId,
    schema: &ArraySchema,
    valid_at_ms: Option<i64>,
    tiles: &mut Vec<(u64, SparseTile)>,
) -> Result<(), ArrayOverlayMergeError> {
    // Every coordinate this transaction wrote shadows its base cell: a
    // tombstone hides it, a staged put replaces it. The staged cells are the
    // read's own result rows, so they are held for the merge.
    let mut shadowed: HashSet<Vec<CoordValue>> = HashSet::new();
    for coord in overlay.tombstoned_coords(array_id) {
        shadowed.insert(coord?);
    }
    let mut puts = Vec::new();
    for cell in overlay.staged_cells(array_id) {
        let (coord, put) = cell?;
        shadowed.insert(coord.clone());
        puts.push((coord, put));
    }
    if !shadowed.is_empty() {
        for (_, tile) in tiles.iter_mut() {
            if tile_has_coord(tile, &shadowed) {
                *tile = retain_rows(schema, tile, |coord| !shadowed.contains(coord))?;
            }
        }
        tiles.retain(|(_, tile)| tile.row_count() > 0);
    }

    // Staged puts, grouped by the tile key each would land in so a shard
    // bucket filter treats them like base tiles.
    let mut by_prefix: HashMap<u64, SparseTileBuilder<'_>> = HashMap::new();
    for (coord, put) in &puts {
        if let Some(vt) = valid_at_ms
            && !(put.valid_from_ms <= vt && vt < put.valid_until_ms)
        {
            continue;
        }
        let prefix = tile_id_for_cell(schema, coord, put.system_from_ms)?.hilbert_prefix;
        by_prefix
            .entry(prefix)
            .or_insert_with(|| SparseTileBuilder::new(schema))
            .push_row(SparseRow::live(
                coord,
                &put.attrs,
                put.surrogate,
                put.valid_from_ms,
                put.valid_until_ms,
            ))?;
    }
    let mut staged: Vec<(u64, SparseTile)> = by_prefix
        .into_iter()
        .map(|(prefix, builder)| (prefix, builder.build()))
        .collect();
    // Deterministic output order across HashMap iteration.
    staged.sort_by_key(|(prefix, _)| *prefix);
    tiles.extend(staged);
    Ok(())
}

/// The coordinate tuple of row `row` in `tile`.
fn row_coord(tile: &SparseTile, row: usize) -> Result<Vec<CoordValue>, ArrayError> {
    tile.dim_dicts
        .iter()
        .map(|dict| {
            let idx = *dict
                .indices
                .get(row)
                .ok_or_else(|| ArrayError::SegmentCorruption {
                    detail: format!("array overlay merge: row {row} index out of range"),
                })? as usize;
            dict.values
                .get(idx)
                .cloned()
                .ok_or_else(|| ArrayError::SegmentCorruption {
                    detail: format!("array overlay merge: dict entry {idx} out of range"),
                })
        })
        .collect()
}

fn tile_has_coord(tile: &SparseTile, coords: &HashSet<Vec<CoordValue>>) -> bool {
    (0..tile.row_count())
        .any(|row| row_coord(tile, row).is_ok_and(|coord| coords.contains(coord.as_slice())))
}

/// Copy of `tile` keeping only rows whose coordinate satisfies `keep`.
/// Sentinel rows (tombstone / erasure) are carried across unchanged so a
/// raw-scan consumer still observes them.
fn retain_rows(
    schema: &ArraySchema,
    tile: &SparseTile,
    keep: impl Fn(&[CoordValue]) -> bool,
) -> Result<SparseTile, ArrayError> {
    let mut builder = SparseTileBuilder::new(schema);
    let mut live_idx = 0usize;
    for row in 0..tile.row_count() {
        let kind = tile.row_kind(row)?;
        let attr_row = live_idx;
        if kind == RowKind::Live {
            live_idx += 1;
        }
        let coord = row_coord(tile, row)?;
        if !keep(&coord) {
            continue;
        }
        let attrs: Vec<_> = if kind == RowKind::Live {
            tile.attr_cols
                .iter()
                .map(|col| {
                    col.get(attr_row)
                        .cloned()
                        .ok_or_else(|| ArrayError::SegmentCorruption {
                            detail: format!(
                                "array overlay merge: attr row {attr_row} out of range"
                            ),
                        })
                })
                .collect::<Result<_, _>>()?
        } else {
            Vec::new()
        };
        // A live row keeps its bound surrogate; a tombstone or erasure row
        // keeps none.
        let surrogate = match kind {
            RowKind::Live => Some(tile.live_surrogate(row)?),
            RowKind::Tombstone | RowKind::GdprErased => None,
        };
        let valid_from_ms = tile.valid_from_ms.get(row).copied().unwrap_or(0);
        let valid_until_ms = tile
            .valid_until_ms
            .get(row)
            .copied()
            .unwrap_or(nodedb_types::OPEN_UPPER);
        builder.push_row(SparseRow {
            coord: &coord,
            attrs: &attrs,
            surrogate,
            valid_from_ms,
            valid_until_ms,
            kind,
        })?;
    }
    Ok(builder.build())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::executor::handlers::transaction::overlay::StagedCellPut;
    use nodedb_array::schema::ArraySchemaBuilder;
    use nodedb_array::schema::attr_spec::{AttrSpec, AttrType};
    use nodedb_array::schema::dim_spec::{DimSpec, DimType};
    use nodedb_array::types::cell_value::value::CellValue;
    use nodedb_array::types::domain::{Domain, DomainBound};
    use nodedb_types::{Surrogate, TenantId};

    fn schema() -> ArraySchema {
        ArraySchemaBuilder::new("a")
            .dim(DimSpec::new(
                "x",
                DimType::Int64,
                Domain::new(DomainBound::Int64(0), DomainBound::Int64(15)),
            ))
            .attr(AttrSpec::new("v", AttrType::Int64, true))
            .tile_extents(vec![4])
            .build()
            .unwrap()
    }

    fn aid() -> ArrayId {
        ArrayId::new(TenantId::new(1), "a")
    }

    fn coord(x: i64) -> Vec<CoordValue> {
        vec![CoordValue::Int64(x)]
    }

    fn put(v: i64) -> StagedCellPut {
        StagedCellPut {
            attrs: vec![CellValue::Int64(v)],
            surrogate: Surrogate::new(1000 + v as u32),
            system_from_ms: 0,
            valid_from_ms: 0,
            valid_until_ms: i64::MAX,
        }
    }

    fn base_tile(schema: &ArraySchema, cells: &[(i64, i64)]) -> SparseTile {
        let mut b = SparseTileBuilder::new(schema);
        for (x, v) in cells {
            b.push_row(SparseRow::live(
                &coord(*x),
                &[CellValue::Int64(*v)],
                Surrogate::new(*x as u32 + 1),
                0,
                i64::MAX,
            ))
            .unwrap();
        }
        b.build()
    }

    fn cells_of(tiles: &[(u64, SparseTile)]) -> Vec<(i64, i64)> {
        let mut out = Vec::new();
        for (_, tile) in tiles {
            let mut live = 0usize;
            for row in 0..tile.row_count() {
                if tile.row_kind(row).unwrap() != RowKind::Live {
                    continue;
                }
                let CoordValue::Int64(x) =
                    tile.dim_dicts[0].values[tile.dim_dicts[0].indices[row] as usize]
                else {
                    panic!("int coord");
                };
                let CellValue::Int64(v) = tile.attr_cols[0][live] else {
                    panic!("int attr");
                };
                live += 1;
                out.push((x, v));
            }
        }
        out.sort_unstable();
        out
    }

    #[test]
    fn tombstone_drops_base_row_and_keeps_siblings() {
        let schema = schema();
        let mut overlay = ArrayTxnOverlay::new();
        overlay.stage_cell_delete(aid(), coord(1)).unwrap();
        let mut tiles = vec![(0u64, base_tile(&schema, &[(1, 10), (2, 20)]))];
        merge_overlay(&overlay, &aid(), &schema, None, &mut tiles).unwrap();
        assert_eq!(cells_of(&tiles), vec![(2, 20)]);
    }

    #[test]
    fn staged_put_shadows_base_row_and_appends() {
        let schema = schema();
        let mut overlay = ArrayTxnOverlay::new();
        overlay.stage_cell_put(aid(), coord(1), put(99)).unwrap();
        overlay.stage_cell_put(aid(), coord(9), put(9)).unwrap();
        let mut tiles = vec![(0u64, base_tile(&schema, &[(1, 10), (2, 20)]))];
        merge_overlay(&overlay, &aid(), &schema, None, &mut tiles).unwrap();
        assert_eq!(cells_of(&tiles), vec![(1, 99), (2, 20), (9, 9)]);
    }

    #[test]
    fn empty_base_tile_is_dropped_after_tombstoning() {
        let schema = schema();
        let mut overlay = ArrayTxnOverlay::new();
        overlay.stage_cell_delete(aid(), coord(1)).unwrap();
        let mut tiles = vec![(0u64, base_tile(&schema, &[(1, 10)]))];
        merge_overlay(&overlay, &aid(), &schema, None, &mut tiles).unwrap();
        assert!(tiles.is_empty());
    }

    #[test]
    fn valid_at_filters_staged_puts() {
        let schema = schema();
        let mut overlay = ArrayTxnOverlay::new();
        overlay
            .stage_cell_put(
                aid(),
                coord(1),
                StagedCellPut {
                    valid_from_ms: 100,
                    valid_until_ms: 200,
                    ..put(1)
                },
            )
            .unwrap();
        let mut tiles = Vec::new();
        merge_overlay(&overlay, &aid(), &schema, Some(50), &mut tiles).unwrap();
        assert!(tiles.is_empty());
        merge_overlay(&overlay, &aid(), &schema, Some(150), &mut tiles).unwrap();
        assert_eq!(cells_of(&tiles), vec![(1, 1)]);
    }

    #[test]
    fn other_array_is_untouched() {
        let schema = schema();
        let mut overlay = ArrayTxnOverlay::new();
        overlay
            .stage_cell_put(ArrayId::new(TenantId::new(1), "b"), coord(1), put(1))
            .unwrap();
        let mut tiles = vec![(0u64, base_tile(&schema, &[(1, 10)]))];
        merge_overlay(&overlay, &aid(), &schema, None, &mut tiles).unwrap();
        assert_eq!(cells_of(&tiles), vec![(1, 10)]);
    }
}
