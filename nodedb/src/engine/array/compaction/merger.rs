// SPDX-License-Identifier: BUSL-1.1

//! Merge a set of segments into one new segment.
//!
//! Algorithm: read all input tiles into a `BTreeMap<TileId, MergedTile>`,
//! folding cells through a per-tile [`SparseTileBuilder`]. Inputs are
//! ordered by flush_lsn ascending (the picker enforces this), so when
//! two segments contain the same coord in the same tile the *later*
//! flush overwrites the earlier one — last-write-wins semantics that
//! match the memtable.
//!
//! When a retention horizon is set, the merger applies cell-level retention
//! after the initial merge. Tile-versions are sparse: each version only
//! stores the cells written at that system-time ordinal. The retention
//! merger therefore operates at cell granularity per Hilbert prefix group,
//! not at the tile-version level, to avoid dropping cells that live
//! exclusively in an older out-of-horizon version.
//!
//! The output is written via [`SegmentWriter`] in TileId order, which
//! preserves Hilbert ordering for the next compaction pass.

use std::collections::{BTreeMap, HashMap};

use nodedb_array::ArrayResult;
use nodedb_array::query::retention::encode_coord_key;
use nodedb_array::schema::ArraySchema;
use nodedb_array::segment::reader::TilePayload;
use nodedb_array::segment::writer::SegmentWriter;
use nodedb_array::tile::dense_tile::DenseTile;
use nodedb_array::tile::sparse_tile::{RowKind, SparseRow, SparseTile, SparseTileBuilder};
use nodedb_array::types::TileId;
use nodedb_array::types::cell_value::value::CellValue;
use nodedb_array::types::coord::value::CoordValue;

use crate::engine::array::store::{ArrayStore, SegmentRef, segment_handle::SegmentHandleError};

#[derive(Debug, thiserror::Error)]
pub enum CompactionError {
    #[error(transparent)]
    Array(#[from] nodedb_array::ArrayError),
    #[error(transparent)]
    Segment(#[from] SegmentHandleError),
    #[error("compaction io: {detail}")]
    Io { detail: String },
}

/// Result of a merge — the new segment file is already on disk and
/// fsync'd. Caller integrates it into the manifest via
/// `ArrayStore::replace_segments`.
pub struct CompactionOutput {
    pub segment_ref: SegmentRef,
    pub removed: Vec<String>,
}

pub struct CompactionMerger;

impl CompactionMerger {
    /// Merge `inputs` into one new segment named `output_id` at
    /// `output_level`. `flush_lsn` on the new ref is the max of the inputs'
    /// lsns (no new WAL writes happen during compaction — recovery already
    /// covers the inputs).
    ///
    /// `output_id` MUST come from [`ArrayStore::allocate_segment_id`], the
    /// store's single monotonic allocator. Deriving a name here instead —
    /// e.g. `max(input seq) + 1` — hands back a number the allocator has not
    /// consumed, so the next flush allocates the same name and renames its
    /// segment over the live compacted one, leaving the manifest naming a
    /// single file at two levels. Allocation happens in the caller because
    /// merging only needs a shared borrow of the store.
    ///
    /// When `retention_horizon` is `Some(h)`, the tile versions at or below
    /// the ordinal `h` are collapsed into a single synthetic ceiling tile per
    /// Hilbert prefix, stamped `h`. Each coordinate's newest state at or
    /// below `h`, the one a read at `h` sees, is carried forward (unless
    /// GDPR-erased), so no cell visible at the horizon is lost.
    pub fn run(
        store: &ArrayStore,
        inputs: &[String],
        output_id: String,
        output_level: u8,
        retention_horizon: Option<i64>,
    ) -> Result<CompactionOutput, CompactionError> {
        let schema = store.schema().clone();
        let schema_hash = store.schema_hash();
        let mut merged: BTreeMap<TileId, MergedTile> = BTreeMap::new();
        let mut max_flush_lsn: u64 = 0;
        for id in inputs {
            let manifest_ref = store
                .manifest()
                .segments
                .iter()
                .find(|s| &s.id == id)
                .ok_or_else(|| CompactionError::Io {
                    detail: format!("compaction input not in manifest: {id}"),
                })?;
            max_flush_lsn = max_flush_lsn.max(manifest_ref.flush_lsn);
            let handle = store.segments.get(id).ok_or_else(|| CompactionError::Io {
                detail: format!("compaction input has no open handle: {id}"),
            })?;
            let reader = handle.reader();
            for (tile_idx, entry) in reader.tiles().iter().enumerate() {
                let tile_id = entry.tile_id;
                let payload = reader.read_tile(tile_idx)?;
                let tile = merged
                    .entry(tile_id)
                    .or_insert_with(|| MergedTile::empty(entry.placement));
                // One tile version is one write's: every copy of it carries
                // the same placement.
                if tile.placement != entry.placement {
                    return Err(nodedb_array::ArrayError::SegmentCorruption {
                        detail: format!(
                            "compaction: tile version ({}, {}) is stored under vShards {} \
                             and {}",
                            tile_id.hilbert_prefix,
                            tile_id.system_from_ms,
                            tile.placement,
                            entry.placement
                        ),
                    }
                    .into());
                }
                tile.absorb(&schema, &payload)?;
            }
        }

        // Apply retention if configured.
        let merged = match retention_horizon {
            None => merged,
            Some(horizon) => apply_retention(merged, horizon)?,
        };

        let kek = store.kek().cloned();
        let root = store.root().to_path_buf();
        let writer_bytes =
            build_segment_bytes(&schema, schema_hash, kek.as_ref(), merged.into_iter())?;
        // The shared WAL helper, as the flush path uses: compaction unlinks its
        // inputs once the manifest names this file, so a rename that reaches
        // disk ahead of the data pages would leave the merged cells with no
        // surviving copy.
        nodedb_wal::segment::atomic_write_fsync(&root, &output_id, &writer_bytes).map_err(|e| {
            CompactionError::Io {
                detail: format!("write merged segment {:?}: {e}", root.join(&output_id)),
            }
        })?;

        // Reopen from the written bytes to pull tile bounds.
        // When encryption is active, writer_bytes is an encrypted SEGA blob;
        // use OwnedSegmentReader to decrypt before inspecting tiles.
        let (min_tile, max_tile, tile_count) = {
            let owned = nodedb_array::segment::reader::OwnedSegmentReader::open_with_kek(
                &writer_bytes,
                kek.as_ref(),
            )?;
            let reader = owned.reader();
            let (mn, mx) = match (reader.tiles().first(), reader.tiles().last()) {
                (Some(a), Some(b)) => (a.tile_id, b.tile_id),
                _ => (TileId::snapshot(0), TileId::snapshot(0)),
            };
            (mn, mx, reader.tile_count() as u32)
        };
        let segment_ref = SegmentRef {
            id: output_id,
            level: output_level,
            min_tile,
            max_tile,
            tile_count,
            flush_lsn: max_flush_lsn,
        };
        Ok(CompactionOutput {
            segment_ref,
            removed: inputs.to_vec(),
        })
    }
}

/// Apply cell-level retention to the merged tile map.
///
/// For each Hilbert prefix group:
/// - Tile versions above the ordinal `horizon` pass through unchanged.
/// - Tile versions at or below `horizon` are collapsed into one synthetic
///   ceiling tile stamped `horizon`:
///     - For each coordinate, the newest row at or below `horizon` wins: the
///       row a read at `horizon` sees. A newer row above the horizon does not
///       remove it, since a read between the horizon and that row still
///       needs it.
///     - GDPR-erased coordinates are excluded entirely — the erasure is
///       propagated by omission.
///     - A tombstone is kept: a segment outside this merge can still hold an
///       older version of its cell that the tombstone hides.
///
/// The ceiling tile sorts before every version above the horizon within the
/// same prefix (TileId order is `(prefix, system_from_ms)` ascending).
fn apply_retention(
    merged: BTreeMap<TileId, MergedTile>,
    horizon: i64,
) -> Result<BTreeMap<TileId, MergedTile>, CompactionError> {
    // Group tile versions by hilbert_prefix.
    let mut by_prefix: HashMap<u64, Vec<(TileId, MergedTile)>> = HashMap::new();
    for (tile_id, mt) in merged {
        by_prefix
            .entry(tile_id.hilbert_prefix)
            .or_default()
            .push((tile_id, mt));
    }

    let mut out: BTreeMap<TileId, MergedTile> = BTreeMap::new();

    for (prefix, versions) in by_prefix {
        // Partition into versions above the horizon and versions at or
        // below it.
        let mut outside: Vec<(TileId, MergedTile)> = Vec::new();
        for (tile_id, mt) in versions {
            if tile_id.system_from_ms > horizon {
                out.insert(tile_id, mt);
            } else {
                outside.push((tile_id, mt));
            }
        }

        // Sort the versions at or below the horizon newest → oldest.
        outside.sort_by_key(|(tid, _)| std::cmp::Reverse(tid.system_from_ms));
        // The ceiling stands for the newest collapsed version, so it keeps
        // that version's placement. No version means nothing to collapse.
        let Some(placement) = outside.first().map(|(_, mt)| mt.placement) else {
            continue;
        };

        // Build ceiling: coord → its newest row at or below the horizon.
        let mut ceiling_rows: HashMap<Vec<u8>, MergedRow> = HashMap::new();
        for (_tile_id, mt) in outside {
            for row in mt.rows {
                let key = encode_coord_key(&row.coord)?;
                // First occurrence (newest) wins.
                ceiling_rows.entry(key).or_insert(row);
            }
        }

        // Build synthetic ceiling MergedTile, excluding GDPR-erased coords.
        let mut ceiling_tile = MergedTile::empty(placement);
        // Deterministic order for stable segment output.
        let mut ceiling_vec: Vec<(Vec<u8>, MergedRow)> = ceiling_rows.into_iter().collect();
        ceiling_vec.sort_by(|a, b| a.0.cmp(&b.0));
        for (_key, row) in ceiling_vec {
            if row.kind == RowKind::GdprErased {
                // GDPR erasure: drop from ceiling entirely.
                continue;
            }
            ceiling_tile.rows.push(row);
        }

        if !ceiling_tile.rows.is_empty() {
            out.insert(TileId::new(prefix, horizon), ceiling_tile);
        }
    }

    Ok(out)
}

fn build_segment_bytes(
    schema: &ArraySchema,
    schema_hash: u64,
    kek: Option<&nodedb_wal::crypto::WalEncryptionKey>,
    tiles: impl Iterator<Item = (TileId, MergedTile)>,
) -> ArrayResult<Vec<u8>> {
    let mut writer = SegmentWriter::new(schema_hash);
    for (tile_id, mt) in tiles {
        let placement = mt.placement;
        let tile = mt.into_sparse(schema)?;
        // Skip tiles that have neither live rows nor sentinel rows.
        let has_any_rows = tile.nnz() > 0 || !tile.row_kinds.is_empty();
        if !has_any_rows {
            continue;
        }
        writer.append_sparse(tile_id, placement, &tile)?;
    }
    writer.finish(kek)
}

/// Row record inside a [`MergedTile`] accumulator.
struct MergedRow {
    coord: Vec<CoordValue>,
    attrs: Vec<CellValue>,
    /// `Some` for a live row, `None` for a tombstone or erasure row.
    surrogate: Option<nodedb_types::Surrogate>,
    valid_from_ms: i64,
    valid_until_ms: i64,
    kind: RowKind,
}

/// Per-tile merge accumulator. Stores the in-progress `coord → row` map so
/// subsequent absorbs can override earlier versions — last-write-wins.
/// Sentinel rows (Tombstone, GdprErased) are stored and passed through
/// unchanged. Retention logic in [`apply_retention`] handles cell-level
/// expiry when a retention horizon is set.
struct MergedTile {
    rows: Vec<MergedRow>,
    /// The vShard whose write stored the tile version.
    placement: u32,
}

impl MergedTile {
    fn empty(placement: u32) -> Self {
        Self {
            rows: Vec::new(),
            placement,
        }
    }

    fn absorb(&mut self, schema: &ArraySchema, payload: &TilePayload) -> ArrayResult<()> {
        match payload {
            TilePayload::Sparse(tile) => self.absorb_sparse(schema, tile),
            TilePayload::Dense(tile) => self.absorb_dense(schema, tile),
        }
    }

    fn absorb_sparse(&mut self, _schema: &ArraySchema, tile: &SparseTile) -> ArrayResult<()> {
        let n = tile.row_count();
        // Attribute columns hold live rows only, so they are read at the
        // live-row index, not the physical row index.
        let mut live_row = 0usize;
        for row in 0..n {
            let coord: Vec<CoordValue> = tile
                .dim_dicts
                .iter()
                .map(|d| d.values[d.indices[row] as usize].clone())
                .collect();
            let kind = tile.row_kind(row)?;
            let (attrs, surrogate, valid_from_ms, valid_until_ms) = match kind {
                RowKind::Live => {
                    let attrs = tile
                        .attr_cols
                        .iter()
                        .map(|col| {
                            col.get(live_row).cloned().ok_or_else(|| {
                                nodedb_array::ArrayError::SegmentCorruption {
                                    detail: format!(
                                        "array compaction: attr column has no live row \
                                         {live_row} for tile row {row}"
                                    ),
                                }
                            })
                        })
                        .collect::<ArrayResult<Vec<CellValue>>>()?;
                    live_row += 1;
                    // A stored live row always holds its bound surrogate.
                    let surrogate = tile.live_surrogate(row)?;
                    let vf = tile.valid_from_ms.get(row).copied().unwrap_or(0);
                    let vu = tile
                        .valid_until_ms
                        .get(row)
                        .copied()
                        .unwrap_or(nodedb_types::OPEN_UPPER);
                    (attrs, Some(surrogate), vf, vu)
                }
                RowKind::Tombstone | RowKind::GdprErased => {
                    (Vec::new(), None, 0, nodedb_types::OPEN_UPPER)
                }
            };
            self.upsert(MergedRow {
                coord,
                attrs,
                surrogate,
                valid_from_ms,
                valid_until_ms,
                kind,
            });
        }
        Ok(())
    }

    fn absorb_dense(&mut self, _schema: &ArraySchema, _tile: &DenseTile) -> ArrayResult<()> {
        Err(nodedb_array::ArrayError::SegmentCorruption {
            detail:
                "compaction merger received a dense tile; only sparse tiles are produced by flush"
                    .into(),
        })
    }

    fn upsert(&mut self, new_row: MergedRow) {
        if let Some(slot) = self.rows.iter_mut().find(|r| r.coord == new_row.coord) {
            *slot = new_row;
        } else {
            self.rows.push(new_row);
        }
    }

    fn into_sparse(self, schema: &ArraySchema) -> ArrayResult<SparseTile> {
        let mut b = SparseTileBuilder::new(schema);
        for row in self.rows {
            b.push_row(SparseRow {
                coord: &row.coord,
                attrs: &row.attrs,
                surrogate: row.surrogate,
                valid_from_ms: row.valid_from_ms,
                valid_until_ms: row.valid_until_ms,
                kind: row.kind,
            })?;
        }
        Ok(b.build())
    }
}

#[cfg(test)]
mod tests {
    use crate::engine::array::engine::{ArrayEngine, ArrayEngineConfig};
    use crate::engine::array::test_support::{aid, flush_if_full, put_one, schema};
    use crate::engine::array::wal::ArrayPutCell;
    use nodedb_array::types::cell_value::value::CellValue;
    use nodedb_array::types::coord::value::CoordValue;
    use tempfile::TempDir;

    fn put_versioned(e: &mut ArrayEngine, x: i64, y: i64, v: i64, sys_ms: i64, lsn: u64) {
        e.put_cells(
            &aid(),
            0,
            vec![ArrayPutCell {
                coord: vec![CoordValue::Int64(x), CoordValue::Int64(y)],
                attrs: vec![CellValue::Int64(v)],
                surrogate: nodedb_types::Surrogate::new((x * 100 + y + 1) as u32),
                system_from_ms: sys_ms,
                valid_from_ms: 0,
                valid_until_ms: i64::MAX,
            }],
            lsn,
        )
        .unwrap();
        flush_if_full(e, &aid(), lsn);
    }

    #[test]
    fn versioned_tiles_preserved_through_merge() {
        // Write 4 versions of the same cell at distinct system_from_ms so each
        // flush produces one L0 segment with a unique TileId.
        let dir = TempDir::new().unwrap();
        let mut cfg = ArrayEngineConfig::new(dir.path().to_path_buf());
        cfg.flush_cell_threshold = 1;
        let mut e = ArrayEngine::new(cfg).unwrap();
        e.open_array(aid(), schema(), 0x1).unwrap();
        put_versioned(&mut e, 0, 0, 10, 100, 1);
        put_versioned(&mut e, 0, 0, 20, 200, 2);
        put_versioned(&mut e, 0, 0, 30, 300, 3);
        put_versioned(&mut e, 0, 0, 40, 400, 4);
        assert_eq!(e.store(&aid()).unwrap().manifest().segments.len(), 4);
        let merged = e.maybe_compact(&aid(), None).unwrap();
        assert!(merged);
        let m = e.store(&aid()).unwrap().manifest();
        assert_eq!(m.segments.len(), 1);
        // All 4 tile versions (distinct system_from_ms) must survive.
        assert_eq!(m.segments[0].tile_count, 4);
    }

    /// A merge with a horizon keeps the version a read at the horizon sees,
    /// even when a newer version above the horizon supersedes it, and drops
    /// the versions that version supersedes.
    #[test]
    fn retention_keeps_the_version_visible_at_the_horizon() {
        use nodedb_array::query::ceiling::CeilingResult;

        let dir = TempDir::new().unwrap();
        let mut cfg = ArrayEngineConfig::new(dir.path().to_path_buf());
        cfg.flush_cell_threshold = 1;
        let mut e = ArrayEngine::new(cfg).unwrap();
        e.open_array(aid(), schema(), 0x1).unwrap();
        put_versioned(&mut e, 0, 0, 10, 100, 1);
        put_versioned(&mut e, 0, 0, 20, 200, 2);
        put_versioned(&mut e, 0, 0, 30, 300, 3);
        put_versioned(&mut e, 0, 0, 40, 400, 4);
        assert!(e.maybe_compact(&aid(), Some(250)).unwrap());

        let store = e.store(&aid()).unwrap();
        assert_eq!(
            store.manifest().segments[0].tile_count,
            3,
            "the ceiling at 250 plus the versions at 300 and 400"
        );
        let at = |sys: i64| match store
            .ceiling_for_coord(&[CoordValue::Int64(0), CoordValue::Int64(0)], sys, None)
            .unwrap()
        {
            CeilingResult::Live(p) => p.attrs,
            other => panic!("expected a live cell at {sys}: {other:?}"),
        };
        assert_eq!(at(250), vec![CellValue::Int64(20)]);
        assert_eq!(at(299), vec![CellValue::Int64(20)]);
        assert_eq!(at(300), vec![CellValue::Int64(30)]);
        assert!(matches!(
            store
                .ceiling_for_coord(&[CoordValue::Int64(0), CoordValue::Int64(0)], 249, None)
                .unwrap(),
            CeilingResult::NotFound
        ));
    }

    /// The compacted segment's name must come out of the store's monotonic
    /// allocator, not from the input names. Deriving `max(input seq) + 1`
    /// yields a number the allocator has not consumed, so the very next flush
    /// allocates the same name and its `rename` lands on top of the live
    /// compacted file — the manifest then names one file at two levels and the
    /// merged cells are gone until a restart re-derives the counter.
    #[test]
    fn compaction_output_name_is_never_handed_out_to_a_later_flush() {
        let dir = TempDir::new().unwrap();
        let mut cfg = ArrayEngineConfig::new(dir.path().to_path_buf());
        cfg.flush_cell_threshold = 1;
        let mut e = ArrayEngine::new(cfg).unwrap();
        e.open_array(aid(), schema(), 0x1).unwrap();
        // Four threshold flushes → four L0 segments → the picker fires.
        for i in 0..4 {
            let lsn = (i as u64) + 1;
            put_one(&mut e, i, 0, i, lsn);
            flush_if_full(&mut e, &aid(), lsn);
        }
        assert!(e.maybe_compact(&aid(), None).unwrap());

        let (root, compacted_id) = {
            let store = e.store(&aid()).unwrap();
            let m = store.manifest();
            assert_eq!(m.segments.len(), 1);
            (store.root().to_path_buf(), m.segments[0].id.clone())
        };
        let compacted_bytes = std::fs::read(root.join(&compacted_id)).unwrap();

        // The next flush must not be able to claim that name.
        put_one(&mut e, 5, 0, 5, 5);
        flush_if_full(&mut e, &aid(), 5);

        let m = e.store(&aid()).unwrap().manifest();
        let ids: Vec<&str> = m.segments.iter().map(|s| s.id.as_str()).collect();
        let unique: std::collections::HashSet<&str> = ids.iter().copied().collect();
        assert_eq!(
            unique.len(),
            ids.len(),
            "manifest names the same segment file twice: {ids:?}"
        );
        assert_eq!(
            std::fs::read(root.join(&compacted_id)).unwrap(),
            compacted_bytes,
            "the flush overwrote the live compacted segment {compacted_id}"
        );
    }

    #[test]
    fn merger_preserves_tombstone_and_erasure_rows_inside_horizon() {
        use crate::engine::array::wal::ArrayDeleteCell;
        use nodedb_array::segment::SegmentReader;
        use nodedb_array::tile::sparse_tile::RowKind;

        // Write a live cell, tombstone, and erasure — each in their own flush
        // so we get separate segments — then compact and confirm all three kinds
        // survive in the merged output.
        let dir = TempDir::new().unwrap();
        let mut cfg = ArrayEngineConfig::new(dir.path().to_path_buf());
        cfg.flush_cell_threshold = 1;
        let mut e = ArrayEngine::new(cfg).unwrap();
        e.open_array(aid(), schema(), 0x1).unwrap();

        // Segment 1: live cell at (1,0)
        put_one(&mut e, 1, 0, 10, 1);
        e.flush(&aid(), crate::types::replay_stamp::ReplayStamp::through(2))
            .unwrap();

        // Segment 2: tombstone at (2,0) system=200
        e.delete_cells(
            &aid(),
            0,
            vec![ArrayDeleteCell {
                coord: vec![CoordValue::Int64(2), CoordValue::Int64(0)],
                system_from_ms: 200,
                erasure: false,
            }],
            3,
        )
        .unwrap();
        e.flush(&aid(), crate::types::replay_stamp::ReplayStamp::through(4))
            .unwrap();

        // Segment 3: GDPR erasure at (3,0) system=300
        e.gdpr_erase_cell(
            &aid(),
            0,
            vec![CoordValue::Int64(3), CoordValue::Int64(0)],
            300,
            5,
        )
        .unwrap();
        e.flush(&aid(), crate::types::replay_stamp::ReplayStamp::through(6))
            .unwrap();

        // Segment 4: another live cell to reach the L0_TRIGGER threshold.
        put_one(&mut e, 4, 0, 40, 7);
        e.flush(&aid(), crate::types::replay_stamp::ReplayStamp::through(8))
            .unwrap();

        loop {
            if !e.maybe_compact(&aid(), None).unwrap() {
                break;
            }
        }

        let store = e.store(&aid()).unwrap();
        let mut found_tombstone = false;
        let mut found_erased = false;
        for seg in &store.manifest().segments {
            let seg_path = store.root().join(&seg.id);
            let bytes = std::fs::read(&seg_path).unwrap();
            let reader = SegmentReader::open(&bytes).unwrap();
            for idx in 0..reader.tile_count() {
                if let nodedb_array::segment::TilePayload::Sparse(tile) =
                    reader.read_tile(idx).unwrap()
                {
                    for &kind_byte in &tile.row_kinds {
                        match RowKind::from_u8(kind_byte).unwrap() {
                            RowKind::Tombstone => found_tombstone = true,
                            RowKind::GdprErased => found_erased = true,
                            RowKind::Live => {}
                        }
                    }
                }
            }
        }
        assert!(
            found_tombstone,
            "merged segment must preserve tombstone rows"
        );
        assert!(
            found_erased,
            "merged segment must preserve GDPR-erased rows"
        );
    }

    /// A live row after a tombstone row reads its own attributes and keeps
    /// its own surrogate: attribute columns hold live rows only, and the
    /// tombstone carries no identity.
    #[test]
    fn a_live_row_after_a_tombstone_keeps_its_attrs_and_identity() {
        use super::MergedTile;
        use nodedb_array::tile::sparse_tile::{RowKind, SparseRow, SparseTileBuilder};

        let s = schema();
        let mut b = SparseTileBuilder::new(&s);
        let dead = [CoordValue::Int64(1), CoordValue::Int64(1)];
        let live = [CoordValue::Int64(2), CoordValue::Int64(2)];
        b.push_row(SparseRow::sentinel(&dead, RowKind::Tombstone))
            .unwrap();
        b.push_row(SparseRow::live(
            &live,
            &[CellValue::Int64(7)],
            nodedb_types::Surrogate::new(9),
            0,
            i64::MAX,
        ))
        .unwrap();
        let mut merged = MergedTile::empty(0);
        merged.absorb_sparse(&s, &b.build()).unwrap();
        let tile = merged.into_sparse(&s).unwrap();
        tile.check_stored_identities().unwrap();
        assert_eq!(tile.attr_cols[0], vec![CellValue::Int64(7)]);
        assert_eq!(
            tile.surrogates,
            vec![None, Some(nodedb_types::Surrogate::new(9))]
        );
    }
}
