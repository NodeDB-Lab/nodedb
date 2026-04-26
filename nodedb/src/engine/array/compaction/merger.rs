//! Merge a set of segments into one new segment.
//!
//! Algorithm: read all input tiles into a `BTreeMap<TileId, MergedTile>`,
//! folding cells through a per-tile [`SparseTileBuilder`]. Inputs are
//! ordered by flush_lsn ascending (the picker enforces this), so when
//! two segments contain the same coord in the same tile the *later*
//! flush overwrites the earlier one — last-write-wins semantics that
//! match the memtable.
//!
//! When `audit_retain_ms` is set, the merger applies cell-level retention
//! after the initial merge. Tile-versions are sparse: each version only
//! stores the cells written at that `system_from_ms`. The retention merger
//! therefore operates at cell granularity per Hilbert prefix group, not at
//! the tile-version level, to avoid dropping cells that live exclusively in
//! an older out-of-horizon version.
//!
//! The output is written via [`SegmentWriter`] in TileId order, which
//! preserves Hilbert ordering for the next compaction pass.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::Path;

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
    /// Merge `inputs` into one new segment at `output_level`. `flush_lsn`
    /// on the new ref is the max of the inputs' lsns (no new WAL writes
    /// happen during compaction — recovery already covers the inputs).
    ///
    /// When `audit_retain_ms` is `Some(w)`, out-of-horizon tile versions
    /// (those with `system_from_ms < now_ms - w`) are collapsed into a
    /// single synthetic ceiling tile per Hilbert prefix. Cell-level semantics
    /// are preserved: each coordinate's newest out-of-horizon state is
    /// carried forward (unless GDPR-erased), so no cells are silently lost.
    pub fn run(
        store: &ArrayStore,
        inputs: &[String],
        output_level: u8,
        audit_retain_ms: Option<i64>,
        now_ms: i64,
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
                merged
                    .entry(tile_id)
                    .or_insert_with(|| MergedTile::empty(&schema))
                    .absorb(&schema, &payload)?;
            }
        }

        // Apply retention if configured.
        let merged = match audit_retain_ms {
            None => merged,
            Some(retain_ms) => {
                let horizon_ms = now_ms.saturating_sub(retain_ms);
                apply_retention(merged, &schema, horizon_ms)?
            }
        };

        let id = next_segment_id_for_compaction(store, inputs);
        let seg_path = store.root().join(&id);
        let writer_bytes = build_segment_bytes(&schema, schema_hash, merged.into_iter())?;
        write_atomic(&seg_path, &writer_bytes).map_err(|e| CompactionError::Io {
            detail: format!("write merged segment {seg_path:?}: {e}"),
        })?;

        // Reopen from disk to pull a fresh handle for the new bounds.
        let handle = nodedb_array::segment::SegmentReader::open(&writer_bytes)?;
        let (min_tile, max_tile) = match (handle.tiles().first(), handle.tiles().last()) {
            (Some(a), Some(b)) => (a.tile_id, b.tile_id),
            _ => (TileId::snapshot(0), TileId::snapshot(0)),
        };
        let segment_ref = SegmentRef {
            id,
            level: output_level,
            min_tile,
            max_tile,
            tile_count: handle.tile_count() as u32,
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
/// - Tile versions with `system_from_ms >= horizon_ms` are in-horizon and
///   pass through unchanged.
/// - Tile versions with `system_from_ms < horizon_ms` are out-of-horizon.
///   Their cells are collapsed into a single synthetic ceiling tile:
///     - For each coordinate, the newest out-of-horizon row wins.
///     - Coordinates already covered by any in-horizon tile version are
///       excluded from the ceiling (the in-horizon version supersedes them).
///     - GDPR-erased coordinates are excluded entirely — the erasure is
///       propagated by omission.
///
/// The ceiling tile receives `system_from_ms = horizon_ms - 1` so it sorts
/// strictly before all in-horizon tiles within the same prefix in the output
/// segment (TileId order is `(prefix, system_from_ms)` ascending).
fn apply_retention(
    merged: BTreeMap<TileId, MergedTile>,
    schema: &ArraySchema,
    horizon_ms: i64,
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
        // Partition into inside-horizon and outside-horizon.
        let mut inside: Vec<(TileId, MergedTile)> = Vec::new();
        let mut outside: Vec<(TileId, MergedTile)> = Vec::new();
        for (tile_id, mt) in versions {
            if tile_id.system_from_ms >= horizon_ms {
                inside.push((tile_id, mt));
            } else {
                outside.push((tile_id, mt));
            }
        }

        // Collect coord keys present in any in-horizon version so they can
        // be excluded from the ceiling (the in-horizon version supersedes).
        let mut inhorizon_coord_keys: HashSet<Vec<u8>> = HashSet::new();
        for (_tile_id, mt) in &inside {
            for row in &mt.rows {
                let key = encode_coord_key(&row.coord)?;
                inhorizon_coord_keys.insert(key);
            }
        }

        // Pass in-horizon tiles through unchanged.
        for (tile_id, mt) in inside {
            out.insert(tile_id, mt);
        }

        // Nothing to collapse.
        if outside.is_empty() {
            continue;
        }

        // Sort outside-horizon versions newest → oldest.
        outside.sort_by_key(|(tid, _)| std::cmp::Reverse(tid.system_from_ms));

        // Build ceiling: coord → (newest out-of-horizon row).
        // Maps encoded coord key → MergedRow.
        let mut ceiling_rows: HashMap<Vec<u8>, MergedRow> = HashMap::new();
        for (_tile_id, mt) in outside {
            for row in mt.rows {
                let key = encode_coord_key(&row.coord)?;
                // Skip coords already covered by in-horizon versions.
                if inhorizon_coord_keys.contains(&key) {
                    continue;
                }
                // First occurrence (newest) wins.
                ceiling_rows.entry(key).or_insert(row);
            }
        }

        // Build synthetic ceiling MergedTile, excluding GDPR-erased coords.
        let mut ceiling_tile = MergedTile::empty(schema);
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
            // Place ceiling just below the horizon so it sorts before all
            // in-horizon tiles for this prefix.
            let ceiling_sys_ms = horizon_ms.saturating_sub(1);
            let ceiling_tile_id = TileId::new(prefix, ceiling_sys_ms);
            out.insert(ceiling_tile_id, ceiling_tile);
        }
    }

    Ok(out)
}

fn next_segment_id_for_compaction(_store: &ArrayStore, inputs: &[String]) -> String {
    // Allocate the next sequence number above any input's. We can't
    // mutably borrow the store here, so derive a fresh id from the
    // largest input sequence number — collision-free because the
    // engine's allocator monotonically advances at every flush.
    let mut max_seq: u64 = 0;
    for id in inputs {
        if let Some((stem, _)) = id.split_once('.')
            && let Ok(n) = stem.parse::<u64>()
        {
            max_seq = max_seq.max(n);
        }
    }
    // Compaction outputs sort after any future flush by reusing the
    // monotonic engine allocator's space (max input seq + 1).
    let combined = max_seq.saturating_add(1);
    format!("{combined:010}.ndas")
}

fn write_atomic(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    let mut tmp = path.to_path_buf();
    tmp.set_extension("ndas.tmp");
    {
        use std::io::Write;
        let mut f = std::fs::File::create(&tmp)?;
        f.write_all(bytes)?;
        f.sync_all()?;
    }
    std::fs::rename(&tmp, path)?;
    if let Some(dir) = path.parent()
        && let Ok(d) = std::fs::File::open(dir)
    {
        let _ = d.sync_all();
    }
    Ok(())
}

fn build_segment_bytes(
    schema: &ArraySchema,
    schema_hash: u64,
    tiles: impl Iterator<Item = (TileId, MergedTile)>,
) -> ArrayResult<Vec<u8>> {
    let mut writer = SegmentWriter::new(schema_hash);
    for (tile_id, mt) in tiles {
        let tile = mt.into_sparse(schema)?;
        // Skip tiles that have neither live rows nor sentinel rows.
        let has_any_rows = tile.nnz() > 0 || !tile.row_kinds.is_empty();
        if !has_any_rows {
            continue;
        }
        writer.append_sparse(tile_id, &tile)?;
    }
    writer.finish()
}

/// Row record inside a [`MergedTile`] accumulator.
struct MergedRow {
    coord: Vec<CoordValue>,
    attrs: Vec<CellValue>,
    surrogate: nodedb_types::Surrogate,
    valid_from_ms: i64,
    valid_until_ms: i64,
    kind: RowKind,
}

/// Per-tile merge accumulator. Stores the in-progress `coord → row` map so
/// subsequent absorbs can override earlier versions — last-write-wins.
/// Sentinel rows (Tombstone, GdprErased) are stored and passed through
/// unchanged; retention logic in [`apply_retention`] handles cell-level
/// expiry when `audit_retain_ms` is set.
struct MergedTile {
    rows: Vec<MergedRow>,
}

impl MergedTile {
    fn empty(_schema: &ArraySchema) -> Self {
        Self { rows: Vec::new() }
    }

    fn absorb(&mut self, schema: &ArraySchema, payload: &TilePayload) -> ArrayResult<()> {
        match payload {
            TilePayload::Sparse(tile) => self.absorb_sparse(schema, tile),
            TilePayload::Dense(tile) => self.absorb_dense(schema, tile),
        }
    }

    fn absorb_sparse(&mut self, _schema: &ArraySchema, tile: &SparseTile) -> ArrayResult<()> {
        let n = tile.row_count();
        for row in 0..n {
            let coord: Vec<CoordValue> = tile
                .dim_dicts
                .iter()
                .map(|d| d.values[d.indices[row] as usize].clone())
                .collect();
            let kind = tile.row_kind(row)?;
            let (attrs, surrogate, valid_from_ms, valid_until_ms) = match kind {
                RowKind::Live => {
                    let attrs: Vec<CellValue> =
                        tile.attr_cols.iter().map(|col| col[row].clone()).collect();
                    let surrogate = tile
                        .surrogates
                        .get(row)
                        .copied()
                        .unwrap_or(nodedb_types::Surrogate::ZERO);
                    let vf = tile.valid_from_ms.get(row).copied().unwrap_or(0);
                    let vu = tile
                        .valid_until_ms
                        .get(row)
                        .copied()
                        .unwrap_or(nodedb_types::OPEN_UPPER);
                    (attrs, surrogate, vf, vu)
                }
                RowKind::Tombstone | RowKind::GdprErased => (
                    Vec::new(),
                    nodedb_types::Surrogate::ZERO,
                    0,
                    nodedb_types::OPEN_UPPER,
                ),
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
    use crate::engine::array::test_support::{aid, put_one, schema};
    use crate::engine::array::wal::ArrayPutCell;
    use nodedb_array::types::cell_value::value::CellValue;
    use nodedb_array::types::coord::value::CoordValue;
    use tempfile::TempDir;

    fn put_versioned(e: &mut ArrayEngine, x: i64, y: i64, v: i64, sys_ms: i64, lsn: u64) {
        e.put_cells(
            &aid(),
            vec![ArrayPutCell {
                coord: vec![CoordValue::Int64(x), CoordValue::Int64(y)],
                attrs: vec![CellValue::Int64(v)],
                surrogate: nodedb_types::Surrogate::ZERO,
                system_from_ms: sys_ms,
                valid_from_ms: 0,
                valid_until_ms: i64::MAX,
            }],
            lsn,
        )
        .unwrap();
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
        let merged = e.maybe_compact(&aid(), None, 0).unwrap();
        assert!(merged);
        let m = e.store(&aid()).unwrap().manifest();
        assert_eq!(m.segments.len(), 1);
        // All 4 tile versions (distinct system_from_ms) must survive.
        assert_eq!(m.segments[0].tile_count, 4);
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
        e.flush(&aid(), 2).unwrap();

        // Segment 2: tombstone at (2,0) system=200
        e.delete_cells(
            &aid(),
            vec![ArrayDeleteCell {
                coord: vec![CoordValue::Int64(2), CoordValue::Int64(0)],
                system_from_ms: 200,
                erasure: false,
            }],
            3,
        )
        .unwrap();
        e.flush(&aid(), 4).unwrap();

        // Segment 3: GDPR erasure at (3,0) system=300
        e.gdpr_erase_cell(
            &aid(),
            vec![CoordValue::Int64(3), CoordValue::Int64(0)],
            300,
            5,
        )
        .unwrap();
        e.flush(&aid(), 6).unwrap();

        // Segment 4: another live cell to reach the L0_TRIGGER threshold.
        put_one(&mut e, 4, 0, 40, 7);
        e.flush(&aid(), 8).unwrap();

        loop {
            if !e.maybe_compact(&aid(), None, 0).unwrap() {
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
}
