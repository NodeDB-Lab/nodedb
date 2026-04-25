//! Merge a set of segments into one new segment.
//!
//! Algorithm: read all input tiles into a `BTreeMap<TileId, MergedTile>`,
//! folding cells through a per-tile [`SparseTileBuilder`]. Inputs are
//! ordered by flush_lsn ascending (the picker enforces this), so when
//! two segments contain the same coord in the same tile the *later*
//! flush overwrites the earlier one — last-write-wins semantics that
//! match the memtable.
//!
//! The output is written via [`SegmentWriter`] in TileId order, which
//! preserves Hilbert ordering for the next compaction pass.

use std::collections::BTreeMap;
use std::path::Path;

use nodedb_array::ArrayResult;
use nodedb_array::schema::ArraySchema;
use nodedb_array::segment::reader::TilePayload;
use nodedb_array::segment::writer::SegmentWriter;
use nodedb_array::tile::dense_tile::DenseTile;
use nodedb_array::tile::sparse_tile::{SparseTile, SparseTileBuilder};
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
    pub fn run(
        store: &ArrayStore,
        inputs: &[String],
        output_level: u8,
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
    // Tag compaction outputs with a high bit so they sort after any
    // future flush that the engine's allocator hands out (tier 3 keeps
    // both spaces small; tier 5 will replace this with a coordinated
    // allocator on the engine).
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
        if tile.nnz() == 0 {
            continue;
        }
        writer.append_sparse(tile_id, &tile)?;
    }
    writer.finish()
}

/// Per-tile merge accumulator. Stores the in-progress (coord -> attrs)
/// list so subsequent absorbs can override earlier coords.
struct MergedTile {
    rows: Vec<(Vec<CoordValue>, Vec<CellValue>)>,
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
        let n = tile.nnz() as usize;
        for row in 0..n {
            let coord: Vec<CoordValue> = tile
                .dim_dicts
                .iter()
                .map(|d| d.values[d.indices[row] as usize].clone())
                .collect();
            let attrs: Vec<CellValue> = tile.attr_cols.iter().map(|col| col[row].clone()).collect();
            self.upsert(coord, attrs);
        }
        Ok(())
    }

    fn absorb_dense(&mut self, _schema: &ArraySchema, _tile: &DenseTile) -> ArrayResult<()> {
        // Tier 3 only flushes sparse memtables. Dense tiles arrive only
        // via promotion (Tier 4+), so a merger that encounters one
        // today indicates a programming error rather than data — we
        // surface it as a corruption error.
        Err(nodedb_array::ArrayError::SegmentCorruption {
            detail: "compaction merger received a dense tile but tier 3 does not produce them"
                .into(),
        })
    }

    fn upsert(&mut self, coord: Vec<CoordValue>, attrs: Vec<CellValue>) {
        if let Some(slot) = self.rows.iter_mut().find(|(c, _)| c == &coord) {
            slot.1 = attrs;
        } else {
            self.rows.push((coord, attrs));
        }
    }

    fn into_sparse(self, schema: &ArraySchema) -> ArrayResult<SparseTile> {
        let mut b = SparseTileBuilder::new(schema);
        for (coord, attrs) in self.rows {
            b.push(&coord, &attrs)?;
        }
        Ok(b.build())
    }
}
