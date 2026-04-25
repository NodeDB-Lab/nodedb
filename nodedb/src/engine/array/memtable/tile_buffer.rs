//! In-memory write buffer for a single array.
//!
//! Cells are bucketed into [`TileBuffer`]s keyed by [`TileId`] (the same
//! Hilbert prefix used on disk), so a flush can drop each bucket through
//! a [`SparseTileBuilder`] without re-bucketing. Deletes are recorded as
//! tombstones against (coord) and applied at materialise / flush time —
//! we deliberately do not splice through the row vector on every delete
//! because typical workloads delete in tile-sized batches.

use std::collections::{BTreeMap, HashSet};

use nodedb_array::ArrayResult;
use nodedb_array::schema::ArraySchema;
use nodedb_array::tile::sparse_tile::{SparseTile, SparseTileBuilder};
use nodedb_array::tile::tile_id_for_cell;
use nodedb_array::types::TileId;
use nodedb_array::types::cell_value::value::CellValue;
use nodedb_array::types::coord::value::CoordValue;

#[derive(Debug, Default, Clone)]
pub struct TileBuffer {
    rows: Vec<(Vec<CoordValue>, Vec<CellValue>)>,
    tombstones: HashSet<Vec<CoordValue>>,
    /// Highest WAL LSN that produced a row or tombstone in this buffer.
    /// The flush watermark is the max across all buffers in a memtable.
    last_lsn: u64,
}

impl TileBuffer {
    pub fn push_row(&mut self, coord: Vec<CoordValue>, attrs: Vec<CellValue>, lsn: u64) {
        // Re-insert overrides earlier writes at the same coord.
        self.rows.retain(|(c, _)| c != &coord);
        self.tombstones.remove(&coord);
        self.rows.push((coord, attrs));
        self.last_lsn = self.last_lsn.max(lsn);
    }

    pub fn tombstone(&mut self, coord: Vec<CoordValue>, lsn: u64) {
        self.rows.retain(|(c, _)| c != &coord);
        self.tombstones.insert(coord);
        self.last_lsn = self.last_lsn.max(lsn);
    }

    pub fn cell_count(&self) -> usize {
        self.rows.len()
    }

    pub fn last_lsn(&self) -> u64 {
        self.last_lsn
    }

    /// Replay buffered rows into a fresh `SparseTile`. Tombstones are
    /// already pre-applied (rows are removed at delete time), so this
    /// just folds the survivors.
    pub fn materialise(&self, schema: &ArraySchema) -> ArrayResult<SparseTile> {
        let mut b = SparseTileBuilder::new(schema);
        for (coord, attrs) in &self.rows {
            b.push(coord, attrs)?;
        }
        Ok(b.build())
    }
}

#[derive(Debug, Default)]
pub struct MemtableStats {
    pub tile_count: usize,
    pub cell_count: usize,
    pub max_lsn: u64,
}

#[derive(Debug, Default)]
pub struct Memtable {
    tiles: BTreeMap<TileId, TileBuffer>,
}

impl Memtable {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn put_cell(
        &mut self,
        schema: &ArraySchema,
        coord: Vec<CoordValue>,
        attrs: Vec<CellValue>,
        lsn: u64,
    ) -> ArrayResult<TileId> {
        let tile = tile_id_for_cell(schema, &coord, 0)?;
        self.tiles
            .entry(tile)
            .or_default()
            .push_row(coord, attrs, lsn);
        Ok(tile)
    }

    pub fn delete_cell(
        &mut self,
        schema: &ArraySchema,
        coord: Vec<CoordValue>,
        lsn: u64,
    ) -> ArrayResult<TileId> {
        let tile = tile_id_for_cell(schema, &coord, 0)?;
        self.tiles.entry(tile).or_default().tombstone(coord, lsn);
        Ok(tile)
    }

    pub fn stats(&self) -> MemtableStats {
        let mut s = MemtableStats::default();
        for b in self.tiles.values() {
            s.tile_count += 1;
            s.cell_count += b.cell_count();
            s.max_lsn = s.max_lsn.max(b.last_lsn());
        }
        s
    }

    pub fn is_empty(&self) -> bool {
        self.tiles.values().all(|b| b.cell_count() == 0)
    }

    pub fn drain_sorted(&mut self) -> Vec<(TileId, TileBuffer)> {
        std::mem::take(&mut self.tiles).into_iter().collect()
    }

    pub fn iter(&self) -> impl Iterator<Item = (&TileId, &TileBuffer)> {
        self.tiles.iter()
    }
}

/// Tracks which buffers in the memtable produced rows in this flush.
/// Returned to the caller so the WAL flush record can list them.
pub struct MemtableEntry {
    pub tile_id: TileId,
    pub tile: SparseTile,
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_array::schema::ArraySchemaBuilder;
    use nodedb_array::schema::attr_spec::{AttrSpec, AttrType};
    use nodedb_array::schema::dim_spec::{DimSpec, DimType};
    use nodedb_array::types::domain::{Domain, DomainBound};

    fn schema() -> ArraySchema {
        ArraySchemaBuilder::new("a")
            .dim(DimSpec::new(
                "x",
                DimType::Int64,
                Domain::new(DomainBound::Int64(0), DomainBound::Int64(15)),
            ))
            .dim(DimSpec::new(
                "y",
                DimType::Int64,
                Domain::new(DomainBound::Int64(0), DomainBound::Int64(15)),
            ))
            .attr(AttrSpec::new("v", AttrType::Int64, true))
            .tile_extents(vec![4, 4])
            .build()
            .unwrap()
    }

    #[test]
    fn put_buckets_into_tile() {
        let s = schema();
        let mut m = Memtable::new();
        m.put_cell(
            &s,
            vec![CoordValue::Int64(1), CoordValue::Int64(2)],
            vec![CellValue::Int64(10)],
            1,
        )
        .unwrap();
        m.put_cell(
            &s,
            vec![CoordValue::Int64(2), CoordValue::Int64(3)],
            vec![CellValue::Int64(20)],
            2,
        )
        .unwrap();
        let stats = m.stats();
        assert_eq!(stats.cell_count, 2);
        assert_eq!(stats.tile_count, 1);
        assert_eq!(stats.max_lsn, 2);
    }

    #[test]
    fn put_then_delete_drops_row() {
        let s = schema();
        let mut m = Memtable::new();
        m.put_cell(
            &s,
            vec![CoordValue::Int64(1), CoordValue::Int64(1)],
            vec![CellValue::Int64(1)],
            1,
        )
        .unwrap();
        m.delete_cell(&s, vec![CoordValue::Int64(1), CoordValue::Int64(1)], 2)
            .unwrap();
        assert_eq!(m.stats().cell_count, 0);
    }

    #[test]
    fn put_overwrites_same_coord() {
        let s = schema();
        let mut m = Memtable::new();
        m.put_cell(
            &s,
            vec![CoordValue::Int64(0), CoordValue::Int64(0)],
            vec![CellValue::Int64(1)],
            1,
        )
        .unwrap();
        m.put_cell(
            &s,
            vec![CoordValue::Int64(0), CoordValue::Int64(0)],
            vec![CellValue::Int64(2)],
            2,
        )
        .unwrap();
        assert_eq!(m.stats().cell_count, 1);
        // surviving value is the last write — fold into tile and check.
        let (_, buf) = m.iter().next().unwrap();
        let t = buf.materialise(&s).unwrap();
        assert_eq!(t.attr_cols[0][0], CellValue::Int64(2));
    }

    #[test]
    fn drain_yields_sorted_tiles() {
        let s = schema();
        let mut m = Memtable::new();
        m.put_cell(
            &s,
            vec![CoordValue::Int64(8), CoordValue::Int64(8)],
            vec![CellValue::Int64(1)],
            1,
        )
        .unwrap();
        m.put_cell(
            &s,
            vec![CoordValue::Int64(0), CoordValue::Int64(0)],
            vec![CellValue::Int64(1)],
            2,
        )
        .unwrap();
        let drained = m.drain_sorted();
        assert!(drained.windows(2).all(|w| w[0].0 < w[1].0));
        assert!(m.is_empty());
    }
}
