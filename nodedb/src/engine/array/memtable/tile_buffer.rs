//! In-memory write buffer for a single array.
//!
//! Cells are bucketed into [`TileBuffer`]s keyed by [`TileId`]
//! (`hilbert_prefix`, `system_from_ms`), so a flush drops each bucket
//! through a [`SparseTileBuilder`] without re-bucketing. Each
//! `TileBuffer` stores raw byte values — either an encoded
//! [`nodedb_array::tile::cell_payload::CellPayload`] or the
//! [`nodedb_array::tile::cell_payload::CELL_TOMBSTONE_SENTINEL`] marker.
//!
//! Deletes are appended as tombstone versions: calling `delete_cell`
//! with `system_from_ms = T` inserts a new tile-version entry at
//! `TileId(prefix, T)` with the tombstone sentinel bytes. The earlier
//! live version at `TileId(prefix, T-1)` is unmodified, which is the
//! correct bitemporal behaviour — the old system-time fact is preserved;
//! the new system-time fact records the deletion.

use std::collections::{BTreeMap, HashMap};

use nodedb_array::ArrayResult;
use nodedb_array::schema::ArraySchema;
use nodedb_array::tile::cell_payload::{
    CELL_GDPR_ERASURE_SENTINEL, CELL_TOMBSTONE_SENTINEL, CellPayload, is_cell_gdpr_erasure,
    is_cell_sentinel, is_cell_tombstone,
};
use nodedb_array::tile::sparse_tile::{RowKind, SparseRow, SparseTile, SparseTileBuilder};
use nodedb_array::tile::tile_id_for_cell;
use nodedb_array::types::TileId;
use nodedb_array::types::cell_value::value::CellValue;
use nodedb_array::types::coord::value::CoordValue;
use nodedb_types::{OPEN_UPPER, Surrogate};

/// Raw-byte write buffer for a single tile version.
///
/// Keys are zerompk-encoded coordinates (`Vec<u8>`). Values are either an
/// encoded `CellPayload` or `CELL_TOMBSTONE_SENTINEL`. Using the encoded form
/// as the key avoids the need for `Ord` or `Hash` on `CoordValue`.
#[derive(Debug, Default, Clone)]
pub struct TileBuffer {
    /// coord_key → payload bytes. coord_key = zerompk of `Vec<CoordValue>`.
    entries: HashMap<Vec<u8>, Vec<u8>>,
    /// Highest WAL LSN that produced a row or tombstone in this buffer.
    last_lsn: u64,
}

impl TileBuffer {
    /// Iterate over all raw coord keys stored in this tile version.
    pub fn all_coord_keys(&self) -> impl Iterator<Item = &[u8]> {
        self.entries.keys().map(|k| k.as_slice())
    }

    fn encode_coord(coord: &Vec<CoordValue>) -> ArrayResult<Vec<u8>> {
        zerompk::to_msgpack_vec(coord).map_err(|e| {
            nodedb_array::error::ArrayError::SegmentCorruption {
                detail: format!("encode coord key: {e}"),
            }
        })
    }

    fn decode_coord(key: &[u8]) -> ArrayResult<Vec<CoordValue>> {
        zerompk::from_msgpack(key).map_err(|e| nodedb_array::error::ArrayError::SegmentCorruption {
            detail: format!("decode coord key: {e}"),
        })
    }

    /// Return the raw stored bytes (encoded `CellPayload` or sentinel) for the
    /// given `coord`, or `None` if the coord is not present in this tile version.
    pub fn get_cell_bytes(&self, coord: &[CoordValue]) -> Option<&[u8]> {
        let key = Self::encode_coord(&coord.to_vec()).ok()?;
        self.entries.get(&key).map(|v| v.as_slice())
    }

    /// Insert or overwrite a cell version. `payload_bytes` must be the
    /// zerompk encoding of a [`CellPayload`].
    pub fn push_raw(
        &mut self,
        coord: &Vec<CoordValue>,
        payload_bytes: Vec<u8>,
        lsn: u64,
    ) -> ArrayResult<()> {
        let key = Self::encode_coord(coord)?;
        self.entries.insert(key, payload_bytes);
        self.last_lsn = self.last_lsn.max(lsn);
        Ok(())
    }

    /// Insert a tombstone sentinel for `coord` in this version.
    pub fn push_tombstone(&mut self, coord: &Vec<CoordValue>, lsn: u64) -> ArrayResult<()> {
        let key = Self::encode_coord(coord)?;
        self.entries.insert(key, CELL_TOMBSTONE_SENTINEL.to_vec());
        self.last_lsn = self.last_lsn.max(lsn);
        Ok(())
    }

    /// Insert a GDPR erasure sentinel for `coord` in this version.
    ///
    /// Distinct from tombstone: byte value `0xFE` vs `0xFF`. The ceiling
    /// resolver and compaction path treat them differently — erasure markers
    /// are physically removed by compaction once outside the audit-retention
    /// horizon, whereas tombstones may be retained.
    pub fn push_erasure(&mut self, coord: &Vec<CoordValue>, lsn: u64) -> ArrayResult<()> {
        let key = Self::encode_coord(coord)?;
        self.entries
            .insert(key, CELL_GDPR_ERASURE_SENTINEL.to_vec());
        self.last_lsn = self.last_lsn.max(lsn);
        Ok(())
    }

    /// Count of entries (including tombstones).
    pub fn entry_count(&self) -> usize {
        self.entries.len()
    }

    /// Count of live (non-sentinel) entries — both tombstones and GDPR erasures
    /// are excluded.
    pub fn live_cell_count(&self) -> usize {
        self.entries
            .values()
            .filter(|v| !is_cell_sentinel(v))
            .count()
    }

    pub fn last_lsn(&self) -> u64 {
        self.last_lsn
    }

    /// Replay buffered entries into a fresh `SparseTile`.
    ///
    /// Live entries produce [`RowKind::Live`] rows with full payload.
    /// Sentinel entries (tombstone / GDPR erasure) are persisted as
    /// [`RowKind::Tombstone`] / [`RowKind::GdprErased`] rows so they
    /// survive the flush → segment round-trip. Compaction's retention
    /// policy is the only path permitted to drop them.
    pub fn materialise(&self, schema: &ArraySchema) -> ArrayResult<SparseTile> {
        let mut b = SparseTileBuilder::new(schema);
        for (coord_key, bytes) in &self.entries {
            let coord = Self::decode_coord(coord_key)?;
            if is_cell_tombstone(bytes) {
                b.push_row(SparseRow {
                    coord: &coord,
                    attrs: &[],
                    surrogate: Surrogate::ZERO,
                    valid_from_ms: 0,
                    valid_until_ms: OPEN_UPPER,
                    kind: RowKind::Tombstone,
                })?;
            } else if is_cell_gdpr_erasure(bytes) {
                b.push_row(SparseRow {
                    coord: &coord,
                    attrs: &[],
                    surrogate: Surrogate::ZERO,
                    valid_from_ms: 0,
                    valid_until_ms: OPEN_UPPER,
                    kind: RowKind::GdprErased,
                })?;
            } else {
                let payload = CellPayload::decode(bytes)?;
                b.push_row(SparseRow {
                    coord: &coord,
                    attrs: &payload.attrs,
                    surrogate: payload.surrogate,
                    valid_from_ms: payload.valid_from_ms,
                    valid_until_ms: payload.valid_until_ms,
                    kind: RowKind::Live,
                })?;
            }
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

/// Arguments for [`Memtable::put_cell`].
pub struct PutCell {
    pub coord: Vec<CoordValue>,
    pub attrs: Vec<CellValue>,
    pub surrogate: Surrogate,
    pub system_from_ms: i64,
    pub valid_from_ms: i64,
    pub valid_until_ms: i64,
    pub lsn: u64,
}

impl Memtable {
    pub fn new() -> Self {
        Self::default()
    }

    /// Insert a cell at `(coord, system_from_ms)`. The `system_from_ms`
    /// participates in the tile key so two writes at different
    /// system-time stamps produce two separate tile entries.
    pub fn put_cell(&mut self, schema: &ArraySchema, args: PutCell) -> ArrayResult<TileId> {
        let PutCell {
            coord,
            attrs,
            surrogate,
            system_from_ms,
            valid_from_ms,
            valid_until_ms,
            lsn,
        } = args;
        let tile = tile_id_for_cell(schema, &coord, system_from_ms)?;
        let payload = CellPayload {
            valid_from_ms,
            valid_until_ms,
            attrs,
            surrogate,
        };
        let bytes = payload.encode()?;
        self.tiles
            .entry(tile)
            .or_default()
            .push_raw(&coord, bytes, lsn)?;
        Ok(tile)
    }

    /// Append a tombstone version for `(coord, system_from_ms)`. Creates a
    /// new tile-version entry in the memtable; does NOT mutate any earlier
    /// version for this coord.
    pub fn delete_cell(
        &mut self,
        schema: &ArraySchema,
        coord: Vec<CoordValue>,
        system_from_ms: i64,
        lsn: u64,
    ) -> ArrayResult<TileId> {
        let tile = tile_id_for_cell(schema, &coord, system_from_ms)?;
        self.tiles
            .entry(tile)
            .or_default()
            .push_tombstone(&coord, lsn)?;
        Ok(tile)
    }

    /// Append a GDPR erasure version for `(coord, system_from_ms)`. Writes
    /// the `0xFE` sentinel rather than the `0xFF` tombstone. The ceiling
    /// resolver returns `CeilingResult::Erased` for this coord at any
    /// `system_as_of >= system_from_ms`.
    pub fn erase_cell(
        &mut self,
        schema: &ArraySchema,
        coord: Vec<CoordValue>,
        system_from_ms: i64,
        lsn: u64,
    ) -> ArrayResult<TileId> {
        let tile = tile_id_for_cell(schema, &coord, system_from_ms)?;
        self.tiles
            .entry(tile)
            .or_default()
            .push_erasure(&coord, lsn)?;
        Ok(tile)
    }

    pub fn stats(&self) -> MemtableStats {
        let mut s = MemtableStats::default();
        for b in self.tiles.values() {
            s.tile_count += 1;
            s.cell_count += b.live_cell_count();
            s.max_lsn = s.max_lsn.max(b.last_lsn());
        }
        s
    }

    pub fn is_empty(&self) -> bool {
        self.tiles.values().all(|b| b.entry_count() == 0)
    }

    /// Decode a raw coord key (zerompk-encoded `Vec<CoordValue>`) back to a
    /// typed coordinate. Exposed so the store's ceiling scan can reconstruct
    /// the full coordinate list from memtable entries.
    pub fn decode_coord_key(key: &[u8]) -> ArrayResult<Vec<CoordValue>> {
        TileBuffer::decode_coord(key)
    }

    /// Drain tiles in `TileId`-ascending order (BTreeMap guarantees this).
    pub fn drain_sorted(&mut self) -> Vec<(TileId, TileBuffer)> {
        std::mem::take(&mut self.tiles).into_iter().collect()
    }

    pub fn iter(&self) -> impl Iterator<Item = (&TileId, &TileBuffer)> {
        self.tiles.iter()
    }

    /// Returns an iterator over all tile versions for `hilbert_prefix` whose
    /// `system_from_ms <= system_as_of`, ordered **newest-first**.
    ///
    /// Used by the bitemporal ceiling resolver in DP read handlers.
    pub fn iter_tile_versions(
        &self,
        hilbert_prefix: u64,
        system_as_of: i64,
    ) -> impl Iterator<Item = (TileId, &TileBuffer)> {
        use nodedb_array::types::TileId as Tid;
        let lo = Tid::new(hilbert_prefix, i64::MIN);
        let hi = Tid::new(hilbert_prefix, system_as_of);
        self.tiles.range(lo..=hi).rev().map(|(&id, buf)| (id, buf))
    }
}

/// One flushed tile record — returned to the flush path so it can build
/// the manifest entry and WAL flush record.
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
    use nodedb_array::tile::cell_payload::OPEN_UPPER;
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

    fn coord(x: i64, y: i64) -> Vec<CoordValue> {
        vec![CoordValue::Int64(x), CoordValue::Int64(y)]
    }

    fn put(c: Vec<CoordValue>, a: Vec<CellValue>, sys: i64, vf: i64, vu: i64, lsn: u64) -> PutCell {
        PutCell {
            coord: c,
            attrs: a,
            surrogate: Surrogate::ZERO,
            system_from_ms: sys,
            valid_from_ms: vf,
            valid_until_ms: vu,
            lsn,
        }
    }

    fn attrs(v: i64) -> Vec<CellValue> {
        vec![CellValue::Int64(v)]
    }

    #[test]
    fn same_coord_different_system_times_produce_two_entries() {
        let s = schema();
        let mut m = Memtable::new();
        m.put_cell(&s, put(coord(1, 2), attrs(10), 100, 100, OPEN_UPPER, 1))
            .unwrap();
        m.put_cell(&s, put(coord(1, 2), attrs(20), 200, 200, OPEN_UPPER, 2))
            .unwrap();
        // Two tile versions — different system_from_ms → different TileId.
        assert_eq!(m.tiles.len(), 2);
        let stats = m.stats();
        // Both are live cells.
        assert_eq!(stats.cell_count, 2);
    }

    #[test]
    fn tombstone_appended_not_in_place() {
        let s = schema();
        let mut m = Memtable::new();
        m.put_cell(&s, put(coord(1, 1), attrs(1), 100, 100, OPEN_UPPER, 1))
            .unwrap();
        // Delete at a later system_from_ms — must NOT remove the earlier tile.
        m.delete_cell(&s, coord(1, 1), 200, 2).unwrap();
        // Two tile versions: the live one and the tombstone one.
        assert_eq!(m.tiles.len(), 2);
        // Only one live cell (the tombstone version doesn't count as live).
        assert_eq!(m.stats().cell_count, 1);
        // The tombstone tile buffer has exactly one entry and it's a tombstone.
        let hilbert = m.tiles.keys().last().unwrap().hilbert_prefix;
        let tombstone_tile_id = TileId::new(hilbert, 200);
        let buf = m.tiles.get(&tombstone_tile_id).unwrap();
        assert_eq!(buf.entry_count(), 1);
        // All entries in the tombstone tile are tombstone sentinels.
        assert_eq!(buf.live_cell_count(), 0);
    }

    #[test]
    fn put_buckets_into_tile() {
        let s = schema();
        let mut m = Memtable::new();
        m.put_cell(&s, put(coord(1, 2), attrs(10), 0, 0, OPEN_UPPER, 1))
            .unwrap();
        m.put_cell(&s, put(coord(2, 3), attrs(20), 0, 0, OPEN_UPPER, 2))
            .unwrap();
        let stats = m.stats();
        assert_eq!(stats.cell_count, 2);
        assert_eq!(stats.tile_count, 1);
        assert_eq!(stats.max_lsn, 2);
    }

    #[test]
    fn get_cell_bytes_returns_stored_bytes() {
        let s = schema();
        let mut m = Memtable::new();
        m.put_cell(&s, put(coord(2, 3), attrs(42), 100, 0, OPEN_UPPER, 1))
            .unwrap();
        // Get the tile buffer for the coord.
        let (_, buf) = m.tiles.iter().next().unwrap();
        let bytes = buf.get_cell_bytes(&coord(2, 3));
        assert!(bytes.is_some(), "should find coord (2,3)");
        let absent = buf.get_cell_bytes(&coord(9, 9));
        assert!(absent.is_none(), "absent coord must return None");
    }

    #[test]
    fn iter_tile_versions_newest_first() {
        let s = schema();
        let mut m = Memtable::new();
        m.put_cell(&s, put(coord(1, 1), attrs(1), 100, 0, OPEN_UPPER, 1))
            .unwrap();
        m.put_cell(&s, put(coord(1, 1), attrs(2), 200, 0, OPEN_UPPER, 2))
            .unwrap();
        m.put_cell(&s, put(coord(1, 1), attrs(3), 300, 0, OPEN_UPPER, 3))
            .unwrap();
        // All three writes map to the same hilbert_prefix (same coord).
        let prefix = m.tiles.keys().next().unwrap().hilbert_prefix;
        let versions: Vec<i64> = m
            .iter_tile_versions(prefix, i64::MAX)
            .map(|(id, _)| id.system_from_ms)
            .collect();
        assert_eq!(versions, vec![300, 200, 100]);
    }

    #[test]
    fn iter_tile_versions_respects_system_as_of() {
        let s = schema();
        let mut m = Memtable::new();
        m.put_cell(&s, put(coord(1, 1), attrs(1), 100, 0, OPEN_UPPER, 1))
            .unwrap();
        m.put_cell(&s, put(coord(1, 1), attrs(2), 200, 0, OPEN_UPPER, 2))
            .unwrap();
        m.put_cell(&s, put(coord(1, 1), attrs(3), 300, 0, OPEN_UPPER, 3))
            .unwrap();
        let prefix = m.tiles.keys().next().unwrap().hilbert_prefix;
        let versions: Vec<i64> = m
            .iter_tile_versions(prefix, 250)
            .map(|(id, _)| id.system_from_ms)
            .collect();
        assert_eq!(versions, vec![200, 100]);
    }

    #[test]
    fn materialise_carries_valid_time_bounds() {
        let s = schema();
        let mut m = Memtable::new();
        m.put_cell(&s, put(coord(3, 3), attrs(77), 100, 500, 800, 1))
            .unwrap();
        let (_, buf) = m.tiles.iter().next().unwrap();
        let tile = buf.materialise(&s).unwrap();
        assert_eq!(tile.valid_from_ms, vec![500]);
        assert_eq!(tile.valid_until_ms, vec![800]);
    }

    #[test]
    fn materialise_persists_tombstone_and_erasure_kinds() {
        use nodedb_array::tile::sparse_tile::RowKind;

        let s = schema();
        let mut m = Memtable::new();
        // Live cell at (1,1), sys=100
        m.put_cell(&s, put(coord(1, 1), attrs(10), 100, 0, OPEN_UPPER, 1))
            .unwrap();
        // Tombstone at (2,2), sys=200
        m.delete_cell(&s, coord(2, 2), 200, 2).unwrap();
        // GDPR erasure at (3,3), sys=300
        m.erase_cell(&s, coord(3, 3), 300, 3).unwrap();

        // Each write lands in its own tile (different sys_from_ms / different coord→tile).
        // Collect all tile kinds across all tile buffers.
        let mut live_found = false;
        let mut tombstone_found = false;
        let mut erased_found = false;
        for (_, buf) in m.iter() {
            let tile = buf.materialise(&s).unwrap();
            for i in 0..tile.row_kinds.len() {
                match RowKind::from_u8(tile.row_kinds[i]).unwrap() {
                    RowKind::Live => live_found = true,
                    RowKind::Tombstone => tombstone_found = true,
                    RowKind::GdprErased => erased_found = true,
                }
            }
        }
        assert!(
            live_found,
            "expected at least one Live row in materialised tiles"
        );
        assert!(
            tombstone_found,
            "expected at least one Tombstone row in materialised tiles"
        );
        assert!(
            erased_found,
            "expected at least one GdprErased row in materialised tiles"
        );
    }

    #[test]
    fn drain_yields_sorted_tiles() {
        let s = schema();
        let mut m = Memtable::new();
        m.put_cell(&s, put(coord(8, 8), attrs(1), 0, 0, OPEN_UPPER, 1))
            .unwrap();
        m.put_cell(&s, put(coord(0, 0), attrs(1), 0, 0, OPEN_UPPER, 2))
            .unwrap();
        let drained = m.drain_sorted();
        assert!(drained.windows(2).all(|w| w[0].0 < w[1].0));
        assert!(m.is_empty());
    }
}
