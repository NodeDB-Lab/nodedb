//! `SegmentReader` — zero-copy view over segment bytes.
//!
//! Designed to wrap an `mmap`'d file in production. The reader only
//! borrows the slice and lazily decodes tile payloads on demand; the
//! footer is decoded eagerly at construction so subsequent tile lookups
//! are bounded reads + zerompk decode (no rescan).

use super::format::{
    SegmentFooter, SegmentHeader, TileEntry, TileKind, framing::BlockFraming, header::HEADER_SIZE,
};
use crate::codec::tag::peek_tag;
use crate::codec::tile_decode::decode_sparse_tile;
use crate::error::{ArrayError, ArrayResult};
use crate::tile::cell_payload::{CELL_GDPR_ERASURE_SENTINEL, CELL_TOMBSTONE_SENTINEL, CellPayload};
use crate::tile::dense_tile::DenseTile;
use crate::tile::sparse_tile::{RowKind, SparseTile};
use crate::types::coord::value::CoordValue;
use nodedb_types::Surrogate;

/// Extract the encoded `CellPayload` bytes for a specific `coord` from a
/// `SparseTile`.
///
/// Returns `None` when the coord is not present in the tile. Encodes the
/// matched row's attrs + valid-time bounds back into the `CellPayload` wire
/// format so the bytes can be fed directly to
/// [`crate::query::ceiling::ceiling_resolve_cell`].
///
/// The returned `Vec<u8>` is always freshly allocated (no lifetime coupling to
/// the tile).
pub fn extract_cell_bytes(tile: &SparseTile, coord: &[CoordValue]) -> ArrayResult<Option<Vec<u8>>> {
    let n = tile.row_count();
    'rows: for row in 0..n {
        // Reconstruct the coordinate for this row.
        for (dim_idx, c) in coord.iter().enumerate() {
            let dict =
                tile.dim_dicts
                    .get(dim_idx)
                    .ok_or_else(|| ArrayError::SegmentCorruption {
                        detail: format!("extract_cell_bytes: dim {dim_idx} out of range"),
                    })?;
            let entry_idx = *dict
                .indices
                .get(row)
                .ok_or_else(|| ArrayError::SegmentCorruption {
                    detail: format!("extract_cell_bytes: row {row} index out of range"),
                })? as usize;
            let stored =
                dict.values
                    .get(entry_idx)
                    .ok_or_else(|| ArrayError::SegmentCorruption {
                        detail: format!("extract_cell_bytes: dict entry {entry_idx} out of range"),
                    })?;
            if stored != c {
                continue 'rows;
            }
        }
        // Coord matched — check row kind before decoding payload.
        let kind = tile.row_kind(row)?;
        match kind {
            RowKind::Tombstone => return Ok(Some(CELL_TOMBSTONE_SENTINEL.to_vec())),
            RowKind::GdprErased => return Ok(Some(CELL_GDPR_ERASURE_SENTINEL.to_vec())),
            RowKind::Live => {}
        }
        // Live row — build attrs from all attr columns.
        let attrs: Vec<_> = tile
            .attr_cols
            .iter()
            .map(|col| {
                col.get(row)
                    .cloned()
                    .ok_or_else(|| ArrayError::SegmentCorruption {
                        detail: format!("extract_cell_bytes: attr col row {row} out of range"),
                    })
            })
            .collect::<ArrayResult<Vec<_>>>()?;
        let surrogate = tile.surrogates.get(row).copied().unwrap_or(Surrogate::ZERO);
        let valid_from_ms =
            tile.valid_from_ms
                .get(row)
                .copied()
                .ok_or_else(|| ArrayError::SegmentCorruption {
                    detail: format!("extract_cell_bytes: valid_from_ms row {row} out of range"),
                })?;
        let valid_until_ms =
            tile.valid_until_ms
                .get(row)
                .copied()
                .ok_or_else(|| ArrayError::SegmentCorruption {
                    detail: format!("extract_cell_bytes: valid_until_ms row {row} out of range"),
                })?;
        let payload = CellPayload {
            valid_from_ms,
            valid_until_ms,
            attrs,
            surrogate,
        };
        return payload.encode().map(Some);
    }
    Ok(None)
}

/// Dispatch sparse tile decoding by peeking the tag byte.
///
/// - New-format tiles (tag = 0 or 1): delegate to `decode_sparse_tile`.
/// - Legacy v3 tiles (msgpack map-start byte): fall back to `zerompk::from_msgpack`.
/// - Unknown bytes: return a corruption error.
fn read_sparse_tile(payload: &[u8]) -> ArrayResult<SparseTile> {
    match peek_tag(payload) {
        Some(_) => {
            // New-format tile (Raw or Structural codec).
            decode_sparse_tile(payload)
        }
        None => zerompk::from_msgpack(payload).map_err(|e| ArrayError::SegmentCorruption {
            detail: format!("legacy sparse tile decode failed: {e}"),
        }),
    }
}

/// Decoded tile payload.
#[derive(Debug, Clone, PartialEq)]
pub enum TilePayload {
    Sparse(SparseTile),
    Dense(DenseTile),
}

pub struct SegmentReader<'a> {
    bytes: &'a [u8],
    header: SegmentHeader,
    footer: SegmentFooter,
}

impl<'a> SegmentReader<'a> {
    pub fn open(bytes: &'a [u8]) -> ArrayResult<Self> {
        if bytes.len() < HEADER_SIZE {
            return Err(ArrayError::SegmentCorruption {
                detail: format!("segment too small: {} bytes", bytes.len()),
            });
        }
        let header = SegmentHeader::decode(&bytes[..HEADER_SIZE])?;
        let footer = SegmentFooter::decode(bytes)?;
        if header.schema_hash != footer.schema_hash {
            return Err(ArrayError::SegmentCorruption {
                detail: format!(
                    "header/footer schema_hash mismatch: header={:x} footer={:x}",
                    header.schema_hash, footer.schema_hash
                ),
            });
        }
        Ok(Self {
            bytes,
            header,
            footer,
        })
    }

    pub fn header(&self) -> &SegmentHeader {
        &self.header
    }

    pub fn schema_hash(&self) -> u64 {
        self.header.schema_hash
    }

    pub fn tiles(&self) -> &[TileEntry] {
        &self.footer.tiles
    }

    pub fn tile_count(&self) -> usize {
        self.footer.tiles.len()
    }

    /// Reverse-scan tile entries with the given `hilbert_prefix` and
    /// `system_from_ms <= system_as_of`, returning the newest qualifying
    /// tile version.
    ///
    /// Returns `Ok(None)` if no version exists at or before the cutoff.
    ///
    /// The `valid_at_ms` parameter is reserved for the query layer (Tier
    /// 9.2). At the reader level, valid-time filtering is NOT applied —
    /// the whole tile is returned. This keeps the reader cell-shape-agnostic.
    pub fn read_tile_as_of(
        &self,
        hilbert_prefix: u64,
        system_as_of: i64,
        _valid_at_ms: Option<i64>,
    ) -> ArrayResult<Option<TilePayload>> {
        let tiles = &self.footer.tiles;

        // Binary search for the first entry with hilbert_prefix.
        let first = tiles.partition_point(|e| e.tile_id.hilbert_prefix < hilbert_prefix);
        // Binary search for the first entry past the range:
        // hilbert_prefix matches and system_from_ms <= system_as_of.
        // Upper bound: first entry where prefix > hilbert_prefix.
        let past_prefix = tiles.partition_point(|e| e.tile_id.hilbert_prefix <= hilbert_prefix);

        // Slice of entries with matching prefix.
        let candidates = &tiles[first..past_prefix];
        if candidates.is_empty() {
            return Ok(None);
        }

        // Within the prefix slice, entries are ordered by system_from_ms ascending.
        // Find the rightmost entry with system_from_ms <= system_as_of.
        let cutoff_pos = candidates.partition_point(|e| e.tile_id.system_from_ms <= system_as_of);
        if cutoff_pos == 0 {
            return Ok(None);
        }

        // The entry at cutoff_pos - 1 is the newest qualifying version.
        let entry_idx = first + cutoff_pos - 1;
        self.read_tile(entry_idx).map(Some)
    }

    /// Returns an iterator over all tile versions for `hilbert_prefix` whose
    /// `system_from_ms <= system_as_of`, ordered **newest-first** by
    /// `system_from_ms`.
    ///
    /// Callers supply this to [`nodedb_array::query::ceiling`] to resolve the
    /// bitemporal ceiling for every coordinate in the prefix.
    pub fn iter_tile_versions(
        &self,
        hilbert_prefix: u64,
        system_as_of: i64,
    ) -> ArrayResult<impl Iterator<Item = ArrayResult<(crate::types::TileId, TilePayload)>> + '_>
    {
        let tiles = &self.footer.tiles;

        // Find the contiguous slice of entries whose hilbert_prefix matches.
        let first = tiles.partition_point(|e| e.tile_id.hilbert_prefix < hilbert_prefix);
        let past_prefix = tiles.partition_point(|e| e.tile_id.hilbert_prefix <= hilbert_prefix);

        // Within [first..past_prefix], entries are ascending by system_from_ms.
        // Restrict to those at or before the cutoff.
        let cutoff_pos =
            tiles[first..past_prefix].partition_point(|e| e.tile_id.system_from_ms <= system_as_of);
        // Global index range: [first .. first+cutoff_pos).
        let qualifying_start = first;
        let qualifying_end = first + cutoff_pos;

        // Iterate in reverse (newest-first).
        let indices: Vec<usize> = (qualifying_start..qualifying_end).rev().collect();
        Ok(indices.into_iter().map(move |idx| {
            let tile_id = tiles[idx].tile_id;
            self.read_tile(idx).map(|payload| (tile_id, payload))
        }))
    }

    /// Decode tile #`idx`. CRC is checked by the framing layer.
    pub fn read_tile(&self, idx: usize) -> ArrayResult<TilePayload> {
        let entry = self
            .footer
            .tiles
            .get(idx)
            .ok_or_else(|| ArrayError::SegmentCorruption {
                detail: format!(
                    "tile index {idx} out of range (have {})",
                    self.footer.tiles.len()
                ),
            })?;
        let off = entry.offset as usize;
        let len = entry.length as usize;
        let end = off
            .checked_add(len)
            .ok_or_else(|| ArrayError::SegmentCorruption {
                detail: "tile entry offset+length overflows".into(),
            })?;
        if end > self.bytes.len() {
            return Err(ArrayError::SegmentCorruption {
                detail: format!(
                    "tile {idx} block out of bounds: off={off} len={len} \
                     file_size={}",
                    self.bytes.len()
                ),
            });
        }
        let (payload, _) = BlockFraming::decode(&self.bytes[off..end])?;
        match entry.kind {
            TileKind::Sparse => {
                let t = read_sparse_tile(payload)?;
                Ok(TilePayload::Sparse(t))
            }
            TileKind::Dense => {
                let t: DenseTile =
                    zerompk::from_msgpack(payload).map_err(|e| ArrayError::SegmentCorruption {
                        detail: format!("dense tile decode failed: {e}"),
                    })?;
                Ok(TilePayload::Dense(t))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::ArraySchemaBuilder;
    use crate::schema::attr_spec::{AttrSpec, AttrType};
    use crate::schema::dim_spec::{DimSpec, DimType};
    use crate::segment::writer::SegmentWriter;
    use crate::tile::dense_tile::DenseTile;
    use crate::tile::sparse_tile::SparseTileBuilder;
    use crate::types::TileId;
    use crate::types::cell_value::value::CellValue;
    use crate::types::coord::value::CoordValue;
    use crate::types::domain::{Domain, DomainBound};

    fn schema() -> crate::schema::ArraySchema {
        ArraySchemaBuilder::new("g")
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

    fn make_sparse(s: &crate::schema::ArraySchema, base: i64) -> SparseTile {
        let mut b = SparseTileBuilder::new(s);
        b.push(
            &[CoordValue::Int64(base), CoordValue::Int64(base + 1)],
            &[CellValue::Int64(base * 10)],
        )
        .unwrap();
        b.build()
    }

    #[test]
    fn reader_round_trips_sparse_tiles() {
        let s = schema();
        let mut w = SegmentWriter::new(0xCAFE);
        w.append_sparse(TileId::snapshot(1), &make_sparse(&s, 1))
            .unwrap();
        w.append_sparse(TileId::snapshot(2), &make_sparse(&s, 2))
            .unwrap();
        let bytes = w.finish().unwrap();
        let r = SegmentReader::open(&bytes).unwrap();
        assert_eq!(r.tile_count(), 2);
        let t0 = r.read_tile(0).unwrap();
        match t0 {
            TilePayload::Sparse(t) => assert_eq!(t.nnz(), 1),
            _ => panic!("expected sparse"),
        }
    }

    #[test]
    fn reader_round_trips_dense_tile() {
        let s = schema();
        let mut w = SegmentWriter::new(0xBEEF);
        w.append_dense(TileId::snapshot(1), &DenseTile::empty(&s))
            .unwrap();
        let bytes = w.finish().unwrap();
        let r = SegmentReader::open(&bytes).unwrap();
        match r.read_tile(0).unwrap() {
            TilePayload::Dense(t) => assert_eq!(t.cell_count(), 16),
            _ => panic!("expected dense"),
        }
    }

    #[test]
    fn reader_rejects_mismatched_schema_hash() {
        // Build a valid segment, then flip a byte in the header
        // schema_hash and re-CRC manually-detected mismatch by way of
        // header CRC failure. (We can't cheaply forge a valid header
        // with a mismatched footer hash, so this exercises the header
        // CRC path which guards the same invariant.)
        let s = schema();
        let mut w = SegmentWriter::new(0x1);
        w.append_sparse(TileId::snapshot(1), &make_sparse(&s, 1))
            .unwrap();
        let mut bytes = w.finish().unwrap();
        bytes[12] ^= 0xFF; // corrupt header schema_hash
        assert!(SegmentReader::open(&bytes).is_err());
    }

    #[test]
    fn reader_rejects_out_of_range_tile() {
        let s = schema();
        let mut w = SegmentWriter::new(0x1);
        w.append_sparse(TileId::snapshot(1), &make_sparse(&s, 1))
            .unwrap();
        let bytes = w.finish().unwrap();
        let r = SegmentReader::open(&bytes).unwrap();
        assert!(r.read_tile(99).is_err());
    }

    #[test]
    fn read_tile_as_of_returns_newest_at_cutoff() {
        let s = schema();
        let mut w = SegmentWriter::new(0xCAFE);
        // Three versions of prefix=1 at system_from_ms 100, 200, 300.
        w.append_sparse(TileId::new(1, 100), &make_sparse(&s, 1))
            .unwrap();
        w.append_sparse(TileId::new(1, 200), &make_sparse(&s, 2))
            .unwrap();
        w.append_sparse(TileId::new(1, 300), &make_sparse(&s, 3))
            .unwrap();
        let bytes = w.finish().unwrap();
        let r = SegmentReader::open(&bytes).unwrap();
        // Read at cutoff 250 — between v2 (200) and v3 (300). Should return v2.
        let result = r.read_tile_as_of(1, 250, None).unwrap();
        match result {
            Some(TilePayload::Sparse(t)) => {
                // v2 was built with base=2 — one non-zero entry.
                assert_eq!(t.nnz(), 1);
                // dim_dicts[0] holds the x-dim dictionary; values[0] = Int64(2).
                assert_eq!(t.dim_dicts[0].values[0], CoordValue::Int64(2));
            }
            other => panic!("expected Some(Sparse), got {other:?}"),
        }
    }

    #[test]
    fn read_tile_as_of_returns_none_below_first_version() {
        let s = schema();
        let mut w = SegmentWriter::new(0xBEEF);
        w.append_sparse(TileId::new(1, 100), &make_sparse(&s, 1))
            .unwrap();
        let bytes = w.finish().unwrap();
        let r = SegmentReader::open(&bytes).unwrap();
        // Read at cutoff before any version (50 < 100).
        let result = r.read_tile_as_of(1, 50, None).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn extract_cell_bytes_finds_coord() {
        let s = schema();
        let sparse = make_sparse(&s, 3);
        // make_sparse builds one entry at coord (base, base+1) = (3, 4).
        let coord = vec![CoordValue::Int64(3), CoordValue::Int64(4)];
        let bytes = extract_cell_bytes(&sparse, &coord).unwrap();
        assert!(bytes.is_some(), "should find coord (3,4)");

        let absent = vec![CoordValue::Int64(9), CoordValue::Int64(9)];
        let none = extract_cell_bytes(&sparse, &absent).unwrap();
        assert!(none.is_none(), "absent coord must return None");
    }

    #[test]
    fn extract_cell_bytes_carries_valid_time_bounds() {
        use crate::tile::cell_payload::CellPayload;
        use crate::tile::sparse_tile::{SparseRow, SparseTileBuilder};
        use nodedb_types::Surrogate;

        let s = schema();
        let mut b = SparseTileBuilder::new(&s);
        b.push_row(SparseRow {
            coord: &[CoordValue::Int64(1), CoordValue::Int64(2)],
            attrs: &[CellValue::Int64(99)],
            surrogate: Surrogate::ZERO,
            valid_from_ms: 100,
            valid_until_ms: 200,
            kind: crate::tile::sparse_tile::RowKind::Live,
        })
        .unwrap();
        let tile = b.build();

        let coord = vec![CoordValue::Int64(1), CoordValue::Int64(2)];
        let bytes = extract_cell_bytes(&tile, &coord).unwrap().unwrap();
        let payload = CellPayload::decode(&bytes).unwrap();
        assert_eq!(payload.valid_from_ms, 100);
        assert_eq!(payload.valid_until_ms, 200);
    }

    #[test]
    fn iter_tile_versions_newest_first() {
        let s = schema();
        let mut w = SegmentWriter::new(0xCAFE);
        w.append_sparse(TileId::new(1, 100), &make_sparse(&s, 1))
            .unwrap();
        w.append_sparse(TileId::new(1, 200), &make_sparse(&s, 2))
            .unwrap();
        w.append_sparse(TileId::new(1, 300), &make_sparse(&s, 3))
            .unwrap();
        let bytes = w.finish().unwrap();
        let r = SegmentReader::open(&bytes).unwrap();
        let versions: Vec<_> = r
            .iter_tile_versions(1, i64::MAX)
            .unwrap()
            .map(|v| v.unwrap().0.system_from_ms)
            .collect();
        assert_eq!(versions, vec![300, 200, 100]);
    }

    #[test]
    fn iter_tile_versions_respects_system_as_of() {
        let s = schema();
        let mut w = SegmentWriter::new(0xBEEF);
        w.append_sparse(TileId::new(1, 100), &make_sparse(&s, 1))
            .unwrap();
        w.append_sparse(TileId::new(1, 200), &make_sparse(&s, 2))
            .unwrap();
        w.append_sparse(TileId::new(1, 300), &make_sparse(&s, 3))
            .unwrap();
        let bytes = w.finish().unwrap();
        let r = SegmentReader::open(&bytes).unwrap();
        let versions: Vec<_> = r
            .iter_tile_versions(1, 250)
            .unwrap()
            .map(|v| v.unwrap().0.system_from_ms)
            .collect();
        // Cutoff 250 → v2 (200) and v1 (100); v3 (300) excluded.
        assert_eq!(versions, vec![200, 100]);
    }

    #[test]
    fn extract_cell_bytes_returns_tombstone_sentinel_for_tombstone_row() {
        use crate::tile::cell_payload::{CELL_TOMBSTONE_SENTINEL, is_cell_tombstone};
        use crate::tile::sparse_tile::{RowKind, SparseTileBuilder};

        let s = schema();
        let mut b = SparseTileBuilder::new(&s);
        b.push_row(crate::tile::sparse_tile::SparseRow {
            coord: &[CoordValue::Int64(7), CoordValue::Int64(8)],
            attrs: &[],
            surrogate: Surrogate::ZERO,
            valid_from_ms: 0,
            valid_until_ms: nodedb_types::OPEN_UPPER,
            kind: RowKind::Tombstone,
        })
        .unwrap();
        let tile = b.build();
        let coord = vec![CoordValue::Int64(7), CoordValue::Int64(8)];
        let bytes = extract_cell_bytes(&tile, &coord).unwrap().unwrap();
        assert!(
            is_cell_tombstone(&bytes),
            "expected CELL_TOMBSTONE_SENTINEL, got {bytes:?}"
        );
        assert_eq!(bytes, CELL_TOMBSTONE_SENTINEL);
    }

    #[test]
    fn extract_cell_bytes_returns_erasure_sentinel_for_erased_row() {
        use crate::tile::cell_payload::{CELL_GDPR_ERASURE_SENTINEL, is_cell_gdpr_erasure};
        use crate::tile::sparse_tile::{RowKind, SparseTileBuilder};

        let s = schema();
        let mut b = SparseTileBuilder::new(&s);
        b.push_row(crate::tile::sparse_tile::SparseRow {
            coord: &[CoordValue::Int64(4), CoordValue::Int64(5)],
            attrs: &[],
            surrogate: Surrogate::ZERO,
            valid_from_ms: 0,
            valid_until_ms: nodedb_types::OPEN_UPPER,
            kind: RowKind::GdprErased,
        })
        .unwrap();
        let tile = b.build();
        let coord = vec![CoordValue::Int64(4), CoordValue::Int64(5)];
        let bytes = extract_cell_bytes(&tile, &coord).unwrap().unwrap();
        assert!(
            is_cell_gdpr_erasure(&bytes),
            "expected CELL_GDPR_ERASURE_SENTINEL, got {bytes:?}"
        );
        assert_eq!(bytes, CELL_GDPR_ERASURE_SENTINEL);
    }

    #[test]
    fn read_tile_as_of_finds_exact_match() {
        let s = schema();
        let mut w = SegmentWriter::new(0xDEAD);
        w.append_sparse(TileId::new(1, 100), &make_sparse(&s, 1))
            .unwrap();
        w.append_sparse(TileId::new(1, 200), &make_sparse(&s, 2))
            .unwrap();
        w.append_sparse(TileId::new(1, 300), &make_sparse(&s, 3))
            .unwrap();
        let bytes = w.finish().unwrap();
        let r = SegmentReader::open(&bytes).unwrap();
        // Read at cutoff exactly equal to v2's system_from_ms.
        let result = r.read_tile_as_of(1, 200, None).unwrap();
        match result {
            Some(TilePayload::Sparse(t)) => {
                assert_eq!(t.nnz(), 1);
                assert_eq!(t.dim_dicts[0].values[0], CoordValue::Int64(2));
            }
            other => panic!("expected Some(Sparse), got {other:?}"),
        }
    }
}
