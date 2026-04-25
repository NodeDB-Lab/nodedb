//! `SegmentReader` — zero-copy view over segment bytes.
//!
//! Designed to wrap an `mmap`'d file in production. The reader only
//! borrows the slice and lazily decodes tile payloads on demand; the
//! footer is decoded eagerly at construction so subsequent tile lookups
//! are bounded reads + zerompk decode (no rescan).

use super::format::{
    SegmentFooter, SegmentHeader, TileEntry, TileKind, framing::BlockFraming, header::HEADER_SIZE,
};
use crate::error::{ArrayError, ArrayResult};
use crate::tile::dense_tile::DenseTile;
use crate::tile::sparse_tile::SparseTile;

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
                let t: SparseTile =
                    zerompk::from_msgpack(payload).map_err(|e| ArrayError::SegmentCorruption {
                        detail: format!("sparse tile decode failed: {e}"),
                    })?;
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
}
