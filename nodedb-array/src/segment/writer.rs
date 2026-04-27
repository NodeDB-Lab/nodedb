//! Streaming `SegmentWriter` — appends tile payloads, finalises with footer.
//!
//! Layout: `[ HEADER | framed_tile_0 | framed_tile_1 | ... | footer_block | trailer ]`.
//! Each tile is encoded as zerompk + framed (length + CRC). Tile
//! offsets and MBRs are recorded in-memory and flushed in the footer.

use super::format::{
    SegmentFooter, SegmentHeader, TileEntry, TileKind, framing::BlockFraming, header::HEADER_SIZE,
};
use crate::error::{ArrayError, ArrayResult};
use crate::tile::dense_tile::DenseTile;
use crate::tile::sparse_tile::SparseTile;
use crate::types::TileId;

pub struct SegmentWriter {
    schema_hash: u64,
    buf: Vec<u8>,
    entries: Vec<TileEntry>,
    finished: bool,
}

impl SegmentWriter {
    pub fn new(schema_hash: u64) -> Self {
        let mut buf = Vec::new();
        SegmentHeader::new(schema_hash).encode_to(&mut buf);
        Self {
            schema_hash,
            buf,
            entries: Vec::new(),
            finished: false,
        }
    }

    pub fn append_sparse(&mut self, tile_id: TileId, tile: &SparseTile) -> ArrayResult<()> {
        let mut payload = Vec::new();
        crate::codec::tile_encode::encode_sparse_tile(tile, &mut payload)?;
        self.append_framed(tile_id, TileKind::Sparse, &payload, tile.mbr.clone())
    }

    pub fn append_dense(&mut self, tile_id: TileId, tile: &DenseTile) -> ArrayResult<()> {
        let payload = zerompk::to_msgpack_vec(tile).map_err(|e| ArrayError::SegmentCorruption {
            detail: format!("dense tile encode failed: {e}"),
        })?;
        self.append_framed(tile_id, TileKind::Dense, &payload, tile.mbr.clone())
    }

    fn append_framed(
        &mut self,
        tile_id: TileId,
        kind: TileKind,
        payload: &[u8],
        mbr: crate::tile::mbr::TileMBR,
    ) -> ArrayResult<()> {
        if let Some(last) = self.entries.last()
            && tile_id <= last.tile_id
        {
            return Err(ArrayError::SegmentCorruption {
                detail: format!(
                    "tile IDs not strictly monotonic ascending: \
                     prev=({},{}) new=({},{})",
                    last.tile_id.hilbert_prefix,
                    last.tile_id.system_from_ms,
                    tile_id.hilbert_prefix,
                    tile_id.system_from_ms,
                ),
            });
        }
        let offset = self.buf.len() as u64;
        let framed_len = BlockFraming::encode(payload, &mut self.buf);
        self.entries.push(TileEntry::new(
            tile_id,
            kind,
            offset,
            framed_len as u32,
            mbr,
        ));
        Ok(())
    }

    pub fn tile_count(&self) -> usize {
        self.entries.len()
    }

    /// Finalise the segment by appending the footer + trailer. Consumes
    /// the writer and returns the complete byte vector.
    pub fn finish(mut self) -> ArrayResult<Vec<u8>> {
        if self.finished {
            return Err(ArrayError::SegmentCorruption {
                detail: "SegmentWriter::finish called twice".into(),
            });
        }
        let footer = SegmentFooter::new(self.schema_hash, std::mem::take(&mut self.entries));
        footer.encode_to(&mut self.buf)?;
        self.finished = true;
        Ok(self.buf)
    }

    pub fn header_size(&self) -> usize {
        HEADER_SIZE
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::ArraySchemaBuilder;
    use crate::schema::attr_spec::{AttrSpec, AttrType};
    use crate::schema::dim_spec::{DimSpec, DimType};
    use crate::tile::sparse_tile::SparseTileBuilder;
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

    fn sparse_tile(s: &crate::schema::ArraySchema) -> SparseTile {
        let mut b = SparseTileBuilder::new(s);
        b.push(
            &[CoordValue::Int64(1), CoordValue::Int64(2)],
            &[CellValue::Int64(10)],
        )
        .unwrap();
        b.push(
            &[CoordValue::Int64(3), CoordValue::Int64(0)],
            &[CellValue::Int64(20)],
        )
        .unwrap();
        b.build()
    }

    #[test]
    fn writer_round_trip_via_footer() {
        let s = schema();
        let mut w = SegmentWriter::new(0x1234);
        w.append_sparse(TileId::snapshot(1), &sparse_tile(&s))
            .unwrap();
        w.append_sparse(TileId::snapshot(2), &sparse_tile(&s))
            .unwrap();
        let bytes = w.finish().unwrap();
        let footer = SegmentFooter::decode(&bytes).unwrap();
        assert_eq!(footer.schema_hash, 0x1234);
        assert_eq!(footer.tiles.len(), 2);
        assert_eq!(footer.tiles[0].tile_id.hilbert_prefix, 1);
        assert_eq!(footer.tiles[1].tile_id.hilbert_prefix, 2);
    }

    #[test]
    fn writer_emits_header_first() {
        let w = SegmentWriter::new(0xAA);
        let bytes = w.finish().unwrap();
        let h = SegmentHeader::decode(&bytes).unwrap();
        assert_eq!(h.schema_hash, 0xAA);
    }

    #[test]
    fn writer_rejects_non_monotonic_tile_ids() {
        let s = schema();
        let mut w = SegmentWriter::new(0x1234);
        w.append_sparse(TileId::new(2, 100), &sparse_tile(&s))
            .unwrap();
        let err = w
            .append_sparse(TileId::new(1, 100), &sparse_tile(&s))
            .unwrap_err();
        assert!(
            matches!(err, crate::error::ArrayError::SegmentCorruption { .. }),
            "expected SegmentCorruption, got {err:?}"
        );
    }

    #[test]
    fn writer_allows_same_prefix_different_system_times() {
        let s = schema();
        let mut w = SegmentWriter::new(0x1234);
        w.append_sparse(TileId::new(5, 100), &sparse_tile(&s))
            .unwrap();
        w.append_sparse(TileId::new(5, 200), &sparse_tile(&s))
            .unwrap();
        let bytes = w.finish().unwrap();
        let footer = SegmentFooter::decode(&bytes).unwrap();
        assert_eq!(footer.tiles.len(), 2);
        assert_eq!(footer.tiles[0].tile_id, TileId::new(5, 100));
        assert_eq!(footer.tiles[1].tile_id, TileId::new(5, 200));
    }
}
