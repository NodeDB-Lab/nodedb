// Structural sparse-tile encoder.
//
// encode_sparse_tile writes the following into `out` (BlockFraming is applied
// by the segment writer around this payload):
//
//   [u8 tag]          CodecTag::Raw or CodecTag::Structural
//   [u8 version = 1]  payload format version
//   [u32 LE cell_count]
//   [u32 LE axis_count]
//   per axis: [u32 LE encoded_len][coord_rle payload]
//   [u32 LE surrogates_len][fastlanes payload]
//   [u32 LE row_kinds_len][raw u8s]
//   [u32 LE system_from_ms_len][gorilla payload — absent for Raw tag]
//   [u32 LE valid_from_ms_len][gorilla payload]
//   [u32 LE valid_until_ms_len][gorilla payload]
//   [u32 LE attr_count]
//   per attr: [u32 LE col_len][column_codec payload]
//
// For CodecTag::Raw, the full SparseTile is serialised with zerompk and
// written verbatim after the two header bytes (tag + version). The decoder
// mirrors this.

use crate::codec::column_codec::{
    encode_attr_col, encode_row_kinds, encode_surrogates, encode_timestamps_col,
};
use crate::codec::coord_rle::encode_coord_axis_rle;
use crate::codec::tag::CodecTag;
use crate::error::{ArrayError, ArrayResult};
use crate::tile::sparse_tile::SparseTile;

const PAYLOAD_VERSION: u8 = 1;

/// Threshold below which we fall back to Raw (zerompk) encoding.
const STRUCTURAL_MIN_CELLS: usize = 8;

fn choose_tag(tile: &SparseTile) -> CodecTag {
    let cell_count = tile.surrogates.len();
    if cell_count < STRUCTURAL_MIN_CELLS {
        return CodecTag::Raw;
    }
    // Sentinel-only: all row_kinds non-zero and no attr columns have data.
    let all_sentinel = !tile.row_kinds.is_empty()
        && tile.row_kinds.iter().all(|&k| k != 0)
        && tile.attr_cols.iter().all(|col| col.is_empty());
    if all_sentinel {
        return CodecTag::Raw;
    }
    CodecTag::Structural
}

fn write_framed(chunk: &[u8], out: &mut Vec<u8>) {
    out.extend_from_slice(&(chunk.len() as u32).to_le_bytes());
    out.extend_from_slice(chunk);
}

/// Encode a `SparseTile` into `out`. The segment writer wraps this payload
/// in BlockFraming (length + CRC).
pub fn encode_sparse_tile(tile: &SparseTile, out: &mut Vec<u8>) -> ArrayResult<()> {
    let tag = choose_tag(tile);
    out.push(tag.as_byte());
    out.push(PAYLOAD_VERSION);

    match tag {
        CodecTag::Raw => encode_raw(tile, out),
        CodecTag::Structural => encode_structural(tile, out),
    }
}

fn encode_raw(tile: &SparseTile, out: &mut Vec<u8>) -> ArrayResult<()> {
    let bytes = zerompk::to_msgpack_vec(tile).map_err(|e| ArrayError::SegmentCorruption {
        detail: format!("raw tile encode: {e}"),
    })?;
    out.extend_from_slice(&bytes);
    Ok(())
}

fn encode_structural(tile: &SparseTile, out: &mut Vec<u8>) -> ArrayResult<()> {
    let cell_count = tile.surrogates.len() as u32;
    let axis_count = tile.dim_dicts.len() as u32;
    out.extend_from_slice(&cell_count.to_le_bytes());
    out.extend_from_slice(&axis_count.to_le_bytes());

    // Coordinate axes.
    for dict in &tile.dim_dicts {
        let mut axis_buf = Vec::new();
        encode_coord_axis_rle(dict, &mut axis_buf)?;
        write_framed(&axis_buf, out);
    }

    // Surrogates.
    let surr_bytes = encode_surrogates(&tile.surrogates);
    write_framed(&surr_bytes, out);

    // Row kinds.
    let rk_bytes = encode_row_kinds(&tile.row_kinds);
    write_framed(&rk_bytes, out);

    // Timestamp columns.
    let sys_bytes = encode_timestamps_col(&[]);
    // system_from_ms is not stored in SparseTile (it's the tile_id's field).
    // We emit an empty placeholder to keep the format symmetric.
    write_framed(&sys_bytes, out);

    let vf_bytes = encode_timestamps_col(&tile.valid_from_ms);
    write_framed(&vf_bytes, out);

    let vu_bytes = encode_timestamps_col(&tile.valid_until_ms);
    write_framed(&vu_bytes, out);

    // Attr columns.
    let attr_count = tile.attr_cols.len() as u32;
    out.extend_from_slice(&attr_count.to_le_bytes());
    for col in &tile.attr_cols {
        let col_bytes = encode_attr_col(col)?;
        write_framed(&col_bytes, out);
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::tile_decode::decode_sparse_tile;
    use crate::schema::ArraySchemaBuilder;
    use crate::schema::attr_spec::{AttrSpec, AttrType};
    use crate::schema::dim_spec::{DimSpec, DimType};
    use crate::tile::sparse_tile::{RowKind, SparseRow, SparseTileBuilder};
    use crate::types::cell_value::value::CellValue;
    use crate::types::coord::value::CoordValue;
    use crate::types::domain::{Domain, DomainBound};
    use nodedb_types::{OPEN_UPPER, Surrogate};

    fn schema() -> crate::schema::ArraySchema {
        ArraySchemaBuilder::new("t")
            .dim(DimSpec::new(
                "x",
                DimType::Int64,
                Domain::new(DomainBound::Int64(0), DomainBound::Int64(1_000_000)),
            ))
            .dim(DimSpec::new(
                "y",
                DimType::Int64,
                Domain::new(DomainBound::Int64(0), DomainBound::Int64(1_000_000)),
            ))
            .attr(AttrSpec::new("v", AttrType::Int64, true))
            .tile_extents(vec![1000, 1000])
            .build()
            .unwrap()
    }

    fn make_tile(s: &crate::schema::ArraySchema, n: usize) -> SparseTile {
        let mut b = SparseTileBuilder::new(s);
        for i in 0..n {
            b.push_row(SparseRow {
                coord: &[CoordValue::Int64(i as i64), CoordValue::Int64(i as i64 * 2)],
                attrs: &[CellValue::Int64(i as i64)],
                surrogate: Surrogate::ZERO,
                valid_from_ms: i as i64 * 10,
                valid_until_ms: OPEN_UPPER,
                kind: RowKind::Live,
            })
            .unwrap();
        }
        b.build()
    }

    #[test]
    fn small_tile_uses_raw_tag() {
        let s = schema();
        let tile = make_tile(&s, 3);
        let mut buf = Vec::new();
        encode_sparse_tile(&tile, &mut buf).unwrap();
        assert_eq!(buf[0], CodecTag::Raw.as_byte());
    }

    #[test]
    fn large_tile_uses_structural_tag() {
        let s = schema();
        let tile = make_tile(&s, 20);
        let mut buf = Vec::new();
        encode_sparse_tile(&tile, &mut buf).unwrap();
        assert_eq!(buf[0], CodecTag::Structural.as_byte());
    }

    #[test]
    fn small_tile_roundtrip() {
        let s = schema();
        let tile = make_tile(&s, 5);
        let mut buf = Vec::new();
        encode_sparse_tile(&tile, &mut buf).unwrap();
        let decoded = decode_sparse_tile(&buf).unwrap();
        assert_eq!(decoded.surrogates, tile.surrogates);
        assert_eq!(decoded.valid_from_ms, tile.valid_from_ms);
        assert_eq!(decoded.row_kinds, tile.row_kinds);
    }

    #[test]
    fn large_tile_roundtrip() {
        let s = schema();
        let tile = make_tile(&s, 100);
        let mut buf = Vec::new();
        encode_sparse_tile(&tile, &mut buf).unwrap();
        let decoded = decode_sparse_tile(&buf).unwrap();
        assert_eq!(decoded.surrogates, tile.surrogates);
        assert_eq!(decoded.attr_cols, tile.attr_cols);
        assert_eq!(decoded.dim_dicts.len(), tile.dim_dicts.len());
    }

    #[test]
    fn sentinel_only_tile_uses_raw() {
        let s = schema();
        let mut b = SparseTileBuilder::new(&s);
        for i in 0..20 {
            b.push_row(SparseRow {
                coord: &[CoordValue::Int64(i), CoordValue::Int64(i)],
                attrs: &[],
                surrogate: Surrogate::ZERO,
                valid_from_ms: 0,
                valid_until_ms: OPEN_UPPER,
                kind: RowKind::Tombstone,
            })
            .unwrap();
        }
        let tile = b.build();
        let mut buf = Vec::new();
        encode_sparse_tile(&tile, &mut buf).unwrap();
        assert_eq!(buf[0], CodecTag::Raw.as_byte());
    }

    #[test]
    fn version_byte_is_one() {
        let s = schema();
        let tile = make_tile(&s, 20);
        let mut buf = Vec::new();
        encode_sparse_tile(&tile, &mut buf).unwrap();
        assert_eq!(buf[1], PAYLOAD_VERSION);
    }

    #[test]
    fn empty_tile_encodes_and_decodes() {
        let s = schema();
        let tile = SparseTile::empty(&s);
        let mut buf = Vec::new();
        encode_sparse_tile(&tile, &mut buf).unwrap();
        let decoded = decode_sparse_tile(&buf).unwrap();
        assert_eq!(decoded.surrogates, tile.surrogates);
    }
}
