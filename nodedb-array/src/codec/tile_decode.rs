// Structural sparse-tile decoder — symmetric to tile_encode.
//
// Validates the version byte, dispatches on CodecTag, and reconstructs
// a SparseTile from the structural payload. Legacy (v3) msgpack tiles are
// NOT handled here; the segment reader peeks the tag and dispatches to
// zerompk for those.

use crate::codec::column_codec::{
    decode_attr_col, decode_row_kinds, decode_surrogates, decode_timestamps_col,
};
use crate::codec::coord_rle::decode_coord_axis_rle;
use crate::codec::limits::{
    MAX_ATTRS_PER_TILE, MAX_AXES_PER_TILE, MAX_CELLS_PER_TILE, check_decoded_size,
};
use crate::codec::tag::{CodecTag, peek_tag};
use crate::error::{ArrayError, ArrayResult};
use crate::tile::mbr::TileMBR;
use crate::tile::sparse_tile::SparseTile;

const SUPPORTED_PAYLOAD_VERSION: u8 = 1;

fn read_framed<'a>(data: &'a [u8], pos: &mut usize) -> ArrayResult<&'a [u8]> {
    if *pos + 4 > data.len() {
        return Err(ArrayError::SegmentCorruption {
            detail: "framed block: truncated length".into(),
        });
    }
    let len = u32::from_le_bytes(data[*pos..*pos + 4].try_into().unwrap()) as usize;
    *pos += 4;
    if *pos + len > data.len() {
        return Err(ArrayError::SegmentCorruption {
            detail: format!(
                "framed block: body truncated (need {len}, have {})",
                data.len() - *pos
            ),
        });
    }
    let slice = &data[*pos..*pos + len];
    *pos += len;
    Ok(slice)
}

/// Decode a tile payload previously written by `encode_sparse_tile`.
///
/// The `payload` slice must start at the tag byte (i.e. after BlockFraming
/// has been unwrapped by the segment reader).
pub fn decode_sparse_tile(payload: &[u8]) -> ArrayResult<SparseTile> {
    if payload.len() < 2 {
        return Err(ArrayError::SegmentCorruption {
            detail: "sparse tile payload too short".into(),
        });
    }

    let tag_result = peek_tag(payload).ok_or_else(|| {
        // peek_tag returns None for both legacy msgpack and unknown bytes.
        // The reader should never call us with a legacy payload.
        ArrayError::SegmentCorruption {
            detail: format!(
                "decode_sparse_tile called with legacy or unknown tag byte: {:#04x}",
                payload[0]
            ),
        }
    })?;

    let version = payload[1];
    if version != SUPPORTED_PAYLOAD_VERSION {
        return Err(ArrayError::SegmentCorruption {
            detail: format!("unsupported tile payload version: {version}"),
        });
    }

    match tag_result {
        CodecTag::Raw => decode_raw(&payload[2..]),
        CodecTag::Structural => decode_structural(&payload[2..]),
    }
}

fn decode_raw(body: &[u8]) -> ArrayResult<SparseTile> {
    zerompk::from_msgpack(body).map_err(|e| ArrayError::SegmentCorruption {
        detail: format!("raw tile decode: {e}"),
    })
}

fn decode_structural(body: &[u8]) -> ArrayResult<SparseTile> {
    let mut pos = 0;

    if pos + 8 > body.len() {
        return Err(ArrayError::SegmentCorruption {
            detail: "structural tile: truncated counts".into(),
        });
    }
    let cell_count = u32::from_le_bytes(body[pos..pos + 4].try_into().unwrap()) as usize;
    pos += 4;
    check_decoded_size(cell_count, MAX_CELLS_PER_TILE, "cell_count")?;
    let axis_count = u32::from_le_bytes(body[pos..pos + 4].try_into().unwrap()) as usize;
    pos += 4;
    check_decoded_size(axis_count, MAX_AXES_PER_TILE, "axis_count")?;

    // Coordinate axes.
    let mut dim_dicts = Vec::with_capacity(axis_count);
    for _ in 0..axis_count {
        let axis_bytes = read_framed(body, &mut pos)?;
        let mut inner_pos = 0;
        let dict = decode_coord_axis_rle(axis_bytes, &mut inner_pos)?;
        dim_dicts.push(dict);
    }

    // Surrogates.
    let surr_bytes = read_framed(body, &mut pos)?;
    let surrogates = decode_surrogates(surr_bytes)?;

    // Row kinds.
    let rk_bytes = read_framed(body, &mut pos)?;
    let row_kinds = decode_row_kinds(rk_bytes)?;

    // system_from_ms placeholder (not stored in SparseTile, just skip).
    let _sys_bytes = read_framed(body, &mut pos)?;

    // valid_from_ms.
    let vf_bytes = read_framed(body, &mut pos)?;
    let valid_from_ms = decode_timestamps_col(vf_bytes)?;

    // valid_until_ms.
    let vu_bytes = read_framed(body, &mut pos)?;
    let valid_until_ms = decode_timestamps_col(vu_bytes)?;

    // Attr columns.
    if pos + 4 > body.len() {
        return Err(ArrayError::SegmentCorruption {
            detail: "structural tile: truncated attr count".into(),
        });
    }
    let attr_count = u32::from_le_bytes(body[pos..pos + 4].try_into().unwrap()) as usize;
    pos += 4;
    check_decoded_size(attr_count, MAX_ATTRS_PER_TILE, "attr_count")?;

    let mut attr_cols = Vec::with_capacity(attr_count);
    for _ in 0..attr_count {
        let col_bytes = read_framed(body, &mut pos)?;
        let col = decode_attr_col(col_bytes)?;
        attr_cols.push(col);
    }

    // Reconstruct MBR from decoded data — we don't persist the MBR inside the
    // structural payload (it lives in the footer TileEntry instead).
    // Use a zero-dimension MBR so the tile can be used for reads; compaction
    // re-derives the full MBR if needed.
    let mbr = TileMBR::new(axis_count, attr_count);

    // Validate sizes match cell_count.
    if surrogates.len() != cell_count {
        return Err(ArrayError::SegmentCorruption {
            detail: format!(
                "structural tile: surrogate count {surr} != cell_count {cell_count}",
                surr = surrogates.len()
            ),
        });
    }

    Ok(SparseTile {
        dim_dicts,
        attr_cols,
        surrogates,
        valid_from_ms,
        valid_until_ms,
        row_kinds,
        mbr,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::tile_encode::encode_sparse_tile;
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
            .attr(AttrSpec::new("v", AttrType::Int64, true))
            .tile_extents(vec![1000])
            .build()
            .unwrap()
    }

    fn make_tile(s: &crate::schema::ArraySchema, n: usize) -> SparseTile {
        let mut b = SparseTileBuilder::new(s);
        for i in 0..n {
            b.push_row(SparseRow {
                coord: &[CoordValue::Int64(i as i64)],
                attrs: &[CellValue::Int64(i as i64 * 2)],
                surrogate: Surrogate::ZERO,
                valid_from_ms: i as i64,
                valid_until_ms: OPEN_UPPER,
                kind: RowKind::Live,
            })
            .unwrap();
        }
        b.build()
    }

    fn roundtrip(tile: &SparseTile) -> SparseTile {
        let mut buf = Vec::new();
        encode_sparse_tile(tile, &mut buf).unwrap();
        decode_sparse_tile(&buf).unwrap()
    }

    #[test]
    fn empty_tile_roundtrip() {
        let s = schema();
        let tile = SparseTile::empty(&s);
        let out = roundtrip(&tile);
        assert_eq!(out.surrogates, tile.surrogates);
        assert_eq!(out.row_kinds, tile.row_kinds);
    }

    #[test]
    fn small_tile_roundtrip() {
        let s = schema();
        let tile = make_tile(&s, 4);
        let out = roundtrip(&tile);
        assert_eq!(out.valid_from_ms, tile.valid_from_ms);
        assert_eq!(out.attr_cols, tile.attr_cols);
    }

    #[test]
    fn structural_tile_roundtrip() {
        let s = schema();
        let tile = make_tile(&s, 50);
        let out = roundtrip(&tile);
        assert_eq!(out.surrogates.len(), tile.surrogates.len());
        assert_eq!(out.valid_from_ms, tile.valid_from_ms);
        assert_eq!(out.valid_until_ms, tile.valid_until_ms);
        assert_eq!(out.attr_cols, tile.attr_cols);
        assert_eq!(out.row_kinds, tile.row_kinds);
    }

    #[test]
    fn one_thousand_cells_roundtrip() {
        let s = schema();
        let tile = make_tile(&s, 1000);
        let out = roundtrip(&tile);
        assert_eq!(out.surrogates.len(), 1000);
        assert_eq!(out.dim_dicts[0].indices, tile.dim_dicts[0].indices);
    }

    #[test]
    fn tombstone_rows_roundtrip() {
        let s = schema();
        let mut b = SparseTileBuilder::new(&s);
        for i in 0..20 {
            b.push_row(SparseRow {
                coord: &[CoordValue::Int64(i)],
                attrs: &[CellValue::Int64(i)],
                surrogate: Surrogate::ZERO,
                valid_from_ms: 0,
                valid_until_ms: OPEN_UPPER,
                kind: RowKind::Live,
            })
            .unwrap();
        }
        b.push_row(SparseRow {
            coord: &[CoordValue::Int64(99)],
            attrs: &[],
            surrogate: Surrogate::ZERO,
            valid_from_ms: 0,
            valid_until_ms: OPEN_UPPER,
            kind: RowKind::Tombstone,
        })
        .unwrap();
        let tile = b.build();
        let out = roundtrip(&tile);
        assert_eq!(out.row_kinds, tile.row_kinds);
    }

    #[test]
    fn invalid_version_returns_error() {
        let s = schema();
        let tile = make_tile(&s, 20);
        let mut buf = Vec::new();
        encode_sparse_tile(&tile, &mut buf).unwrap();
        // Corrupt the version byte.
        buf[1] = 99;
        let err = decode_sparse_tile(&buf).unwrap_err();
        assert!(matches!(err, ArrayError::SegmentCorruption { .. }));
    }

    #[test]
    fn valid_time_variants_roundtrip() {
        let s = schema();
        let mut b = SparseTileBuilder::new(&s);
        b.push_row(SparseRow {
            coord: &[CoordValue::Int64(1)],
            attrs: &[CellValue::Int64(10)],
            surrogate: Surrogate::ZERO,
            valid_from_ms: 100,
            valid_until_ms: 500,
            kind: RowKind::Live,
        })
        .unwrap();
        b.push_row(SparseRow {
            coord: &[CoordValue::Int64(2)],
            attrs: &[CellValue::Int64(20)],
            surrogate: Surrogate::ZERO,
            valid_from_ms: 200,
            valid_until_ms: OPEN_UPPER,
            kind: RowKind::Live,
        })
        .unwrap();
        // need >=8 to get Structural
        for i in 3..20 {
            b.push_row(SparseRow {
                coord: &[CoordValue::Int64(i)],
                attrs: &[CellValue::Int64(i)],
                surrogate: Surrogate::ZERO,
                valid_from_ms: i * 10,
                valid_until_ms: OPEN_UPPER,
                kind: RowKind::Live,
            })
            .unwrap();
        }
        let tile = b.build();
        let out = roundtrip(&tile);
        assert_eq!(out.valid_from_ms[0], 100);
        assert_eq!(out.valid_until_ms[0], 500);
        assert_eq!(out.valid_until_ms[1], OPEN_UPPER);
    }
}
