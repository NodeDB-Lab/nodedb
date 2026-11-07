// SPDX-License-Identifier: Apache-2.0

// Structural sparse-tile decoder — symmetric to tile_encode.
//
// Validates the version byte, dispatches on CodecTag, and reconstructs
// a SparseTile from the structural payload. A payload whose first byte is
// no CodecTag is corruption.

use crate::codec::attr_col::decode_attr_col;
use crate::codec::column_codec::{decode_row_kinds, decode_surrogates, decode_timestamps_col};
use crate::codec::coord_rle::decode_coord_axis_rle;
use crate::codec::frame::{take_bytes, take_len};
use crate::codec::limits::{
    MAX_ATTRS_PER_TILE, MAX_AXES_PER_TILE, MAX_CELLS_PER_TILE, check_decoded_size, checked_capacity,
};
use crate::codec::tag::{CodecTag, peek_tag};
use crate::error::{ArrayError, ArrayResult};
use crate::tile::mbr::{MbrBuilder, TileMBR};
use crate::tile::sparse_tile::{DimDict, RowKind, SparseTile};
use crate::types::cell_value::value::CellValue;
use crate::types::coord::value::CoordValue;

const SUPPORTED_PAYLOAD_VERSION: u8 = 1;

fn read_framed<'a>(data: &'a [u8], pos: &mut usize) -> ArrayResult<&'a [u8]> {
    let len = take_len(data, pos, "framed block: truncated length")?;
    take_bytes(data, pos, len, "framed block: body truncated")
}

/// Decode a tile payload previously written by `encode_sparse_tile`.
///
/// The `payload` slice must start at the tag byte (i.e. after BlockFraming
/// has been unwrapped by the segment reader). A structural payload stores no
/// MBR, so this rebuilds it from the rows. A segment reader holds the MBR in
/// the tile's footer entry and decodes with [`decode_sparse_tile_with_mbr`].
pub fn decode_sparse_tile(payload: &[u8]) -> ArrayResult<SparseTile> {
    decode_sparse_tile_inner(payload, None)
}

/// Decode a tile payload whose MBR is `stored_mbr`, the one its segment
/// footer entry holds. A raw payload carries its own MBR and keeps it.
pub fn decode_sparse_tile_with_mbr(payload: &[u8], stored_mbr: TileMBR) -> ArrayResult<SparseTile> {
    decode_sparse_tile_inner(payload, Some(stored_mbr))
}

fn decode_sparse_tile_inner(
    payload: &[u8],
    stored_mbr: Option<TileMBR>,
) -> ArrayResult<SparseTile> {
    if payload.len() < 2 {
        return Err(ArrayError::SegmentCorruption {
            detail: "sparse tile payload too short".into(),
        });
    }

    let tag_result = peek_tag(payload).ok_or_else(|| ArrayError::SegmentCorruption {
        detail: format!(
            "sparse tile payload has unknown tag byte: {:#04x}",
            payload[0]
        ),
    })?;

    let version = payload[1];
    if version != SUPPORTED_PAYLOAD_VERSION {
        return Err(ArrayError::SegmentCorruption {
            detail: format!("unsupported tile payload version: {version}"),
        });
    }

    match tag_result {
        CodecTag::Raw => decode_raw(&payload[2..]),
        CodecTag::Structural => decode_structural(&payload[2..], stored_mbr),
    }
}

fn decode_raw(body: &[u8]) -> ArrayResult<SparseTile> {
    let tile: SparseTile =
        zerompk::from_msgpack(body).map_err(|e| ArrayError::SegmentCorruption {
            detail: format!("raw tile decode: {e}"),
        })?;
    tile.check_stored_identities()?;
    Ok(tile)
}

fn decode_structural(body: &[u8], stored_mbr: Option<TileMBR>) -> ArrayResult<SparseTile> {
    let mut pos = 0;

    let cell_count = take_len(body, &mut pos, "structural tile: truncated cell count")?;
    check_decoded_size(cell_count, MAX_CELLS_PER_TILE, "cell_count")?;
    let axis_count = take_len(body, &mut pos, "structural tile: truncated axis count")?;
    let axis_capacity = checked_capacity(
        axis_count,
        MAX_AXES_PER_TILE,
        body.len() - pos,
        4,
        "axis_count",
    )?;

    // Coordinate axes.
    let mut dim_dicts = Vec::with_capacity(axis_capacity);
    for _ in 0..axis_count {
        let axis_bytes = read_framed(body, &mut pos)?;
        let mut inner_pos = 0;
        let dict = decode_coord_axis_rle(axis_bytes, &mut inner_pos)?;
        dim_dicts.push(dict);
    }

    // Surrogates: live rows only, in row order.
    let surr_bytes = read_framed(body, &mut pos)?;
    let live_surrogates = decode_surrogates(surr_bytes)?;

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
    let attr_count = take_len(body, &mut pos, "structural tile: truncated attr count")?;
    let attr_capacity = checked_capacity(
        attr_count,
        MAX_ATTRS_PER_TILE,
        body.len() - pos,
        4,
        "attr_count",
    )?;

    // An attr column holds one value per live row.
    let live_rows = row_kinds
        .iter()
        .filter(|&&kind| kind == RowKind::Live.as_u8())
        .count();
    let mut attr_cols = Vec::with_capacity(attr_capacity);
    for _ in 0..attr_count {
        let col_bytes = read_framed(body, &mut pos)?;
        let col = decode_attr_col(col_bytes, live_rows)?;
        attr_cols.push(col);
    }

    // Validate sizes match cell_count.
    if row_kinds.len() != cell_count {
        return Err(ArrayError::SegmentCorruption {
            detail: format!(
                "structural tile: row kind count {kinds} != cell_count {cell_count}",
                kinds = row_kinds.len()
            ),
        });
    }
    let surrogates = spread_live_surrogates(&row_kinds, live_surrogates)?;
    // The structural payload stores no MBR. Readers count and bound a tile by
    // its MBR: the segment footer's copy when the caller has it, else one
    // rebuilt from the decoded rows.
    let mbr = match stored_mbr {
        Some(mbr) => mbr,
        None => rebuild_mbr(&dim_dicts, &attr_cols, &row_kinds)?,
    };

    let tile = SparseTile {
        dim_dicts,
        attr_cols,
        surrogates,
        valid_from_ms,
        valid_until_ms,
        row_kinds,
        mbr,
    };
    tile.check_stored_identities()?;
    Ok(tile)
}

/// The MBR the tile builder folds for these rows: every live row's coords
/// and attrs, and no sentinel row. Attr columns hold one value per live row.
fn rebuild_mbr(
    dim_dicts: &[DimDict],
    attr_cols: &[Vec<CellValue>],
    row_kinds: &[u8],
) -> ArrayResult<TileMBR> {
    let mut mbr = MbrBuilder::new(dim_dicts.len(), attr_cols.len());
    let mut coord: Vec<CoordValue> = Vec::with_capacity(dim_dicts.len());
    let mut attrs: Vec<CellValue> = Vec::with_capacity(attr_cols.len());
    let mut live = 0usize;
    for (row, &kind) in row_kinds.iter().enumerate() {
        if RowKind::from_u8(kind)? != RowKind::Live {
            continue;
        }
        coord.clear();
        for dict in dim_dicts {
            let value = dict
                .indices
                .get(row)
                .and_then(|&i| dict.values.get(i as usize))
                .ok_or_else(|| ArrayError::SegmentCorruption {
                    detail: format!("structural tile: row {row} has no coordinate on an axis"),
                })?;
            coord.push(value.clone());
        }
        attrs.clear();
        for col in attr_cols {
            let value = col.get(live).ok_or_else(|| ArrayError::SegmentCorruption {
                detail: format!("structural tile: live row {live} has no value in an attr column"),
            })?;
            attrs.push(value.clone());
        }
        mbr.fold(&coord, &attrs);
        live += 1;
    }
    Ok(mbr.build())
}

/// Place the live-row surrogate column back onto every row: a live row takes
/// the next surrogate, a tombstone or erasure row holds none. The column must
/// hold exactly one surrogate per live row.
fn spread_live_surrogates(
    row_kinds: &[u8],
    live_surrogates: Vec<nodedb_types::Surrogate>,
) -> ArrayResult<Vec<Option<nodedb_types::Surrogate>>> {
    let live_count = live_surrogates.len();
    let mut live = live_surrogates.into_iter();
    let mut surrogates = Vec::with_capacity(row_kinds.len());
    for (row, &kind) in row_kinds.iter().enumerate() {
        if RowKind::from_u8(kind)? == RowKind::Live {
            let surrogate = live.next().ok_or_else(|| ArrayError::SegmentCorruption {
                detail: format!(
                    "structural tile: live row {row} has no surrogate ({live_count} stored)"
                ),
            })?;
            surrogates.push(Some(surrogate));
        } else {
            surrogates.push(None);
        }
    }
    if live.next().is_some() {
        return Err(ArrayError::SegmentCorruption {
            detail: format!("structural tile: {live_count} surrogates stored for fewer live rows"),
        });
    }
    Ok(surrogates)
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
                surrogate: Some(Surrogate::new(i as u32 + 1)),
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
                surrogate: Some(Surrogate::new(i as u32 + 1)),
                valid_from_ms: 0,
                valid_until_ms: OPEN_UPPER,
                kind: RowKind::Live,
            })
            .unwrap();
        }
        b.push_row(SparseRow {
            coord: &[CoordValue::Int64(99)],
            attrs: &[],
            surrogate: None,
            valid_from_ms: 0,
            valid_until_ms: OPEN_UPPER,
            kind: RowKind::Tombstone,
        })
        .unwrap();
        let tile = b.build();
        let out = roundtrip(&tile);
        assert_eq!(out.row_kinds, tile.row_kinds);
        assert_eq!(out.surrogates, tile.surrogates);
        assert_eq!(out.surrogates[20], None);
    }

    /// A structural payload whose live-row surrogate column is one short is
    /// refused: a live row never decodes without its identity.
    #[test]
    fn a_live_row_without_a_stored_surrogate_is_refused() {
        let kinds = [
            RowKind::Live.as_u8(),
            RowKind::Tombstone.as_u8(),
            RowKind::Live.as_u8(),
        ];
        assert!(spread_live_surrogates(&kinds, vec![Surrogate::new(1)]).is_err());
        assert!(
            spread_live_surrogates(
                &kinds,
                vec![Surrogate::new(1), Surrogate::new(2), Surrogate::new(3)]
            )
            .is_err(),
            "a surrogate left over after the last live row is refused"
        );
        assert_eq!(
            spread_live_surrogates(&kinds, vec![Surrogate::new(1), Surrogate::new(2)]).unwrap(),
            vec![Some(Surrogate::new(1)), None, Some(Surrogate::new(2))]
        );
    }

    #[test]
    fn a_hostile_frame_length_is_corruption() {
        let data = u32::MAX.to_le_bytes();
        let mut pos = 0;
        let error = read_framed(&data, &mut pos).unwrap_err();
        assert!(matches!(error, ArrayError::SegmentCorruption { .. }));
    }

    #[test]
    fn a_position_near_the_top_of_the_address_space_is_corruption() {
        let mut pos = usize::MAX - 2;
        assert!(read_framed(&[0u8; 16], &mut pos).is_err());
    }

    #[test]
    fn a_structural_body_of_every_short_length_is_corruption() {
        for len in 0..8 {
            let error = decode_structural(&vec![0u8; len], None).unwrap_err();
            assert!(matches!(error, ArrayError::SegmentCorruption { .. }));
        }
    }

    /// A body holds no axes, so the next frame is the surrogates. Its length
    /// field is all ones.
    #[test]
    fn a_hostile_length_inside_a_tile_is_corruption() {
        let mut payload = vec![CodecTag::Structural as u8, SUPPORTED_PAYLOAD_VERSION];
        payload.extend_from_slice(&0u32.to_le_bytes());
        payload.extend_from_slice(&0u32.to_le_bytes());
        payload.extend_from_slice(&u32::MAX.to_le_bytes());
        let error = decode_sparse_tile(&payload).unwrap_err();
        assert!(matches!(error, ArrayError::SegmentCorruption { .. }));
    }

    #[test]
    fn every_truncation_of_a_real_tile_is_refused_without_a_panic() {
        let s = schema();
        let mut buf = Vec::new();
        encode_sparse_tile(&make_tile(&s, 50), &mut buf).unwrap();
        for len in 0..buf.len() {
            assert!(decode_sparse_tile(&buf[..len]).is_err(), "length {len}");
        }
    }

    #[test]
    fn huge_axis_count_in_tiny_structural_payload_is_rejected_before_allocation() {
        let mut body = Vec::new();
        body.extend_from_slice(&0u32.to_le_bytes());
        body.extend_from_slice(&(MAX_AXES_PER_TILE as u32).to_le_bytes());
        let error = decode_structural(&body, None).unwrap_err();
        assert!(matches!(error, ArrayError::SegmentCorruption { .. }));
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
            surrogate: Some(Surrogate::new(1)),
            valid_from_ms: 100,
            valid_until_ms: 500,
            kind: RowKind::Live,
        })
        .unwrap();
        b.push_row(SparseRow {
            coord: &[CoordValue::Int64(2)],
            attrs: &[CellValue::Int64(20)],
            surrogate: Some(Surrogate::new(2)),
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
                surrogate: Some(Surrogate::new(i as u32)),
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
