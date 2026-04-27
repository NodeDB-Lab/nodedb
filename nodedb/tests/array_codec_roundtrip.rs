// Integration tests for the structural sparse-tile codec introduced in segment
// format version 4. Tests cover: large roundtrip, legacy payload compatibility,
// compression ratio, and mixed sparse+dense segment decoding.

use nodedb_array::codec::tile_encode::encode_sparse_tile;
use nodedb_array::schema::ArraySchemaBuilder;
use nodedb_array::schema::attr_spec::{AttrSpec, AttrType};
use nodedb_array::schema::dim_spec::{DimSpec, DimType};
use nodedb_array::tile::dense_tile::DenseTile;
use nodedb_array::tile::sparse_tile::{RowKind, SparseRow, SparseTile, SparseTileBuilder};
use nodedb_array::types::TileId;
use nodedb_array::types::cell_value::value::CellValue;
use nodedb_array::types::coord::value::CoordValue;
use nodedb_array::types::domain::{Domain, DomainBound};
use nodedb_array::{SegmentReader, SegmentWriter, TilePayload};
use nodedb_types::{OPEN_UPPER, Surrogate};

fn genomic_schema() -> nodedb_array::ArraySchema {
    ArraySchemaBuilder::new("genome")
        .dim(DimSpec::new(
            "chrom",
            DimType::Int64,
            Domain::new(DomainBound::Int64(0), DomainBound::Int64(24)),
        ))
        .dim(DimSpec::new(
            "pos",
            DimType::Int64,
            Domain::new(DomainBound::Int64(0), DomainBound::Int64(300_000_000)),
        ))
        .attr(AttrSpec::new("qual", AttrType::Float64, true))
        .tile_extents(vec![1, 1_000_000])
        .build()
        .unwrap()
}

fn simple_schema() -> nodedb_array::ArraySchema {
    ArraySchemaBuilder::new("s")
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

/// Test 1: 10k-cell tile write + read exact equality.
#[test]
fn ten_k_tile_roundtrip_exact_equality() {
    let s = genomic_schema();
    let mut b = SparseTileBuilder::new(&s);
    for i in 0..10_000usize {
        b.push_row(SparseRow {
            coord: &[
                CoordValue::Int64((i % 24) as i64),
                CoordValue::Int64(i as i64 * 100),
            ],
            attrs: &[CellValue::Float64(i as f64 * 0.1)],
            surrogate: Surrogate::new(i as u32),
            valid_from_ms: i as i64 * 10,
            valid_until_ms: OPEN_UPPER,
            kind: RowKind::Live,
        })
        .unwrap();
    }
    let tile = b.build();

    let mut seg_writer = SegmentWriter::new(0xDEAD_BEEF);
    seg_writer
        .append_sparse(TileId::snapshot(1), &tile)
        .unwrap();
    let bytes = seg_writer.finish().unwrap();

    let reader = SegmentReader::open(&bytes).unwrap();
    assert_eq!(reader.tile_count(), 1);
    match reader.read_tile(0).unwrap() {
        TilePayload::Sparse(decoded) => {
            assert_eq!(decoded.surrogates, tile.surrogates);
            assert_eq!(decoded.valid_from_ms, tile.valid_from_ms);
            assert_eq!(decoded.valid_until_ms, tile.valid_until_ms);
            assert_eq!(decoded.row_kinds, tile.row_kinds);
            assert_eq!(decoded.attr_cols.len(), tile.attr_cols.len());
            assert_eq!(decoded.attr_cols[0], tile.attr_cols[0]);
            assert_eq!(decoded.dim_dicts.len(), tile.dim_dicts.len());
            for (d, o) in decoded.dim_dicts.iter().zip(tile.dim_dicts.iter()) {
                assert_eq!(d.indices, o.indices);
                assert_eq!(d.values, o.values);
            }
        }
        TilePayload::Dense(_) => panic!("expected Sparse"),
    }
}

/// Test 2: Legacy tile payload (raw zerompk msgpack) is still decodable by the
/// tag-dispatch layer, even after the segment format was bumped to v4.
///
/// The reader's logic is `match peek_tag { Some(_) => decode_sparse_tile,
/// None => zerompk::from_msgpack }`. This test proves:
///   1. `peek_tag` on a real zerompk-of-`SparseTile` payload returns `None`
///      (so the reader will take the legacy fallback branch, regardless of
///      whether zerompk emits a map-header or array-header byte).
///   2. `zerompk::from_msgpack` — the body of that None branch — decodes the
///      payload back into the original `SparseTile`.
/// Together these are equivalent to invoking the reader's full dispatch path
/// with a v3-style payload.
#[test]
fn legacy_v3_tile_payload_readable() {
    let s = simple_schema();
    let mut b = SparseTileBuilder::new(&s);
    for i in 0..5 {
        b.push_row(SparseRow {
            coord: &[CoordValue::Int64(i)],
            attrs: &[CellValue::Int64(i * 10)],
            surrogate: Surrogate::ZERO,
            valid_from_ms: 0,
            valid_until_ms: OPEN_UPPER,
            kind: RowKind::Live,
        })
        .unwrap();
    }
    let tile = b.build();

    // Simulate the v3 writer: raw zerompk with no tag byte.
    let raw_msgpack = zerompk::to_msgpack_vec(&tile).unwrap();

    // The codec's tag dispatch should fall back to zerompk for this payload
    // (peek_tag returns None for any non-tag byte; reader then tries zerompk).
    let decoded = nodedb_array::codec::tag::peek_tag(&raw_msgpack);
    assert_eq!(
        decoded, None,
        "legacy payload should yield None from peek_tag"
    );

    // Directly decode via zerompk to confirm the legacy path produces the
    // same tile the new path would.
    let recovered: SparseTile = zerompk::from_msgpack(&raw_msgpack).unwrap();
    assert_eq!(recovered.surrogates, tile.surrogates);
    assert_eq!(recovered.attr_cols, tile.attr_cols);
}

/// Test 3: Compression ratio — structural codec ≤ 30% of raw msgpack for a
/// sorted 1k-cell coordinate-heavy tile.
#[test]
fn structural_codec_compresses_sorted_tile() {
    let s = genomic_schema();
    let mut b = SparseTileBuilder::new(&s);
    // Sorted: chrom fixed at 1, pos monotonically increasing.
    for i in 0..1_000usize {
        b.push_row(SparseRow {
            coord: &[
                CoordValue::Int64(1),
                CoordValue::Int64(100_000 + i as i64 * 10),
            ],
            attrs: &[CellValue::Float64(i as f64)],
            surrogate: Surrogate::new(i as u32),
            valid_from_ms: i as i64 * 1000,
            valid_until_ms: OPEN_UPPER,
            kind: RowKind::Live,
        })
        .unwrap();
    }
    let tile = b.build();

    let raw_size = zerompk::to_msgpack_vec(&tile).unwrap().len();
    let mut structural_buf = Vec::new();
    encode_sparse_tile(&tile, &mut structural_buf).unwrap();
    let structural_size = structural_buf.len();

    // Must compress to ≤ 30 % of raw msgpack on a sorted coord-heavy tile.
    // Coords go through delta-varint, surrogates through fastlanes,
    // valid_from_ms through gorilla, and the f64 attr column through gorilla
    // XOR — all four dominant byte sources have monotonic-aware codecs.
    let threshold = raw_size * 30 / 100;
    assert!(
        structural_size <= threshold,
        "structural ({structural_size} bytes) > 30% of raw ({raw_size} bytes, threshold {threshold})"
    );
}

/// Test 4: Mixed segment (Sparse + Dense tiles) — both decode correctly.
#[test]
fn mixed_sparse_dense_segment_decodes() {
    let s = simple_schema();

    let mut b = SparseTileBuilder::new(&s);
    for i in 0..20 {
        b.push_row(SparseRow {
            coord: &[CoordValue::Int64(i)],
            attrs: &[CellValue::Int64(i * 7)],
            surrogate: Surrogate::new(i as u32),
            valid_from_ms: 0,
            valid_until_ms: OPEN_UPPER,
            kind: RowKind::Live,
        })
        .unwrap();
    }
    let sparse = b.build();
    let dense = DenseTile::empty(&s);

    let mut w = SegmentWriter::new(0xCAFE_BABE);
    w.append_sparse(TileId::new(1, 100), &sparse).unwrap();
    w.append_dense(TileId::new(2, 100), &dense).unwrap();
    let bytes = w.finish().unwrap();

    let r = SegmentReader::open(&bytes).unwrap();
    assert_eq!(r.tile_count(), 2);

    match r.read_tile(0).unwrap() {
        TilePayload::Sparse(t) => {
            assert_eq!(t.surrogates.len(), 20);
            assert_eq!(t.attr_cols[0][0], CellValue::Int64(0));
            assert_eq!(t.attr_cols[0][1], CellValue::Int64(7));
        }
        TilePayload::Dense(_) => panic!("tile 0 should be Sparse"),
    }

    match r.read_tile(1).unwrap() {
        TilePayload::Dense(t) => {
            // DenseTile::empty for a 1D schema with 1000-extent = 1000 cells.
            assert_eq!(t.cell_count(), 1000);
        }
        TilePayload::Sparse(_) => panic!("tile 1 should be Dense"),
    }
}
