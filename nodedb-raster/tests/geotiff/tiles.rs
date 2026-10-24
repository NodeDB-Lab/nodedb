// SPDX-License-Identifier: Apache-2.0

//! Tiled and planar GeoTIFFs, with edge tiles that overhang the image.

use nodedb_raster::{BlockKind, BlockWindow, Interleave, SampleType};

use crate::fixtures::{
    DEFLATE, LZW, NONE, Pred, RawImage, RawLayout, Sample, encode_chunks, encode_raw, expected,
    planes,
};
use crate::image::{open, read_all};

/// 40 x 24 pixels in 16 x 16 tiles: the right column is 8 wide and the
/// bottom row 8 tall.
const W: u32 = 40;
const H: u32 = 24;
const TILE: RawLayout = RawLayout::Tiles {
    width: 16,
    height: 16,
};

struct Case {
    bands: u32,
    planar: bool,
    layout: RawLayout,
    predictor: Pred,
    compression: u16,
}

fn encode<T: Sample>(case: &Case, bits: u16, sample_format: u16) -> (Vec<u8>, Vec<Vec<f64>>) {
    let bands = planes::<T>(W, H, case.bands);
    let chunks = encode_chunks(
        &bands,
        (W, H),
        case.layout,
        case.planar,
        case.predictor,
        case.compression,
    );
    let bytes = encode_raw(&RawImage {
        width: W,
        height: H,
        bits,
        sample_format,
        samples: u16::try_from(case.bands).expect("few bands"),
        photometric: 1,
        planar: case.planar,
        compression: case.compression,
        predictor: case.predictor.code(),
        layout: case.layout,
        chunks,
        extras: &[],
    });
    (bytes, expected(&bands))
}

#[test]
fn chunky_tiles_crop_edge_tiles() {
    let case = Case {
        bands: 3,
        planar: false,
        layout: TILE,
        predictor: Pred::None,
        compression: DEFLATE,
    };
    let (bytes, want) = encode::<u16>(&case, 16, 1);
    let reader = open(&bytes).expect("opens");
    assert_eq!(reader.layout().kind, BlockKind::Tiles);
    assert_eq!(reader.layout().interleave, Interleave::Chunky);
    assert_eq!(reader.block_count(), 6);
    let corner = reader.read_block(5).expect("corner tile");
    assert_eq!(
        corner.window,
        BlockWindow {
            outer: Vec::new(),
            col_off: 32,
            row_off: 16,
            width: 8,
            height: 8
        }
    );
    assert!(corner.bands.iter().all(|band| band.len() == 64));
    assert_eq!(read_all(&reader), want);
}

#[test]
fn planar_tiles_crop_edge_tiles_in_every_plane() {
    let case = Case {
        bands: 2,
        planar: true,
        layout: TILE,
        predictor: Pred::None,
        compression: LZW,
    };
    let (bytes, want) = encode::<i16>(&case, 16, 2);
    let reader = open(&bytes).expect("opens");
    assert_eq!(reader.layout().interleave, Interleave::Planar);
    assert_eq!(reader.sample_type(), SampleType::I16);
    let bottom = reader.read_block(4).expect("bottom tile");
    assert_eq!((bottom.window.width, bottom.window.height), (16, 8));
    assert!(bottom.bands.iter().all(|band| band.len() == 16 * 8));
    assert_eq!(read_all(&reader), want);
}

#[test]
fn planar_strips_round_trip() {
    let case = Case {
        bands: 3,
        planar: true,
        layout: RawLayout::Strips { rows_per_strip: 5 },
        predictor: Pred::None,
        compression: NONE,
    };
    let (bytes, want) = encode::<u8>(&case, 8, 1);
    let reader = open(&bytes).expect("opens");
    assert_eq!(reader.layout().kind, BlockKind::Strips);
    assert_eq!(reader.block_count(), H.div_ceil(5));
    assert_eq!(read_all(&reader), want);
}

#[test]
fn tiles_with_horizontal_predictor_round_trip() {
    let case = Case {
        bands: 2,
        planar: false,
        layout: TILE,
        predictor: Pred::Horizontal,
        compression: DEFLATE,
    };
    let (bytes, want) = encode::<u16>(&case, 16, 1);
    let reader = open(&bytes).expect("opens");
    assert_eq!(read_all(&reader), want);
}

#[test]
fn planar_float_tiles_with_floating_point_predictor_round_trip() {
    let case = Case {
        bands: 2,
        planar: true,
        layout: TILE,
        predictor: Pred::FloatingPoint,
        compression: DEFLATE,
    };
    let (bytes, want) = encode::<f32>(&case, 32, 3);
    let reader = open(&bytes).expect("opens");
    assert_eq!(reader.sample_type(), SampleType::F32);
    assert_eq!(read_all(&reader), want);
}

#[test]
fn u32_and_f64_tiles_round_trip() {
    let case = Case {
        bands: 1,
        planar: false,
        layout: TILE,
        predictor: Pred::None,
        compression: LZW,
    };
    let (bytes, want) = encode::<u32>(&case, 32, 1);
    let reader = open(&bytes).expect("opens");
    assert_eq!(reader.sample_type(), SampleType::U32);
    assert_eq!(read_all(&reader), want);

    let (bytes, want) = encode::<f64>(&case, 64, 3);
    let reader = open(&bytes).expect("opens");
    assert_eq!(reader.sample_type(), SampleType::F64);
    assert_eq!(read_all(&reader), want);
}
