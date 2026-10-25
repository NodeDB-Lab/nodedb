// SPDX-License-Identifier: Apache-2.0

//! Refusals and hostile input: every failure is a typed error, never a
//! panic and never an allocation past the cap.

use nodedb_raster::tiff::geokeys::{GT_MODEL_TYPE, PROJECTED_CS_TYPE};
use nodedb_raster::{RasterError, RasterLimits};
use tiff::encoder::colortype::{Gray16, Gray64, GrayI64};
use tiff::encoder::{Compression, DeflateLevel};
use tiff::tags::Tag;

use crate::fixtures::{
    DEFLATE, Extra, LIMITS, Pred, RawImage, RawLayout, StripSpec, encode_chunks, encode_raw,
    encode_strips, geokeys, planes, strip_spec,
};
use crate::image::{decode_everything, open};

/// A small planar tiled GeoTIFF with GeoKeys: every code path the reader
/// has, in a few hundred bytes.
fn small_fixture() -> Vec<u8> {
    let (width, height) = (12, 10);
    let layout = RawLayout::Tiles {
        width: 16,
        height: 16,
    };
    let bands = planes::<u16>(width, height, 2);
    let chunks = encode_chunks(
        &bands,
        (width, height),
        layout,
        true,
        Pred::Horizontal,
        DEFLATE,
    );
    let extras = [
        Extra::Doubles(Tag::ModelPixelScaleTag, vec![10.0, 10.0, 0.0]),
        Extra::Doubles(
            Tag::ModelTiepointTag,
            vec![0.0, 0.0, 0.0, 500_000.0, 300_000.0, 0.0],
        ),
        Extra::Shorts(
            Tag::GeoKeyDirectoryTag,
            geokeys(&[(GT_MODEL_TYPE, 1), (PROJECTED_CS_TYPE, 32648)]),
        ),
        Extra::Ascii(Tag::GdalNodata, "0".to_owned()),
    ];
    encode_raw(&RawImage {
        width,
        height,
        bits: 16,
        sample_format: 1,
        samples: 2,
        photometric: 1,
        planar: true,
        compression: DEFLATE,
        predictor: Pred::Horizontal.code(),
        layout,
        chunks,
        extras: &extras,
    })
}

#[test]
fn the_small_fixture_decodes() {
    decode_everything(&small_fixture(), LIMITS).expect("fixture is valid");
}

#[test]
fn an_oversize_strip_is_a_typed_limit_error() {
    let data = planes::<u16>(64, 64, 1);
    let spec = StripSpec {
        rows_per_strip: Some(64),
        ..strip_spec(64, 64)
    };
    let bytes = encode_strips::<Gray16>(&spec, &data[0]);
    let tight = RasterLimits::new(4096);
    assert!(matches!(
        decode_everything(&bytes, tight),
        Err(RasterError::BlockTooLarge {
            needed_bytes: 8192,
            limit_bytes: 4096
        })
    ));
    decode_everything(&bytes, RasterLimits::new(8192)).expect("fits at the cap");
}

#[test]
fn an_oversize_tile_is_a_typed_limit_error() {
    let tight = RasterLimits::new(16 * 16 * 2 * 2 - 1);
    assert!(matches!(
        decode_everything(&small_fixture(), tight),
        Err(RasterError::BlockTooLarge { .. })
    ));
}

#[test]
fn every_truncation_is_a_typed_error() {
    let bytes = small_fixture();
    for len in 0..bytes.len() {
        let result = decode_everything(&bytes[..len], LIMITS);
        assert!(result.is_err(), "a {len}-byte prefix decoded");
    }
}

#[test]
fn corrupt_bytes_never_panic() {
    let bytes = small_fixture();
    for position in 0..bytes.len() {
        for flip in [0xFF_u8, 0x01, 0x80] {
            let mut corrupt = bytes.clone();
            corrupt[position] ^= flip;
            // Ok or a typed error are both fine. A panic fails the test.
            let _ = decode_everything(&corrupt, RasterLimits::new(1 << 20));
        }
    }
}

#[test]
fn garbage_is_malformed() {
    for bytes in [
        &b""[..],
        b"II",
        b"hello world",
        b"MM\x00\x2a\xff\xff\xff\xff",
    ] {
        assert!(
            matches!(open(bytes), Err(RasterError::Malformed(_))),
            "{bytes:?}"
        );
    }
}

#[test]
fn a_reduced_resolution_first_ifd_is_refused() {
    let data = planes::<u16>(4, 4, 1);
    let extras = [Extra::Long(Tag::NewSubfileType, 1)];
    let spec = StripSpec {
        extras: &extras,
        ..strip_spec(4, 4)
    };
    assert!(matches!(
        open(&encode_strips::<Gray16>(&spec, &data[0])),
        Err(RasterError::ReducedResolutionFirstImage)
    ));
}

#[test]
fn unsupported_sample_types_are_refused_at_open() {
    let spec = StripSpec {
        compression: Compression::Deflate(DeflateLevel::Fast),
        ..strip_spec(4, 4)
    };
    let wide_signed = vec![-3_i64; 16];
    assert!(matches!(
        open(&encode_strips::<GrayI64>(&spec, &wide_signed)),
        Err(RasterError::UnsupportedSampleType { bits: 64, .. })
    ));
    let wide = vec![7_u64; 16];
    assert!(matches!(
        open(&encode_strips::<Gray64>(&spec, &wide)),
        Err(RasterError::UnsupportedSampleType { bits: 64, .. })
    ));
}

fn raw_gray16(compression: u16, photometric: u16) -> Vec<u8> {
    let layout = RawLayout::Strips { rows_per_strip: 4 };
    let bands = planes::<u16>(4, 4, 1);
    encode_raw(&RawImage {
        width: 4,
        height: 4,
        bits: 16,
        sample_format: 1,
        samples: 1,
        photometric,
        planar: false,
        compression,
        predictor: 1,
        layout,
        chunks: encode_chunks(&bands, (4, 4), layout, false, Pred::None, DEFLATE),
        extras: &[],
    })
}

#[test]
fn unsupported_compression_is_refused_at_open() {
    // 2 is CCITT RLE, 34925 LZMA, 50001 WebP.
    for code in [2, 34925, 50001] {
        assert!(matches!(
            open(&raw_gray16(code, 1)),
            Err(RasterError::UnsupportedCompression { code: c }) if c == code
        ));
    }
}

#[test]
fn transforming_photometrics_are_refused_at_open() {
    // The decoder inverts WhiteIsZero samples. A palette stores indexes.
    for photometric in [0, 3] {
        assert!(matches!(
            open(&raw_gray16(DEFLATE, photometric)),
            Err(RasterError::UnsupportedPhotometric { code }) if code == photometric
        ));
    }
}

#[test]
fn an_out_of_range_block_is_a_typed_error() {
    let reader = open(&small_fixture()).expect("opens");
    let count = reader.block_count();
    assert!(matches!(
        reader.read_block(count),
        Err(RasterError::BlockOutOfRange { .. })
    ));
}
