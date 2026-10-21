// SPDX-License-Identifier: Apache-2.0

//! Strip GeoTIFFs: every codec, both integer and float predictors, every
//! supported sample type, and multi-band chunky pixels.

use nodedb_raster::{BlockKind, Interleave, SampleType};
use tiff::encoder::colortype::{
    ColorType, Gray8, Gray16, Gray32, Gray32Float, Gray64Float, GrayI8, GrayI16, GrayI32, RGB8,
    RGB16,
};
use tiff::encoder::{Compression, DeflateLevel, Predictor, TiffValue};

use crate::fixtures::{
    DEFLATE, LZW, Pred, RawImage, RawLayout, Sample, StripSpec, encode_chunks, encode_raw,
    encode_strips, expected, interleave, planes, strip_spec,
};
use crate::image::{open, read_all};

const W: u32 = 10;
const H: u32 = 7;
const DEFLATE_BALANCED: Compression = Compression::Deflate(DeflateLevel::Balanced);

#[test]
fn every_strip_codec_round_trips() {
    let planes = planes::<u16>(W, H, 1);
    // PackBits keeps the encoder's one row per strip.
    let cases = [
        (Compression::Uncompressed, Some(3), 3),
        (Compression::Lzw, Some(3), 3),
        (DEFLATE_BALANCED, Some(3), 3),
        (Compression::Packbits, None, H),
    ];
    for (compression, rows_per_strip, blocks) in cases {
        let spec = StripSpec {
            compression,
            rows_per_strip,
            ..strip_spec(W, H)
        };
        let bytes = encode_strips::<Gray16>(&spec, &planes[0]);
        let reader = open(&bytes).expect("opens");
        assert_eq!(reader.layout().kind, BlockKind::Strips);
        assert_eq!(reader.block_count(), blocks);
        assert_eq!(read_all(&reader), expected(&planes));
    }
}

#[test]
fn last_strip_is_cropped_to_the_image() {
    let planes = planes::<u16>(W, H, 1);
    let spec = StripSpec {
        rows_per_strip: Some(3),
        ..strip_spec(W, H)
    };
    let bytes = encode_strips::<Gray16>(&spec, &planes[0]);
    let reader = open(&bytes).expect("opens");
    let last = reader.read_block(2).expect("last strip");
    assert_eq!(
        (last.window.row_off, last.window.width, last.window.height),
        (6, W, 1)
    );
    assert_eq!(last.bands[0].len(), W as usize);
}

#[test]
fn horizontal_predictor_round_trips() {
    let gray = planes::<u16>(W, H, 1);
    let spec = StripSpec {
        compression: Compression::Lzw,
        predictor: Predictor::Horizontal,
        rows_per_strip: Some(2),
        ..strip_spec(W, H)
    };
    let reader = open(&encode_strips::<Gray16>(&spec, &gray[0])).expect("opens");
    assert_eq!(read_all(&reader), expected(&gray));

    let signed = planes::<i16>(W, H, 1);
    let spec = StripSpec {
        compression: DEFLATE_BALANCED,
        predictor: Predictor::Horizontal,
        ..strip_spec(W, H)
    };
    let reader = open(&encode_strips::<GrayI16>(&spec, &signed[0])).expect("opens");
    assert_eq!(read_all(&reader), expected(&signed));

    let rgb = planes::<u16>(W, H, 3);
    let spec = StripSpec {
        compression: DEFLATE_BALANCED,
        predictor: Predictor::Horizontal,
        rows_per_strip: Some(4),
        ..strip_spec(W, H)
    };
    let reader = open(&encode_strips::<RGB16>(&spec, &interleave(&rgb))).expect("opens");
    assert_eq!(reader.band_count(), 3);
    assert_eq!(read_all(&reader), expected(&rgb));
}

#[test]
fn floating_point_predictor_round_trips() {
    let layout = RawLayout::Strips { rows_per_strip: 3 };
    // libtiff applies a predictor under LZW, deflate and ZSTD only.
    for compression in [LZW, DEFLATE] {
        let two_band = planes::<f32>(W, H, 2);
        let chunks = encode_chunks(
            &two_band,
            (W, H),
            layout,
            false,
            Pred::FloatingPoint,
            compression,
        );
        let bytes = encode_raw(&RawImage {
            width: W,
            height: H,
            bits: 32,
            sample_format: 3,
            samples: 2,
            photometric: 1,
            planar: false,
            compression,
            predictor: Pred::FloatingPoint.code(),
            layout,
            chunks,
            extras: &[],
        });
        let reader = open(&bytes).expect("opens");
        assert_eq!(reader.sample_type(), SampleType::F32);
        assert_eq!(read_all(&reader), expected(&two_band));
    }

    let single = planes::<f64>(W, H, 1);
    let chunks = encode_chunks(&single, (W, H), layout, false, Pred::FloatingPoint, DEFLATE);
    let bytes = encode_raw(&RawImage {
        width: W,
        height: H,
        bits: 64,
        sample_format: 3,
        samples: 1,
        photometric: 1,
        planar: false,
        compression: DEFLATE,
        predictor: Pred::FloatingPoint.code(),
        layout,
        chunks,
        extras: &[],
    });
    let reader = open(&bytes).expect("opens");
    assert_eq!(reader.sample_type(), SampleType::F64);
    assert_eq!(read_all(&reader), expected(&single));
}

fn round_trip<C: ColorType>(sample_type: SampleType)
where
    C::Inner: Sample,
    [C::Inner]: TiffValue,
{
    let planes = planes::<C::Inner>(W, H, 1);
    let spec = StripSpec {
        compression: DEFLATE_BALANCED,
        rows_per_strip: Some(2),
        ..strip_spec(W, H)
    };
    let reader = open(&encode_strips::<C>(&spec, &planes[0])).expect("opens");
    assert_eq!(reader.sample_type(), sample_type);
    assert_eq!(read_all(&reader), expected(&planes));
}

#[test]
fn every_supported_sample_type_round_trips() {
    round_trip::<Gray8>(SampleType::U8);
    round_trip::<GrayI8>(SampleType::I8);
    round_trip::<GrayI32>(SampleType::I32);
    round_trip::<Gray16>(SampleType::U16);
    round_trip::<GrayI16>(SampleType::I16);
    round_trip::<Gray32>(SampleType::U32);
    round_trip::<Gray32Float>(SampleType::F32);
    round_trip::<Gray64Float>(SampleType::F64);
}

#[test]
fn multi_band_chunky_pixels_split_per_band() {
    let multiband = planes::<u16>(W, H, 4);
    let spec = StripSpec {
        compression: Compression::Lzw,
        rows_per_strip: Some(4),
        extra_samples: 3,
        ..strip_spec(W, H)
    };
    let reader = open(&encode_strips::<Gray16>(&spec, &interleave(&multiband))).expect("opens");
    assert_eq!(reader.band_count(), 4);
    assert_eq!(reader.layout().interleave, Interleave::Chunky);
    assert_eq!(read_all(&reader), expected(&multiband));

    let rgb = planes::<u8>(W, H, 3);
    let spec = StripSpec {
        compression: Compression::Packbits,
        ..strip_spec(W, H)
    };
    let reader = open(&encode_strips::<RGB8>(&spec, &interleave(&rgb))).expect("opens");
    assert_eq!(read_all(&reader), expected(&rgb));
}
