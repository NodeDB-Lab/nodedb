// SPDX-License-Identifier: BUSL-1.1

//! The scene GeoTIFF, written in the test with the `tiff` encoder.
//!
//! 256 x 192 pixels, 3 bands of u16, chunky 64 x 64 tiles, deflate with
//! horizontal predictor 2. The geotransform is
//! `[600000, 30, 0, 700000, 0, -30]` in EPSG:32647, written as a tiepoint,
//! a pixel scale and GeoKeys. `GDAL_NODATA` is `0`, and one 10 x 10 block
//! holds 0 in every band. Every other sample is
//! `b_k(r, c) = 1000 k + (7 r + 13 c) mod 997`.

use std::io::{Cursor, Write};
use std::ops::Range;

use tiff::encoder::TiffEncoder;
use tiff::encoder::compression::{CompressionAlgorithm, Deflate};
use tiff::tags::Tag;

pub const WIDTH: u32 = 256;
pub const HEIGHT: u32 = 192;
pub const TILE: u32 = 64;
pub const BANDS: u32 = 3;
/// The file geotransform, pixel-corner, GDAL order.
pub const GT: [f64; 6] = [600_000.0, 30.0, 0.0, 700_000.0, 0.0, -30.0];
/// The all-zero block: rows 100..110, cols 120..130.
pub const NODATA_ROWS: Range<u32> = 100..110;
pub const NODATA_COLS: Range<u32> = 120..130;
/// Pixels the file holds minus the all-nodata block.
pub const KEPT_PIXELS: u64 = (WIDTH * HEIGHT) as u64 - 100;

/// GeoKey ids and values.
const GT_MODEL_TYPE: u16 = 1024;
const GT_RASTER_TYPE: u16 = 1025;
const PROJECTED_CS_TYPE: u16 = 3072;
const MODEL_PROJECTED: u16 = 1;
const RASTER_PIXEL_IS_AREA: u16 = 1;
const EPSG_UTM_47N: u16 = 32647;

/// Whether pixel `(r, c)` lies in the all-zero block.
pub fn in_nodata_block(r: u32, c: u32) -> bool {
    NODATA_ROWS.contains(&r) && NODATA_COLS.contains(&c)
}

/// The value of band `k` (1-based) at pixel `(r, c)`.
pub fn sample(k: u32, r: u32, c: u32) -> u16 {
    if in_nodata_block(r, c) {
        return 0;
    }
    let value = 1000 * k + (7 * r + 13 * c) % 997;
    u16::try_from(value).expect("band values fit u16")
}

/// The world position of the centre of pixel `(r, c)`.
pub fn pixel_centre(r: u32, c: u32) -> (f64, f64) {
    let (u, v) = (f64::from(c) + 0.5, f64::from(r) + 0.5);
    (GT[0] + u * GT[1] + v * GT[2], GT[3] + u * GT[4] + v * GT[5])
}

/// The encoded bytes of tile `(tx, ty)`: chunky samples, horizontal
/// differencing per row at the pixel stride, then deflate.
fn tile_bytes(tx: u32, ty: u32) -> Vec<u8> {
    let mut raw = Vec::with_capacity((TILE * TILE * BANDS * 2) as usize);
    let mut row_samples = Vec::with_capacity((TILE * BANDS) as usize);
    for r in 0..TILE {
        row_samples.clear();
        let row = ty * TILE + r;
        for c in 0..TILE {
            let col = tx * TILE + c;
            row_samples.extend((1..=BANDS).map(|k| sample(k, row, col)));
        }
        let stride = BANDS as usize;
        for (i, value) in row_samples.iter().enumerate() {
            let encoded = if i < stride {
                *value
            } else {
                value.wrapping_sub(row_samples[i - stride])
            };
            raw.extend_from_slice(&encoded.to_ne_bytes());
        }
    }
    let mut out = Vec::new();
    Deflate::default()
        .write_to(&mut out, &raw)
        .expect("deflate writes");
    out
}

/// The whole scene GeoTIFF, with `GDAL_NODATA` 0.
pub fn scene_tiff() -> Vec<u8> {
    scene_tiff_with_nodata(Some("0"))
}

/// The whole scene GeoTIFF, with `nodata` as its `GDAL_NODATA` tag, or no
/// tag when `None`.
pub fn scene_tiff_with_nodata(nodata: Option<&str>) -> Vec<u8> {
    let mut cursor = Cursor::new(Vec::new());
    {
        let mut encoder = TiffEncoder::new(&mut cursor).expect("encoder");
        let mut dir = encoder.image_directory().expect("directory");
        let mut offsets = Vec::new();
        let mut counts = Vec::new();
        for ty in 0..HEIGHT / TILE {
            for tx in 0..WIDTH / TILE {
                let tile = tile_bytes(tx, ty);
                let offset = dir.write_data(&tile[..]).expect("tile writes");
                offsets.push(u32::try_from(offset).expect("small file"));
                counts.push(u32::try_from(tile.len()).expect("small tile"));
            }
        }
        let bands = BANDS as usize;
        dir.write_tag(Tag::ImageWidth, WIDTH).expect("tag");
        dir.write_tag(Tag::ImageLength, HEIGHT).expect("tag");
        dir.write_tag(Tag::BitsPerSample, &vec![16u16; bands][..])
            .expect("tag");
        dir.write_tag(Tag::SampleFormat, &vec![1u16; bands][..])
            .expect("tag");
        dir.write_tag(Tag::SamplesPerPixel, BANDS as u16)
            .expect("tag");
        dir.write_tag(Tag::PhotometricInterpretation, 1u16)
            .expect("tag");
        dir.write_tag(Tag::PlanarConfiguration, 1u16).expect("tag");
        dir.write_tag(Tag::Compression, 8u16).expect("tag");
        dir.write_tag(Tag::Predictor, 2u16).expect("tag");
        dir.write_tag(Tag::TileWidth, TILE).expect("tag");
        dir.write_tag(Tag::TileLength, TILE).expect("tag");
        dir.write_tag(Tag::TileOffsets, &offsets[..]).expect("tag");
        dir.write_tag(Tag::TileByteCounts, &counts[..])
            .expect("tag");
        dir.write_tag(Tag::ModelPixelScaleTag, &[GT[1], -GT[5], 0.0][..])
            .expect("tag");
        dir.write_tag(
            Tag::ModelTiepointTag,
            &[0.0, 0.0, 0.0, GT[0], GT[3], 0.0][..],
        )
        .expect("tag");
        let geokeys: [u16; 16] = [
            1,
            1,
            0,
            3,
            GT_MODEL_TYPE,
            0,
            1,
            MODEL_PROJECTED,
            GT_RASTER_TYPE,
            0,
            1,
            RASTER_PIXEL_IS_AREA,
            PROJECTED_CS_TYPE,
            0,
            1,
            EPSG_UTM_47N,
        ];
        dir.write_tag(Tag::GeoKeyDirectoryTag, &geokeys[..])
            .expect("tag");
        if let Some(nodata) = nodata {
            dir.write_tag(Tag::GdalNodata, nodata).expect("tag");
        }
        dir.finish().expect("directory writes");
    }
    cursor.into_inner()
}

/// `bytes` in a temp file whose path ends in `.tif`. The file lives as
/// long as the handle.
pub fn write_tiff(bytes: &[u8]) -> tempfile::NamedTempFile {
    let mut file = tempfile::Builder::new()
        .suffix(".tif")
        .tempfile()
        .expect("temp file");
    file.write_all(bytes).expect("temp file writes");
    file.flush().expect("temp file flushes");
    file
}

/// The scene in a temp file, and its absolute path.
pub fn scene_file() -> (tempfile::NamedTempFile, String) {
    let file = write_tiff(&scene_tiff());
    let path = file.path().to_string_lossy().into_owned();
    (file, path)
}

/// The scene without a `GDAL_NODATA` tag in a temp file, and its path.
pub fn scene_file_without_nodata() -> (tempfile::NamedTempFile, String) {
    let file = write_tiff(&scene_tiff_with_nodata(None));
    let path = file.path().to_string_lossy().into_owned();
    (file, path)
}
