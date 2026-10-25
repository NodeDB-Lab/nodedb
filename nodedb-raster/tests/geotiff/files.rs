// SPDX-License-Identifier: Apache-2.0

//! Every generated fixture of `tests/fixtures/manifest.tsv`: codec x
//! predictor x strips/tiles x classic/BigTIFF x chunky/planar, sample
//! types, LERC with masks, JPEG, COG overviews and masks, a Landsat-shaped
//! COG. Expected samples are the shared pattern, or GDAL's decode.

use std::io::Read;
use std::path::PathBuf;

use flate2::read::ZlibDecoder;
use nodedb_raster::{
    RasterBlock, RasterReader, SampleType, TiffOpener, TiffReader, open_all, read_block_all,
};

use crate::fixtures::{LIMITS, expected, planes};

/// One manifest row.
pub struct Fixture {
    pub id: String,
    pub file: String,
    pub image: u32,
    pub dtype: String,
    pub width: u32,
    pub height: u32,
    pub bands: u16,
    pub expect: String,
    pub mask: String,
    pub tol: f64,
}

pub fn fixture_path(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join(name)
}

pub fn fixture_bytes(name: &str) -> Vec<u8> {
    std::fs::read(fixture_path(name)).unwrap_or_else(|e| panic!("fixture {name}: {e}"))
}

pub fn manifest() -> Vec<Fixture> {
    let text = std::fs::read_to_string(fixture_path("manifest.tsv")).expect("manifest");
    text.lines()
        .filter(|line| !line.starts_with('#') && !line.is_empty())
        .map(|line| {
            let f: Vec<&str> = line.split('\t').collect();
            assert_eq!(f.len(), 10, "manifest row {line}");
            Fixture {
                id: f[0].to_owned(),
                file: f[1].to_owned(),
                image: f[2].parse().expect("image"),
                dtype: f[3].to_owned(),
                width: f[4].parse().expect("width"),
                height: f[5].parse().expect("height"),
                bands: f[6].parse().expect("bands"),
                expect: f[7].to_owned(),
                mask: f[8].to_owned(),
                tol: f[9].parse().expect("tol"),
            }
        })
        .collect()
}

pub fn fixture(id: &str) -> Fixture {
    manifest()
        .into_iter()
        .find(|f| f.id == id)
        .unwrap_or_else(|| panic!("no fixture {id}"))
}

fn inflate(name: &str) -> Vec<u8> {
    let mut out = Vec::new();
    ZlibDecoder::new(fixture_bytes(name).as_slice())
        .read_to_end(&mut out)
        .expect("inflates");
    out
}

fn sample_type(dtype: &str) -> SampleType {
    match dtype {
        "uint8" => SampleType::U8,
        "int8" => SampleType::I8,
        "uint16" => SampleType::U16,
        "int16" => SampleType::I16,
        "uint32" => SampleType::U32,
        "int32" => SampleType::I32,
        "float32" => SampleType::F32,
        "float64" => SampleType::F64,
        other => panic!("dtype {other}"),
    }
}

/// The expected samples, one row-major `f64` plane per band.
fn expected_planes(f: &Fixture) -> Vec<Vec<f64>> {
    let (w, h, b) = (f.width, f.height, u32::from(f.bands));
    if f.expect == "pattern" {
        return match sample_type(&f.dtype) {
            SampleType::U8 => expected(&planes::<u8>(w, h, b)),
            SampleType::I8 => expected(&planes::<i8>(w, h, b)),
            SampleType::U16 => expected(&planes::<u16>(w, h, b)),
            SampleType::I16 => expected(&planes::<i16>(w, h, b)),
            SampleType::U32 => expected(&planes::<u32>(w, h, b)),
            SampleType::I32 => expected(&planes::<i32>(w, h, b)),
            SampleType::F32 => expected(&planes::<f32>(w, h, b)),
            SampleType::F64 => expected(&planes::<f64>(w, h, b)),
        };
    }
    let raw = inflate(&f.expect);
    let st = sample_type(&f.dtype);
    let values: Vec<f64> = raw
        .chunks_exact(st.byte_len())
        .map(|c| match st {
            SampleType::U8 => f64::from(c[0]),
            SampleType::I8 => f64::from(c[0] as i8),
            SampleType::U16 => f64::from(u16::from_le_bytes([c[0], c[1]])),
            SampleType::I16 => f64::from(i16::from_le_bytes([c[0], c[1]])),
            SampleType::U32 => f64::from(u32::from_le_bytes([c[0], c[1], c[2], c[3]])),
            SampleType::I32 => f64::from(i32::from_le_bytes([c[0], c[1], c[2], c[3]])),
            SampleType::F32 => f64::from(f32::from_le_bytes([c[0], c[1], c[2], c[3]])),
            SampleType::F64 => f64::from_le_bytes(c.try_into().expect("8 bytes")),
        })
        .collect();
    let pixels = (w * h) as usize;
    assert_eq!(values.len(), pixels * usize::from(f.bands), "{}", f.id);
    values.chunks(pixels).map(<[f64]>::to_vec).collect()
}

pub fn open_fixture(f: &Fixture) -> (TiffReader, Vec<u8>) {
    let bytes = fixture_bytes(&f.file);
    let opener = TiffOpener::new().with_image(f.image);
    let reader = open_all(&opener, &bytes, LIMITS).unwrap_or_else(|e| panic!("{}: {e}", f.id));
    (reader, bytes)
}

/// Every pixel of every band, and the mask (all valid when absent).
pub fn decode_image(reader: &TiffReader, bytes: &[u8]) -> (Vec<Vec<f64>>, Vec<bool>) {
    let header = reader.header();
    let width = header.width as usize;
    let pixels = width * header.height as usize;
    let mut planes = vec![vec![f64::NAN; pixels]; header.band_count()];
    let mut mask = vec![true; pixels];
    for index in 0..reader.block_count() {
        let block: RasterBlock = read_block_all(reader, index, bytes).expect("block decodes");
        let w = &block.window;
        for r in 0..w.height as usize {
            for c in 0..w.width as usize {
                let at = (w.row_off as usize + r) * width + w.col_off as usize + c;
                let local = r * w.width as usize + c;
                mask[at] = block.any_valid(local);
                for (band, samples) in block.bands.iter().enumerate() {
                    planes[band][at] = samples.value_f64(local).expect("in window");
                }
            }
        }
    }
    (planes, mask)
}

fn check(f: &Fixture) {
    let (reader, bytes) = open_fixture(f);
    let header = reader.header();
    assert_eq!(
        (header.width, header.height),
        (f.width, f.height),
        "{}",
        f.id
    );
    assert_eq!(header.band_count(), usize::from(f.bands), "{}", f.id);
    let st = sample_type(&f.dtype);
    assert!(header.bands.iter().all(|b| b.sample_type == st), "{}", f.id);
    let (got, mask) = decode_image(&reader, &bytes);
    let want_mask: Vec<bool> = if f.mask == "-" {
        vec![true; mask.len()]
    } else {
        inflate(&f.mask).iter().map(|v| *v != 0).collect()
    };
    assert_eq!(mask, want_mask, "{}: mask", f.id);
    let want = expected_planes(f);
    for (band, (g, w)) in got.iter().zip(&want).enumerate() {
        for (i, (a, b)) in g.iter().zip(w).enumerate() {
            if !want_mask[i] {
                continue;
            }
            let close = (a - b).abs() <= f.tol || (a.is_nan() && b.is_nan());
            assert!(close, "{}: band {band} pixel {i}: got {a}, want {b}", f.id);
        }
    }
}

#[test]
fn every_manifest_fixture_decodes_to_its_expected_samples() {
    let fixtures = manifest();
    assert!(fixtures.len() > 100, "the manifest lists the codec matrix");
    for f in &fixtures {
        check(f);
    }
}

#[test]
fn the_matrix_covers_both_byte_orders_and_bigtiff() {
    let ids: Vec<String> = manifest().into_iter().map(|f| f.id).collect();
    for codec in ["none", "lzw", "deflate", "packbits", "zstd"] {
        for part in [
            "-be",
            "-le",
            "-big-",
            "-classic-",
            "-separate-",
            "-contig-",
            "-tile-",
            "-strip-",
        ] {
            assert!(
                ids.iter()
                    .any(|id| id.starts_with(&format!("m-{codec}-")) && id.contains(part)),
                "{codec} lacks {part}"
            );
        }
    }
}
