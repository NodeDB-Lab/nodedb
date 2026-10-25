// SPDX-License-Identifier: Apache-2.0

//! In-test GeoTIFF builders and whole-image readback.
//!
//! Strip fixtures go through the tiff `ImageEncoder`. Tiled, planar, and
//! floating-point-predictor fixtures write each chunk by hand through the
//! `DirectoryEncoder`, since `ImageEncoder` writes chunky strips only.

use std::io::{Cursor, Seek, Write};

use nodedb_raster::RasterLimits;
use tiff::encoder::colortype::ColorType;
use tiff::encoder::compression::{CompressionAlgorithm, Deflate, Lzw};
use tiff::encoder::{Compression, DirectoryEncoder, Predictor, TiffEncoder, TiffKind, TiffValue};
use tiff::tags::{ExtraSamples, Tag};

/// Decode cap for fixtures that are meant to fit.
pub const LIMITS: RasterLimits = RasterLimits::new(64 * 1024 * 1024);

/// TIFF compression codes for hand-written chunks.
pub const NONE: u16 = 1;
pub const LZW: u16 = 5;
pub const DEFLATE: u16 = 8;

/// One extra IFD 0 tag.
pub enum Extra {
    Shorts(Tag, Vec<u16>),
    Doubles(Tag, Vec<f64>),
    Ascii(Tag, String),
    Long(Tag, u32),
}

fn write_extras<W: Write + Seek, K: TiffKind>(
    dir: &mut DirectoryEncoder<'_, W, K>,
    extras: &[Extra],
) {
    for extra in extras {
        let written = match extra {
            Extra::Shorts(tag, values) => dir.write_tag(*tag, &values[..]),
            Extra::Doubles(tag, values) => dir.write_tag(*tag, &values[..]),
            Extra::Ascii(tag, text) => dir.write_tag(*tag, text.as_str()),
            Extra::Long(tag, value) => dir.write_tag(*tag, *value),
        };
        written.expect("extra tag writes");
    }
}

/// A GeoKey directory of inline short keys, sorted ascending.
pub fn geokeys(entries: &[(u16, u16)]) -> Vec<u16> {
    let count = u16::try_from(entries.len()).expect("few keys");
    let mut dir = vec![1, 1, 0, count];
    for (key, value) in entries {
        dir.extend([*key, 0, 1, *value]);
    }
    dir
}

/// A sample type the fixtures can write.
pub trait Sample: Copy + Default {
    /// Append the native-endian bytes.
    fn put(self, out: &mut Vec<u8>);
    /// The big-endian bytes, for the floating-point predictor.
    fn be(self) -> Vec<u8>;
    /// A deterministic sample from a pattern counter.
    fn from_pattern(counter: u32) -> Self;
    fn to_f64(self) -> f64;
    fn wrapping_diff(self, previous: Self) -> Self;
}

macro_rules! int_sample {
    ($t:ty, $offset:expr) => {
        impl Sample for $t {
            fn put(self, out: &mut Vec<u8>) {
                out.extend_from_slice(&self.to_ne_bytes());
            }
            fn be(self) -> Vec<u8> {
                self.to_be_bytes().to_vec()
            }
            fn from_pattern(counter: u32) -> Self {
                (counter as $t).wrapping_sub($offset)
            }
            fn to_f64(self) -> f64 {
                f64::from(self)
            }
            fn wrapping_diff(self, previous: Self) -> Self {
                self.wrapping_sub(previous)
            }
        }
    };
}

macro_rules! float_sample {
    ($t:ty) => {
        impl Sample for $t {
            fn put(self, out: &mut Vec<u8>) {
                out.extend_from_slice(&self.to_ne_bytes());
            }
            fn be(self) -> Vec<u8> {
                self.to_be_bytes().to_vec()
            }
            fn from_pattern(counter: u32) -> Self {
                (counter as $t) * 0.25 - 40.0
            }
            fn to_f64(self) -> f64 {
                f64::from(self)
            }
            fn wrapping_diff(self, previous: Self) -> Self {
                self - previous
            }
        }
    };
}

int_sample!(u8, 0);
int_sample!(i8, 60);
int_sample!(i32, 70_000);
int_sample!(u16, 0);
int_sample!(i16, 300);
int_sample!(u32, 0);
float_sample!(f32);
float_sample!(f64);

/// One row-major plane per band, every sample distinct within a band.
pub fn planes<T: Sample>(width: u32, height: u32, bands: u32) -> Vec<Vec<T>> {
    (0..bands)
        .map(|band| {
            (0..height)
                .flat_map(|y| (0..width).map(move |x| band * 97 + y * 31 + x * 7 + 1))
                .map(T::from_pattern)
                .collect()
        })
        .collect()
}

/// Interleave `planes` pixel by pixel, the chunky order.
pub fn interleave<T: Sample>(planes: &[Vec<T>]) -> Vec<T> {
    let pixels = planes.first().map_or(0, Vec::len);
    (0..pixels)
        .flat_map(|i| planes.iter().map(move |plane| plane[i]))
        .collect()
}

/// `planes` as `f64`, the shape `read_all` returns.
pub fn expected<T: Sample>(planes: &[Vec<T>]) -> Vec<Vec<f64>> {
    planes
        .iter()
        .map(|plane| plane.iter().map(|s| s.to_f64()).collect())
        .collect()
}

/// Options of a strip fixture written through `ImageEncoder`.
pub struct StripSpec<'a> {
    pub width: u32,
    pub height: u32,
    pub compression: Compression,
    pub predictor: Predictor,
    pub rows_per_strip: Option<u32>,
    pub extra_samples: usize,
    pub extras: &'a [Extra],
}

/// An uncompressed single-band strip spec.
pub fn strip_spec(width: u32, height: u32) -> StripSpec<'static> {
    StripSpec {
        width,
        height,
        compression: Compression::Uncompressed,
        predictor: Predictor::None,
        rows_per_strip: None,
        extra_samples: 0,
        extras: &[],
    }
}

/// Encode chunky strips of color type `C`.
pub fn encode_strips<C: ColorType>(spec: &StripSpec<'_>, data: &[C::Inner]) -> Vec<u8>
where
    [C::Inner]: TiffValue,
{
    let mut cursor = Cursor::new(Vec::new());
    {
        let mut encoder = TiffEncoder::new(&mut cursor)
            .expect("encoder")
            .with_compression(spec.compression)
            .with_predictor(spec.predictor);
        let mut image = encoder
            .new_image::<C>(spec.width, spec.height)
            .expect("image");
        if spec.extra_samples > 0 {
            image
                .extra_samples(&vec![ExtraSamples::Unspecified; spec.extra_samples])
                .expect("extra samples");
        }
        if let Some(rows) = spec.rows_per_strip {
            image.rows_per_strip(rows).expect("rows per strip");
        }
        write_extras(image.encoder(), spec.extras);
        image.write_data(data).expect("image data writes");
    }
    cursor.into_inner()
}

/// How a hand-written image stores its chunks.
#[derive(Clone, Copy)]
pub enum RawLayout {
    Strips { rows_per_strip: u32 },
    Tiles { width: u32, height: u32 },
}

/// A hand-written IFD 0.
pub struct RawImage<'a> {
    pub width: u32,
    pub height: u32,
    pub bits: u16,
    pub sample_format: u16,
    pub samples: u16,
    pub photometric: u16,
    pub planar: bool,
    pub compression: u16,
    pub predictor: u16,
    pub layout: RawLayout,
    /// Encoded chunks in file order: every block of band 0 first when
    /// planar.
    pub chunks: Vec<Vec<u8>>,
    pub extras: &'a [Extra],
}

/// Write `image` as a standard TIFF.
pub fn encode_raw(image: &RawImage<'_>) -> Vec<u8> {
    let mut cursor = Cursor::new(Vec::new());
    {
        let mut encoder = TiffEncoder::new(&mut cursor).expect("encoder");
        let mut dir = encoder.image_directory().expect("directory");
        let mut offsets = Vec::new();
        let mut counts = Vec::new();
        for chunk in &image.chunks {
            let offset = dir.write_data(&chunk[..]).expect("chunk writes");
            offsets.push(u32::try_from(offset).expect("small file"));
            counts.push(u32::try_from(chunk.len()).expect("small chunk"));
        }
        let samples = usize::from(image.samples);
        dir.write_tag(Tag::ImageWidth, image.width).expect("tag");
        dir.write_tag(Tag::ImageLength, image.height).expect("tag");
        dir.write_tag(Tag::BitsPerSample, &vec![image.bits; samples][..])
            .expect("tag");
        dir.write_tag(Tag::SampleFormat, &vec![image.sample_format; samples][..])
            .expect("tag");
        dir.write_tag(Tag::SamplesPerPixel, image.samples)
            .expect("tag");
        dir.write_tag(Tag::PhotometricInterpretation, image.photometric)
            .expect("tag");
        dir.write_tag(
            Tag::PlanarConfiguration,
            if image.planar { 2u16 } else { 1 },
        )
        .expect("tag");
        dir.write_tag(Tag::Compression, image.compression)
            .expect("tag");
        dir.write_tag(Tag::Predictor, image.predictor).expect("tag");
        match image.layout {
            RawLayout::Strips { rows_per_strip } => {
                dir.write_tag(Tag::RowsPerStrip, rows_per_strip)
                    .expect("tag");
                dir.write_tag(Tag::StripOffsets, &offsets[..]).expect("tag");
                dir.write_tag(Tag::StripByteCounts, &counts[..])
                    .expect("tag");
            }
            RawLayout::Tiles { width, height } => {
                dir.write_tag(Tag::TileWidth, width).expect("tag");
                dir.write_tag(Tag::TileLength, height).expect("tag");
                dir.write_tag(Tag::TileOffsets, &offsets[..]).expect("tag");
                dir.write_tag(Tag::TileByteCounts, &counts[..])
                    .expect("tag");
            }
        }
        write_extras(&mut dir, image.extras);
        dir.finish().expect("directory writes");
    }
    cursor.into_inner()
}

/// The predictor a hand-written chunk is encoded with.
#[derive(Clone, Copy, PartialEq)]
pub enum Pred {
    None,
    Horizontal,
    FloatingPoint,
}

impl Pred {
    pub fn code(self) -> u16 {
        match self {
            Pred::None => 1,
            Pred::Horizontal => 2,
            Pred::FloatingPoint => 3,
        }
    }
}

/// Split `planes` into chunk sample runs in file order. Tiles are padded
/// to full size. The last strip keeps only the image's rows.
pub fn chunk_samples<T: Sample>(
    planes: &[Vec<T>],
    width: u32,
    height: u32,
    layout: RawLayout,
    planar: bool,
) -> Vec<Vec<T>> {
    let (chunk_w, chunk_h, crop_rows) = match layout {
        RawLayout::Strips { rows_per_strip } => (width, rows_per_strip, true),
        RawLayout::Tiles { width, height } => (width, height, false),
    };
    let bands = planes.len();
    let groups: Vec<Vec<usize>> = if planar {
        (0..bands).map(|band| vec![band]).collect()
    } else {
        vec![(0..bands).collect()]
    };
    let mut chunks = Vec::new();
    for group in groups {
        for ty in 0..height.div_ceil(chunk_h) {
            for tx in 0..width.div_ceil(chunk_w) {
                let mut chunk = Vec::new();
                for r in 0..chunk_h {
                    let y = ty * chunk_h + r;
                    if crop_rows && y >= height {
                        break;
                    }
                    for c in 0..chunk_w {
                        let x = tx * chunk_w + c;
                        for &band in &group {
                            chunk.push(if x < width && y < height {
                                planes[band][(y * width + x) as usize]
                            } else {
                                T::default()
                            });
                        }
                    }
                }
                chunks.push(chunk);
            }
        }
    }
    chunks
}

/// Encode one chunk: predictor per row, then compression.
pub fn chunk_bytes<T: Sample>(
    chunk: &[T],
    row_samples: usize,
    samples_per_pixel: usize,
    predictor: Pred,
    compression: u16,
) -> Vec<u8> {
    let mut raw = Vec::new();
    for row in chunk.chunks(row_samples) {
        match predictor {
            Pred::None => row.iter().for_each(|s| s.put(&mut raw)),
            Pred::Horizontal => {
                for (i, sample) in row.iter().enumerate() {
                    let encoded = if i < samples_per_pixel {
                        *sample
                    } else {
                        sample.wrapping_diff(row[i - samples_per_pixel])
                    };
                    encoded.put(&mut raw);
                }
            }
            Pred::FloatingPoint => raw.extend(float_predict(row, samples_per_pixel)),
        }
    }
    compress(&raw, compression)
}

/// Byte-plane shuffle of big-endian samples, then byte differencing at
/// the pixel stride (TIFF Technical Note 3).
fn float_predict<T: Sample>(row: &[T], samples_per_pixel: usize) -> Vec<u8> {
    let be: Vec<Vec<u8>> = row.iter().map(|s| s.be()).collect();
    let width = be.first().map_or(0, Vec::len);
    let n = row.len();
    let mut shuffled = vec![0u8; n * width];
    for (i, bytes) in be.iter().enumerate() {
        for (k, byte) in bytes.iter().enumerate() {
            shuffled[k * n + i] = *byte;
        }
    }
    for i in (samples_per_pixel..shuffled.len()).rev() {
        shuffled[i] = shuffled[i].wrapping_sub(shuffled[i - samples_per_pixel]);
    }
    shuffled
}

/// Compress `raw` with TIFF compression `code`.
pub fn compress(raw: &[u8], code: u16) -> Vec<u8> {
    let mut out = Vec::new();
    match code {
        NONE => out.extend_from_slice(raw),
        LZW => {
            Lzw.write_to(&mut out, raw).expect("lzw");
        }
        DEFLATE => {
            Deflate::default().write_to(&mut out, raw).expect("deflate");
        }
        other => panic!("fixture compression {other} is not built"),
    }
    out
}

/// Encoded chunks of `planes` under `layout`.
pub fn encode_chunks<T: Sample>(
    planes: &[Vec<T>],
    image: (u32, u32),
    layout: RawLayout,
    planar: bool,
    predictor: Pred,
    compression: u16,
) -> Vec<Vec<u8>> {
    let chunk_w = match layout {
        RawLayout::Strips { .. } => image.0,
        RawLayout::Tiles { width: tile_w, .. } => tile_w,
    };
    let spp = if planar { 1 } else { planes.len() };
    chunk_samples(planes, image.0, image.1, layout, planar)
        .iter()
        .map(|chunk| chunk_bytes(chunk, chunk_w as usize * spp, spp, predictor, compression))
        .collect()
}
