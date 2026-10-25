// SPDX-License-Identifier: Apache-2.0

//! Typed errors of the raster readers.

use std::io;

use nodedb_crs::{CrsParseError, UnknownSrid};

use crate::codec::error::CodecError;
use crate::sample::SampleFormatCode;
use crate::source::range::ByteRange;
use crate::tiff::error::{TiffFormatError, UnsupportedTiff};
use crate::tiff::geokeys::GeoKeyError;

/// Every way opening or decoding a raster can fail.
#[derive(Debug, thiserror::Error)]
pub enum RasterError {
    /// The byte source failed for a reason outside the file's bytes.
    #[error("raster read failed: {0}")]
    Io(#[from] io::Error),

    /// The bytes do not form a valid TIFF: truncated, corrupt, or
    /// self-contradictory.
    #[error("malformed TIFF: {0}")]
    Malformed(#[from] TiffFormatError),

    /// A compressed chunk does not decode.
    #[error("corrupt compressed block: {0}")]
    Codec(#[from] CodecError),

    /// The TIFF is valid, but it uses a feature the decoder does not read.
    #[error("unsupported TIFF feature: {0}")]
    UnsupportedTiff(#[from] UnsupportedTiff),

    /// The byte source has no object with this id.
    #[error("raster source object {object} does not exist")]
    SourceMissing { object: u32 },

    /// A source returned more bytes than the range asked for.
    #[error("raster source returned {got} bytes for the {len}-byte range {range:?}")]
    RangeOverrun {
        range: ByteRange,
        len: u64,
        got: usize,
    },

    /// The header ranges an open needs exceed the header byte cap.
    #[error(
        "raster header data exceeds the {limit_bytes}-byte header cap; \
         the file has too many blocks or too large tag values"
    )]
    HeaderTooLarge { limit_bytes: usize },

    /// An open asked for ranges it already holds, or kept asking past the
    /// round cap.
    #[error("raster open made no progress after {rounds} fetch rounds")]
    OpenStalled { rounds: usize },

    /// One block decodes to more bytes than the caller allows, or one
    /// compressed chunk is larger than that cap.
    #[error(
        "raster block needs {needed_bytes} bytes, over the {limit_bytes}-byte limit; \
         rewrite the file with smaller strips or tiles"
    )]
    BlockTooLarge {
        needed_bytes: u64,
        limit_bytes: usize,
    },

    /// A tag value exceeds the tag value cap.
    #[error("TIFF tag {tag} declares a {bytes}-byte value, over the {limit_bytes}-byte cap")]
    TagValueTooLarge {
        tag: u16,
        bytes: u64,
        limit_bytes: usize,
    },

    /// The compression method has no decoder.
    #[error(
        "TIFF compression code {code} is not supported; supported: none (1), LZW (5), \
         JPEG (7), deflate (8, 32946), PackBits (32773), LERC (34887), ZSTD (50000)"
    )]
    UnsupportedCompression { code: u16 },

    /// The sample format and width are outside the supported types.
    #[error(
        "TIFF samples of format {format} with {bits} bits are not supported; \
         supported: u8, i8, u16, i16, u32, i32, f32, f64"
    )]
    UnsupportedSampleType { format: SampleFormatCode, bits: u16 },

    /// The photometric interpretation transforms samples on decode, or
    /// stores palette indexes.
    #[error(
        "TIFF photometric interpretation {code} is not supported; supported: \
         BlackIsZero (1), RGB (2), CMYK (5), YCbCr (6) with JPEG or without subsampling"
    )]
    UnsupportedPhotometric { code: u16 },

    /// The first image of the IFD chain is an overview.
    #[error(
        "TIFF first image is a reduced-resolution image; \
         the full-resolution image must be the first IFD"
    )]
    ReducedResolutionFirstImage,

    /// The caller selected an image the file does not hold.
    #[error("TIFF image {index} is out of range; the file has {count} images")]
    ImageOutOfRange { index: u32, count: u32 },

    /// The GeoKey directory does not parse.
    #[error("malformed GeoKey directory: {0}")]
    GeoKeys(#[from] GeoKeyError),

    /// A georeference tag holds the wrong number of values.
    #[error("GeoTIFF tag {tag} holds {found} values, expected {expected}")]
    GeoTagLength {
        tag: &'static str,
        found: usize,
        expected: &'static str,
    },

    /// Tiepoints without a pixel scale or a transformation are ground
    /// control points.
    #[error(
        "GeoTIFF georeferences by ground control points; \
         warp it to an affine grid first"
    )]
    GroundControlPoints,

    /// A pixel scale without a tiepoint places no pixel in the world.
    #[error("GeoTIFF has ModelPixelScaleTag but no ModelTiepointTag")]
    PixelScaleWithoutTiepoint,

    /// The model transformation is not a 2D affine transform.
    #[error(
        "GeoTIFF ModelTransformationTag is not a 2D affine transform: \
         the z terms must be 0 and the last row 0 0 0 1"
    )]
    NonAffineTransformation,

    /// The geotransform holds a non-finite term or maps the plane to a line.
    #[error("GeoTIFF geotransform {transform:?} is degenerate")]
    DegenerateTransform { transform: [f64; 6] },

    /// The GeoKey model type is neither projected (1) nor geographic (2).
    #[error(
        "GeoTIFF model type {model} is not supported; supported: projected (1), geographic (2)"
    )]
    UnsupportedModelType { model: u16 },

    /// The model type names a CRS key the directory does not hold.
    #[error("GeoTIFF model type {model} needs GeoKey {key}, which is absent")]
    MissingCrsKey { model: u16, key: u16 },

    /// The CRS code names no defined CRS.
    #[error("GeoTIFF CRS: {0}")]
    UnknownCrs(#[from] UnknownSrid),

    /// A user-defined CRS (GeoKey value 32767) uses a GeoKey value the
    /// decoder does not read.
    #[error("GeoTIFF user-defined CRS: {detail}")]
    UnsupportedUserCrs { detail: String },

    /// A user-defined CRS decodes to PROJ text the CRS parser refuses.
    #[error("GeoTIFF user-defined CRS: {0}")]
    UserCrsParse(#[from] CrsParseError),

    /// A user-defined CRS matches no registered CRS by projection,
    /// ellipsoid, unit and datum.
    #[error(
        "GeoTIFF user-defined CRS matches no registered CRS: {proj}; \
         define it with CREATE SPATIAL_REF_SYS and load again"
    )]
    UnmatchedUserCrs { proj: String },

    /// A user-defined CRS names an ellipsoid but no datum, and registered
    /// CRSs of its shape sit on more than one datum.
    #[error(
        "GeoTIFF user-defined CRS {proj} names no datum and matches CRSs on different \
         datums, SRID {first} and SRID {second}; write the datum code into the file, \
         or define the CRS with CREATE SPATIAL_REF_SYS and load again"
    )]
    AmbiguousUserCrs {
        proj: String,
        first: nodedb_crs::Srid,
        second: nodedb_crs::Srid,
    },

    /// A nodata value (`GDAL_NODATA` or a `GDAL_METADATA` item) is not a
    /// number.
    #[error("raster nodata '{text}' is not a number")]
    InvalidNodata { text: String },

    /// A `GDAL_METADATA` band number or numeric item does not parse.
    #[error("GDAL_METADATA item '{name}' holds '{text}', which is not a valid value")]
    InvalidMetadata { name: String, text: String },

    /// The caller asked for a block the image does not have.
    #[error("raster block {index} is out of range; the image has {count} blocks")]
    BlockOutOfRange { index: u32, count: u32 },

    /// The caller passed a different number of byte buffers than
    /// `block_ranges` returned.
    #[error("raster block {index} got {found} byte buffers, expected {expected}")]
    BlockBuffers {
        index: u32,
        found: usize,
        expected: usize,
    },

    /// A block decoded to fewer samples than it holds.
    #[error("raster block {index} decoded {found} bytes, expected at least {expected}")]
    ShortBlock {
        index: u32,
        found: usize,
        expected: usize,
    },
}
