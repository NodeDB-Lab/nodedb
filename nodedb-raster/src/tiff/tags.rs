// SPDX-License-Identifier: Apache-2.0

//! TIFF tag codes and field types the reader uses.

pub const NEW_SUBFILE_TYPE: u16 = 254;
pub const SUBFILE_TYPE: u16 = 255;
pub const IMAGE_WIDTH: u16 = 256;
pub const IMAGE_LENGTH: u16 = 257;
pub const BITS_PER_SAMPLE: u16 = 258;
pub const COMPRESSION: u16 = 259;
pub const PHOTOMETRIC: u16 = 262;
pub const STRIP_OFFSETS: u16 = 273;
pub const SAMPLES_PER_PIXEL: u16 = 277;
pub const ROWS_PER_STRIP: u16 = 278;
pub const STRIP_BYTE_COUNTS: u16 = 279;
pub const PLANAR_CONFIG: u16 = 284;
pub const PREDICTOR: u16 = 317;
pub const TILE_WIDTH: u16 = 322;
pub const TILE_LENGTH: u16 = 323;
pub const TILE_OFFSETS: u16 = 324;
pub const TILE_BYTE_COUNTS: u16 = 325;
pub const SAMPLE_FORMAT: u16 = 339;
pub const JPEG_TABLES: u16 = 347;
pub const YCBCR_SUBSAMPLING: u16 = 530;
pub const MODEL_PIXEL_SCALE: u16 = 33550;
pub const MODEL_TIEPOINT: u16 = 33922;
pub const MODEL_TRANSFORMATION: u16 = 34264;
pub const GEO_KEY_DIRECTORY: u16 = 34735;
pub const GEO_DOUBLE_PARAMS: u16 = 34736;
pub const GEO_ASCII_PARAMS: u16 = 34737;
pub const GDAL_METADATA: u16 = 42112;
pub const GDAL_NODATA: u16 = 42113;
pub const LERC_PARAMETERS: u16 = 50674;

/// `NewSubfileType` bit of a reduced-resolution image.
pub const SUBFILE_REDUCED: u64 = 1;
/// `NewSubfileType` bit of a transparency mask.
pub const SUBFILE_MASK: u64 = 4;
/// `SubfileType` value of a reduced-resolution image.
pub const OLD_SUBFILE_REDUCED: u64 = 2;

/// Tags whose out-of-line values the image and mask decode reads.
pub const IMAGE_TAGS: [u16; 9] = [
    BITS_PER_SAMPLE,
    SAMPLE_FORMAT,
    STRIP_OFFSETS,
    STRIP_BYTE_COUNTS,
    TILE_OFFSETS,
    TILE_BYTE_COUNTS,
    JPEG_TABLES,
    YCBCR_SUBSAMPLING,
    LERC_PARAMETERS,
];

/// Tags whose out-of-line values the georeference read takes from the
/// first image.
pub const GEO_TAGS: [u16; 8] = [
    MODEL_PIXEL_SCALE,
    MODEL_TIEPOINT,
    MODEL_TRANSFORMATION,
    GEO_KEY_DIRECTORY,
    GEO_DOUBLE_PARAMS,
    GEO_ASCII_PARAMS,
    GDAL_METADATA,
    GDAL_NODATA,
];

/// The block index tags, bounded by the header cap instead of the tag
/// value cap.
pub const fn is_block_index(tag: u16) -> bool {
    matches!(
        tag,
        STRIP_OFFSETS | STRIP_BYTE_COUNTS | TILE_OFFSETS | TILE_BYTE_COUNTS
    )
}

/// A TIFF field type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FieldType {
    Byte,
    Ascii,
    Short,
    Long,
    Rational,
    SByte,
    Undefined,
    SShort,
    SLong,
    SRational,
    Float,
    Double,
    Ifd,
    Long8,
    SLong8,
    Ifd8,
}

impl FieldType {
    /// The field type of code `code`, or `None` for an unknown code.
    pub const fn from_code(code: u16) -> Option<Self> {
        match code {
            1 => Some(FieldType::Byte),
            2 => Some(FieldType::Ascii),
            3 => Some(FieldType::Short),
            4 => Some(FieldType::Long),
            5 => Some(FieldType::Rational),
            6 => Some(FieldType::SByte),
            7 => Some(FieldType::Undefined),
            8 => Some(FieldType::SShort),
            9 => Some(FieldType::SLong),
            10 => Some(FieldType::SRational),
            11 => Some(FieldType::Float),
            12 => Some(FieldType::Double),
            13 => Some(FieldType::Ifd),
            16 => Some(FieldType::Long8),
            17 => Some(FieldType::SLong8),
            18 => Some(FieldType::Ifd8),
            _ => None,
        }
    }

    /// The type code.
    pub const fn code(self) -> u16 {
        match self {
            FieldType::Byte => 1,
            FieldType::Ascii => 2,
            FieldType::Short => 3,
            FieldType::Long => 4,
            FieldType::Rational => 5,
            FieldType::SByte => 6,
            FieldType::Undefined => 7,
            FieldType::SShort => 8,
            FieldType::SLong => 9,
            FieldType::SRational => 10,
            FieldType::Float => 11,
            FieldType::Double => 12,
            FieldType::Ifd => 13,
            FieldType::Long8 => 16,
            FieldType::SLong8 => 17,
            FieldType::Ifd8 => 18,
        }
    }

    /// Bytes per value.
    pub const fn width(self) -> u64 {
        match self {
            FieldType::Byte | FieldType::Ascii | FieldType::SByte | FieldType::Undefined => 1,
            FieldType::Short | FieldType::SShort => 2,
            FieldType::Long | FieldType::SLong | FieldType::Float | FieldType::Ifd => 4,
            FieldType::Rational
            | FieldType::SRational
            | FieldType::Double
            | FieldType::Long8
            | FieldType::SLong8
            | FieldType::Ifd8 => 8,
        }
    }
}
