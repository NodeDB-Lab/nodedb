// SPDX-License-Identifier: Apache-2.0

//! Typed errors of the TIFF container layer.

use crate::limits::CapacityExceeded;
use crate::sample::SampleType;

/// Every way TIFF structure can be malformed.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum TiffFormatError {
    #[error("not a TIFF: the file opens with neither 'II*' nor 'MM*' nor BigTIFF 'II+'/'MM+'")]
    NotTiff,

    #[error("BigTIFF header declares {offset_size}-byte offsets; BigTIFF offsets are 8 bytes")]
    BigTiffOffsetSize { offset_size: u16 },

    #[error("the file ends at byte {end}, inside a {len}-byte structure at byte {offset}")]
    Truncated { offset: u64, len: u64, end: u64 },

    #[error("the IFD chain loops back to byte {offset}")]
    IfdLoop { offset: u64 },

    #[error("the IFD chain holds more than {limit} IFDs")]
    TooManyIfds { limit: usize },

    #[error("the IFD at byte {offset} declares {count} entries, over the {limit}-entry cap")]
    TooManyEntries { offset: u64, count: u64, limit: u64 },

    #[error(transparent)]
    Capacity(#[from] CapacityExceeded),

    #[error("the IFD chain holds no full-resolution image")]
    NoImage,

    #[error("tag {tag} has field type {kind}, which does not fit its value")]
    FieldType { tag: u16, kind: u16 },

    #[error("required tag {tag} ({name}) is absent")]
    MissingTag { tag: u16, name: &'static str },

    #[error("tag {tag} holds {found} values, expected {expected}")]
    TagCount { tag: u16, found: u64, expected: u64 },

    #[error("tag {tag} declares a value whose offset and length overflow")]
    ValueOverflow { tag: u16 },

    #[error("tag {tag} holds {value}, which is out of range")]
    TagValue { tag: u16, value: u64 },

    #[error("tag {tag} holds ASCII text that is not UTF-8")]
    AsciiEncoding { tag: u16 },

    #[error("a {len}-byte range at byte {offset} overflows the file offsets")]
    OffsetOverflow { offset: u64, len: u64 },

    #[error("chunk {chunk} at byte {offset} with {len} bytes overflows the file offsets")]
    ChunkOverflow { chunk: u64, offset: u64, len: u64 },

    #[error(
        "the image is {width}x{height} in {block_width}x{block_height} blocks: too many blocks"
    )]
    TooManyBlocks {
        width: u32,
        height: u32,
        block_width: u32,
        block_height: u32,
    },
}

/// A valid TIFF feature the reader does not decode.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum UnsupportedTiff {
    #[error("bands of different bit widths ({first} and {other} bits)")]
    MixedBitsPerSample { first: u16, other: u16 },

    #[error("bands of different sample formats ({first} and {other})")]
    MixedSampleFormats { first: u16, other: u16 },

    #[error("predictor {code}; supported: none (1), horizontal (2), floating point (3)")]
    Predictor { code: u16 },

    #[error("the floating-point predictor (3) over {sample_type} samples")]
    FloatPredictorOnIntegers { sample_type: SampleType },

    #[error("JPEG over {sample_type} samples; JPEG decodes 8-bit samples only")]
    JpegSampleType { sample_type: SampleType },

    #[error("JPEG chunks of {samples} samples per pixel; supported: 1 and 3")]
    JpegSamples { samples: u16 },

    #[error("JPEG chunks of {width}x{height} pixels; JPEG frames are at most 65535 on a side")]
    JpegFrameSize { width: u32, height: u32 },

    #[error("YCbCr subsampling {horizontal}x{vertical} without JPEG compression")]
    YCbCrSubsampling { horizontal: u16, vertical: u16 },

    #[error("LERC additional compression {code}; supported: none (0), deflate (1), ZSTD (2)")]
    LercCompression { code: u64 },

    #[error("mask IFD of {bits}-bit samples; supported: 1 and 8")]
    MaskBits { bits: u16 },
}
