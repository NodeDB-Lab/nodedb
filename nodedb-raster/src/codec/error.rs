// SPDX-License-Identifier: Apache-2.0

//! Typed errors of the chunk codecs.

use crate::limits::CapacityExceeded;

/// Every way a compressed chunk can fail to decode.
#[derive(Debug, thiserror::Error)]
pub enum CodecError {
    #[error("LZW: {0}")]
    Lzw(#[source] weezl::LzwError),

    #[error("deflate: {0}")]
    Deflate(#[source] flate2::DecompressError),

    #[error("ZSTD: {0}")]
    Zstd(#[source] ruzstd::decoding::errors::FrameDecoderError),

    #[error("JPEG: {0}")]
    Jpeg(#[source] zune_jpeg::errors::DecodeErrors),

    #[error("LERC: {0}")]
    Lerc(#[source] lerc::LercError),

    /// A Lerc2 blob header or checksum is invalid.
    #[error("LERC blob at byte {offset}: {reason}")]
    LercBlob { offset: usize, reason: &'static str },

    /// A PackBits run reads or writes past the end of its chunk.
    #[error("PackBits run at byte {offset} overruns the chunk")]
    PackBitsOverrun { offset: usize },

    /// The chunk asks for a decode allocation over the ceiling.
    #[error(transparent)]
    Capacity(#[from] CapacityExceeded),

    /// The stream ended before the chunk was whole.
    #[error("the stream decodes to {found} bytes, expected {expected}")]
    Short { expected: usize, found: usize },

    /// The stream holds more data than the chunk.
    #[error("the stream decodes to more than the {expected}-byte chunk")]
    Long { expected: usize },

    /// The stream ends before its end marker.
    #[error("the stream ends before its end marker, after {found} decoded bytes")]
    Truncated { found: usize },

    /// The JPEG frame does not match the chunk.
    #[error(
        "JPEG frame is {width}x{height} with {components} components; the chunk is \
         {expected_width}x{expected_height} with {expected_components}"
    )]
    JpegShape {
        width: u32,
        height: u32,
        components: usize,
        expected_width: u32,
        expected_height: u32,
        expected_components: usize,
    },

    /// The LERC blob does not match the chunk.
    #[error(
        "LERC blob is {width}x{height}, depth {depth}, {bands} bands of {data_type}; the chunk \
         is {expected_width}x{expected_height}, depth {expected_depth} of {expected_type}"
    )]
    LercShape {
        width: u32,
        height: u32,
        depth: u32,
        bands: u32,
        data_type: &'static str,
        expected_width: u32,
        expected_height: u32,
        expected_depth: u32,
        expected_type: &'static str,
    },
}
