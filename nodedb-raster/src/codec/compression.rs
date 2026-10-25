// SPDX-License-Identifier: Apache-2.0

//! The TIFF compression methods the reader decodes.

use crate::codec::error::CodecError;
use crate::codec::{deflate, lzw, packbits, zstd};
use crate::error::RasterError;

/// TIFF `Compression` codes.
pub const CODE_NONE: u16 = 1;
pub const CODE_LZW: u16 = 5;
pub const CODE_JPEG: u16 = 7;
pub const CODE_DEFLATE: u16 = 8;
pub const CODE_PACKBITS: u16 = 32773;
pub const CODE_DEFLATE_PKZIP: u16 = 32946;
pub const CODE_LERC: u16 = 34887;
pub const CODE_ZSTD: u16 = 50000;

/// A byte-stream codec: the chunk decodes to its raw sample bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamCodec {
    None,
    Lzw,
    Deflate,
    PackBits,
    Zstd,
}

impl StreamCodec {
    /// Decode `data` into exactly `expected` bytes. Output past
    /// `expected` is ignored. Fewer bytes are [`CodecError::Short`].
    pub fn decompress(self, data: &[u8], expected: usize) -> Result<Vec<u8>, CodecError> {
        match self {
            StreamCodec::None => {
                let raw = data.get(..expected).ok_or(CodecError::Short {
                    expected,
                    found: data.len(),
                })?;
                Ok(raw.to_vec())
            }
            StreamCodec::Lzw => lzw::decode(data, expected),
            StreamCodec::Deflate => deflate::inflate(data, expected),
            StreamCodec::PackBits => packbits::unpack(data, expected),
            StreamCodec::Zstd => zstd::decode(data, expected),
        }
    }

    /// Decode `data`, a stream of unknown decoded size, into at most `max`
    /// bytes. More is [`CodecError::Long`].
    pub fn decompress_bounded(self, data: &[u8], max: usize) -> Result<Vec<u8>, CodecError> {
        match self {
            StreamCodec::None if data.len() <= max => Ok(data.to_vec()),
            StreamCodec::None => Err(CodecError::Long { expected: max }),
            StreamCodec::Deflate => deflate::inflate_bounded(data, max),
            StreamCodec::Zstd => zstd::decode_bounded(data, max),
            StreamCodec::Lzw => lzw::decode_bounded(data, max),
            StreamCodec::PackBits => packbits::unpack_bounded(data, max),
        }
    }
}

/// The byte-stream wrapper around a LERC blob (`LercParameters` value 2).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LercWrap {
    None,
    Deflate,
    Zstd,
}

impl LercWrap {
    /// The wrapper of `LercParameters` additional-compression code `code`.
    pub fn from_code(code: u64) -> Option<Self> {
        match code {
            0 => Some(LercWrap::None),
            1 => Some(LercWrap::Deflate),
            2 => Some(LercWrap::Zstd),
            _ => None,
        }
    }

    /// The stream codec that unwraps the blob, if any.
    pub const fn stream(self) -> Option<StreamCodec> {
        match self {
            LercWrap::None => None,
            LercWrap::Deflate => Some(StreamCodec::Deflate),
            LercWrap::Zstd => Some(StreamCodec::Zstd),
        }
    }
}

/// How a chunk is compressed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Compression {
    Stream(StreamCodec),
    Jpeg,
    Lerc(LercWrap),
}

impl Compression {
    /// The method of TIFF compression code `code`. `lerc` is the LERC
    /// wrapper from `LercParameters`.
    pub fn from_code(code: u16, lerc: LercWrap) -> Result<Self, RasterError> {
        match code {
            CODE_NONE => Ok(Compression::Stream(StreamCodec::None)),
            CODE_LZW => Ok(Compression::Stream(StreamCodec::Lzw)),
            CODE_JPEG => Ok(Compression::Jpeg),
            CODE_DEFLATE | CODE_DEFLATE_PKZIP => Ok(Compression::Stream(StreamCodec::Deflate)),
            CODE_PACKBITS => Ok(Compression::Stream(StreamCodec::PackBits)),
            CODE_LERC => Ok(Compression::Lerc(lerc)),
            CODE_ZSTD => Ok(Compression::Stream(StreamCodec::Zstd)),
            code => Err(RasterError::UnsupportedCompression { code }),
        }
    }

    /// Whether libtiff applies the `Predictor` tag under this method: LZW,
    /// deflate and ZSTD only.
    pub const fn takes_predictor(self) -> bool {
        matches!(
            self,
            Compression::Stream(StreamCodec::Lzw | StreamCodec::Deflate | StreamCodec::Zstd)
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_supported_code_maps() {
        for code in [1, 5, 7, 8, 32773, 32946, 34887, 50000] {
            assert!(
                Compression::from_code(code, LercWrap::None).is_ok(),
                "{code}"
            );
        }
        for code in [2, 3, 4, 6, 34925, 50001, 34712] {
            assert!(matches!(
                Compression::from_code(code, LercWrap::None),
                Err(RasterError::UnsupportedCompression { code: c }) if c == code
            ));
        }
    }

    #[test]
    fn predictor_applies_to_lzw_deflate_and_zstd_only() {
        let takes = |code| {
            Compression::from_code(code, LercWrap::None)
                .map(Compression::takes_predictor)
                .ok()
        };
        assert_eq!(takes(5), Some(true));
        assert_eq!(takes(8), Some(true));
        assert_eq!(takes(50000), Some(true));
        assert_eq!(takes(1), Some(false));
        assert_eq!(takes(32773), Some(false));
        assert_eq!(takes(34887), Some(false));
    }

    #[test]
    fn raw_chunks_need_the_whole_chunk() {
        let codec = StreamCodec::None;
        assert_eq!(codec.decompress(&[1, 2, 3], 2).ok(), Some(vec![1, 2]));
        assert!(matches!(
            codec.decompress(&[1], 2),
            Err(CodecError::Short {
                expected: 2,
                found: 1
            })
        ));
    }
}
