// SPDX-License-Identifier: Apache-2.0

//! The closed set of sample types the readers decode.

use std::fmt;

use crate::error::RasterError;

/// TIFF `SampleFormat` 1: unsigned integer.
pub const FORMAT_UINT: u16 = 1;
/// TIFF `SampleFormat` 2: two's complement signed integer.
pub const FORMAT_INT: u16 = 2;
/// TIFF `SampleFormat` 3: IEEE floating point.
pub const FORMAT_IEEE_FP: u16 = 3;

/// A TIFF `SampleFormat` tag value, kept for error messages.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SampleFormatCode(pub u16);

impl fmt::Display for SampleFormatCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.0 {
            1 => f.write_str("unsigned integer (1)"),
            2 => f.write_str("signed integer (2)"),
            3 => f.write_str("IEEE float (3)"),
            4 => f.write_str("undefined (4)"),
            code => write!(f, "code {code}"),
        }
    }
}

/// One band sample type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SampleType {
    U8,
    I8,
    U16,
    I16,
    U32,
    I32,
    F32,
    F64,
}

impl SampleType {
    /// The sample type for a TIFF sample format code and bit width. Any
    /// other pair fails with [`RasterError::UnsupportedSampleType`].
    pub fn from_tiff(format: u16, bits: u16) -> Result<Self, RasterError> {
        match (format, bits) {
            (FORMAT_UINT, 8) => Ok(SampleType::U8),
            (FORMAT_INT, 8) => Ok(SampleType::I8),
            (FORMAT_UINT, 16) => Ok(SampleType::U16),
            (FORMAT_INT, 16) => Ok(SampleType::I16),
            (FORMAT_UINT, 32) => Ok(SampleType::U32),
            (FORMAT_INT, 32) => Ok(SampleType::I32),
            (FORMAT_IEEE_FP, 32) => Ok(SampleType::F32),
            (FORMAT_IEEE_FP, 64) => Ok(SampleType::F64),
            _ => Err(RasterError::UnsupportedSampleType {
                format: SampleFormatCode(format),
                bits,
            }),
        }
    }

    /// Bytes per sample.
    pub const fn byte_len(self) -> usize {
        match self {
            SampleType::U8 | SampleType::I8 => 1,
            SampleType::U16 | SampleType::I16 => 2,
            SampleType::U32 | SampleType::I32 | SampleType::F32 => 4,
            SampleType::F64 => 8,
        }
    }

    /// True for `f32` and `f64`.
    pub const fn is_float(self) -> bool {
        matches!(self, SampleType::F32 | SampleType::F64)
    }

    /// The lower-case Rust name, such as `u16`.
    pub const fn name(self) -> &'static str {
        match self {
            SampleType::U8 => "u8",
            SampleType::I8 => "i8",
            SampleType::U16 => "u16",
            SampleType::I16 => "i16",
            SampleType::U32 => "u32",
            SampleType::I32 => "i32",
            SampleType::F32 => "f32",
            SampleType::F64 => "f64",
        }
    }
}

impl fmt::Display for SampleType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn supported_pairs_map_to_their_type() {
        let cases = [
            (FORMAT_UINT, 8, SampleType::U8),
            (FORMAT_INT, 8, SampleType::I8),
            (FORMAT_INT, 16, SampleType::I16),
            (FORMAT_INT, 32, SampleType::I32),
            (FORMAT_UINT, 32, SampleType::U32),
            (FORMAT_IEEE_FP, 64, SampleType::F64),
        ];
        for (format, bits, want) in cases {
            assert_eq!(SampleType::from_tiff(format, bits).ok(), Some(want));
        }
    }

    #[test]
    fn unsupported_pairs_are_typed_errors() {
        for (format, bits) in [
            (FORMAT_UINT, 64),
            (FORMAT_INT, 64),
            (FORMAT_IEEE_FP, 16),
            (FORMAT_UINT, 1),
            (4, 8),
        ] {
            assert!(matches!(
                SampleType::from_tiff(format, bits),
                Err(RasterError::UnsupportedSampleType { .. })
            ));
        }
    }
}
