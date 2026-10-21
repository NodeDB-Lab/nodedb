// SPDX-License-Identifier: Apache-2.0

//! The TIFF file header: byte order, classic or BigTIFF, first IFD.

use crate::tiff::byte_order::{ByteOrder, array};
use crate::tiff::error::TiffFormatError;

const CLASSIC_MAGIC: u16 = 42;
const BIG_MAGIC: u16 = 43;
/// Bytes a BigTIFF header occupies; a classic header is 8.
pub const MAX_HEADER_LEN: u64 = 16;

/// Classic TIFF or BigTIFF, in one byte order.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Format {
    pub order: ByteOrder,
    pub big: bool,
}

impl Format {
    /// Bytes of one IFD entry.
    pub const fn entry_len(self) -> u64 {
        if self.big { 20 } else { 12 }
    }

    /// Bytes of the entry count that opens an IFD.
    pub const fn count_len(self) -> u64 {
        if self.big { 8 } else { 2 }
    }

    /// Bytes of an offset: the next-IFD link and an out-of-line value.
    pub const fn offset_len(self) -> u64 {
        if self.big { 8 } else { 4 }
    }

    /// Bytes of an IFD with `count` entries, or `None` on overflow.
    pub const fn ifd_len(self, count: u64) -> Option<u64> {
        match count.checked_mul(self.entry_len()) {
            Some(entries) => entries.checked_add(self.count_len() + self.offset_len()),
            None => None,
        }
    }
}

/// The parsed file header.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FileHeader {
    pub format: Format,
    pub first_ifd: u64,
}

/// Parse the header at the start of `bytes`. `bytes` holds the first
/// [`MAX_HEADER_LEN`] bytes of the file, fewer for a shorter file.
pub fn parse_header(bytes: &[u8]) -> Result<FileHeader, TiffFormatError> {
    let truncated = |len| TiffFormatError::Truncated {
        offset: 0,
        len,
        end: bytes.len() as u64,
    };
    let order = match bytes.get(..2) {
        Some(b"II") => ByteOrder::Little,
        Some(b"MM") => ByteOrder::Big,
        Some(_) => return Err(TiffFormatError::NotTiff),
        None => return Err(truncated(8)),
    };
    let magic = array::<2>(bytes, 2)
        .map(|raw| order.u16(raw))
        .ok_or(truncated(8))?;
    match magic {
        CLASSIC_MAGIC => {
            let first = array::<4>(bytes, 4).ok_or(truncated(8))?;
            Ok(FileHeader {
                format: Format { order, big: false },
                first_ifd: u64::from(order.u32(first)),
            })
        }
        BIG_MAGIC => {
            let raw = array::<2>(bytes, 4).ok_or(truncated(MAX_HEADER_LEN))?;
            let offset_size = order.u16(raw);
            if offset_size != 8 {
                return Err(TiffFormatError::BigTiffOffsetSize { offset_size });
            }
            let first = array::<8>(bytes, 8).ok_or(truncated(MAX_HEADER_LEN))?;
            Ok(FileHeader {
                format: Format { order, big: true },
                first_ifd: order.u64(first),
            })
        }
        _ => Err(TiffFormatError::NotTiff),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classic_and_big_headers_parse_in_both_orders() {
        let classic = parse_header(b"II*\0\x08\0\0\0").expect("classic");
        assert_eq!(classic.first_ifd, 8);
        assert!(!classic.format.big);
        let big = parse_header(b"MM\0+\0\x08\0\0\0\0\0\0\0\0\0\x10").expect("big");
        assert_eq!(big.first_ifd, 16);
        assert_eq!(big.format.order, ByteOrder::Big);
        assert!(big.format.big);
        assert_eq!(big.format.ifd_len(2), Some(8 + 40 + 8));
    }

    #[test]
    fn garbage_and_short_headers_are_typed_errors() {
        assert_eq!(parse_header(b"hello"), Err(TiffFormatError::NotTiff));
        assert!(matches!(
            parse_header(b"II*"),
            Err(TiffFormatError::Truncated { .. })
        ));
        assert!(matches!(
            parse_header(b""),
            Err(TiffFormatError::Truncated { .. })
        ));
        assert_eq!(
            parse_header(b"II+\0\x04\0\0\0"),
            Err(TiffFormatError::BigTiffOffsetSize { offset_size: 4 })
        );
        assert_eq!(parse_header(b"II\x2b\x01"), Err(TiffFormatError::NotTiff));
    }
}
