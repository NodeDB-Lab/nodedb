// SPDX-License-Identifier: Apache-2.0

//! The IFD chain: every IFD's offset, size and subfile kind.

use crate::error::RasterError;
use crate::tiff::error::TiffFormatError;
use crate::tiff::fetch::Fetcher;
use crate::tiff::file_header::Format;
use crate::tiff::ifd::{Ifd, entry_count, parse_ifd};
use crate::tiff::tags::{
    IMAGE_LENGTH, IMAGE_WIDTH, NEW_SUBFILE_TYPE, OLD_SUBFILE_REDUCED, SUBFILE_MASK,
    SUBFILE_REDUCED, SUBFILE_TYPE,
};
use crate::tiff::values::{TagValues, prefetch};

/// Most IFDs a chain may hold. A COG holds two per overview level.
pub const MAX_IFDS: usize = 256;

/// The tags the chain walk reads from every IFD.
const SUMMARY_TAGS: [u16; 4] = [NEW_SUBFILE_TYPE, SUBFILE_TYPE, IMAGE_WIDTH, IMAGE_LENGTH];

/// One IFD of the chain.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IfdSummary {
    pub offset: u64,
    pub width: u64,
    pub height: u64,
    /// A reduced-resolution image: an overview.
    pub reduced: bool,
    /// A transparency mask.
    pub mask: bool,
}

/// The full IFD at `offset`, or `None` when this round lacks its bytes.
pub fn read_ifd(
    fetcher: &mut Fetcher<'_>,
    format: Format,
    offset: u64,
) -> Result<Option<Ifd>, RasterError> {
    let Some(head) = fetcher.bytes(offset, format.count_len())? else {
        return Ok(None);
    };
    let count = entry_count(format, offset, head)?;
    let len = format
        .ifd_len(count)
        .ok_or(TiffFormatError::OffsetOverflow {
            offset,
            len: u64::MAX,
        })?;
    let Some(body) = fetcher.bytes(offset, len)? else {
        return Ok(None);
    };
    Ok(Some(parse_ifd(format, offset, body)?))
}

/// Walk the chain from `first`. `None` when this round lacks the bytes of
/// an IFD or of a summary tag value.
pub fn walk(
    fetcher: &mut Fetcher<'_>,
    format: Format,
    first: u64,
    header_bytes: usize,
) -> Result<Option<Vec<IfdSummary>>, RasterError> {
    let mut chain: Vec<IfdSummary> = Vec::new();
    let mut offset = first;
    while offset != 0 {
        if chain.iter().any(|seen| seen.offset == offset) {
            return Err(TiffFormatError::IfdLoop { offset }.into());
        }
        if chain.len() >= MAX_IFDS {
            return Err(TiffFormatError::TooManyIfds { limit: MAX_IFDS }.into());
        }
        let Some(ifd) = read_ifd(fetcher, format, offset)? else {
            return Ok(None);
        };
        prefetch(&ifd, &SUMMARY_TAGS, fetcher, header_bytes)?;
        if fetcher.pending() {
            return Ok(None);
        }
        let values = TagValues {
            ifd: &ifd,
            cache: fetcher.cache(),
            object: fetcher.object(),
            order: format.order,
            header_bytes,
        };
        let subfile = values.uint(NEW_SUBFILE_TYPE)?.unwrap_or(0);
        let old_subfile = values.uint(SUBFILE_TYPE)?.unwrap_or(0);
        chain.push(IfdSummary {
            offset,
            width: values.uint(IMAGE_WIDTH)?.unwrap_or(0),
            height: values.uint(IMAGE_LENGTH)?.unwrap_or(0),
            reduced: subfile & SUBFILE_REDUCED != 0 || old_subfile == OLD_SUBFILE_REDUCED,
            mask: subfile & SUBFILE_MASK != 0,
        });
        offset = ifd.next;
    }
    Ok(Some(chain))
}
