// SPDX-License-Identifier: Apache-2.0

//! One IFD: its entry table and the link to the next IFD.

use crate::limits::checked_raster_capacity;
use crate::tiff::byte_order::array;
use crate::tiff::error::TiffFormatError;
use crate::tiff::file_header::Format;
use crate::tiff::tags::FieldType;

/// Most entries one IFD may declare. Real images use a few dozen.
pub const MAX_IFD_ENTRIES: u64 = 4096;

/// Where an entry's value bytes are.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ValueLocation {
    /// In the entry itself: the first `len` bytes of `raw`.
    Inline { raw: [u8; 8], len: usize },
    /// `len` bytes at file offset `offset`.
    At { offset: u64, len: u64 },
}

/// One IFD entry of a known field type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Entry {
    pub tag: u16,
    pub kind: FieldType,
    pub count: u64,
    pub location: ValueLocation,
}

impl Entry {
    /// The value's byte length.
    pub fn byte_len(&self) -> u64 {
        match self.location {
            ValueLocation::Inline { len, .. } => len as u64,
            ValueLocation::At { len, .. } => len,
        }
    }
}

/// A parsed IFD. Entries of unknown field type are skipped.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Ifd {
    pub offset: u64,
    pub entries: Vec<Entry>,
    pub next: u64,
}

impl Ifd {
    /// The entry of `tag`, when present.
    pub fn entry(&self, tag: u16) -> Option<&Entry> {
        self.entries.iter().find(|entry| entry.tag == tag)
    }
}

/// The entry count of the IFD whose first bytes are `head`.
pub fn entry_count(format: Format, offset: u64, head: &[u8]) -> Result<u64, TiffFormatError> {
    let truncated = TiffFormatError::Truncated {
        offset,
        len: format.count_len(),
        end: offset.saturating_add(head.len() as u64),
    };
    let count = if format.big {
        format.order.u64(array::<8>(head, 0).ok_or(truncated)?)
    } else {
        u64::from(format.order.u16(array::<2>(head, 0).ok_or(truncated)?))
    };
    if count > MAX_IFD_ENTRIES {
        return Err(TiffFormatError::TooManyEntries {
            offset,
            count,
            limit: MAX_IFD_ENTRIES,
        });
    }
    Ok(count)
}

/// Parse the IFD at file offset `offset` from `bytes`, which start at that
/// offset and hold the whole IFD.
pub fn parse_ifd(format: Format, offset: u64, bytes: &[u8]) -> Result<Ifd, TiffFormatError> {
    let count = entry_count(format, offset, bytes)?;
    let total = format
        .ifd_len(count)
        .ok_or(TiffFormatError::TooManyEntries {
            offset,
            count,
            limit: MAX_IFD_ENTRIES,
        })?;
    if (bytes.len() as u64) < total {
        return Err(TiffFormatError::Truncated {
            offset,
            len: total,
            end: offset.saturating_add(bytes.len() as u64),
        });
    }
    // `count` is at most MAX_IFD_ENTRIES, so these fit any usize.
    let entry_len = format.entry_len() as usize;
    let first = format.count_len() as usize;
    let capacity = checked_raster_capacity(count as usize)?;
    let mut entries = Vec::with_capacity(capacity);
    for index in 0..count as usize {
        let at = first + index * entry_len;
        if let Some(entry) = parse_entry(format, bytes, at)? {
            entries.push(entry);
        }
    }
    let next_at = first + count as usize * entry_len;
    let next = if format.big {
        array::<8>(bytes, next_at).map(|raw| format.order.u64(raw))
    } else {
        array::<4>(bytes, next_at).map(|raw| u64::from(format.order.u32(raw)))
    }
    .unwrap_or(0);
    Ok(Ifd {
        offset,
        entries,
        next,
    })
}

/// The entry at `at`, or `None` for an unknown field type.
fn parse_entry(format: Format, bytes: &[u8], at: usize) -> Result<Option<Entry>, TiffFormatError> {
    let order = format.order;
    let short = |off: usize| array::<2>(bytes, at + off).map(|raw| order.u16(raw));
    let (Some(tag), Some(code)) = (short(0), short(2)) else {
        return Ok(None);
    };
    let (count, value_at) = if format.big {
        let count = array::<8>(bytes, at + 4).map(|raw| order.u64(raw));
        (count, at + 12)
    } else {
        let count = array::<4>(bytes, at + 4).map(|raw| u64::from(order.u32(raw)));
        (count, at + 8)
    };
    let inline_len = format.offset_len() as usize;
    let (Some(count), Some(field)) = (count, bytes.get(value_at..value_at + inline_len)) else {
        return Ok(None);
    };
    let Some(kind) = FieldType::from_code(code) else {
        return Ok(None);
    };
    let len = count
        .checked_mul(kind.width())
        .ok_or(TiffFormatError::ValueOverflow { tag })?;
    let location = if len <= inline_len as u64 {
        let mut raw = [0u8; 8];
        raw[..inline_len].copy_from_slice(field);
        ValueLocation::Inline {
            raw,
            len: len as usize,
        }
    } else {
        let offset = if format.big {
            array::<8>(field, 0).map(|raw| order.u64(raw))
        } else {
            array::<4>(field, 0).map(|raw| u64::from(order.u32(raw)))
        }
        .unwrap_or(0);
        offset
            .checked_add(len)
            .ok_or(TiffFormatError::ValueOverflow { tag })?;
        ValueLocation::At { offset, len }
    };
    Ok(Some(Entry {
        tag,
        kind,
        count,
        location,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tiff::byte_order::ByteOrder;

    const CLASSIC_LE: Format = Format {
        order: ByteOrder::Little,
        big: false,
    };

    fn entry(tag: u16, kind: u16, count: u32, value: u32) -> Vec<u8> {
        let mut out = tag.to_le_bytes().to_vec();
        out.extend(kind.to_le_bytes());
        out.extend(count.to_le_bytes());
        out.extend(value.to_le_bytes());
        out
    }

    #[test]
    fn entries_parse_inline_and_out_of_line() {
        let mut bytes = 3u16.to_le_bytes().to_vec();
        bytes.extend(entry(256, 3, 1, 640));
        bytes.extend(entry(273, 4, 10, 1000));
        bytes.extend(entry(999, 77, 1, 0));
        bytes.extend(500u32.to_le_bytes());
        let ifd = parse_ifd(CLASSIC_LE, 8, &bytes).expect("parses");
        assert_eq!(ifd.next, 500);
        assert_eq!(ifd.entries.len(), 2, "the unknown type is skipped");
        let width = ifd.entry(256).expect("width");
        assert!(matches!(
            width.location,
            ValueLocation::Inline { len: 2, .. }
        ));
        let offsets = ifd.entry(273).expect("offsets");
        assert_eq!(
            offsets.location,
            ValueLocation::At {
                offset: 1000,
                len: 40
            }
        );
    }

    #[test]
    fn truncated_and_oversized_tables_are_typed_errors() {
        let mut bytes = 2u16.to_le_bytes().to_vec();
        bytes.extend(entry(256, 3, 1, 640));
        assert!(matches!(
            parse_ifd(CLASSIC_LE, 8, &bytes),
            Err(TiffFormatError::Truncated { .. })
        ));
        assert!(matches!(
            entry_count(CLASSIC_LE, 8, &u16::MAX.to_le_bytes()),
            Err(TiffFormatError::TooManyEntries { .. })
        ));
        let big = Format {
            order: ByteOrder::Little,
            big: true,
        };
        let mut huge = 1u64.to_le_bytes().to_vec();
        huge.extend(273u16.to_le_bytes());
        huge.extend(16u16.to_le_bytes());
        huge.extend(u64::MAX.to_le_bytes());
        huge.extend(0u64.to_le_bytes());
        huge.extend(0u64.to_le_bytes());
        assert_eq!(
            parse_ifd(big, 16, &huge),
            Err(TiffFormatError::ValueOverflow { tag: 273 })
        );
    }
}
