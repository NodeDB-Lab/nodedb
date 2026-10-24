// SPDX-License-Identifier: Apache-2.0

//! Typed tag values of one IFD, read from the open cache.
//!
//! An open round first calls [`prefetch`] for the tags it reads, so every
//! out-of-line value is cached before [`TagValues`] decodes it. Each value
//! is bounded: the block index by the header cap, any other tag by the tag
//! value cap.

use std::borrow::Cow;

use crate::error::RasterError;
use crate::limits::MAX_TAG_VALUE_BYTES;
use crate::source::cache::{Lookup, RangeCache};
use crate::source::range::{ByteRange, ObjectId};
use crate::tiff::byte_order::ByteOrder;
use crate::tiff::error::TiffFormatError;
use crate::tiff::fetch::Fetcher;
use crate::tiff::ifd::{Entry, Ifd, ValueLocation};
use crate::tiff::tags::{FieldType, is_block_index};

/// The byte cap of `tag`'s value.
fn value_cap(tag: u16, header_bytes: usize) -> usize {
    if is_block_index(tag) {
        header_bytes
    } else {
        MAX_TAG_VALUE_BYTES
    }
}

/// Refuse a value over its cap.
fn check_cap(entry: &Entry, header_bytes: usize) -> Result<(), RasterError> {
    let limit_bytes = value_cap(entry.tag, header_bytes);
    if entry.byte_len() > limit_bytes as u64 {
        return Err(RasterError::TagValueTooLarge {
            tag: entry.tag,
            bytes: entry.byte_len(),
            limit_bytes,
        });
    }
    Ok(())
}

/// Record every out-of-line value of `tags` in `ifd` the cache lacks.
pub fn prefetch(
    ifd: &Ifd,
    tags: &[u16],
    fetcher: &mut Fetcher<'_>,
    header_bytes: usize,
) -> Result<(), RasterError> {
    for &tag in tags {
        let Some(entry) = ifd.entry(tag) else {
            continue;
        };
        check_cap(entry, header_bytes)?;
        if let ValueLocation::At { offset, len } = entry.location {
            fetcher.bytes(offset, len)?;
        }
    }
    Ok(())
}

/// An entry and its value bytes.
pub type RawValue<'a> = (Entry, Cow<'a, [u8]>);

/// Typed access to the cached tag values of one IFD.
#[derive(Debug, Clone, Copy)]
pub struct TagValues<'a> {
    pub ifd: &'a Ifd,
    pub cache: &'a RangeCache,
    pub object: ObjectId,
    pub order: ByteOrder,
    pub header_bytes: usize,
}

impl<'a> TagValues<'a> {
    /// The value bytes of `tag`, when present.
    pub fn raw(&self, tag: u16) -> Result<Option<RawValue<'a>>, RasterError> {
        let Some(entry) = self.ifd.entry(tag).copied() else {
            return Ok(None);
        };
        check_cap(&entry, self.header_bytes)?;
        let bytes = match entry.location {
            ValueLocation::Inline { raw, len } => {
                Cow::Owned(raw.get(..len).unwrap_or_default().to_vec())
            }
            ValueLocation::At { offset, len } => {
                let range = ByteRange::new(self.object, offset, offset.saturating_add(len));
                match self.cache.get(range) {
                    Lookup::Bytes(bytes) if bytes.len() as u64 == len => Cow::Borrowed(bytes),
                    Lookup::Bytes(bytes) => {
                        return Err(TiffFormatError::Truncated {
                            offset,
                            len,
                            end: offset.saturating_add(bytes.len() as u64),
                        }
                        .into());
                    }
                    Lookup::Absent => {
                        return Err(RasterError::SourceMissing {
                            object: self.object.0,
                        });
                    }
                    Lookup::Missing => return Err(RasterError::OpenStalled { rounds: 0 }),
                }
            }
        };
        Ok(Some((entry, bytes)))
    }

    /// The unsigned integers of `tag` (BYTE, SHORT, LONG, LONG8, IFD,
    /// IFD8), when present.
    pub fn uints(&self, tag: u16) -> Result<Option<Vec<u64>>, RasterError> {
        let Some((entry, bytes)) = self.raw(tag)? else {
            return Ok(None);
        };
        let width = match entry.kind {
            FieldType::Byte | FieldType::Undefined => 1,
            FieldType::Short => 2,
            FieldType::Long | FieldType::Ifd => 4,
            FieldType::Long8 | FieldType::Ifd8 => 8,
            FieldType::Ascii
            | FieldType::Rational
            | FieldType::SByte
            | FieldType::SShort
            | FieldType::SLong
            | FieldType::SRational
            | FieldType::Float
            | FieldType::Double
            | FieldType::SLong8 => return Err(field_type(&entry)),
        };
        let values = bytes
            .chunks_exact(width)
            .filter_map(|raw| self.order.uint_at(raw, 0, width))
            .collect();
        Ok(Some(values))
    }

    /// The first unsigned integer of `tag`, when present.
    pub fn uint(&self, tag: u16) -> Result<Option<u64>, RasterError> {
        match self.uints(tag)? {
            None => Ok(None),
            Some(values) => values.first().copied().map(Some).ok_or(
                TiffFormatError::TagCount {
                    tag,
                    found: 0,
                    expected: 1,
                }
                .into(),
            ),
        }
    }

    /// The first value of `tag` as a u16, refusing larger values.
    pub fn u16_value(&self, tag: u16) -> Result<Option<u16>, RasterError> {
        self.uint(tag)?
            .map(|value| {
                u16::try_from(value).map_err(|_| TiffFormatError::TagValue { tag, value }.into())
            })
            .transpose()
    }

    /// The SHORT values of `tag`, when present.
    pub fn u16s(&self, tag: u16) -> Result<Option<Vec<u16>>, RasterError> {
        let Some((entry, bytes)) = self.raw(tag)? else {
            return Ok(None);
        };
        if entry.kind != FieldType::Short {
            return Err(field_type(&entry));
        }
        let values = bytes
            .as_chunks::<2>()
            .0
            .iter()
            .map(|raw| self.order.u16(*raw))
            .collect();
        Ok(Some(values))
    }

    /// The numbers of `tag` as doubles (DOUBLE, FLOAT, SHORT, LONG), when
    /// present.
    pub fn f64s(&self, tag: u16) -> Result<Option<Vec<f64>>, RasterError> {
        let Some((entry, bytes)) = self.raw(tag)? else {
            return Ok(None);
        };
        let order = self.order;
        let values = match entry.kind {
            FieldType::Double => bytes
                .as_chunks::<8>()
                .0
                .iter()
                .map(|raw| f64::from_bits(order.u64(*raw)))
                .collect(),
            FieldType::Float => bytes
                .as_chunks::<4>()
                .0
                .iter()
                .map(|raw| f64::from(f32::from_bits(order.u32(*raw))))
                .collect(),
            FieldType::Short | FieldType::Long => {
                let width = entry.kind.width() as usize;
                bytes
                    .chunks_exact(width)
                    .filter_map(|raw| order.uint_at(raw, 0, width))
                    .map(|value| value as f64)
                    .collect()
            }
            FieldType::Byte
            | FieldType::Ascii
            | FieldType::Rational
            | FieldType::SByte
            | FieldType::Undefined
            | FieldType::SShort
            | FieldType::SLong
            | FieldType::SRational
            | FieldType::Ifd
            | FieldType::Long8
            | FieldType::SLong8
            | FieldType::Ifd8 => return Err(field_type(&entry)),
        };
        Ok(Some(values))
    }

    /// The ASCII text of `tag` without its NUL terminators, when present.
    pub fn ascii(&self, tag: u16) -> Result<Option<String>, RasterError> {
        let Some((entry, bytes)) = self.raw(tag)? else {
            return Ok(None);
        };
        if !matches!(
            entry.kind,
            FieldType::Ascii | FieldType::Byte | FieldType::Undefined
        ) {
            return Err(field_type(&entry));
        }
        let text = std::str::from_utf8(&bytes)
            .map_err(|_| TiffFormatError::AsciiEncoding { tag })?
            .trim_end_matches('\0');
        Ok(Some(text.to_owned()))
    }
}

fn field_type(entry: &Entry) -> RasterError {
    TiffFormatError::FieldType {
        tag: entry.tag,
        kind: entry.kind.code(),
    }
    .into()
}
