// SPDX-License-Identifier: Apache-2.0

//! The block index: `StripOffsets`/`TileOffsets` and their byte counts,
//! kept as the file stores them and decoded per lookup.

use crate::error::RasterError;
use crate::tiff::byte_order::ByteOrder;
use crate::tiff::error::TiffFormatError;
use crate::tiff::tags::FieldType;
use crate::tiff::values::TagValues;

/// One offset or byte count array.
#[derive(Debug, Clone, PartialEq, Eq)]
struct ChunkTable {
    bytes: Vec<u8>,
    width: usize,
    order: ByteOrder,
}

impl ChunkTable {
    fn read(
        values: &TagValues<'_>,
        tag: u16,
        name: &'static str,
        chunks: u64,
    ) -> Result<Self, RasterError> {
        let (entry, bytes) = values
            .raw(tag)?
            .ok_or(TiffFormatError::MissingTag { tag, name })?;
        let width = match entry.kind {
            FieldType::Short => 2,
            FieldType::Long | FieldType::Ifd => 4,
            FieldType::Long8 | FieldType::Ifd8 => 8,
            FieldType::Byte
            | FieldType::Ascii
            | FieldType::Rational
            | FieldType::SByte
            | FieldType::Undefined
            | FieldType::SShort
            | FieldType::SLong
            | FieldType::SRational
            | FieldType::Float
            | FieldType::Double
            | FieldType::SLong8 => {
                return Err(TiffFormatError::FieldType {
                    tag,
                    kind: entry.kind.code(),
                }
                .into());
            }
        };
        if entry.count < chunks {
            return Err(TiffFormatError::TagCount {
                tag,
                found: entry.count,
                expected: chunks,
            }
            .into());
        }
        Ok(Self {
            bytes: bytes.into_owned(),
            width,
            order: values.order,
        })
    }

    fn get(&self, index: u64) -> Option<u64> {
        let at = usize::try_from(index).ok()?.checked_mul(self.width)?;
        self.order.uint_at(&self.bytes, at, self.width)
    }
}

/// Where each chunk lies in the file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChunkIndex {
    offsets: ChunkTable,
    counts: ChunkTable,
    chunks: u64,
}

/// One chunk's file bytes. A zero offset or length marks a chunk the file
/// omits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChunkSpan {
    pub offset: u64,
    pub len: u64,
}

impl ChunkSpan {
    /// True for a chunk the file omits.
    pub const fn is_sparse(&self) -> bool {
        self.offset == 0 || self.len == 0
    }
}

impl ChunkIndex {
    /// Read the index of `chunks` chunks from the offset and byte count
    /// tags.
    pub fn read(
        values: &TagValues<'_>,
        (offsets_tag, offsets_name): (u16, &'static str),
        (counts_tag, counts_name): (u16, &'static str),
        chunks: u64,
    ) -> Result<Self, RasterError> {
        Ok(Self {
            offsets: ChunkTable::read(values, offsets_tag, offsets_name, chunks)?,
            counts: ChunkTable::read(values, counts_tag, counts_name, chunks)?,
            chunks,
        })
    }

    /// The file bytes of chunk `chunk`.
    pub fn span(&self, chunk: u64) -> Result<ChunkSpan, RasterError> {
        let lookup = |table: &ChunkTable| {
            table.get(chunk).ok_or(TiffFormatError::TagCount {
                tag: 0,
                found: chunk,
                expected: self.chunks,
            })
        };
        let offset = lookup(&self.offsets)?;
        let len = lookup(&self.counts)?;
        if offset.checked_add(len).is_none() {
            return Err(TiffFormatError::ChunkOverflow { chunk, offset, len }.into());
        }
        Ok(ChunkSpan { offset, len })
    }
}
