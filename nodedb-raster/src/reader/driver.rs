// SPDX-License-Identifier: Apache-2.0

//! A synchronous driver: answer an opener's rounds and a reader's block
//! ranges from a blocking range source.

use crate::block::RasterBlock;
use crate::error::RasterError;
use crate::limits::RasterLimits;
use crate::reader::traits::{Opened, RasterOpener, RasterReader};
use crate::source::cache::{Lookup, RangeCache};
use crate::source::range::{ByteRange, ObjectId};

/// Open rounds before the driver gives up. A COG opens in one or two.
const MAX_OPEN_ROUNDS: usize = 64;

/// A blocking range source.
pub trait RangeRead {
    /// The bytes of `range`, fewer where the object ends, or `None` when
    /// the object does not exist. The readers bound every range they name,
    /// so an implementation can allocate `range.len()` bytes.
    fn read_range(&mut self, range: ByteRange) -> Result<Option<Vec<u8>>, RasterError>;
}

/// An in-memory source: one object, the whole slice.
#[derive(Debug, Clone, Copy)]
pub struct SliceSource<'a> {
    object: ObjectId,
    bytes: &'a [u8],
}

impl<'a> SliceSource<'a> {
    /// `bytes` as `object`.
    pub const fn new(object: ObjectId, bytes: &'a [u8]) -> Self {
        Self { object, bytes }
    }
}

impl RangeRead for SliceSource<'_> {
    fn read_range(&mut self, range: ByteRange) -> Result<Option<Vec<u8>>, RasterError> {
        if range.object != self.object {
            return Ok(None);
        }
        let len = self.bytes.len();
        let start = usize::try_from(range.start).map_or(len, |s| s.min(len));
        let end = usize::try_from(range.end).map_or(len, |e| e.min(len));
        Ok(Some(
            self.bytes
                .get(start..end.max(start))
                .unwrap_or_default()
                .to_vec(),
        ))
    }
}

/// Run `opener` to completion, fetching each round's ranges from `source`.
pub fn open_with<O, S>(
    opener: &O,
    source: &mut S,
    limits: RasterLimits,
) -> Result<O::Reader, RasterError>
where
    O: RasterOpener,
    S: RangeRead + ?Sized,
{
    let mut cache = RangeCache::new(limits.header_bytes);
    for _ in 0..MAX_OPEN_ROUNDS {
        match opener.open(&cache, limits)? {
            Opened::Ready(reader) => return Ok(reader),
            Opened::Need(ranges) => {
                let mut fetched = false;
                for range in ranges {
                    if matches!(cache.get(range), Lookup::Missing) {
                        let bytes = source.read_range(range)?;
                        cache.insert(range, bytes)?;
                        fetched = true;
                    }
                }
                if !fetched {
                    return Err(RasterError::OpenStalled {
                        rounds: MAX_OPEN_ROUNDS,
                    });
                }
            }
        }
    }
    Err(RasterError::OpenStalled {
        rounds: MAX_OPEN_ROUNDS,
    })
}

/// Fetch the ranges of block `index` from `source` and decode it.
pub fn read_block_with<R, S>(
    reader: &R,
    index: u32,
    source: &mut S,
) -> Result<RasterBlock, RasterError>
where
    R: RasterReader + ?Sized,
    S: RangeRead + ?Sized,
{
    let ranges = reader.block_ranges(index)?;
    let mut owned: Vec<Option<Vec<u8>>> = Vec::with_capacity(ranges.len());
    for range in &ranges {
        owned.push(if range.is_empty() {
            Some(Vec::new())
        } else {
            source.read_range(*range)?
        });
    }
    let borrowed: Vec<Option<&[u8]>> = owned.iter().map(Option::as_deref).collect();
    reader.decode_block(index, &borrowed)
}

/// Open `bytes`, a whole single-object source, with `opener`.
pub fn open_all<O: RasterOpener>(
    opener: &O,
    bytes: &[u8],
    limits: RasterLimits,
) -> Result<O::Reader, RasterError> {
    open_with(opener, &mut SliceSource::new(ObjectId(0), bytes), limits)
}

/// Decode block `index` of a reader opened over `bytes`.
pub fn read_block_all<R: RasterReader + ?Sized>(
    reader: &R,
    index: u32,
    bytes: &[u8],
) -> Result<RasterBlock, RasterError> {
    read_block_with(reader, index, &mut SliceSource::new(ObjectId(0), bytes))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_slice_source_clips_at_the_end_and_knows_one_object() {
        let mut source = SliceSource::new(ObjectId(0), &[1, 2, 3, 4]);
        let read = |source: &mut SliceSource<'_>, object, start, end| {
            source
                .read_range(ByteRange::new(ObjectId(object), start, end))
                .expect("reads")
        };
        assert_eq!(read(&mut source, 0, 1, 3), Some(vec![2, 3]));
        assert_eq!(read(&mut source, 0, 2, 100), Some(vec![3, 4]));
        assert_eq!(read(&mut source, 0, 9, 12), Some(vec![]));
        assert_eq!(read(&mut source, 1, 0, 2), None);
    }
}
