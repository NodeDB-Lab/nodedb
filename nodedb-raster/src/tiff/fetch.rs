// SPDX-License-Identifier: Apache-2.0

//! `Fetcher`: reads an open round's bytes from the cache and records the
//! ranges the cache lacks.

use crate::error::RasterError;
use crate::source::cache::{Lookup, RangeCache};
use crate::source::range::{ByteRange, COALESCE_GAP, ObjectId, coalesce};
use crate::tiff::error::TiffFormatError;

/// Smallest range a miss asks for. It covers the header and IFD 0 of a
/// cloud-optimised GeoTIFF, GDAL's ghost area included.
pub const MIN_FETCH: u64 = 64 * 1024;

/// Cache reads of one open round.
#[derive(Debug)]
pub struct Fetcher<'c> {
    cache: &'c RangeCache,
    object: ObjectId,
    need: Vec<ByteRange>,
}

impl<'c> Fetcher<'c> {
    pub fn new(cache: &'c RangeCache, object: ObjectId) -> Self {
        Self {
            cache,
            object,
            need: Vec::new(),
        }
    }

    /// The cache this round reads.
    pub fn cache(&self) -> &'c RangeCache {
        self.cache
    }

    /// The object this round reads.
    pub fn object(&self) -> ObjectId {
        self.object
    }

    /// The `len` bytes at `offset`. `Ok(None)` when the cache lacks them:
    /// the range, widened to [`MIN_FETCH`], is recorded as needed.
    pub fn bytes(&mut self, offset: u64, len: u64) -> Result<Option<&'c [u8]>, RasterError> {
        match self.lookup(offset, len)? {
            Some(bytes) if bytes.len() as u64 == len => Ok(Some(bytes)),
            Some(bytes) => Err(TiffFormatError::Truncated {
                offset,
                len,
                end: offset.saturating_add(bytes.len() as u64),
            }
            .into()),
            None => Ok(None),
        }
    }

    /// Up to `len` bytes at `offset`: fewer only where the object ends.
    pub fn prefix(&mut self, offset: u64, len: u64) -> Result<Option<&'c [u8]>, RasterError> {
        self.lookup(offset, len)
    }

    /// True when this round recorded a needed range.
    pub fn pending(&self) -> bool {
        !self.need.is_empty()
    }

    /// The needed ranges, coalesced.
    pub fn into_need(self) -> Vec<ByteRange> {
        coalesce(self.need, COALESCE_GAP)
    }

    fn lookup(&mut self, offset: u64, len: u64) -> Result<Option<&'c [u8]>, RasterError> {
        let end = offset
            .checked_add(len)
            .ok_or(TiffFormatError::OffsetOverflow { offset, len })?;
        match self.cache.get(ByteRange::new(self.object, offset, end)) {
            Lookup::Bytes(bytes) => Ok(Some(bytes)),
            Lookup::Absent => Err(RasterError::SourceMissing {
                object: self.object.0,
            }),
            Lookup::Missing => {
                let fetch_end = offset.saturating_add(len.max(MIN_FETCH));
                self.need
                    .push(ByteRange::new(self.object, offset, fetch_end));
                Ok(None)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn misses_ask_for_at_least_the_minimum_fetch() {
        let cache = RangeCache::new(1 << 20);
        let mut fetcher = Fetcher::new(&cache, ObjectId(0));
        assert_eq!(fetcher.bytes(0, 16).ok(), Some(None));
        assert_eq!(fetcher.bytes(100, 8).ok(), Some(None));
        assert!(fetcher.pending());
        assert_eq!(
            fetcher.into_need(),
            vec![ByteRange::new(ObjectId(0), 0, 100 + MIN_FETCH)]
        );
    }

    #[test]
    fn hits_and_the_object_end_answer_from_the_cache() {
        let mut cache = RangeCache::new(1 << 20);
        cache
            .insert(ByteRange::new(ObjectId(0), 0, MIN_FETCH), Some(vec![1; 10]))
            .expect("fits");
        let mut fetcher = Fetcher::new(&cache, ObjectId(0));
        assert_eq!(fetcher.bytes(2, 4).ok(), Some(Some(&[1u8; 4][..])));
        assert_eq!(fetcher.prefix(0, 16).ok(), Some(Some(&[1u8; 10][..])));
        assert!(matches!(
            fetcher.bytes(8, 4),
            Err(RasterError::Malformed(TiffFormatError::Truncated {
                end: 10,
                ..
            }))
        ));
        assert!(!fetcher.pending());
    }
}
