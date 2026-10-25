// SPDX-License-Identifier: Apache-2.0

//! `RangeCache`: the fetched ranges an open reads from, under a byte cap.
//!
//! An entry shorter than its range marks the end of its object: a lookup
//! past that end answers with the bytes up to the end, possibly none. An
//! entry without bytes marks a missing object.

use crate::error::RasterError;
use crate::source::range::ByteRange;

/// The answer of a cache lookup.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Lookup<'a> {
    /// The bytes of the range. Shorter than the range only where the
    /// object ends.
    Bytes(&'a [u8]),
    /// The object does not exist.
    Absent,
    /// The cache does not hold the range yet.
    Missing,
}

#[derive(Debug)]
struct Entry {
    range: ByteRange,
    bytes: Option<Vec<u8>>,
}

/// Fetched ranges, bounded by a byte cap.
#[derive(Debug)]
pub struct RangeCache {
    entries: Vec<Entry>,
    held_bytes: usize,
    limit_bytes: usize,
}

impl RangeCache {
    /// An empty cache that holds at most `limit_bytes` bytes.
    pub fn new(limit_bytes: usize) -> Self {
        Self {
            entries: Vec::new(),
            held_bytes: 0,
            limit_bytes,
        }
    }

    /// Store the answer for `range`: its bytes, or `None` for a missing
    /// object. Fewer bytes than the range marks the end of the object.
    pub fn insert(&mut self, range: ByteRange, bytes: Option<Vec<u8>>) -> Result<(), RasterError> {
        if let Some(data) = &bytes {
            if data.len() as u64 > range.len() {
                return Err(RasterError::RangeOverrun {
                    range,
                    len: range.len(),
                    got: data.len(),
                });
            }
            let held = self
                .held_bytes
                .checked_add(data.len())
                .filter(|held| *held <= self.limit_bytes)
                .ok_or(RasterError::HeaderTooLarge {
                    limit_bytes: self.limit_bytes,
                })?;
            self.held_bytes = held;
        }
        self.entries.push(Entry { range, bytes });
        Ok(())
    }

    /// The cached bytes of `range`.
    pub fn get(&self, range: ByteRange) -> Lookup<'_> {
        for entry in &self.entries {
            if entry.range.object != range.object {
                continue;
            }
            let Some(data) = &entry.bytes else {
                return Lookup::Absent;
            };
            if entry.range.start > range.start {
                continue;
            }
            let held = data.len() as u64;
            let at_end = held < entry.range.len();
            let available_end = entry.range.start.saturating_add(held);
            let skip = range.start - entry.range.start;
            if range.end <= available_end {
                let (Ok(from), Ok(to)) = (
                    usize::try_from(skip),
                    usize::try_from(range.end - entry.range.start),
                ) else {
                    continue;
                };
                if let Some(bytes) = data.get(from..to.max(from)) {
                    return Lookup::Bytes(bytes);
                }
            } else if at_end {
                let from = usize::try_from(skip.min(held)).unwrap_or(data.len());
                return Lookup::Bytes(data.get(from..).unwrap_or_default());
            }
        }
        Lookup::Missing
    }

    /// Bytes the cache holds.
    pub fn held_bytes(&self) -> usize {
        self.held_bytes
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::source::range::ObjectId;

    fn r(start: u64, end: u64) -> ByteRange {
        ByteRange::new(ObjectId(0), start, end)
    }

    #[test]
    fn covered_ranges_hit_and_others_miss() {
        let mut cache = RangeCache::new(1024);
        cache
            .insert(r(10, 20), Some((10..20).collect()))
            .expect("fits");
        assert_eq!(cache.get(r(12, 15)), Lookup::Bytes(&[12, 13, 14]));
        assert_eq!(
            cache.get(r(10, 20)),
            Lookup::Bytes(&[10, 11, 12, 13, 14, 15, 16, 17, 18, 19])
        );
        assert_eq!(cache.get(r(5, 12)), Lookup::Missing);
        assert_eq!(cache.get(r(15, 25)), Lookup::Missing);
        assert_eq!(
            cache.get(ByteRange::new(ObjectId(1), 10, 12)),
            Lookup::Missing
        );
    }

    #[test]
    fn a_short_entry_marks_the_object_end() {
        let mut cache = RangeCache::new(1024);
        cache.insert(r(0, 100), Some(vec![7; 8])).expect("fits");
        assert_eq!(cache.get(r(4, 50)), Lookup::Bytes(&[7; 4]));
        assert_eq!(cache.get(r(60, 70)), Lookup::Bytes(&[]));
    }

    #[test]
    fn a_missing_object_is_absent() {
        let mut cache = RangeCache::new(1024);
        cache
            .insert(ByteRange::new(ObjectId(3), 0, 10), None)
            .expect("fits");
        assert_eq!(cache.get(ByteRange::new(ObjectId(3), 4, 8)), Lookup::Absent);
    }

    #[test]
    fn the_byte_cap_and_overruns_are_typed_errors() {
        let mut cache = RangeCache::new(10);
        cache.insert(r(0, 8), Some(vec![0; 8])).expect("fits");
        assert!(matches!(
            cache.insert(r(8, 16), Some(vec![0; 8])),
            Err(RasterError::HeaderTooLarge { limit_bytes: 10 })
        ));
        assert!(matches!(
            cache.insert(r(0, 2), Some(vec![0; 3])),
            Err(RasterError::RangeOverrun { got: 3, .. })
        ));
        assert_eq!(cache.held_bytes(), 8);
    }
}
