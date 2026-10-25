// SPDX-License-Identifier: Apache-2.0

//! Byte ranges of source objects.
//!
//! A source holds one or more objects: one file, or the files of a
//! multi-file format. A reader names the bytes it needs as half-open
//! ranges of an object, and the caller fetches them over any transport.

/// One object of a source. A single-file source has object 0.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ObjectId(pub u32);

/// The half-open byte range `start..end` of `object`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ByteRange {
    pub object: ObjectId,
    pub start: u64,
    pub end: u64,
}

/// Ranges of one object closer than this many bytes merge into one fetch.
pub const COALESCE_GAP: u64 = 64 * 1024;

impl ByteRange {
    /// The range `start..end` of `object`.
    pub const fn new(object: ObjectId, start: u64, end: u64) -> Self {
        Self { object, start, end }
    }

    /// Bytes in the range. An inverted range is empty.
    pub const fn len(&self) -> u64 {
        self.end.saturating_sub(self.start)
    }

    /// True when the range holds no byte.
    pub const fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Sort `ranges` and merge those of one object that overlap or lie within
/// `gap` bytes of each other. Empty ranges are dropped.
pub fn coalesce(mut ranges: Vec<ByteRange>, gap: u64) -> Vec<ByteRange> {
    ranges.retain(|range| !range.is_empty());
    ranges.sort_by_key(|range| (range.object, range.start, range.end));
    let mut merged: Vec<ByteRange> = Vec::with_capacity(ranges.len());
    for range in ranges {
        match merged.last_mut() {
            Some(last)
                if last.object == range.object && range.start <= last.end.saturating_add(gap) =>
            {
                last.end = last.end.max(range.end);
            }
            Some(_) | None => merged.push(range),
        }
    }
    merged
}

#[cfg(test)]
mod tests {
    use super::*;

    fn r(object: u32, start: u64, end: u64) -> ByteRange {
        ByteRange::new(ObjectId(object), start, end)
    }

    #[test]
    fn close_ranges_of_one_object_merge() {
        let merged = coalesce(
            vec![r(0, 200_000, 200_100), r(0, 0, 10), r(0, 100, 50_000)],
            COALESCE_GAP,
        );
        assert_eq!(merged, vec![r(0, 0, 50_000), r(0, 200_000, 200_100)]);
    }

    #[test]
    fn objects_never_merge_and_empty_ranges_drop() {
        let merged = coalesce(vec![r(1, 0, 4), r(0, 0, 4), r(0, 9, 9)], COALESCE_GAP);
        assert_eq!(merged, vec![r(0, 0, 4), r(1, 0, 4)]);
    }

    #[test]
    fn overlaps_merge_to_the_union() {
        let merged = coalesce(vec![r(0, 0, 100), r(0, 50, 80), r(0, 90, 150)], 0);
        assert_eq!(merged, vec![r(0, 0, 150)]);
        assert_eq!(r(0, 9, 3).len(), 0);
    }
}
