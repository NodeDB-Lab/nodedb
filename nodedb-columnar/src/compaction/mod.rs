//! Segment compaction: merge segments, drop deleted rows, re-encode.
//!
//! Compaction reads one or more source segments along with their delete
//! bitmaps, filters out deleted rows, and writes a new compacted segment.
//! The caller is responsible for the atomic metadata swap (WAL commit marker
//! → swap segment references → delete old files).
//!
//! Triggered when a segment's delete ratio exceeds the threshold (default 20%)
//! or when the segment count exceeds a limit.

pub mod extract;
pub mod segment;
pub mod segments;

#[cfg(test)]
mod tests;

pub use segment::{CompactionResult, DEFAULT_DELETE_RATIO_THRESHOLD, compact_segment};
pub use segments::compact_segments;
