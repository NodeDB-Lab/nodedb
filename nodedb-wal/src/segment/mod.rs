// SPDX-License-Identifier: Apache-2.0

//! WAL segment management.
//!
//! The WAL is split into fixed-size segment files for efficient truncation.
//! Each segment is a standalone WAL file containing records within an LSN range.
//!
//! ## Naming convention
//!
//! Segments are named `wal-{first_lsn:020}.seg` — zero-padded for lexicographic
//! ordering. This guarantees `ls` and `readdir` return segments in LSN order.
//!
//! ## Lifecycle
//!
//! 1. Writer creates a new segment when the current segment exceeds `target_size`.
//! 2. The active segment is the one being appended to.
//! 3. `truncate_before(lsn)` deletes all sealed segments whose `max_lsn < lsn`.
//! 4. The active segment is NEVER deleted — only sealed (closed) segments are eligible.
//!
//! ## Recovery
//!
//! On startup, all segment files in the WAL directory are discovered via `readdir`,
//! sorted by first_lsn, and replayed in order. The last segment is the active one.

pub mod atomic_io;
pub mod checkpoint_frame;
pub mod checkpoint_keyed_frame;
pub mod continuity;
pub mod decrypt;
pub mod discovery;
pub mod meta;
pub mod orphan;
pub mod retained;
pub mod truncate;

pub use atomic_io::{
    atomic_swap_dirs_fsync, atomic_write_fsync, fsync_directory, read_checkpoint_dontneed,
};
pub use checkpoint_frame::{read_checkpoint_framed, write_checkpoint_framed};
pub use checkpoint_keyed_frame::{
    KeyedCheckpoint, MAX_CHECKPOINT_KEY_LEN, read_checkpoint_key, read_checkpoint_keyed,
    write_checkpoint_keyed,
};
pub use continuity::SegmentContinuity;
pub use decrypt::SegmentDecryptor;
pub use discovery::discover_segments;
pub use meta::{
    DEFAULT_SEGMENT_TARGET_SIZE, SegmentMeta, parse_segment_filename, segment_filename,
    segment_path,
};
pub use orphan::check_resume_above_previous;
pub use retained::check_retained_floor;
pub use truncate::{TruncateResult, truncate_segments};
