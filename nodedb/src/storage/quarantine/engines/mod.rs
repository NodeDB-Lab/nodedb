// SPDX-License-Identifier: BUSL-1.1

pub mod columnar;
pub mod fts;
pub mod raft;
pub mod vector;

pub use columnar::{ColumnarOrQuarantine, open_segment_with_quarantine};
pub use fts::{FtsOrQuarantine, FtsSegmentRef, validate_fts_segment_bytes};
pub use raft::{RaftOrQuarantine, decode_snapshot_chunk_with_quarantine};
pub use vector::{VectorOrQuarantine, VectorQuarantineOpen, open_vector_segment_with_quarantine};
