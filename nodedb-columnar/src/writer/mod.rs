// SPDX-License-Identifier: Apache-2.0

//! Segment writer: drains a memtable into a compressed columnar segment.

mod block;
mod encode;
mod segment_writer;
mod stats;

pub use encode::compute_schema_hash;

pub use segment_writer::{
    PROFILE_PLAIN, PROFILE_SPATIAL, PROFILE_TIMESERIES, SegmentWriter, select_codec_for_profile,
};
