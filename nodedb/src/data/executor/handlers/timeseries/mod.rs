// SPDX-License-Identifier: BUSL-1.1

//! Data Plane handlers for timeseries scan and ingest.

mod admission;
pub mod aggregate;
pub mod cagg_cut;
pub mod cagg_fold;
pub mod cagg_persist;
#[cfg(test)]
pub mod cagg_test_support;
pub mod cell_read;
pub mod encode;
mod events;
pub mod flush;
mod flush_buckets;
pub mod flush_commit;
#[cfg(test)]
mod flush_crash_tests;
mod flush_interval;
mod flush_rollforward;
mod group_flush;
pub mod ingest;
mod ingest_dispatch;
pub mod ingest_formats;
mod ingest_resolved;
mod ingest_resolved_fit;
mod ingest_resolved_outcome;
mod ingest_resolved_returning;
mod ingest_schema;
pub mod merge_commit;
mod merge_select;
mod msgpack_decode;
mod normalize;
pub mod partition_merge;
pub mod partition_read;
pub mod partition_rewrite;
mod partitions;
pub mod paths;
pub mod raw_scan;
pub mod read_aggregate;
mod redo_ingest;
mod resolve_ingest;
mod resolve_rows;
mod returning_preview;
mod rls_gate;
mod scan;
mod sort;
mod staged_fold;
mod tier_fold;
mod tier_route;
mod time_range;
pub mod truncate;

pub(in crate::data::executor) use ingest_dispatch::{TimeseriesApplyMode, TimeseriesIngestExec};
pub(in crate::data::executor) use resolve_ingest::StampedIngest;
pub(in crate::data::executor) use resolve_rows::TsResolveInput;
pub(in crate::data::executor) use rls_gate::{MsgpackBatch, admit_ilp_lines, admit_msgpack_rows};
pub(in crate::data::executor) use scan::TimeseriesScanParams;
pub(in crate::data::executor) use truncate::{is_truncating_leftover, remove_truncating_leftovers};
