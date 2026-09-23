// SPDX-License-Identifier: BUSL-1.1

//! Data Plane handlers for timeseries scan and ingest.

mod admission;
pub mod aggregate;
pub mod encode;
pub mod flush;
pub mod ingest;
mod ingest_dispatch;
pub mod ingest_formats;
mod ingest_schema;
mod msgpack_decode;
mod normalize;
pub mod paths;
pub mod raw_scan;
mod redo_ingest;
mod resolve_ingest;
mod rls_gate;
mod scan;
mod sort;
mod time_range;
pub mod truncate;

pub(in crate::data::executor) use ingest_dispatch::{TimeseriesApplyMode, TimeseriesIngestExec};
pub(in crate::data::executor) use resolve_ingest::StampedIngest;
pub(in crate::data::executor) use rls_gate::{admit_ilp_lines, admit_msgpack_rows};
pub(in crate::data::executor) use scan::TimeseriesScanParams;
pub(in crate::data::executor) use truncate::{is_truncating_leftover, remove_truncating_leftovers};
