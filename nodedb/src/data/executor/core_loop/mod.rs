// SPDX-License-Identifier: BUSL-1.1

mod accessors;
mod bitemporal_time;
pub(in crate::data::executor) mod checkpoint_floors;
mod columnar_schema_seed;
pub(in crate::data::executor) mod commit_pending;
mod decode_stored;
pub(in crate::data::executor) mod deferred;
mod doc_config_seed;
pub(in crate::data::executor) mod event_emit;
pub(in crate::data::executor) mod filter_match;
mod graph_partition;
pub(in crate::data::executor) mod index_value_versions;
pub(in crate::data::executor) mod maintenance;
mod open;
pub mod pressure;
pub(in crate::data::executor) mod priority_queues;
mod response;
mod segment_keks;
mod state;
mod test_governor;
mod tick;
mod ts_declared_schema;
mod vector_index_rebuild;
mod vector_index_seed;
pub(in crate::data::executor) mod write_index;

pub use doc_config_seed::DocConfigSeedEntry;
pub(in crate::data::executor) use segment_keks::SegmentKeks;
pub use state::CoreLoop;
pub use test_governor::test_governor;
pub(in crate::data::executor) use ts_declared_schema::TsGroupKeyKind;
/// Shared test fixtures (`make_core_with_dir`, `make_default_task`), kept
/// alongside the write-version-index tests that exercise the same `CoreLoop`
/// apply chokepoints. Re-exported here so external test modules keep using
/// the pre-existing `core_loop::tests::` path.
#[cfg(test)]
pub(crate) use write_index::tests;
