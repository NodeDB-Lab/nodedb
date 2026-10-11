// SPDX-License-Identifier: BUSL-1.1

mod accessors;
pub(in crate::data::executor) mod apply_scope;
mod bitemporal_time;
pub(in crate::data::executor) mod calvin_state;
pub(in crate::data::executor) mod checkpoint_floors;
mod columnar_schema_seed;
pub(in crate::data::executor) mod commit_pending;
mod crdt_dead_letters;
mod declared_columns;
mod decode_stored;
pub(in crate::data::executor) mod deferred;
mod doc_config_seed;
pub(in crate::data::executor) mod event_emit;
mod event_emit_engines;
pub(in crate::data::executor) mod event_image;
pub(in crate::data::executor) mod event_outlet;
pub(in crate::data::executor) use event_emit_engines::KvWriteEvent;
pub(in crate::data::executor) mod fail_stop;
pub(in crate::data::executor) mod filter_match;
mod graph_partition;
pub(in crate::data::executor) mod idempotency;
pub(in crate::data::executor) mod index_value_versions;
pub(in crate::data::executor) mod maintenance;
pub(in crate::data::executor) mod maintenance_state;
mod open;
pub mod pressure;
pub(in crate::data::executor) mod priority_queues;
pub(in crate::data::executor) mod redo_image;
mod response;
mod segment_keks;
mod state;
mod test_governor;
mod tick;
mod ts_declared_schema;
pub(in crate::data::executor) mod vector_build_queue;
mod vector_index_rebuild;
mod vector_index_seed;
pub(in crate::data::executor) mod write_index;
pub(in crate::data::executor) mod write_set_journal;

pub(in crate::data::executor) use crdt_dead_letters::crdt_rejection;
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
