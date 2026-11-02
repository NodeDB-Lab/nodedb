// SPDX-License-Identifier: BUSL-1.1

mod accessors;
pub(in crate::data::executor) mod apply_scope;
pub(in crate::data::executor) mod bitemporal_time;
pub(in crate::data::executor) mod calvin_state;
pub(in crate::data::executor) mod checkpoint_floors;
pub(in crate::data::executor) mod columnar_history_charge;
mod columnar_schema_seed;
pub(in crate::data::executor) mod commit_pending;
mod crdt_dead_letters;
mod crs_context;
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
mod graph_view;
pub(in crate::data::executor) mod hash_chain_state;
pub(in crate::data::executor) mod idempotency;
pub(in crate::data::executor) mod index_charge;
pub(in crate::data::executor) mod maintenance;
pub(in crate::data::executor) mod maintenance_state;
pub(in crate::data::executor) mod newest_ordinals;
mod open;
pub mod pressure;
pub(in crate::data::executor) mod priority_queues;
mod read_vshards;
pub(in crate::data::executor) mod redo_image;
mod response;
mod segment_keks;
mod state;
mod test_governor;
mod tick;
mod ts_declared_schema;
pub(in crate::data::executor) mod ts_replay_state;
pub(in crate::data::executor) mod txn_validate;
pub(in crate::data::executor) mod vector_build_queue;
mod vector_index_rebuild;
mod vector_index_seed;
pub(in crate::data::executor) mod version_purge;
pub(in crate::data::executor) mod write_note;
pub(in crate::data::executor) mod write_set_journal;

pub(in crate::data::executor) use crdt_dead_letters::crdt_rejection;
pub use doc_config_seed::DocConfigSeedEntry;
pub(in crate::data::executor) use segment_keks::SegmentKeks;
pub use state::CoreLoop;
pub use test_governor::test_governor;
pub(in crate::data::executor) use ts_declared_schema::TsGroupKeyKind;
/// Shared test fixtures (`make_core_with_dir`, `make_default_task`), kept
/// alongside the write-note tests that exercise the same `CoreLoop` apply
/// chokepoints. External test modules reach them as `core_loop::tests::`.
#[cfg(test)]
pub(crate) use write_note::tests;
