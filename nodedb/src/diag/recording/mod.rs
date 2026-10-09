// SPDX-License-Identifier: BUSL-1.1

//! Recording implementation of capture sites outside the WAL, grouped by the
//! subsystem that detects them.
//!
//! Each function is called only from the one site that detects its failure,
//! never re-emitted as the error propagates. `Capture::emit` never panics
//! and returns `None` when unrecorded, so the result is deliberately
//! discarded.

mod catalog;
mod columnar;
mod continuous_agg;
mod crdt;
mod data_plane;
mod event_image;
mod index_rebuild;
mod ingest;
mod lease;
mod outcome_floor;
mod quota;
mod raft_apply;
mod recovery;
mod redo_stream;
mod retention;
mod shared;
mod vector;
mod vector_build;

pub use catalog::{
    catalog_apply_orphan_row, collection_purge_row_missing, consumer_group_offsets_retained,
    metadata_apply_wedged, synonym_group_not_applied,
};
pub use columnar::{columnar_segment_corrupt, timeseries_partition_unreadable};
pub use continuous_agg::continuous_aggregate_not_applied;
pub use crdt::{
    crdt_dead_letter_not_enqueued, crdt_dead_letter_not_restored, crdt_dead_letter_not_stored,
    history_compaction_not_applied,
};
pub use data_plane::{
    calvin_apply_halted, calvin_completion_timeout, data_plane_core_fail_stopped,
    data_plane_response_lost, data_plane_responses_lost,
};
pub use event_image::{strict_row_image_unrendered, timeseries_row_image_undecodable};
pub use index_rebuild::{IndexRebuildTarget, index_rebuild_not_installed};
pub use ingest::{ilp_invalid_utf8_drop, ilp_line_read_drop};
pub use lease::descriptor_lease_not_renewed;
pub use outcome_floor::{write_window_held, write_window_leaked};
pub use quota::{
    quota_row_invalid, quota_row_undecodable, quota_row_write_failed, quota_scope_purge_incomplete,
    quota_scope_replay_aborted, scope_quota_not_installed,
};
pub use raft_apply::{
    raft_entries_reapplied, raft_entry_reapplied, replicated_write_parked, replicated_writes_parked,
};
pub use recovery::{
    batch_insert_without_surrogates, fts_index_update_failed, orphaned_index_entry_after_delete,
    replay_record_unapplied, strict_row_undecodable, wal_archival_failed_truncation_held,
    write_acked_without_durability,
};
pub use redo_stream::{
    redo_abandon_given_up, redo_snapshot_debt_not_recorded, redo_stream_lost_here,
};
pub use retention::retention_autowire_orphaned;
pub use shared::entry_kind;
pub use vector::vector_index_not_applied;
pub use vector_build::{
    VectorBuildTarget, vector_build_failed, vector_builder_disconnected,
    vector_builder_spawn_failed, vector_rebuild_unreadable,
};
