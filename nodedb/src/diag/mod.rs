// SPDX-License-Identifier: BUSL-1.1

//! Black-box recorder wiring for capture sites outside the WAL. One report
//! per root cause, filed at the detecting site, never re-emitted. This
//! crate hosts the recorder (`bootstrap::diagnostics` calls `faultbox::init`),
//! so these entry points are unconditional — no feature gate, no fallback.

mod context;
mod recording;

pub use context::{DATABASE_SCOPE, IlpFlushOutcome, LostResponseWrite, TENANT_SCOPE};
pub use recording::{
    IndexRebuildTarget, VectorBuildTarget, batch_insert_without_surrogates, calvin_apply_halted,
    calvin_completion_timeout, catalog_apply_orphan_row, collection_purge_row_missing,
    consumer_group_offsets_retained, continuous_aggregate_not_applied,
    crdt_dead_letter_not_enqueued, crdt_dead_letter_not_restored, crdt_dead_letter_not_stored,
    data_plane_core_fail_stopped, data_plane_response_lost, data_plane_responses_lost,
    descriptor_lease_not_renewed, entry_kind, fts_index_update_failed,
    history_compaction_not_applied, ilp_invalid_utf8_drop, ilp_line_read_drop,
    index_rebuild_not_installed, metadata_apply_wedged, orphaned_index_entry_after_delete,
    quota_row_invalid, quota_row_undecodable, quota_row_write_failed, quota_scope_purge_incomplete,
    quota_scope_replay_aborted, raft_entries_reapplied, raft_entry_reapplied,
    replay_record_unapplied, replicated_write_parked, replicated_writes_parked,
    retention_autowire_orphaned, scope_quota_not_installed, strict_row_image_unrendered,
    strict_row_undecodable, synonym_group_not_applied, vector_build_failed,
    vector_builder_disconnected, vector_builder_spawn_failed, vector_index_not_applied,
    vector_rebuild_unreadable, wal_archival_failed_truncation_held, write_acked_without_durability,
    write_window_held, write_window_leaked,
};
