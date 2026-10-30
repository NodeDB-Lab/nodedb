// SPDX-License-Identifier: BUSL-1.1

//! Black-box recorder wiring for capture sites outside the WAL. One report
//! per root cause, filed at the detecting site, never re-emitted. This
//! crate hosts the recorder (`bootstrap::diagnostics` calls `faultbox::init`),
//! so these entry points are unconditional — no feature gate, no fallback.

mod context;
mod recording;

pub use context::{DATABASE_SCOPE, IlpFlushOutcome, LostResponseWrite, TENANT_SCOPE};
pub use recording::{
    IndexRebuildTarget, VectorBuildTarget, apply_panicked, array_snapshot_buffer_poisoned,
    audit_flush_not_persisted, audit_wal_append_failed, batch_insert_without_surrogates,
    calvin_apply_halted, calvin_barrier_log_store_failed, calvin_completion_timeout,
    calvin_left_state_not_forgotten, calvin_stage_refused_internal, catalog_apply_orphan_row,
    collection_purge_row_missing, columnar_segment_corrupt, constraint_install_deferred,
    consumer_group_offsets_retained, continuous_aggregate_not_applied,
    continuous_aggregate_state_fault, crdt_dead_letter_not_enqueued, crdt_dead_letter_not_restored,
    crdt_dead_letter_not_stored, custom_crs_set_not_applied, cut_barrier_not_placed,
    data_plane_core_fail_stopped, data_plane_response_lost, data_plane_responses_lost,
    ddl_cleanup_not_completed, descriptor_drain_not_ended, descriptor_lease_not_renewed,
    entry_kind, fts_index_update_failed, held_entry_debt_not_recorded,
    history_compaction_not_applied, ilp_invalid_utf8_drop, ilp_line_read_drop,
    index_rebuild_not_installed, metadata_apply_wedged, orphaned_index_entry_after_delete,
    quota_row_invalid, quota_row_undecodable, quota_row_write_failed, quota_scope_purge_incomplete,
    quota_scope_replay_aborted, raft_entries_reapplied, raft_entry_reapplied,
    redo_abandon_given_up, redo_entry_malformed, redo_snapshot_debt_not_recorded,
    redo_stream_lost_here, replay_record_unapplied, retention_autowire_orphaned,
    scope_quota_not_installed, strict_row_image_unrendered, strict_row_undecodable,
    synonym_group_not_applied, timeseries_flush_not_installed, timeseries_idle_flush_failed,
    timeseries_partition_unreadable, timeseries_row_image_undecodable, vector_build_failed,
    vector_builder_disconnected, vector_builder_spawn_failed, vector_index_not_applied,
    vector_payload_undecodable, vector_rebuild_unreadable, vshard_departure_purge_failed,
    wal_archival_failed_truncation_held, write_acked_without_durability, write_window_held,
    write_window_leaked,
};
