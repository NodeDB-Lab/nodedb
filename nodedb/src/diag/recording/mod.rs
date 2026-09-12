// SPDX-License-Identifier: BUSL-1.1

//! Recording implementation of capture sites outside the WAL, grouped by the
//! subsystem that detects them.
//!
//! Each function is called only from the one site that detects its failure,
//! never re-emitted as the error propagates. `Capture::emit` never panics
//! and returns `None` when unrecorded, so the result is deliberately
//! discarded.

mod catalog;
mod crdt;
mod data_plane;
mod ingest;
mod quota;
mod recovery;
mod retention;
mod shared;
mod vector;

pub use catalog::{
    catalog_apply_orphan_row, collection_purge_row_missing, consumer_group_offsets_retained,
    metadata_apply_wedged, synonym_group_not_applied,
};
pub use crdt::history_compaction_not_applied;
pub use data_plane::{
    calvin_completion_timeout, data_plane_response_lost, data_plane_responses_lost,
};
pub use ingest::{ilp_invalid_utf8_drop, ilp_line_read_drop};
pub use quota::{
    quota_row_invalid, quota_row_undecodable, quota_row_write_failed, quota_scope_purge_incomplete,
    quota_scope_replay_aborted, scope_quota_not_installed,
};
pub use recovery::{
    batch_insert_without_surrogates, fts_index_update_failed, orphaned_index_entry_after_delete,
    replay_record_unapplied, strict_row_undecodable, wal_archival_failed_truncation_held,
    write_acked_without_durability,
};
pub use retention::retention_autowire_orphaned;
pub use shared::entry_kind;
pub use vector::vector_index_not_applied;
