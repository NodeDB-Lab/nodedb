// SPDX-License-Identifier: BUSL-1.1

//! Applied-prefix bookkeeping: persist the group's durable applied floor,
//! then fire the Raft log-compaction trigger against it.

use std::sync::Arc;

use tracing::debug;

use crate::control::distributed_applier::applied_index::save_applied_index;
use crate::control::state::SharedState;

/// Record entry `applied_index` of `group_id` as durably applied: persist the
/// group's durable applied floor, then fire the compaction trigger against it.
///
/// `applied_index` MUST be the highest CONTIGUOUS successfully-applied entry —
/// [`crate::control::distributed_applier::applied_index::AppliedPrefix::floor`]
/// — not merely some entry that happened to succeed. Everything at and below
/// it must have applied with its redo record already WAL-fsync-durable,
/// because that is the fact the floor asserts and the next boot resumes Raft
/// delivery above it on the strength of it.
///
/// Called once per apply batch: each call is a redb commit and therefore an
/// fsync, and one per entry is slow enough to stall the raft loop it runs on.
///
/// Order is load-bearing: the floor lands first because compaction is itself
/// gated on the floor (it may only discard entries the engines can no longer
/// need the log for). Compacting first would either be refused or, if the gate
/// used the delivery watermark, discard entries whose redo is not yet fsynced.
pub(super) fn record_durable_apply(state: &Arc<SharedState>, group_id: u64, applied_index: u64) {
    save_applied_index(state, group_id, applied_index);
    maybe_compact_log(state, group_id, applied_index);
}

/// Fire the Raft log-compaction trigger for `group_id` up to the
/// data-plane applied index `applied_index`, if a compactor is wired.
///
/// Gated by the caller on data-plane apply completion. A no-op when no
/// compactor is installed (single-node mode) or when the group's
/// `log_compaction_threshold` is `None`.
fn maybe_compact_log(state: &Arc<SharedState>, group_id: u64, applied_index: u64) {
    let Some(compactor) = state.raft_compactor.get() else {
        return;
    };
    match compactor(group_id, applied_index) {
        Ok(true) => {
            debug!(
                group_id,
                applied_index, "raft log compacted past data-plane applied watermark"
            );
        }
        Ok(false) => {}
        Err(e) => {
            tracing::warn!(
                group_id,
                applied_index,
                error = %e,
                "raft log compaction failed"
            );
        }
    }
}
