// SPDX-License-Identifier: BUSL-1.1

//! Propose a `ReplicatedEntry` through Raft with transparent leader-change retry.
//!
//! Shared by the pgwire write dispatch path and the durable RESTORE re-issue
//! path: both must replicate a write to the vshard's Raft group and tolerate a
//! mid-flight leader change (the previous leader's entry being overwritten by a
//! new leader's election no-op) by re-proposing the same payload.

use std::sync::Arc;

use super::types::{AsyncRaftProposer, ReplicatedEntry};
use crate::control::state::SharedState;

/// First backoff before a re-proposal. Each retry doubles it up to
/// [`MAX_BACKOFF`].
const FIRST_BACKOFF: std::time::Duration = std::time::Duration::from_millis(10);

/// Longest wait between two re-proposals.
const MAX_BACKOFF: std::time::Duration = std::time::Duration::from_millis(200);

/// Propose `entry` via `proposer` and return the Data Plane apply payload bytes
/// together with the write's per-collection version (as an
/// [`crate::types::Lsn`]): the written collection's `coll_write_lsn` after the
/// write, stamped by the applying replica from the WAL LSN it minted for the
/// entry's redo record. `Lsn::ZERO` when the write's plan names no single user
/// collection. See [`AsyncRaftProposer`] for why this is a WAL LSN and never the
/// Raft log index.
///
/// Re-proposes the same payload until the statement deadline while the group
/// has no leader to take it:
/// - [`crate::Error::RetryableLeaderChange`]: a new leader's election no-op
///   overwrote the previous leader's entry;
/// - [`crate::Error::NoLeader`]: the group is electing, or a leadership
///   transfer is in flight.
///
/// The encoded `ReplicatedEntry` carries enough identity (collection, PK,
/// surrogate) to be replayable, and its idempotency key makes a copy that
/// committed twice apply once. Only propose-layer machinery failures map to
/// [`crate::Error::Dispatch`]; a classified apply verdict passes through.
pub(crate) async fn propose_replicated_entry(
    state: &SharedState,
    proposer: &Arc<AsyncRaftProposer>,
    mut entry: ReplicatedEntry,
) -> crate::Result<(Vec<u8>, crate::types::Lsn)> {
    // The write's commit instant. Stamped once, before the first propose, so
    // every re-proposal and every replica's apply carries the same value.
    entry.write_hlc = state.hlc_clock.now().wall_ns;
    // The catalog this write was planned against: every replica applies it
    // only once its own metadata apply reached this index.
    entry.metadata_floor = state
        .applied_index_watcher(nodedb_cluster::METADATA_GROUP_ID)
        .current();
    let idempotency_key = entry.idempotency_key;
    let data = entry.to_bytes();
    let vshard_id = entry.vshard_id;

    // The statement deadline. Every attempt, and each attempt's wait for the
    // local apply, ends at this one instant.
    let deadline = tokio::time::Instant::now()
        + std::time::Duration::from_secs(state.tuning.network.default_deadline_secs);
    let mut backoff = FIRST_BACKOFF;
    let mut attempt: u32 = 0;
    let payload = loop {
        attempt += 1;
        let error = match proposer(vshard_id, idempotency_key, data.clone(), deadline).await {
            Ok(p) => break Ok(p),
            Err(error @ crate::Error::RetryableLeaderChange { .. }) => {
                state
                    .raft_propose_leader_change_retries
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                error
            }
            Err(error @ crate::Error::NoLeader { .. }) => error,
            // Only a machinery failure is re-wrapped. A state-machine verdict
            // (constraint, authz, conflict) carries the client's SQLSTATE.
            Err(other) if crate::error_classify::is_unclassified_failure(&other) => {
                return Err(crate::Error::Dispatch {
                    detail: format!("raft propose failed: {other}"),
                });
            }
            Err(other) => return Err(other),
        };
        if tokio::time::Instant::now() + backoff >= deadline {
            break Err(error);
        }
        tracing::warn!(
            attempt,
            vshard_id,
            error = %error,
            "raft proposal found no leader to take it; re-proposing"
        );
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(MAX_BACKOFF);
    };
    // The waiter resolved only once this node's own apply ran the entry, and
    // that apply recorded the entry's commit stamp on the tenant's observed
    // write high-water before resolving it.
    payload
}
