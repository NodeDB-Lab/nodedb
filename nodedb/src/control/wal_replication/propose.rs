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

/// The deadline a proposal made now carries: the running statement's
/// deadline, or the node default outside a statement.
pub(crate) fn statement_propose_deadline(state: &SharedState) -> tokio::time::Instant {
    tokio::time::Instant::from_std(crate::control::server::shared::session::statement_deadline(
        state.tuning.network.default_deadline_secs,
    ))
}

/// Stamp this node's metadata floor on `entry`: the catalog the write was
/// planned against, including the batch being applied now (see
/// `AppliedIndexWatcher::floor`). Every replica applies the entry only once
/// its own metadata apply reached this index. Every proposal of a
/// user-collection write stamps it before encoding.
pub(crate) fn stamp_metadata_floor(state: &SharedState, entry: &mut ReplicatedEntry) {
    entry.metadata_floor = state
        .applied_index_watcher(nodedb_cluster::METADATA_GROUP_ID)
        .floor();
}

/// Stamp the incarnation this node's catalog holds for each collection
/// `entry` names. A collection with no catalog row stays `Hlc::ZERO`, and a
/// replica applies its write by key alone.
pub(crate) fn stamp_collection_incarnations(
    state: &SharedState,
    entry: &mut ReplicatedEntry,
) -> crate::Result<()> {
    let catalog = state.credentials.catalog();
    let database_id = crate::types::DatabaseId::new(entry.database_id);
    for named in &mut entry.incarnations {
        named.incarnation =
            catalog.incarnation_of(database_id, entry.tenant_id, &named.collection)?;
    }
    Ok(())
}

/// Propose `entry` via `proposer` and return the Data Plane apply payload
/// bytes and the versions the write stamped. Each version is the data-group
/// log position of the entry that applied the write, so it is the same on
/// every replica. A write whose plan names no single user collection stamps
/// none.
///
/// An edge write is not proposed. It runs as a Calvin transaction
/// (`planner::calvin::edge_sequencing`), and its applied payload and read
/// version come back the same way.
///
/// `deadline` is the caller's deadline. Every attempt, each attempt's wait at
/// the leader's write gate, and each wait for the local apply end at it.
///
/// Re-proposes the same payload until `deadline` while the group has no
/// leader to take it:
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
    deadline: tokio::time::Instant,
) -> crate::Result<super::types::AppliedOutput> {
    // An edge write runs as a Calvin transaction, never as a data-group
    // entry, so every edge version of a collection takes a Calvin ordinal.
    if let Some(response) =
        crate::control::planner::calvin::sequence_replicated_edge_write(state, &entry).await?
    {
        return Ok((response.payload.to_vec(), response.read_versions));
    }
    // The write's commit instant. Stamped once, before the first propose, so
    // every re-proposal and every replica's apply carries the same value.
    entry.write_hlc = state.hlc_clock.now().wall_ns;
    stamp_metadata_floor(state, &mut entry);
    crate::control::array_catalog::cell_route::stamp_incarnation(state, &mut entry);
    stamp_collection_incarnations(state, &mut entry)?;
    let idempotency_key = entry.idempotency_key;
    let data = entry.encode()?;
    let vshard_id = entry.vshard_id;

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
