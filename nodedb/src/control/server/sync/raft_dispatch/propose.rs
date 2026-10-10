// SPDX-License-Identifier: BUSL-1.1

//! Raft proposal for sync writes.
//!
//! `start_raft` installs the `async_raft_proposer` on every node. When the
//! write plan maps to a `ReplicatedEntry`, the write is proposed to the Raft
//! group and blocks here until the entry is committed to a quorum and applied
//! on the local node. An acknowledged sync write survives leader failover.
//!
//! The idempotency gate embedded in every `ReplicatedEntry` runs on every
//! replica via the replicated provenance. A reconnecting Lite client that
//! re-sends a delta on failover is deduplicated on the new leader.

use std::sync::Arc;
use std::time::Duration;

use crate::bridge::envelope::PhysicalPlan;
use crate::control::server::dispatch_utils::RecordOwner;
use crate::control::state::SharedState;
use crate::control::wal_replication::{
    AsyncRaftProposer, ReplicableWrite, ReplicatedEntry, to_replicated_entry,
};
use crate::event::EventSource;

/// Propose a sync write's `plan` through its vShard's data group and return
/// the apply payload once the entry applied on this node.
///
/// The entry's apply appends the write's redo record on every replica.
///
/// Every sync write plan has a replicated form. A plan without one is
/// refused.
pub(crate) async fn propose_sync_plan(
    state: &SharedState,
    owner: RecordOwner,
    plan: &PhysicalPlan,
    event_source: EventSource,
) -> crate::Result<Vec<u8>> {
    let RecordOwner {
        tenant_id,
        database_id,
        vshard_id,
    } = owner;
    let proposer = state.async_raft_proposer()?;
    // The entry carries resolved rows: a timeseries ingest resolves here, on
    // the proposer, before the entry exists.
    let resolved = crate::control::write_resolve::resolve_for_log(
        state,
        crate::control::write_resolve::WriteResolveContext {
            tenant_id,
            database_id,
        },
        vshard_id,
        plan,
    )
    .await?;
    let proposed = resolved.as_ref().unwrap_or(plan);
    let replicable = ReplicableWrite::decide_for_replication(proposed)?;
    let entry = to_replicated_entry(tenant_id, database_id, vshard_id, &replicable)?.ok_or(
        crate::Error::Internal {
            detail: format!(
                "sync write to '{}' has no replicated form",
                plan.collection().unwrap_or("<unknown>")
            ),
        },
    )?;
    propose_sync_write(state, entry.with_event_source(event_source), proposer).await
}

/// Propose a `ReplicatedEntry` through Raft and block until the entry is
/// committed to a quorum and applied on the local node.
///
/// Returns the apply-payload bytes produced by the Data Plane after the entry
/// is applied. These bytes carry the `SyncAckResult` that the handler decodes
/// to determine the idempotency gate verdict.
///
/// Retries transparently up to five times on [`crate::Error::RetryableLeaderChange`]
/// (leader failover during the propose). All attempts share one statement
/// deadline, so a retry gets only the time that remains. Only propose-layer machinery failures
/// map to [`crate::Error::Dispatch`]; a classified apply verdict passes through.
///
/// An edge write is not proposed. It runs as a Calvin transaction
/// (`planner::calvin::edge_sequencing`), and its applied payload comes back.
pub(crate) async fn propose_sync_write(
    state: &SharedState,
    mut entry: ReplicatedEntry,
    proposer: &Arc<AsyncRaftProposer>,
) -> crate::Result<Vec<u8>> {
    if let Some(response) =
        crate::control::planner::calvin::sequence_replicated_edge_write(state, &entry).await?
    {
        return Ok(response.payload.to_vec());
    }
    // The proposer below takes prebuilt bytes, so the floor is stamped here:
    // replicas hold the write until they applied the collection's DDL. The
    // commit instant is stamped once, before the first propose, so every
    // retry and every replica dates the write alike.
    entry.write_hlc = state.hlc_clock.now().wall_ns;
    crate::control::wal_replication::stamp_metadata_floor(state, &mut entry);
    crate::control::wal_replication::stamp_collection_incarnations(state, &mut entry)?;
    let idempotency_key = entry.idempotency_key;
    let data = entry.encode()?;
    let vshard_id = entry.vshard_id;
    let deadline = tokio::time::Instant::now()
        + Duration::from_secs(state.tuning.network.default_deadline_secs);

    const BACKOFF_MS: [u64; 5] = [10, 25, 50, 100, 200];
    let mut payload: Option<Vec<u8>> = None;
    let mut last_err: Option<crate::Error> = None;

    for (attempt, backoff_ms) in BACKOFF_MS.iter().enumerate() {
        match proposer(vshard_id, idempotency_key, data.clone(), deadline).await {
            // The write's versions ride alongside the payload. The sync-ack
            // path needs only the payload bytes.
            Ok((p, _write_versions)) => {
                payload = Some(p);
                break;
            }
            Err(crate::Error::RetryableLeaderChange {
                group_id,
                log_index,
            }) => {
                state
                    .raft_propose_leader_change_retries
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                tracing::warn!(
                    attempt,
                    group_id,
                    log_index,
                    "raft entry overwritten by leader change — re-proposing"
                );
                last_err = Some(crate::Error::RetryableLeaderChange {
                    group_id,
                    log_index,
                });
                let backoff = Duration::from_millis(*backoff_ms);
                if tokio::time::Instant::now() + backoff >= deadline {
                    break;
                }
                tokio::time::sleep(backoff).await;
                continue;
            }
            // Only a machinery failure is re-wrapped. A state-machine verdict
            // (constraint, authz, conflict) carries the client's SQLSTATE.
            Err(other) if crate::error_classify::is_unclassified_failure(&other) => {
                return Err(crate::Error::Dispatch {
                    detail: format!("raft propose failed: {other}"),
                });
            }
            Err(other) => return Err(other),
        }
    }

    payload.ok_or_else(|| {
        last_err.unwrap_or_else(|| crate::Error::Dispatch {
            detail: "raft propose retries exhausted".into(),
        })
    })
}
