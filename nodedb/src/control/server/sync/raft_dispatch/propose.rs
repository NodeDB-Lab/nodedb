// SPDX-License-Identifier: BUSL-1.1

//! Raft proposal for sync writes.
//!
//! In a multi-node deployment the `async_raft_proposer` field is set on
//! `SharedState` after Raft starts. When it is `Some` **and** the write plan
//! maps to a `ReplicatedEntry`, the write is proposed to the Raft group and
//! blocks here until the entry is committed to a quorum and applied on the
//! local node. That gives quorum-durable ACK semantics: an acknowledged sync
//! write cannot be lost on leader failover.
//!
//! The idempotency gate embedded in every `ReplicatedEntry` runs on every
//! replica via the replicated provenance, so a reconnecting Lite client that
//! re-sends a delta on failover will be deduplicated on the new leader.
//!
//! Single-node deployments never set `async_raft_proposer`, so they always fall
//! through to the local Data Plane path — zero overhead.

use std::sync::Arc;
use std::time::Duration;

use crate::control::state::SharedState;
use crate::control::wal_replication::{AsyncRaftProposer, ReplicatedEntry};

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
pub(crate) async fn propose_sync_write(
    state: &SharedState,
    entry: ReplicatedEntry,
    proposer: &Arc<AsyncRaftProposer>,
) -> crate::Result<Vec<u8>> {
    let idempotency_key = entry.idempotency_key;
    let data = entry.to_bytes();
    let vshard_id = entry.vshard_id;
    let deadline = tokio::time::Instant::now()
        + Duration::from_secs(state.tuning.network.default_deadline_secs);

    const BACKOFF_MS: [u64; 5] = [10, 25, 50, 100, 200];
    let mut payload: Option<Vec<u8>> = None;
    let mut last_err: Option<crate::Error> = None;

    for (attempt, backoff_ms) in BACKOFF_MS.iter().enumerate() {
        match proposer(vshard_id, idempotency_key, data.clone(), deadline).await {
            // The committed log index rides alongside the payload; the sync-ack
            // path only needs the payload bytes.
            Ok((p, _committed_version)) => {
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
