// SPDX-License-Identifier: BUSL-1.1

//! Generic per-entry apply path: decode the replicated entry, route
//! Raft-native array cell writes through the array-open bootstrap, and
//! dispatch everything else through the shared Control-Plane write funnel.

use std::sync::Arc;

use tracing::debug;

use nodedb_physical::physical_plan::ArrayOp;
use nodedb_raft::message::LogEntry;

use crate::bridge::envelope::{PhysicalPlan, Status};
use crate::control::array_sync::raft_apply::{
    AppliedPosition, ArrayCellTarget, apply_array_cell_write,
};
use crate::control::distributed_applier::applied_index::AppliedPrefix;
use crate::control::distributed_applier::propose_tracker::{AppliedWrite, ProposeTracker};
use crate::control::server::dispatch_utils::{
    ChangeFeedOwner, SubmitWrite, WalDurability, WriteOrdering, submit_write,
};
use crate::control::state::SharedState;
use crate::control::wal_replication::from_replicated_entry;
use crate::types::{DatabaseId, TraceId};

use super::helpers::{committed_response_result, deterministic_crdt_fence_noop};

/// Decode `entry` and apply it: Raft-native array cell writes route through
/// the array-open bootstrap and the write funnel; everything else dispatches
/// through the write funnel directly. Records the outcome into `prefix`.
pub(super) async fn apply_generic_entry(
    state: &Arc<SharedState>,
    tracker: &Arc<ProposeTracker>,
    prefix: &mut AppliedPrefix,
    group_id: u64,
    entry: &LogEntry,
    applied_key: u64,
    database_id: DatabaseId,
) {
    let decoded = from_replicated_entry(&entry.data, Some(state.surrogate_assigner.as_ref()));
    let (tenant_id, vshard_id, plan, resolved_now_ms) = match decoded {
        Ok(Some(t)) => t,
        Ok(None) => {
            // Couldn't deserialize — might be a different format or corrupted.
            debug!(
                group_id,
                index = entry.index,
                "skipping non-ReplicatedEntry commit"
            );
            tracker.complete(
                group_id,
                entry.index,
                applied_key,
                Ok(AppliedWrite::unversioned(Vec::new())),
            );
            // Prefix-neutral. This is a pure shape check over
            // `entry.data`, so a re-delivery on the next boot decodes to
            // `None` again and skips again — stalling the floor behind
            // it buys nothing and costs a double-apply of every later
            // write in the batch. It applied no state, so it must not
            // advance the floor either.
            prefix.skip();
            return;
        }
        Err(e) => {
            tracing::warn!(
                group_id,
                index = entry.index,
                error = %e,
                "failed to decode replicated entry (surrogate bind error)"
            );
            tracker.complete(
                group_id,
                entry.index,
                applied_key,
                Err(crate::Error::Internal {
                    detail: format!("decode replicated entry: {e}"),
                }),
            );
            // Breaks the prefix, unlike the `Ok(None)` skip above: this
            // IS a write, and it failed against live surrogate-assigner
            // state rather than on its own bytes, so a re-delivery can
            // legitimately succeed. Holding the floor below it is what
            // keeps it replayable.
            prefix.record(entry.index, false);
            return;
        }
    };

    // Raft-native array cell writes (`ArrayCellPut` / `ArrayCellDelete`)
    // decode to `PhysicalPlan::Array(Put | Delete)`. A follower must
    // OPEN the array on the Data Plane before applying, so these route
    // through the array-open bootstrap first — and then through the same
    // write funnel as the generic branch below, which is what gives them
    // a redo record and the fsync the applied floor asserts. No other
    // `ReplicatedWrite` variant decodes to a `PhysicalPlan::Array`, so
    // this match is exact.
    if matches!(
        plan,
        PhysicalPlan::Array(ArrayOp::Put { .. } | ArrayOp::Delete { .. })
    ) {
        let applied_ok = apply_array_cell_write(
            state,
            tracker,
            AppliedPosition {
                group_id,
                log_index: entry.index,
                applied_key,
            },
            ArrayCellTarget {
                tenant_id,
                database_id,
                vshard: vshard_id,
                resolved_now_ms,
            },
            plan,
        )
        .await;
        prefix.record(entry.index, applied_ok);
        return;
    }

    let submitted = submit_write(
        state,
        SubmitWrite {
            tenant_id,
            database_id,
            vshard_id,
            plan,
            trace_id: TraceId::generate(),
            // Cluster mode has exactly ONE write-apply path — this loop;
            // the proposing node does not execute locally before commit
            // either. Tagging these `RaftFollower` would mean AFTER
            // triggers, DML audit, and CRDT packaging never fire anywhere
            // in cluster mode, so the committed write keeps the `User`
            // source its proposer had.
            event_source: crate::event::EventSource::User,
            txn_id: None,
            // Auth ran on the proposing node before the entry was
            // proposed; the committed entry carries no session user.
            user_id: None,
            // The redo record is appended HERE, on this replica, from the
            // committed plan — the leader's WAL LSN is deliberately not
            // carried on the wire, and the memory-only engines have no
            // other durability path. `now_override` pins a TTL-bearing KV
            // write's `expire_at_ms` to the instant the proposing node
            // resolved, so this replica's redo record and its live apply
            // install the byte-identical value every other replica does.
            durability: WalDurability::AppendHere {
                now_override: resolved_now_ms,
            },
            // Raft committed this entry at a fixed log index; every
            // replica applies it in that order. Re-entering the
            // write-admission gate would re-decide an ordering that is
            // already final.
            ordering: WriteOrdering::AlreadyOrdered,
            // This loop runs on EVERY replica, so it must not publish:
            // the node that proposed this entry already published the
            // write's change event once, after commit + apply. Emitting
            // here would give each subscriber one copy per replica plus
            // a NOTIFY fan-out from each. See [`ChangeFeedOwner`].
            change_feed: ChangeFeedOwner::Unowned,
        },
    )
    .await
    .map(|outcome| outcome.response);

    // The funnel returns an error-status response as `Ok`; a committed
    // entry that failed to apply must surface to the propose waiter as a
    // failure, not as an empty success.
    let result = match submitted {
        // The response carries this replica's post-write
        // `coll_write_lsn` for the written collection, which the
        // proposer needs as its read-your-writes floor: the version is
        // minted here (the funnel's WAL append) and never travels on the
        // wire, so the propose waiter is the only place it can be
        // handed back.
        Ok(resp) if resp.status == Status::Ok => Ok(AppliedWrite::from_response(&resp)),
        Ok(resp) => committed_response_result(&resp),
        Err(e) => {
            tracing::warn!(
                group_id,
                index = entry.index,
                error = %e,
                "applying committed write failed"
            );
            // Passed through: the typed error already carries the
            // caller's classification.
            Err(e)
        }
    };

    let applied_ok = result.is_ok() || deterministic_crdt_fence_noop(&result);
    tracker.complete(group_id, entry.index, applied_key, result);

    // Extend the batch's durable prefix. On success `submit_write`'s
    // durable-at-ack barrier has already fsynced this entry's redo,
    // which is exactly the fact the floor asserts — `entry.index` is
    // the data-plane applied watermark here, NOT raft's commit index.
    // On failure the engines did not persist this index, so it is
    // neither a safe compaction boundary nor a safe restart floor;
    // breaking the prefix is what keeps a genuinely failed apply
    // replayable rather than silently skipped.
    prefix.record(entry.index, applied_ok);
}
