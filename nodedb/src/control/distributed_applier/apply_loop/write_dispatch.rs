// SPDX-License-Identifier: BUSL-1.1

//! Generic per-entry apply path: decode the replicated entry, route
//! Raft-native array cell writes through the array-open bootstrap, and
//! enqueue everything else through the shared Control-Plane write funnel.
//!
//! The enqueue runs in log order when the entry starts. The outcome is
//! collected by the returned apply, in any order.

use tracing::debug;

use nodedb_physical::physical_plan::ArrayOp;
use nodedb_raft::message::LogEntry;

use crate::bridge::envelope::{PhysicalPlan, Status};
use crate::control::array_sync::raft_apply::{
    AppliedPosition, ArrayCellTarget, apply_array_cell_write,
};
use crate::control::distributed_applier::propose_tracker::{AppliedWrite, ProposeTracker};
use crate::control::server::dispatch_utils::{
    ChangeFeedOwner, SubmitWrite, WalDurability, WriteOrdering, enqueue_write,
    error_is_final_refusal,
};
use crate::control::wal_replication::from_replicated_entry;
use crate::types::{DatabaseId, TraceId};

use crate::control::server::shared::write_admission::plan_writes_user_data;

use super::context::{ApplyContext, FinishedApply, Started, StartedEntry};
use super::helpers::{committed_response_result, deterministic_crdt_fence_noop};
use super::proposal_gate::{EntryOutcome, ledger_outcome};
use super::start::Prepared;

/// What a generic entry's apply takes from its decoded envelope.
#[derive(Debug, Clone, Copy)]
pub(super) struct EntryScope {
    /// Database scope of the entry.
    pub database_id: DatabaseId,
    /// The source the proposer stamped. Every replica gives the write's
    /// events this source.
    pub event_source: crate::event::EventSource,
}

/// Prepare a generic entry. `exclusive` marks a Raft-native array cell write:
/// its apply awaits the array-open bootstrap and its own write, so it runs
/// with nothing else of its group in flight. Every other entry leaves as its
/// enqueue.
pub(super) fn prepare_generic_entry<'a>(
    ctx: ApplyContext<'a>,
    pos: AppliedPosition,
    entry: LogEntry,
    scope: EntryScope,
    exclusive: bool,
) -> Prepared<'a> {
    if !exclusive {
        return Prepared::Enqueue(Box::pin(enqueue_generic_entry(ctx, pos, entry, scope)));
    }
    Prepared::Exclusive(Box::pin(async move {
        let outcome = match enqueue_generic_entry(ctx, pos, entry, scope).await.started {
            Started::Running(apply) => return apply.await,
            Started::Concluded(outcome) => outcome,
        };
        FinishedApply {
            group_id: pos.group_id,
            log_index: pos.log_index,
            outcome,
        }
    }))
}

/// Decode `entry` and enqueue it: Raft-native array cell writes route through
/// the array-open bootstrap and the write funnel, and conclude here; everything
/// else is enqueued through the write funnel directly.
async fn enqueue_generic_entry<'a>(
    ctx: ApplyContext<'a>,
    pos: AppliedPosition,
    entry: LogEntry,
    scope: EntryScope,
) -> StartedEntry<'a> {
    let EntryScope {
        database_id,
        event_source,
    } = scope;
    let ApplyContext { state, tracker, .. } = ctx;
    let AppliedPosition {
        group_id,
        log_index,
        applied_key,
        ..
    } = pos;
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
            return StartedEntry::concluded(EntryOutcome::Skipped);
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
            return StartedEntry::concluded(EntryOutcome::Applied {
                durable: false,
                result: None,
            });
        }
    };

    // Raft-native array cell writes (`ArrayCellPut` / `ArrayCellDelete`)
    // decode to `PhysicalPlan::Array(Put | Delete)`. A follower must
    // OPEN the array on the Data Plane before applying, so these route
    // through the array-open bootstrap first — and then through the same
    // write funnel as the generic branch below, which is what gives them
    // a redo record and the fsync the applied floor asserts. No other
    // `ReplicatedWrite` variant decodes to a `PhysicalPlan::Array`, so
    // this match is exact, and the caller runs them as exclusive entries.
    if matches!(
        plan,
        PhysicalPlan::Array(ArrayOp::Put { .. } | ArrayOp::Delete { .. })
    ) {
        let applied_ok = apply_array_cell_write(
            state,
            tracker,
            pos,
            ArrayCellTarget {
                tenant_id,
                database_id,
                vshard: vshard_id,
                resolved_now_ms,
            },
            plan,
        )
        .await;
        return StartedEntry::concluded(EntryOutcome::Applied {
            durable: applied_ok,
            result: None,
        });
    }

    let collection = plan
        .named_collections()
        .first()
        .map(|collection| (*collection).to_owned());
    let user_write = plan_writes_user_data(&plan);
    debug!(
        group_id,
        log_index,
        tenant_id = tenant_id.as_u64(),
        vshard_id = vshard_id.as_u32(),
        collection = collection.as_deref().unwrap_or(""),
        user_write,
        "applying a committed write entry"
    );
    let enqueued = enqueue_write(
        state,
        SubmitWrite {
            tenant_id,
            database_id,
            vshard_id,
            plan,
            trace_id: TraceId::generate(),
            // Cluster mode has exactly ONE write-apply path: this loop. The
            // proposing node does not execute locally before commit either.
            // So the committed write keeps the source its proposer stamped
            // on the entry. A client write stays `User`, and a restored row
            // stays `Restore`, on every replica.
            event_source,
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
                apply_key: applied_key,
                commit_hlc: pos.carried_commit_hlc(),
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
    .await;
    let started = match enqueued {
        Ok(pending) => Started::Running(Box::pin(async move {
            let submitted = pending.finish(state).await.map(|outcome| outcome.response);
            FinishedApply {
                group_id,
                log_index,
                outcome: conclude_generic_entry(tracker, pos, submitted),
            }
        })),
        Err(error) => Started::Concluded(conclude_generic_entry(tracker, pos, Err(error))),
    };
    StartedEntry {
        started,
        collection,
        user_write,
    }
}

/// Resolve a generic entry's waiter from what the funnel returned, and report
/// the outcome its group's prefix records.
fn conclude_generic_entry(
    tracker: &ProposeTracker,
    pos: AppliedPosition,
    submitted: crate::Result<crate::bridge::envelope::Response>,
) -> EntryOutcome {
    let AppliedPosition {
        group_id,
        log_index,
        applied_key,
        ..
    } = pos;

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
                index = log_index,
                error = %e,
                "applying committed write failed"
            );
            // Passed through: the typed error already carries the
            // caller's classification.
            Err(e)
        }
    };

    // A final refusal is the entry's outcome: its marker carries the key.
    let applied_ok = result.is_ok()
        || deterministic_crdt_fence_noop(&result)
        || result.as_ref().is_err_and(error_is_final_refusal);
    let applied = ledger_outcome(&result);
    tracker.complete(group_id, log_index, applied_key, result);

    // Extend the group's durable prefix. On success the funnel's
    // durable-at-ack barrier has already fsynced this entry's redo,
    // which is exactly the fact the floor asserts — `log_index` is
    // the data-plane applied watermark here, NOT raft's commit index.
    // On failure the engines did not persist this index, so it is
    // neither a safe compaction boundary nor a safe restart floor;
    // breaking the prefix is what keeps a genuinely failed apply
    // replayable rather than silently skipped.
    EntryOutcome::Applied {
        durable: applied_ok,
        result: Some(applied),
    }
}
