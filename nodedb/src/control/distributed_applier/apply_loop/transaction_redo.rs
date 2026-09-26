// SPDX-License-Identifier: BUSL-1.1

//! Apply path for a committed `ReplicatedWrite::TransactionRedo` entry.
//!
//! Every replica, the proposer included, applies the entry through
//! [`enqueue_transaction_redo`]: the redo record is appended to this node's WAL,
//! its header carrying the entry's idempotency key, and installed through the
//! WAL replay arms. The key makes the record the entry's applied-marker, so an
//! entry re-delivered after a restart is recognised by the proposal ledger and
//! skipped before it reaches here.
//!
//! A refusal the Data Plane proves applied nothing (a constraint verdict) is
//! final: every replica reaches it at the same log position against the same
//! state, and the funnel cancels the record in the WAL before it returns. The
//! cancelling marker carries the entry's key, so the ledger counts the
//! refusal as the entry's outcome. It advances the durable prefix like a
//! success, because replaying the entry can only refuse it again.

use crate::bridge::envelope::Status;
use crate::control::array_sync::raft_apply::AppliedPosition;
use crate::control::distributed_applier::propose_tracker::{AppliedWrite, ProposeTracker};
use crate::control::server::dispatch_utils::{SubmitOutcome, refusal_is_final};
use crate::control::wal_replication::ReplicatedEntry;
use crate::control::wal_replication::decode::transaction_redo_payload;
use crate::control::wal_replication::transaction_redo::{RedoTarget, enqueue_transaction_redo};
use crate::types::{DatabaseId, TenantId, VShardId};

use super::context::{ApplyContext, FinishedApply, Started, StartedEntry};
use super::helpers::committed_response_result;
use super::proposal_gate::{EntryOutcome, ledger_outcome};
use super::start::Prepared;

/// Prepare one committed `TransactionRedo` entry. Its enqueue appends the
/// record and hands it to its core. The apply that follows resolves the
/// propose waiter. Its outcome says whether the entry's effect is durable on
/// this node, which is what the group's applied prefix records.
pub(super) fn prepare_transaction_redo_entry<'a>(
    ctx: ApplyContext<'a>,
    pos: AppliedPosition,
    entry: &ReplicatedEntry,
) -> Prepared<'a> {
    let ApplyContext { state, tracker, .. } = ctx;
    let payload = match transaction_redo_payload(&entry.write) {
        Ok(payload) => payload,
        Err(error) => {
            tracker.complete(pos.group_id, pos.log_index, pos.applied_key, Err(error));
            // The entry's own bytes are malformed; a re-delivery decodes the
            // same bytes and fails the same way, so it holds the floor rather
            // than skipping a committed transaction.
            return Prepared::Concluded(EntryOutcome::Applied {
                durable: false,
                result: None,
            });
        }
    };
    let target = RedoTarget {
        tenant_id: TenantId::new(entry.tenant_id),
        database_id: DatabaseId::new(entry.database_id),
        vshard_id: VShardId::new(entry.vshard_id),
    };
    Prepared::Enqueue(Box::pin(async move {
        let collection = payload.collections.first().cloned();
        let enqueued = enqueue_transaction_redo(
            state,
            target,
            &payload,
            pos.applied_key,
            pos.carried_commit_hlc(),
        )
        .await;
        let started = match enqueued {
            Ok(pending) => Started::Running(Box::pin(async move {
                let submitted = pending.finish(state).await;
                FinishedApply {
                    group_id: pos.group_id,
                    log_index: pos.log_index,
                    outcome: conclude_transaction_redo(tracker, pos, submitted),
                }
            })),
            Err(error) => Started::Concluded(conclude_transaction_redo(tracker, pos, Err(error))),
        };
        // A committed transaction's redo carries the rows it wrote.
        StartedEntry {
            started,
            collection,
            user_write: true,
        }
    }))
}

/// Resolve a transaction redo's waiter from what the funnel returned.
fn conclude_transaction_redo(
    tracker: &ProposeTracker,
    pos: AppliedPosition,
    submitted: crate::Result<SubmitOutcome>,
) -> EntryOutcome {
    let (result, durable) = match submitted {
        Ok(outcome) if outcome.response.status == Status::Ok => {
            (Ok(AppliedWrite::from_response(&outcome.response)), true)
        }
        Ok(outcome) => {
            let refused_finally = outcome
                .response
                .error_code
                .as_deref()
                .is_some_and(refusal_is_final);
            (
                committed_response_result(&outcome.response),
                refused_finally,
            )
        }
        Err(error) => {
            tracing::warn!(
                group_id = pos.group_id,
                index = pos.log_index,
                error = %error,
                "applying committed transaction redo failed"
            );
            (Err(error), false)
        }
    };
    let applied = ledger_outcome(&result);
    tracker.complete(pos.group_id, pos.log_index, pos.applied_key, result);
    EntryOutcome::Applied {
        durable,
        result: Some(applied),
    }
}
