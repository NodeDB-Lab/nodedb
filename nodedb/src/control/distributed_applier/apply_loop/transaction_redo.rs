//! Apply path for a committed `ReplicatedWrite::TransactionRedo` entry.
//!
//! Every replica, the proposer included, applies the entry through
//! [`apply_transaction_redo`]: the redo record is appended to this node's WAL,
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

use std::sync::Arc;

use crate::bridge::envelope::Status;
use crate::control::array_sync::raft_apply::AppliedPosition;
use crate::control::distributed_applier::propose_tracker::{AppliedWrite, ProposeTracker};
use crate::control::server::dispatch_utils::refusal_is_final;
use crate::control::state::SharedState;
use crate::control::wal_replication::ReplicatedEntry;
use crate::control::wal_replication::decode::transaction_redo_payload;
use crate::control::wal_replication::transaction_redo::{RedoTarget, apply_transaction_redo};
use crate::types::{DatabaseId, TenantId, VShardId};

use super::helpers::committed_response_result;
use super::proposal_gate::{EntryOutcome, ledger_outcome};

/// Apply one committed `TransactionRedo` entry and resolve its propose
/// waiter. The outcome says whether the entry's effect is durable on this
/// node, which is what the caller's applied prefix records.
pub(super) async fn apply_transaction_redo_entry(
    state: &Arc<SharedState>,
    tracker: &Arc<ProposeTracker>,
    pos: AppliedPosition,
    entry: &ReplicatedEntry,
) -> EntryOutcome {
    let payload = match transaction_redo_payload(&entry.write) {
        Ok(payload) => payload,
        Err(error) => {
            tracker.complete(pos.group_id, pos.log_index, pos.applied_key, Err(error));
            // The entry's own bytes are malformed; a re-delivery decodes the
            // same bytes and fails the same way, so it holds the floor rather
            // than skipping a committed transaction.
            return EntryOutcome::Applied {
                durable: false,
                result: None,
            };
        }
    };
    let target = RedoTarget {
        tenant_id: TenantId::new(entry.tenant_id),
        database_id: DatabaseId::new(entry.database_id),
        vshard_id: VShardId::new(entry.vshard_id),
    };
    let submitted = apply_transaction_redo(state, target, &payload, pos.applied_key).await;

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
