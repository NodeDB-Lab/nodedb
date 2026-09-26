// SPDX-License-Identifier: BUSL-1.1

//! Hold a replicated write until this node's catalog reached the one its
//! proposer planned it against.
//!
//! A data group and the metadata group apply on independent loops. Without
//! this hold, a replica can apply a write to a collection before it applied
//! the metadata entries its proposer had already applied:
//!
//! - a same-name collection's purge, whose storage reclaim then removes the
//!   write's rows;
//! - the collection's creation, whose registration the write needs.
//!
//! The proposer stamps its applied metadata index on the entry
//! (`ReplicatedEntry::metadata_floor`). The hold runs before the write's
//! enqueue, so the group's later entries wait behind it in log order.

use std::sync::Arc;
use std::time::Duration;

use nodedb_cluster::{METADATA_GROUP_ID, WaitOutcome};

use crate::control::distributed_applier::propose_tracker::ProposeTracker;
use crate::control::state::SharedState;

use super::context::{FinishedApply, StartedEntry};
use super::proposal_gate::{EntryOutcome, ledger_outcome};
use super::start::Prepared;

/// How long one wait slice lasts before the hold logs that it still waits.
const WAIT_SLICE: Duration = Duration::from_secs(5);

/// The entry a hold belongs to.
#[derive(Debug, Clone, Copy)]
pub(super) struct HeldEntry {
    pub group_id: u64,
    pub log_index: u64,
    pub proposal_key: u64,
    /// The metadata index the write waits for. `0` holds nothing.
    pub metadata_floor: u64,
}

/// Put the metadata hold in front of `prepared`'s enqueue or apply.
pub(super) fn hold_for_metadata<'a>(
    state: &'a Arc<SharedState>,
    tracker: &'a Arc<ProposeTracker>,
    held: HeldEntry,
    prepared: Prepared<'a>,
) -> Prepared<'a> {
    if held.metadata_floor == 0
        || state.applied_index_watcher(METADATA_GROUP_ID).current() >= held.metadata_floor
    {
        return prepared;
    }
    match prepared {
        Prepared::Enqueue(enqueue) => Prepared::Enqueue(Box::pin(async move {
            match await_metadata_floor(state, held).await {
                Ok(()) => enqueue.await,
                Err(error) => StartedEntry::concluded(conclude_unapplied(tracker, held, error)),
            }
        })),
        Prepared::Exclusive(apply) => Prepared::Exclusive(Box::pin(async move {
            match await_metadata_floor(state, held).await {
                Ok(()) => apply.await,
                Err(error) => FinishedApply {
                    group_id: held.group_id,
                    log_index: held.log_index,
                    outcome: conclude_unapplied(tracker, held, error),
                },
            }
        })),
        other @ (Prepared::Concluded(_) | Prepared::Barrier) => other,
    }
}

/// Wait until this node applied the metadata group through the entry's
/// floor. The wait has no deadline: applying the write earlier breaks the
/// order it holds. It fails only when the metadata group left this node.
async fn await_metadata_floor(state: &SharedState, held: HeldEntry) -> crate::Result<()> {
    let watcher = state.applied_index_watcher(METADATA_GROUP_ID);
    loop {
        let waiting = Arc::clone(&watcher);
        let floor = held.metadata_floor;
        let outcome = tokio::task::spawn_blocking(move || waiting.wait_for(floor, WAIT_SLICE))
            .await
            .map_err(|e| crate::Error::Internal {
                detail: format!(
                    "raft group {} entry {}: the metadata catch-up wait did not finish: {e}",
                    held.group_id, held.log_index
                ),
            })?;
        match outcome {
            WaitOutcome::Reached => return Ok(()),
            WaitOutcome::TimedOut => tracing::warn!(
                group_id = held.group_id,
                log_index = held.log_index,
                metadata_floor = floor,
                metadata_applied = watcher.current(),
                "a replicated write waits for this node's metadata apply to reach the \
                 catalog its proposer planned it against"
            ),
            WaitOutcome::GroupGone => {
                return Err(crate::Error::Internal {
                    detail: format!(
                        "raft group {} entry {}: the metadata group left this node before it \
                         applied index {floor}, the catalog the write was planned against; \
                         the write stays unapplied and replays on the next boot",
                        held.group_id, held.log_index
                    ),
                });
            }
        }
    }
}

/// Resolve the entry's waiter with `error`. The entry is not durable, so it
/// holds the group's applied floor and replays on the next boot.
fn conclude_unapplied(
    tracker: &ProposeTracker,
    held: HeldEntry,
    error: crate::Error,
) -> EntryOutcome {
    let result = Err(error);
    let applied = ledger_outcome(&result);
    tracker.complete(held.group_id, held.log_index, held.proposal_key, result);
    EntryOutcome::Applied {
        durable: false,
        result: Some(applied),
    }
}
