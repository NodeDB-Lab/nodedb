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
    tracing::debug!(
        group_id = held.group_id,
        log_index = held.log_index,
        metadata_floor = held.metadata_floor,
        metadata_applied = watcher.current(),
        "a replicated write holds its group's lane for this node's metadata apply"
    );
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

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, Ordering};

    use super::*;
    use crate::bridge::dispatch::Dispatcher;
    use crate::wal::WalManager;

    /// A follower whose metadata apply lags a collection's creation holds a
    /// data entry for that collection until the creation applied. The
    /// metadata watcher bumps only after the applier returned, and its
    /// return includes the creation's post-apply storage clear. The write
    /// therefore lands after the clear, and survives it.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_lagging_followers_write_waits_for_the_create() {
        let directory = tempfile::tempdir().expect("temporary WAL directory");
        let wal = Arc::new(
            WalManager::open_for_testing(&directory.path().join("hold.wal")).expect("test WAL"),
        );
        let (dispatcher, _sides) = Dispatcher::new(1, 64);
        let state = SharedState::new(dispatcher, wal).expect("shared state");
        let tracker = Arc::new(ProposeTracker::new());
        let watcher = state.applied_index_watcher(METADATA_GROUP_ID);
        let create_index = watcher.current() + 2;

        let enqueued = Arc::new(AtomicBool::new(false));
        let flag = Arc::clone(&enqueued);
        let write = Prepared::Enqueue(Box::pin(async move {
            flag.store(true, Ordering::SeqCst);
            StartedEntry::concluded(EntryOutcome::Skipped)
        }));
        let held = HeldEntry {
            group_id: 7,
            log_index: 1,
            proposal_key: 0,
            metadata_floor: create_index,
        };
        let Prepared::Enqueue(mut hold) = hold_for_metadata(&state, &tracker, held, write) else {
            panic!("a write below its floor stays an enqueue behind the hold");
        };

        assert!(
            tokio::time::timeout(Duration::from_millis(200), &mut hold)
                .await
                .is_err(),
            "the write waits while the create is unapplied"
        );
        assert!(!enqueued.load(Ordering::SeqCst));

        watcher.bump(create_index);
        tokio::time::timeout(Duration::from_secs(10), hold)
            .await
            .expect("the write proceeds once the create applied");
        assert!(enqueued.load(Ordering::SeqCst));
    }
}
