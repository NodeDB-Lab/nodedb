// SPDX-License-Identifier: BUSL-1.1

//! Firing held events' actions on the partition's owner.
//!
//! 1. Every replica of a write holds each of its firing events in its
//!    ledger, under the event's replicated position (`hold`).
//! 2. The node that holds the leader lease of the partition's data group
//!    owns the partition. It fires the held events in position order, from
//!    the replicated cursor, and commits the cursor past them. It checks the
//!    lease before each event and before the cursor commit, so a node whose
//!    lease lapsed or moved to a later term stops before it acts. The lease
//!    lapses before another node can win the group's election, so two owners
//!    never fire at once.
//! 3. A new owner resumes after the cursor. It never skips an event: every
//!    replica held it, and only the cursor releases it.
//! 4. An event the old owner fired after its last cursor commit fires again
//!    on the new owner. Each action names its event by the event's
//!    replicated position (`position::action_identity`). A trigger body and
//!    a DEFINE EVENT action commit that key with their writes on every
//!    replica, and an action whose key is recorded does not run again. A
//!    body's cross-node writes travel as one request keyed the same way,
//!    which its receiver applies once.
//! 5. An event whose action failed holds its partition's cursor. The owner
//!    retries the failed actions with backoff, and dead-letters them once
//!    they spent their attempts. The event stays held on every replica until
//!    then, so a new owner fires it again: an action that committed does not
//!    run again, and one that failed runs.
//! 6. An event whose collection has no action at firing passes with no work.
//!    The owner commits the cursor over such events at most once per
//!    [`IDLE_CURSOR_COMMIT`], so a write without actions costs no metadata
//!    proposal of its own.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::watch;
use tokio::time::Instant;
use tracing::{debug, error, warn};

use crate::control::state::SharedState;
use crate::event::action::{ActionRetryQueue, FailedAction};
use crate::event::cdc::CdcOffset;
use crate::event::topic::committed::key::delivery_lease;
use crate::event::types::WriteEvent;

use super::cursor::{commit_fired, fired_through};
use super::held::HeldAction;
use super::hold::lane;

/// Events one firing pass fires from one partition.
const FIRING_BATCH: usize = 256;
/// Longest pause between firing passes that found nothing to fire. A held
/// event wakes the task at once.
const FIRING_IDLE: Duration = Duration::from_millis(100);
/// Longest a partition's cursor waits over events that fired no action.
pub const IDLE_CURSOR_COMMIT: Duration = Duration::from_secs(2);
/// Attempts a failed action gets before the owner dead-letters it.
const MAX_ACTION_ATTEMPTS: u32 = 5;
/// Longest backoff between retries of an event's failed actions.
const MAX_RETRY_BACKOFF: Duration = Duration::from_secs(2);

/// The lease a partition is owned under: `(group, term)`.
type Lease = (u64, u64);

/// An event whose actions failed, with what still owes a retry.
#[derive(Debug)]
struct Failing {
    lease: Lease,
    position: CdcOffset,
    actions: Vec<FailedAction>,
    retry_at: Instant,
}

/// What this node's firing task keeps between passes, per partition.
#[derive(Debug, Default)]
pub struct FiringState {
    /// Events fired past the committed cursor, under the lease that fired
    /// them. A cursor commit that did not land leaves them here, so the same
    /// owner does not fire them again. A new lease starts from the cursor.
    ahead: HashMap<u32, (Lease, CdcOffset)>,
    /// The event holding each partition's cursor on a failed action.
    failing: HashMap<u32, Failing>,
    /// When each partition's cursor was last committed.
    committed_at: HashMap<u32, Instant>,
}

impl FiringState {
    /// Where the owner under `lease` resumes `partition`, whose cursor is at
    /// `cursor`.
    fn resume(&self, partition: u32, lease: Lease, cursor: CdcOffset) -> CdcOffset {
        match self.ahead.get(&partition) {
            Some((fired_under, position)) if *fired_under == lease && *position > cursor => {
                *position
            }
            _ => cursor,
        }
    }

    fn record(&mut self, partition: u32, lease: Lease, position: CdcOffset) {
        self.ahead.insert(partition, (lease, position));
    }

    /// Whether the cursor over events with no action is due a commit.
    fn idle_commit_due(&self, partition: u32) -> bool {
        self.committed_at
            .get(&partition)
            .is_none_or(|at| at.elapsed() >= IDLE_CURSOR_COMMIT)
    }
}

/// Whether this node's catalog holds an action for `event`'s collection: an
/// enabled AFTER trigger the Event Plane fires, or a DEFINE EVENT.
fn has_actions(state: &SharedState, event: &WriteEvent) -> bool {
    state
        .trigger_registry
        .interest()
        .contains(event.database_id, &event.collection)
        || state
            .credentials
            .catalog()
            .event_definitions(
                event.database_id,
                event.tenant_id.as_u64(),
                &event.collection,
            )
            .is_some_and(|defs| !defs.is_empty())
}

/// Fire the held events of every partition this node owns, and release on
/// every node the events its partition's cursor passed.
///
/// Returns how many events the pass fired under a committed cursor.
pub async fn fire_held_actions(state: &Arc<SharedState>, firing: &mut FiringState) -> usize {
    let Some(lane) = lane(state) else {
        return 0;
    };
    #[cfg(feature = "failpoints")]
    if !crate::control::fail_gate::action_firing(
        state.node_id,
        "before_firing",
        state.shutdown.raw_receiver(),
    )
    .await
    {
        return 0;
    }
    let partitions = match lane.ledger.partitions() {
        Ok(partitions) => partitions,
        Err(error) => {
            warn!(error = %error, "trigger action ledger unreadable; no firing this pass");
            return 0;
        }
    };
    let mut fired = 0;
    for partition in partitions {
        let through = fired_through(state, partition);
        if let Err(error) = lane.ledger.release_through(partition, through) {
            warn!(partition, error = %error, "fired trigger actions not released");
        }
        let Some(lease) = delivery_lease(state, partition) else {
            firing.failing.remove(&partition);
            continue;
        };
        if retry_failing(state, firing, partition, lease).await {
            continue;
        }
        let from = firing.resume(partition, lease, through);
        let held = match lane.ledger.held_after(partition, from, FIRING_BATCH) {
            Ok(held) => held,
            Err(error) => {
                warn!(partition, error = %error, "held trigger actions unreadable");
                continue;
            }
        };
        let owner = Owner {
            partition,
            lease,
            resumed: from,
            cursor: through,
        };
        fired += fire_partition(state, firing, owner, held).await;
    }
    fired
}

/// Retry the failing event of `partition`, when it has one. Returns whether
/// it still holds the partition's cursor.
async fn retry_failing(
    state: &Arc<SharedState>,
    firing: &mut FiringState,
    partition: u32,
    lease: Lease,
) -> bool {
    let Some(failing) = firing.failing.remove(&partition) else {
        return false;
    };
    // Another lease fires the event again from the cursor.
    if failing.lease != lease {
        return false;
    }
    if Instant::now() < failing.retry_at {
        firing.failing.insert(partition, failing);
        return true;
    }
    let mut scratch = ActionRetryQueue::in_memory();
    for action in &failing.actions {
        crate::event::trigger::dispatcher::retry_action(action, state, &mut scratch).await;
    }
    let remaining = dead_letter_spent(state, scratch.take_all());
    if remaining.is_empty() {
        firing.record(partition, lease, failing.position);
        return false;
    }
    firing.failing.insert(
        partition,
        Failing {
            retry_at: retry_at(&remaining),
            actions: remaining,
            ..failing
        },
    );
    true
}

/// When the next retry of `actions` is due.
fn retry_at(actions: &[FailedAction]) -> Instant {
    let attempts = actions.iter().map(|a| a.attempts).max().unwrap_or(1);
    let backoff = Duration::from_millis(100u64 << attempts.min(5)).min(MAX_RETRY_BACKOFF);
    Instant::now() + backoff
}

/// Dead-letter every action of `actions` that spent its attempts. Returns
/// the actions that still retry, and those the DLQ refused.
fn dead_letter_spent(state: &SharedState, actions: Vec<FailedAction>) -> Vec<FailedAction> {
    let (spent, retrying): (Vec<_>, Vec<_>) = actions
        .into_iter()
        .partition(|action| action.attempts >= MAX_ACTION_ATTEMPTS);
    let mut remaining = retrying;
    let Some(dlq) = state.trigger_dlq.get() else {
        remaining.extend(spent);
        return remaining;
    };
    let mut dlq = dlq.lock().unwrap_or_else(|p| p.into_inner());
    for action in spent {
        if let Err(error) = dlq.enqueue(action.clone()) {
            error!(
                owner = %action.owner(),
                error = %error,
                "trigger DLQ refused an exhausted action; it keeps its event's cursor"
            );
            remaining.push(action);
        }
    }
    remaining
}

/// The partition a firing pass owns, the lease it owns it under, where it
/// resumed, and the committed cursor.
struct Owner {
    partition: u32,
    lease: Lease,
    resumed: CdcOffset,
    cursor: CdcOffset,
}

/// Fire one partition's held events in order, stopping once this node loses
/// the lease or an event's action fails, and commit the cursor past every
/// event fired under the lease. Returns how many fired under a committed
/// cursor.
async fn fire_partition(
    state: &Arc<SharedState>,
    firing: &mut FiringState,
    owner: Owner,
    held: Vec<(CdcOffset, HeldAction)>,
) -> usize {
    let Owner {
        partition,
        lease,
        resumed,
        cursor,
    } = owner;
    // Events fired earlier under this lease whose cursor commit did not land.
    let mut last = (resumed > cursor).then_some(resumed);
    let mut acted = last.is_some();
    let mut fired = 0;
    for (position, action) in held {
        if delivery_lease(state, partition) != Some(lease) {
            break;
        }
        let event = match action.to_event(position) {
            Ok(event) => event,
            // A held event that does not rebuild fires nothing on any owner.
            Err(error) => {
                warn!(
                    partition,
                    position = %position,
                    error = %error,
                    "a held trigger action does not rebuild; it is not fired"
                );
                fired += 1;
                last = Some(position);
                firing.record(partition, lease, position);
                continue;
            }
        };
        if has_actions(state, &event) {
            acted = true;
            let mut scratch = ActionRetryQueue::in_memory();
            crate::event::trigger::dispatcher::dispatch_triggers(&event, state, &mut scratch).await;
            crate::control::event_trigger::process_write_event(
                Arc::clone(state),
                &event,
                &mut scratch,
            )
            .await;
            let failed = dead_letter_spent(state, scratch.take_all());
            if !failed.is_empty() {
                firing.failing.insert(
                    partition,
                    Failing {
                        lease,
                        position,
                        retry_at: retry_at(&failed),
                        actions: failed,
                    },
                );
                break;
            }
        }
        fired += 1;
        last = Some(position);
        firing.record(partition, lease, position);
    }
    let Some(through) = last else {
        return fired;
    };
    if !acted && !firing.idle_commit_due(partition) {
        return fired;
    }
    // A lease lost mid-batch leaves the cursor to the next owner, which fires
    // the rest. An action that already committed does not run again.
    if delivery_lease(state, partition) != Some(lease) {
        return fired;
    }
    #[cfg(feature = "failpoints")]
    if !crate::control::fail_gate::action_firing(
        state.node_id,
        "before_cursor_commit",
        state.shutdown.raw_receiver(),
    )
    .await
    {
        return fired;
    }
    match commit_fired(state, partition, through).await {
        Ok(()) => {
            firing.committed_at.insert(partition, Instant::now());
            fired
        }
        Err(error) => {
            debug!(
                partition,
                error = %error,
                "trigger firing cursor not committed; the next pass commits it again"
            );
            0
        }
    }
}

/// Run firing passes until shutdown. One task per node.
pub fn spawn_action_firing(
    state: Arc<SharedState>,
    mut shutdown: watch::Receiver<bool>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut firing = FiringState::default();
        loop {
            if *shutdown.borrow() {
                return;
            }
            if fire_held_actions(&state, &mut firing).await > 0 {
                tokio::task::yield_now().await;
                continue;
            }
            let Some(lane) = lane(&state) else {
                tokio::select! {
                    _ = tokio::time::sleep(FIRING_IDLE) => {}
                    _ = shutdown.changed() => {}
                }
                continue;
            };
            tokio::select! {
                _ = tokio::time::sleep(FIRING_IDLE) => {}
                _ = lane.wake.notified() => {}
                _ = shutdown.changed() => {}
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_owner_resumes_past_what_it_fired_under_its_own_lease_only() {
        let cursor = CdcOffset::data_event(0, 5, 1);
        let fired = CdcOffset::data_event(0, 9, 2);
        let mut firing = FiringState::default();
        assert_eq!(firing.resume(3, (1, 4), cursor), cursor);

        firing.record(3, (1, 4), fired);
        assert_eq!(firing.resume(3, (1, 4), cursor), fired);
        // A later term is a new lease: it starts from the cursor.
        assert_eq!(firing.resume(3, (1, 5), cursor), cursor);
        // Another partition keeps its own progress.
        assert_eq!(firing.resume(4, (1, 4), cursor), cursor);
        // A cursor past the recorded progress wins.
        let later = CdcOffset::data_event(0, 12, 1);
        assert_eq!(firing.resume(3, (1, 4), later), later);
    }

    #[test]
    fn a_partition_never_committed_is_due_an_idle_commit() {
        let mut firing = FiringState::default();
        assert!(firing.idle_commit_due(7));
        firing.committed_at.insert(7, Instant::now());
        assert!(!firing.idle_commit_due(7));
    }
}
