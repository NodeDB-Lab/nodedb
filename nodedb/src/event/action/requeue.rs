// SPDX-License-Identifier: BUSL-1.1

//! Handing an action back to the Event Plane after it reached the DLQ.
//!
//! An action in the DLQ has spent its attempts. Putting it back is an operator
//! decision — the condition that failed it, a dropped collection or an offline
//! shard, is fixed outside the database. The operator's statement runs on the
//! Control Plane, and the action must run on the consumer that owns its
//! vShard, so it is left here and collected by that consumer on its next
//! retry poll.

use std::sync::Mutex;

use super::capture::RetryStoreCaptures;
use super::record::FailedAction;

/// An action sent back for another attempt, and the DLQ entry that keeps it
/// until the retry queue durably holds it.
#[derive(Debug)]
pub struct RequeuedAction {
    pub action: FailedAction,
    pub dlq_entry: u64,
}

/// Actions an operator has sent back for another attempt, parked per consumer
/// core, and the retry store capture requests each consumer answers.
///
/// Bounded per core: an operator requeueing faster than the Event Plane drains
/// must be refused rather than allowed to grow the inbox without limit.
pub struct ActionRequeueInbox {
    per_core: Vec<Mutex<Vec<RequeuedAction>>>,
    capacity_per_core: usize,
    retry_captures: RetryStoreCaptures,
}

/// Default depth of one core's inbox. Requeue is a manual operation; a
/// backlog this deep already means the operator is queueing faster than the
/// Event Plane can retry.
const DEFAULT_CAPACITY_PER_CORE: usize = 1024;

/// Why a requeue was refused.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum RequeueError {
    #[error("this node runs no event consumers, so there is nothing to requeue onto")]
    NoConsumers,

    #[error("the requeue inbox for core {core_id} is full ({capacity} actions awaiting pickup)")]
    InboxFull { core_id: usize, capacity: usize },
}

impl ActionRequeueInbox {
    /// One inbox per consumer core.
    pub fn for_cores(num_cores: usize) -> Self {
        Self {
            per_core: (0..num_cores).map(|_| Mutex::new(Vec::new())).collect(),
            capacity_per_core: DEFAULT_CAPACITY_PER_CORE,
            retry_captures: RetryStoreCaptures::for_cores(num_cores),
        }
    }

    /// The retry store capture requests, one slot list per consumer core.
    pub fn retry_captures(&self) -> &RetryStoreCaptures {
        &self.retry_captures
    }

    /// The consumer that owns `vshard_id`.
    ///
    /// The same `vshard_id % num_cores` mapping the Data Plane and WAL replay
    /// use, so a requeued action lands on the consumer that saw its source
    /// write.
    fn core_of(&self, vshard_id: u32) -> Option<usize> {
        let cores = self.per_core.len();
        if cores == 0 {
            return None;
        }
        Some(vshard_id as usize % cores)
    }

    /// Park an action for the consumer that owns its source vShard.
    ///
    /// The attempt count is reset: an operator requeue is a fresh decision
    /// that the action can now succeed, so it gets a full budget rather than
    /// the exhausted one that put it in the DLQ.
    pub fn submit(&self, mut action: FailedAction, dlq_entry: u64) -> Result<usize, RequeueError> {
        let core_id = self
            .core_of(action.key.source_vshard)
            .ok_or(RequeueError::NoConsumers)?;
        action.attempts = 0;

        let mut inbox = self.per_core[core_id]
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        if inbox.len() >= self.capacity_per_core {
            return Err(RequeueError::InboxFull {
                core_id,
                capacity: self.capacity_per_core,
            });
        }
        inbox.push(RequeuedAction { action, dlq_entry });
        Ok(core_id)
    }

    /// Take everything parked for `core_id`. Empty for an unknown core.
    pub fn take_for_core(&self, core_id: usize) -> Vec<RequeuedAction> {
        match self.per_core.get(core_id) {
            Some(inbox) => {
                std::mem::take(&mut *inbox.lock().unwrap_or_else(|poison| poison.into_inner()))
            }
            None => Vec::new(),
        }
    }

    /// Whether `core_id` has anything awaiting pickup.
    pub fn has_work_for(&self, core_id: usize) -> bool {
        self.per_core.get(core_id).is_some_and(|inbox| {
            !inbox
                .lock()
                .unwrap_or_else(|poison| poison.into_inner())
                .is_empty()
        })
    }

    /// Actions awaiting pickup across every core.
    pub fn pending(&self) -> usize {
        self.per_core
            .iter()
            .map(|inbox| {
                inbox
                    .lock()
                    .unwrap_or_else(|poison| poison.into_inner())
                    .len()
            })
            .sum()
    }
}

#[cfg(test)]
mod tests {
    use super::super::record::{ActionContext, ActionId, ActionKey, ActionPayload};
    use super::*;
    use nodedb_types::DatabaseId;

    fn action(vshard: u32, attempts: u32) -> FailedAction {
        FailedAction {
            key: ActionKey {
                source_lsn: 1,
                source_sequence: 1,
                source_vshard: vshard,
                action: ActionId::TriggerRow {
                    trigger_name: "audit".into(),
                },
            },
            payload: ActionPayload::TriggerRow {
                operation: "INSERT".into(),
                new_fields: None,
                old_fields: None,
            },
            context: ActionContext {
                database_id: DatabaseId::DEFAULT,
                tenant_id: 1,
                collection: "orders".into(),
                row_id: "r-1".into(),
                cascade_depth: 0,
            },
            attempts,
            last_error: "shard unavailable".into(),
        }
    }

    #[test]
    fn an_action_lands_on_the_core_owning_its_vshard() {
        let inbox = ActionRequeueInbox::for_cores(4);
        assert_eq!(inbox.submit(action(6, 5), 11).expect("submit"), 2);
        assert!(inbox.take_for_core(0).is_empty());
        let taken = inbox.take_for_core(2);
        assert_eq!(taken.len(), 1);
        assert_eq!(
            taken[0].dlq_entry, 11,
            "the pickup names the entry to settle"
        );
    }

    #[test]
    fn requeueing_restores_a_full_attempt_budget() {
        let inbox = ActionRequeueInbox::for_cores(1);
        inbox.submit(action(0, 5), 1).expect("submit");
        let taken = inbox.take_for_core(0);
        assert_eq!(
            taken[0].action.attempts, 0,
            "an operator requeue is a fresh decision, not a sixth attempt"
        );
    }

    #[test]
    fn taking_an_inbox_empties_it() {
        let inbox = ActionRequeueInbox::for_cores(1);
        inbox.submit(action(0, 0), 1).expect("submit");
        assert_eq!(inbox.take_for_core(0).len(), 1);
        assert!(inbox.take_for_core(0).is_empty());
        assert_eq!(inbox.pending(), 0);
    }

    #[test]
    fn a_node_with_no_consumers_refuses_the_requeue() {
        let inbox = ActionRequeueInbox::for_cores(0);
        assert_eq!(
            inbox.submit(action(0, 0), 1),
            Err(RequeueError::NoConsumers)
        );
    }

    #[test]
    fn a_full_inbox_refuses_rather_than_growing() {
        let mut inbox = ActionRequeueInbox::for_cores(1);
        inbox.capacity_per_core = 2;
        inbox.submit(action(0, 0), 1).expect("first");
        inbox.submit(action(0, 0), 2).expect("second");
        assert!(matches!(
            inbox.submit(action(0, 0), 3),
            Err(RequeueError::InboxFull { core_id: 0, .. })
        ));
    }

    #[test]
    fn a_core_reports_whether_it_has_work_waiting() {
        let inbox = ActionRequeueInbox::for_cores(2);
        assert!(!inbox.has_work_for(0));
        inbox.submit(action(0, 0), 1).expect("submit");
        assert!(inbox.has_work_for(0));
        assert!(!inbox.has_work_for(1));
        assert!(!inbox.has_work_for(9), "an unknown core never has work");
    }

    #[test]
    fn an_unknown_core_yields_nothing() {
        let inbox = ActionRequeueInbox::for_cores(2);
        assert!(inbox.take_for_core(9).is_empty());
    }
}
