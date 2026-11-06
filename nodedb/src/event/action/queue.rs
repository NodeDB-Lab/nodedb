// SPDX-License-Identifier: BUSL-1.1

//! Backoff retry queue for failed deferred actions.
//!
//! One entry is one action, keyed by [`ActionKey`]. Re-enqueuing an action
//! already in the queue updates that entry instead of adding a second: a
//! duplicate would retry the same action twice per round and double every
//! side effect it has.

use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use tracing::{debug, warn};

use super::record::{ActionKey, FailedAction};
use super::store::ActionStore;

/// Attempts allowed before an action goes to the DLQ.
const DEFAULT_MAX_RETRIES: u32 = 5;

/// First backoff delay. Doubles per attempt: 100, 200, 400, 800, 1600ms.
const BASE_BACKOFF: Duration = Duration::from_millis(100);

/// Ceiling on the backoff delay.
const MAX_BACKOFF: Duration = Duration::from_secs(10);

/// One queued action and when it next becomes due.
struct QueuedAction {
    action: FailedAction,
    next_retry_at: Instant,
}

/// Where a queue keeps its pending actions across restarts.
enum Durability {
    /// Nothing is kept; the queue lives only as long as the process.
    None,
    /// A store file that has not been opened yet.
    ///
    /// Almost every node runs with no failed actions at all, and opening a
    /// redb database per core costs real time on the startup path that
    /// readiness is measured against. The file is created the first time an
    /// action actually needs keeping.
    Deferred { data_dir: PathBuf, core_id: usize },
    /// An open store.
    Open(Box<ActionStore>),
}

/// Retry queue for deferred actions, durable when opened with a store.
pub struct ActionRetryQueue {
    queue: VecDeque<QueuedAction>,
    max_retries: u32,
    durability: Durability,
}

impl ActionRetryQueue {
    /// Open a durable queue under `data_dir`, restoring anything a previous
    /// run left pending.
    ///
    /// Restored actions are due immediately: the backoff they were waiting out
    /// was a live-process timer, and the restart already spent at least that
    /// long.
    pub fn open(data_dir: &Path, core_id: usize) -> crate::Result<Self> {
        let store = ActionStore::open(data_dir, core_id)?;
        let pending = store.load_all()?;
        let now = Instant::now();
        let queue = pending
            .into_iter()
            .map(|action| QueuedAction {
                action,
                next_retry_at: now,
            })
            .collect();
        Ok(Self {
            queue,
            max_retries: DEFAULT_MAX_RETRIES,
            durability: Durability::Open(Box::new(store)),
        })
    }

    /// Open a durable queue for one consumer core, falling back to an
    /// in-memory queue when the node has no data directory.
    ///
    /// A node with no data directory keeps nothing else across restarts
    /// either, so a durable action queue there would outlive the data its
    /// actions target.
    pub fn for_core(data_dir: &Path, core_id: usize) -> Self {
        if data_dir.as_os_str().is_empty() {
            return Self::in_memory();
        }
        // Only pay for opening the store when a previous run left actions in
        // it. With no file there is nothing to restore, and creating one now
        // would put a redb database creation per core on the startup path for
        // a queue that is almost always empty.
        if !ActionStore::path_for(data_dir, core_id).exists() {
            return Self {
                queue: VecDeque::new(),
                max_retries: DEFAULT_MAX_RETRIES,
                durability: Durability::Deferred {
                    data_dir: data_dir.to_path_buf(),
                    core_id,
                },
            };
        }
        match Self::open(data_dir, core_id) {
            Ok(queue) => queue,
            Err(e) => {
                warn!(
                    core_id,
                    error = %e,
                    "opening the durable action retry queue failed; \
                     retries for this core will not survive a restart"
                );
                Self::in_memory()
            }
        }
    }

    /// A queue that keeps nothing across restarts. For tests and for nodes
    /// running without an Event Plane data directory.
    pub fn in_memory() -> Self {
        Self {
            queue: VecDeque::new(),
            max_retries: DEFAULT_MAX_RETRIES,
            durability: Durability::None,
        }
    }

    /// A consistent image of the durable store, or `None` when this core has
    /// never kept an action and so has no store file.
    pub fn store_image(&self) -> crate::Result<Option<Vec<u8>>> {
        match &self.durability {
            Durability::Open(store) => store.image().map(Some),
            Durability::None | Durability::Deferred { .. } => Ok(None),
        }
    }

    /// The store, opening it now if this is the first action worth keeping.
    fn store(&mut self) -> Option<&ActionStore> {
        if let Durability::Deferred { data_dir, core_id } = &self.durability {
            let (data_dir, core_id) = (data_dir.clone(), *core_id);
            self.durability = match ActionStore::open(&data_dir, core_id) {
                Ok(store) => Durability::Open(Box::new(store)),
                Err(e) => {
                    warn!(
                        core_id,
                        error = %e,
                        "opening the durable action retry queue failed; \
                         retries for this core will not survive a restart"
                    );
                    Durability::None
                }
            };
        }
        match &self.durability {
            Durability::Open(store) => Some(store),
            Durability::None | Durability::Deferred { .. } => None,
        }
    }

    /// Queue an action for retry, counting this failure as an attempt.
    ///
    /// An action already queued under the same key replaces the queued entry
    /// rather than joining it.
    ///
    /// Returns `true` when the action's durable store holds it. On `false` it
    /// retries from memory only, and a restart loses it.
    pub fn enqueue(&mut self, mut action: FailedAction) -> bool {
        action.attempts = action.attempts.saturating_add(1);
        let backoff = compute_backoff(action.attempts);
        let next_retry_at = Instant::now() + backoff;

        debug!(
            owner = %action.owner(),
            collection = %action.context.collection,
            attempt = action.attempts,
            backoff_ms = backoff.as_millis(),
            "deferred action queued for retry"
        );

        let durable = match self.store() {
            Some(store) => match store.put(&action) {
                Ok(_) => true,
                Err(e) => {
                    // The action still retries from memory this run; only a
                    // restart loses it. Report rather than drop the retry.
                    warn!(owner = %action.owner(), error = %e, "persist pending action failed");
                    false
                }
            },
            None => false,
        };

        self.hold(action, next_retry_at);
        durable
    }

    /// Keep `action` in memory, replacing a queued entry with the same key.
    fn hold(&mut self, action: FailedAction, next_retry_at: Instant) {
        if let Some(existing) = self
            .queue
            .iter_mut()
            .find(|queued| queued.action.key == action.key)
        {
            existing.action = action;
            existing.next_retry_at = next_retry_at;
            return;
        }

        self.queue.push_back(QueuedAction {
            action,
            next_retry_at,
        });
    }

    /// Take every action whose backoff has elapsed.
    ///
    /// Returns `(ready_to_retry, exhausted)`. An exhausted action has spent
    /// its attempts and belongs in the DLQ. It stays in the durable set until
    /// the caller calls [`Self::complete`] after the DLQ accepts it, so a
    /// crash in between re-offers it to the DLQ after the restart. When the
    /// DLQ refuses it, the caller hands it back with [`Self::hold_exhausted`].
    pub fn drain_due(&mut self) -> (Vec<FailedAction>, Vec<FailedAction>) {
        let now = Instant::now();
        let mut ready = Vec::new();
        let mut exhausted = Vec::new();

        // Backoffs differ per action, so the queue is not ordered by due
        // time. Every entry is checked, and entries not yet due keep their
        // order.
        let (due, waiting): (VecDeque<_>, VecDeque<_>) = std::mem::take(&mut self.queue)
            .into_iter()
            .partition(|q| q.next_retry_at <= now);
        self.queue = waiting;

        for queued in due {
            if queued.action.attempts >= self.max_retries {
                warn!(
                    owner = %queued.action.owner(),
                    collection = %queued.action.context.collection,
                    attempts = queued.action.attempts,
                    "deferred action exhausted its retries, routing to DLQ"
                );
                exhausted.push(queued.action);
            } else {
                ready.push(queued.action);
            }
        }

        (ready, exhausted)
    }

    /// Keep an exhausted action the DLQ refused. The attempt count is not
    /// changed, so the action drains as exhausted again after `MAX_BACKOFF`
    /// and is offered to the DLQ once more. The durable set still holds it.
    pub fn hold_exhausted(&mut self, action: FailedAction) {
        self.hold(action, Instant::now() + MAX_BACKOFF);
    }

    /// Drop an action from the durable set once it has run to completion or
    /// the DLQ has accepted it.
    ///
    /// A crash before this lands replays the action, which is what makes the
    /// delivery guarantee at-least-once rather than at-most-once.
    pub fn complete(&mut self, key: &ActionKey) {
        self.forget(key);
    }

    fn forget(&mut self, key: &ActionKey) {
        // Never opens the store: with nothing kept there is nothing to forget.
        if let Durability::Open(store) = &self.durability
            && let Err(e) = store.remove(key)
        {
            warn!(error = %e, "removing completed action from the durable set failed");
        }
    }

    /// Take every queued action, due or not. For an in-memory queue whose
    /// owner keeps the retry state elsewhere: the trigger action lane
    /// collects one event's failures in one.
    pub fn take_all(&mut self) -> Vec<FailedAction> {
        std::mem::take(&mut self.queue)
            .into_iter()
            .map(|queued| queued.action)
            .collect()
    }

    /// Actions currently awaiting retry.
    pub fn len(&self) -> usize {
        self.queue.len()
    }

    pub fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }

    /// Time until the next action is due, or `None` when the queue is empty.
    pub fn next_retry_delay(&self) -> Option<Duration> {
        self.queue
            .iter()
            .map(|queued| queued.next_retry_at)
            .min()
            .map(|at| at.saturating_duration_since(Instant::now()))
    }
}

impl Default for ActionRetryQueue {
    fn default() -> Self {
        Self::in_memory()
    }
}

/// Exponential backoff: `BASE_BACKOFF * 2^(attempt-1)`, capped.
fn compute_backoff(attempt: u32) -> Duration {
    let multiplier = 1u64 << (attempt.saturating_sub(1).min(20));
    BASE_BACKOFF
        .saturating_mul(multiplier as u32)
        .min(MAX_BACKOFF)
}

#[cfg(test)]
mod tests {
    use super::super::record::{ActionContext, ActionId, ActionPayload};
    use super::*;
    use nodedb_types::DatabaseId;

    fn action(trigger: &str, lsn: u64) -> FailedAction {
        FailedAction {
            key: ActionKey {
                source_lsn: lsn,
                source_sequence: 1,
                source_vshard: 0,
                action: ActionId::TriggerRow {
                    trigger_name: trigger.into(),
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
                row_id: "order-1".into(),
                cascade_depth: 0,
            },
            attempts: 0,
            last_error: "timeout".into(),
        }
    }

    fn due_now(queue: &mut ActionRetryQueue) {
        for queued in queue.queue.iter_mut() {
            queued.next_retry_at = Instant::now();
        }
    }

    /// A DLQ requeue resolves its entry only on a durable hold.
    #[test]
    fn enqueue_reports_whether_the_store_holds_the_action() {
        assert!(!ActionRetryQueue::in_memory().enqueue(action("audit", 10)));
        let dir = tempfile::tempdir().expect("tempdir");
        let mut queue = ActionRetryQueue::open(dir.path(), 0).expect("open");
        assert!(queue.enqueue(action("audit", 10)));
    }

    #[test]
    fn backoff_doubles_and_caps() {
        assert_eq!(compute_backoff(1), Duration::from_millis(100));
        assert_eq!(compute_backoff(2), Duration::from_millis(200));
        assert_eq!(compute_backoff(5), Duration::from_millis(1600));
        assert!(compute_backoff(30) <= MAX_BACKOFF);
    }

    #[test]
    fn enqueue_counts_the_attempt_and_drains_when_due() {
        let mut queue = ActionRetryQueue::in_memory();
        queue.enqueue(action("audit", 10));
        due_now(&mut queue);
        let (ready, exhausted) = queue.drain_due();
        assert_eq!(ready.len(), 1);
        assert_eq!(ready[0].attempts, 1);
        assert!(exhausted.is_empty());
    }

    #[test]
    fn re_enqueueing_the_same_action_updates_it_rather_than_duplicating() {
        let mut queue = ActionRetryQueue::in_memory();
        queue.enqueue(action("audit", 10));
        let mut second = action("audit", 10);
        second.attempts = 1;
        queue.enqueue(second);
        assert_eq!(queue.len(), 1, "one action, one entry");
        due_now(&mut queue);
        let (ready, _) = queue.drain_due();
        assert_eq!(ready[0].attempts, 2);
    }

    #[test]
    fn different_actions_of_one_write_queue_separately() {
        let mut queue = ActionRetryQueue::in_memory();
        queue.enqueue(action("audit", 10));
        queue.enqueue(action("notify", 10));
        assert_eq!(queue.len(), 2);
    }

    #[test]
    fn the_same_action_from_a_later_write_queues_separately() {
        let mut queue = ActionRetryQueue::in_memory();
        queue.enqueue(action("audit", 10));
        queue.enqueue(action("audit", 11));
        assert_eq!(queue.len(), 2);
    }

    #[test]
    fn an_action_out_of_attempts_is_reported_exhausted() {
        let mut queue = ActionRetryQueue::in_memory();
        let mut spent = action("audit", 10);
        spent.attempts = DEFAULT_MAX_RETRIES;
        queue.enqueue(spent);
        due_now(&mut queue);
        let (ready, exhausted) = queue.drain_due();
        assert!(ready.is_empty());
        assert_eq!(exhausted.len(), 1);
    }

    #[test]
    fn an_action_behind_a_longer_backoff_still_drains_when_due() {
        let mut queue = ActionRetryQueue::in_memory();
        let mut spent = action("audit", 10);
        spent.attempts = DEFAULT_MAX_RETRIES;
        queue.hold_exhausted(spent);
        queue.enqueue(action("notify", 11));
        queue.queue[1].next_retry_at = Instant::now();
        let (ready, exhausted) = queue.drain_due();
        assert_eq!(ready.len(), 1, "a later entry is not blocked by the head");
        assert!(exhausted.is_empty());
        assert_eq!(queue.len(), 1);
    }

    #[test]
    fn an_exhausted_action_stays_durable_until_the_dlq_takes_it() {
        let dir = tempfile::tempdir().expect("tempdir");
        let key = action("audit", 10).key.clone();
        {
            let mut queue = ActionRetryQueue::open(dir.path(), 0).expect("open");
            let mut spent = action("audit", 10);
            spent.attempts = DEFAULT_MAX_RETRIES - 1;
            queue.enqueue(spent);
            due_now(&mut queue);
            let (_, exhausted) = queue.drain_due();
            assert_eq!(exhausted.len(), 1);
            // The DLQ refused it: the queue keeps it.
            queue.hold_exhausted(exhausted.into_iter().next().expect("one"));
            assert_eq!(queue.len(), 1);
        }
        {
            let mut queue = ActionRetryQueue::open(dir.path(), 0).expect("reopen");
            assert_eq!(queue.len(), 1, "a restart re-offers it to the DLQ");
            queue.complete(&key);
        }
        let reopened = ActionRetryQueue::open(dir.path(), 0).expect("reopen");
        assert_eq!(reopened.len(), 0, "once the DLQ takes it, it is gone");
    }

    #[test]
    fn a_core_with_no_pending_actions_creates_no_file() {
        let dir = tempfile::tempdir().expect("tempdir");
        let queue = ActionRetryQueue::for_core(dir.path(), 0);
        assert_eq!(queue.len(), 0);
        assert!(
            !ActionStore::path_for(dir.path(), 0).exists(),
            "startup must not pay for a store no action has needed yet"
        );
    }

    #[test]
    fn the_first_queued_action_creates_the_store() {
        let dir = tempfile::tempdir().expect("tempdir");
        {
            // redb holds an exclusive lock on the file, so the writer must be
            // gone before another queue can open it. One queue per core in a
            // single process is the only arrangement production uses.
            let mut queue = ActionRetryQueue::for_core(dir.path(), 0);
            queue.enqueue(action("audit", 10));
            assert!(ActionStore::path_for(dir.path(), 0).exists());
        }

        let reopened = ActionRetryQueue::for_core(dir.path(), 0);
        assert_eq!(reopened.len(), 1);
    }

    #[test]
    fn a_durable_queue_restores_pending_actions() {
        let dir = tempfile::tempdir().expect("tempdir");
        {
            let mut queue = ActionRetryQueue::open(dir.path(), 0).expect("open");
            queue.enqueue(action("audit", 10));
            queue.enqueue(action("notify", 10));
        }
        let reopened = ActionRetryQueue::open(dir.path(), 0).expect("reopen");
        assert_eq!(reopened.len(), 2, "a restart must not lose pending actions");
    }

    /// A captured store image placed into another data directory brings the
    /// pending actions back, as a snapshot restore does.
    #[test]
    fn pending_retries_survive_a_restore_of_the_store_image() {
        let source = tempfile::tempdir().expect("tempdir");
        let mut queue = ActionRetryQueue::for_core(source.path(), 0);
        assert_eq!(queue.store_image().expect("image"), None, "no store yet");
        queue.enqueue(action("audit", 10));
        queue.enqueue(action("notify", 11));
        let image = queue
            .store_image()
            .expect("image")
            .expect("a store holds the pending actions");

        let target = tempfile::tempdir().expect("tempdir");
        let path = ActionStore::path_for(target.path(), 0);
        std::fs::create_dir_all(path.parent().expect("parent")).expect("mkdir");
        std::fs::write(&path, image).expect("place image");
        let restored = ActionRetryQueue::for_core(target.path(), 0);
        assert_eq!(restored.len(), 2, "a restore must not lose pending retries");
    }

    #[test]
    fn a_completed_action_does_not_come_back_after_a_restart() {
        let dir = tempfile::tempdir().expect("tempdir");
        let key = action("audit", 10).key.clone();
        {
            let mut queue = ActionRetryQueue::open(dir.path(), 0).expect("open");
            queue.enqueue(action("audit", 10));
            queue.complete(&key);
        }
        let reopened = ActionRetryQueue::open(dir.path(), 0).expect("reopen");
        assert_eq!(reopened.len(), 0);
    }
}
