// SPDX-License-Identifier: BUSL-1.1

//! What the data-group apply loop tells a vShard's scheduler about each
//! stamped Calvin redo it concluded.
//!
//! The apply loop pushes one [`CalvinApplyEvent`] per position, keyed by
//! `(epoch, position)`. The scheduler drains every event in key order. The
//! inbox is bounded: a push into a full inbox waits for room, and its entry
//! holds its own place in the group's lane meanwhile. Nothing is dropped.
//!
//! A vShard with no running scheduler has no registered inbox. The apply
//! loop then pushes nothing: the ledger it marks seeds the next scheduler.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};

use tokio::sync::Notify;

use crate::bridge::envelope::Response;

/// How the apply loop concluded one stamped Calvin redo.
#[derive(Debug, Clone)]
pub enum CalvinApplyEvent {
    /// The install is durable and the ledger holds the position. `reply` is
    /// the slice's rendered answer. Every replica renders the same one.
    RedoApplied {
        reply: Response,
        /// The slice writes a row the statement names.
        primary_write: bool,
        /// The slice answers rows, not a count.
        returning: bool,
    },
    /// The install refused for good. Every replica refuses the same entry.
    /// The position stays claimed, so no later copy installs.
    RedoRefused { error: String },
    /// The install did not become durable on this replica. The claim is
    /// released, and a restart applies the entry again.
    RedoNotApplied { error: String },
}

#[derive(Debug, Default)]
struct InboxState {
    events: BTreeMap<(u64, u32), CalvinApplyEvent>,
    /// The scheduler stopped: a push returns at once.
    closed: bool,
}

/// One vShard's inbox of apply events.
#[derive(Debug)]
pub struct CalvinVShardInbox {
    state: Mutex<InboxState>,
    /// Wakes the scheduler: an event arrived.
    ready: Notify,
    /// Wakes a waiting push: the scheduler took events.
    room: Notify,
    capacity: usize,
}

impl CalvinVShardInbox {
    /// An empty inbox holding at most `capacity` events. A zero capacity
    /// holds one.
    pub fn new(capacity: usize) -> Self {
        Self {
            state: Mutex::new(InboxState::default()),
            ready: Notify::new(),
            room: Notify::new(),
            capacity: capacity.max(1),
        }
    }

    /// Hand `event` for `(epoch, position)` to the scheduler. Waits while
    /// the inbox is full. Returns once the event is in, or at once when the
    /// scheduler stopped.
    pub async fn push(&self, epoch: u64, position: u32, event: CalvinApplyEvent) {
        let mut event = Some(event);
        loop {
            let room = self.room.notified();
            tokio::pin!(room);
            room.as_mut().enable();
            {
                let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
                if state.closed {
                    return;
                }
                let key = (epoch, position);
                if state.events.len() < self.capacity || state.events.contains_key(&key) {
                    if let Some(event) = event.take() {
                        state.events.insert(key, event);
                    }
                    drop(state);
                    self.ready.notify_one();
                    return;
                }
            }
            room.await;
        }
    }

    /// Take every waiting event, in `(epoch, position)` order.
    pub fn take_all(&self) -> Vec<((u64, u32), CalvinApplyEvent)> {
        let taken = {
            let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
            std::mem::take(&mut state.events)
        };
        if !taken.is_empty() {
            self.room.notify_waiters();
        }
        taken.into_iter().collect()
    }

    /// Resolves once an event waits.
    pub async fn ready(&self) {
        loop {
            let ready = self.ready.notified();
            tokio::pin!(ready);
            ready.as_mut().enable();
            if !self
                .state
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .events
                .is_empty()
            {
                return;
            }
            ready.await;
        }
    }

    /// The scheduler stopped: every waiting push and every later push
    /// returns at once.
    fn close(&self) {
        self.state.lock().unwrap_or_else(|p| p.into_inner()).closed = true;
        self.room.notify_waiters();
    }

    /// How many events wait.
    #[cfg(test)]
    pub fn waiting(&self) -> usize {
        self.state
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .events
            .len()
    }
}

type Inboxes = Arc<Mutex<HashMap<u32, Arc<CalvinVShardInbox>>>>;

/// The inbox of every vShard whose scheduler runs on this node.
#[derive(Debug, Default)]
pub struct CalvinInboxes {
    by_vshard: Inboxes,
}

impl CalvinInboxes {
    /// Register the inbox of the scheduler starting for `vshard_id`. It
    /// replaces a predecessor's.
    pub fn register(&self, vshard_id: u32, capacity: usize) -> InboxHandle {
        let inbox = Arc::new(CalvinVShardInbox::new(capacity));
        let replaced = self
            .by_vshard
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .insert(vshard_id, Arc::clone(&inbox));
        if let Some(previous) = replaced {
            previous.close();
        }
        InboxHandle {
            vshard_id,
            inbox,
            by_vshard: Arc::clone(&self.by_vshard),
        }
    }

    /// The inbox of `vshard_id`'s running scheduler.
    pub fn get(&self, vshard_id: u32) -> Option<Arc<CalvinVShardInbox>> {
        self.by_vshard
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .get(&vshard_id)
            .cloned()
    }
}

/// A scheduler's registered inbox. Dropping it closes the inbox and
/// removes it, unless a successor replaced it.
#[derive(Debug)]
pub struct InboxHandle {
    vshard_id: u32,
    inbox: Arc<CalvinVShardInbox>,
    by_vshard: Inboxes,
}

impl InboxHandle {
    /// The inbox this handle registered.
    pub fn inbox(&self) -> &Arc<CalvinVShardInbox> {
        &self.inbox
    }
}

impl Drop for InboxHandle {
    fn drop(&mut self) {
        self.inbox.close();
        let mut by_vshard = self.by_vshard.lock().unwrap_or_else(|p| p.into_inner());
        if by_vshard
            .get(&self.vshard_id)
            .is_some_and(|inbox| Arc::ptr_eq(inbox, &self.inbox))
        {
            by_vshard.remove(&self.vshard_id);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    fn refused(text: &str) -> CalvinApplyEvent {
        CalvinApplyEvent::RedoRefused {
            error: text.to_owned(),
        }
    }

    /// Events leave the inbox in `(epoch, position)` order, whatever order
    /// they arrived in.
    #[tokio::test]
    async fn events_drain_in_position_order() {
        let inbox = CalvinVShardInbox::new(8);
        inbox.push(4, 1, refused("b")).await;
        inbox.push(3, 7, refused("a")).await;
        inbox.push(4, 0, refused("c")).await;
        let keys: Vec<(u64, u32)> = inbox.take_all().into_iter().map(|(k, _)| k).collect();
        assert_eq!(keys, vec![(3, 7), (4, 0), (4, 1)]);
        assert_eq!(inbox.waiting(), 0);
    }

    /// A push into a full inbox waits until the scheduler takes events.
    #[tokio::test]
    async fn a_full_inbox_holds_the_push_until_events_leave() {
        let inbox = Arc::new(CalvinVShardInbox::new(1));
        inbox.push(1, 0, refused("first")).await;
        let pushing = {
            let inbox = Arc::clone(&inbox);
            tokio::spawn(async move { inbox.push(1, 1, refused("second")).await })
        };
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(!pushing.is_finished(), "the push waits for room");
        assert_eq!(inbox.take_all().len(), 1);
        tokio::time::timeout(Duration::from_secs(5), pushing)
            .await
            .expect("the push finishes once room frees")
            .expect("push task");
        assert_eq!(inbox.waiting(), 1);
    }

    /// A stopped scheduler's inbox takes nothing, and a waiting push
    /// returns.
    #[tokio::test]
    async fn a_dropped_handle_releases_a_waiting_push() {
        let inboxes = CalvinInboxes::default();
        let handle = inboxes.register(5, 1);
        let inbox = Arc::clone(handle.inbox());
        inbox.push(1, 0, refused("first")).await;
        let pushing = {
            let inbox = Arc::clone(&inbox);
            tokio::spawn(async move { inbox.push(1, 1, refused("second")).await })
        };
        drop(handle);
        tokio::time::timeout(Duration::from_secs(5), pushing)
            .await
            .expect("a closed inbox releases the push")
            .expect("push task");
        assert!(inboxes.get(5).is_none());
    }

    /// A successor's registration replaces the inbox, and the predecessor's
    /// drop leaves it in place.
    #[test]
    fn a_successor_keeps_its_inbox_when_the_predecessor_drops() {
        let inboxes = CalvinInboxes::default();
        let first = inboxes.register(2, 4);
        let second = inboxes.register(2, 4);
        drop(first);
        let current = inboxes.get(2).expect("the successor's inbox");
        assert!(Arc::ptr_eq(&current, second.inbox()));
    }
}
