// SPDX-License-Identifier: BUSL-1.1

//! One server-push channel of a session, as the session loop waits on it.
//!
//! The loop waits on every queue at once, so a push wakes it with no inbound
//! frame. Waiting takes the first item off the channel and holds it in the
//! queue's buffer. The pump takes the buffered item before any further item,
//! so waiting loses and reorders nothing. The buffer holds at most one item:
//! a wait buffers an item only into an empty buffer.
//!
//! Every item has a position: how many items the queue received before it.
//! A resync reads [`OutboundQueue::end`] before its snapshot read, and the
//! pump then skips the stale items before that position. Nothing is moved
//! or dropped in place, so the queue stays bounded by the channel.

use tokio::sync::mpsc::Receiver;

/// What a wait on a queue found.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum QueueWake {
    /// An item is ready.
    Ready,
    /// The producer side closed: the registration behind the queue is gone.
    Closed,
}

/// A bounded receiver, the item a wait took off it, and the count of items
/// taken off it so far.
pub(super) struct OutboundQueue<T> {
    rx: Option<Receiver<T>>,
    buffered: Option<T>,
    /// Items taken off the channel since the receiver was attached,
    /// buffered or pushed.
    taken: u64,
}

impl<T> Default for OutboundQueue<T> {
    fn default() -> Self {
        Self {
            rx: None,
            buffered: None,
            taken: 0,
        }
    }
}

impl<T> OutboundQueue<T> {
    /// Attach the receiver of a new registration. Positions start again at
    /// zero.
    pub(super) fn attach(&mut self, rx: Receiver<T>) {
        self.rx = Some(rx);
        self.buffered = None;
        self.taken = 0;
    }

    /// Wait until an item is ready or the producer closes. A queue with no
    /// receiver never wakes.
    pub(super) async fn wait(&mut self) -> QueueWake {
        if self.buffered.is_some() {
            return QueueWake::Ready;
        }
        let Some(rx) = self.rx.as_mut() else {
            return std::future::pending().await;
        };
        match rx.recv().await {
            Some(item) => {
                self.buffered = Some(item);
                self.taken += 1;
                QueueWake::Ready
            }
            None => {
                self.rx = None;
                QueueWake::Closed
            }
        }
    }

    /// The next item without waiting: the buffered item first, then the
    /// channel.
    pub(super) fn next(&mut self) -> Option<T> {
        self.next_positioned().map(|(_, item)| item)
    }

    /// The next item and its position.
    pub(super) fn next_positioned(&mut self) -> Option<(u64, T)> {
        if let Some(item) = self.buffered.take() {
            return Some((self.taken - 1, item));
        }
        let item = self.rx.as_mut()?.try_recv().ok()?;
        self.taken += 1;
        Some((self.taken - 1, item))
    }

    /// The position the next item sent to the queue takes. Every item queued
    /// now has a lower position.
    pub(super) fn end(&self) -> u64 {
        self.taken + self.rx.as_ref().map_or(0, |rx| rx.len() as u64)
    }

    /// Whether nothing is waiting to be pushed.
    pub(super) fn is_empty(&self) -> bool {
        self.buffered.is_none() && self.rx.as_ref().is_none_or(Receiver::is_empty)
    }

    /// Whether a receiver is attached.
    #[cfg(test)]
    pub(super) fn is_attached(&self) -> bool {
        self.rx.is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn waiting_holds_the_item_for_the_next_take() {
        let (tx, rx) = tokio::sync::mpsc::channel(4);
        let mut queue = OutboundQueue::default();
        queue.attach(rx);
        tx.send(1).await.expect("send");
        tx.send(2).await.expect("send");

        assert_eq!(queue.wait().await, QueueWake::Ready);
        assert_eq!(queue.next(), Some(1));
        assert_eq!(queue.next(), Some(2));
        assert_eq!(queue.next(), None);
    }

    #[tokio::test]
    async fn items_queued_before_the_end_have_lower_positions() {
        let (tx, rx) = tokio::sync::mpsc::channel(8);
        let mut queue = OutboundQueue::default();
        queue.attach(rx);
        for item in [10, 11, 12] {
            tx.send(item).await.expect("send");
        }
        assert_eq!(queue.wait().await, QueueWake::Ready);

        let end = queue.end();
        assert_eq!(end, 3);
        tx.send(13).await.expect("send");

        let positioned: Vec<(u64, i32)> = std::iter::from_fn(|| queue.next_positioned()).collect();
        assert_eq!(positioned, vec![(0, 10), (1, 11), (2, 12), (3, 13)]);
        assert!(
            positioned
                .iter()
                .all(|(at, item)| (*at < end) == (*item < 13)),
            "an item sent after the end lands at or past it"
        );
    }

    #[tokio::test]
    async fn closed_producer_detaches_the_queue() {
        let (tx, rx) = tokio::sync::mpsc::channel::<u8>(1);
        let mut queue = OutboundQueue::default();
        queue.attach(rx);
        drop(tx);

        assert_eq!(queue.wait().await, QueueWake::Closed);
        assert!(!queue.is_attached());
    }

    #[tokio::test]
    async fn detached_queue_never_wakes() {
        let mut queue = OutboundQueue::<u8>::default();
        let waited = tokio::time::timeout(std::time::Duration::from_millis(20), queue.wait()).await;
        assert!(waited.is_err());
    }
}
