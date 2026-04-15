//! `DisseminationQueue` — bounded, decaying piggyback buffer.
//!
//! The queue is a `HashMap<NodeId, PendingUpdate>` so that a fresher
//! rumour about the same node **replaces** its stale predecessor in
//! place — otherwise the queue would balloon with outdated tombstones.
//!
//! Emission picks the `max_piggyback` least-disseminated entries,
//! increments their send counters, and drops any entry that has now
//! reached the `lambda_log_n` fanout threshold.

use std::collections::HashMap;
use std::sync::Mutex;

use nodedb_types::NodeId;

use crate::swim::member::record::MemberUpdate;

use super::entry::PendingUpdate;

/// Bounded dissemination buffer keyed by `NodeId`.
#[derive(Debug, Default)]
pub struct DisseminationQueue {
    inner: Mutex<HashMap<NodeId, PendingUpdate>>,
}

impl DisseminationQueue {
    /// Fresh empty queue.
    pub fn new() -> Self {
        Self::default()
    }

    /// Insert or replace the entry for `update.node_id`. The send
    /// counter resets to 0 so a fresh rumour is always gossiped anew.
    pub fn enqueue(&self, update: MemberUpdate) {
        let mut guard = self.inner.lock().expect("dissemination lock poisoned");
        guard.insert(update.node_id.clone(), PendingUpdate::new(update));
    }

    /// Total number of rumours currently in the queue.
    pub fn len(&self) -> usize {
        self.inner
            .lock()
            .expect("dissemination lock poisoned")
            .len()
    }

    /// True when the queue is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Drop every rumour. Used by tests and on shutdown.
    pub fn clear(&self) {
        self.inner
            .lock()
            .expect("dissemination lock poisoned")
            .clear();
    }

    /// Return up to `max` least-disseminated updates for a single
    /// outgoing message. Increments each returned entry's `sent_count`
    /// and drops entries whose new count has reached `lambda_log_n`.
    ///
    /// `lambda_log_n` is `ceil(lambda * log2(cluster_size + 1))` —
    /// computed by the caller because it depends on the current
    /// membership size and the [`super::super::config::SwimConfig`]
    /// `fanout_lambda` knob.
    pub fn take_for_message(&self, max: usize, lambda_log_n: u32) -> Vec<MemberUpdate> {
        if max == 0 {
            return Vec::new();
        }
        let mut guard = self.inner.lock().expect("dissemination lock poisoned");

        // Sort a snapshot of keys by (sent_count, node_id) ascending;
        // we can't use BinaryHeap directly without cloning because the
        // values need to be mutated in place after the decision.
        let mut keys: Vec<NodeId> = guard.keys().cloned().collect();
        keys.sort_by(|a, b| {
            let pa = &guard[a];
            let pb = &guard[b];
            pa.sent_count
                .cmp(&pb.sent_count)
                .then_with(|| a.as_str().cmp(b.as_str()))
                .then_with(|| pa.update.incarnation.cmp(&pb.update.incarnation))
        });
        keys.truncate(max);

        let mut out = Vec::with_capacity(keys.len());
        for k in keys {
            if let Some(pending) = guard.get_mut(&k) {
                pending.record_sent();
                out.push(pending.update.clone());
                if pending.sent_count >= lambda_log_n {
                    guard.remove(&k);
                }
            }
        }
        out
    }

    /// Compute `ceil(lambda * log2(cluster_size + 1))`. Exposed so the
    /// runner can pass the result straight into [`take_for_message`].
    pub fn fanout_threshold(cluster_size: usize, lambda: u32) -> u32 {
        let n = (cluster_size + 1).max(2) as f64;
        (lambda as f64 * n.log2()).ceil() as u32
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::swim::incarnation::Incarnation;
    use crate::swim::member::MemberState;

    fn upd(id: &str, inc: u64) -> MemberUpdate {
        MemberUpdate {
            node_id: NodeId::new(id),
            addr: "127.0.0.1:7000".to_string(),
            state: MemberState::Alive,
            incarnation: Incarnation::new(inc),
        }
    }

    #[test]
    fn enqueue_replaces_by_node_id() {
        let q = DisseminationQueue::new();
        q.enqueue(upd("n1", 1));
        q.enqueue(upd("n1", 5));
        assert_eq!(q.len(), 1);
        // Taking it returns the latest incarnation.
        let out = q.take_for_message(10, 4);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].incarnation, Incarnation::new(5));
    }

    #[test]
    fn take_caps_at_max() {
        let q = DisseminationQueue::new();
        q.enqueue(upd("n1", 0));
        q.enqueue(upd("n2", 0));
        q.enqueue(upd("n3", 0));
        let out = q.take_for_message(2, 10);
        assert_eq!(out.len(), 2);
    }

    #[test]
    fn take_zero_max_returns_empty() {
        let q = DisseminationQueue::new();
        q.enqueue(upd("n1", 0));
        let out = q.take_for_message(0, 4);
        assert!(out.is_empty());
    }

    #[test]
    fn entries_drop_after_fanout_threshold() {
        let q = DisseminationQueue::new();
        q.enqueue(upd("n1", 0));
        // threshold = 2 → second take should drain and drop.
        let _ = q.take_for_message(1, 2);
        assert_eq!(q.len(), 1);
        let _ = q.take_for_message(1, 2);
        assert_eq!(q.len(), 0);
    }

    #[test]
    fn least_disseminated_wins() {
        let q = DisseminationQueue::new();
        q.enqueue(upd("a", 0));
        // Drain "a" twice so its sent_count reaches 2.
        let _ = q.take_for_message(1, 10);
        let _ = q.take_for_message(1, 10);
        // Now enqueue a fresh "b" with sent_count=0.
        q.enqueue(upd("b", 0));
        // Next take should pick "b" (count=0) over "a" (count=2).
        let out = q.take_for_message(1, 10);
        assert_eq!(out[0].node_id.as_str(), "b");
    }

    #[test]
    fn fanout_threshold_formula() {
        // 7-node cluster, lambda=3 → ceil(3 * log2(8)) = 9.
        assert_eq!(DisseminationQueue::fanout_threshold(7, 3), 9);
        // 1-node cluster, lambda=3 → ceil(3 * log2(2)) = 3.
        assert_eq!(DisseminationQueue::fanout_threshold(1, 3), 3);
        // 0-node cluster, lambda=3 → ceil(3 * log2(2)) = 3 (clamped).
        assert_eq!(DisseminationQueue::fanout_threshold(0, 3), 3);
    }

    #[test]
    fn clear_empties_queue() {
        let q = DisseminationQueue::new();
        q.enqueue(upd("n1", 0));
        q.enqueue(upd("n2", 0));
        q.clear();
        assert!(q.is_empty());
    }
}
