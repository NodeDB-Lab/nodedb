//! `PendingUpdate` — one membership delta waiting to be gossiped.
//!
//! Each entry tracks how many outgoing datagrams have already carried it
//! so the queue can pick the least-disseminated rumours first and drop
//! them after `fanout_lambda * log(n)` sends (Lifeguard §4.4).

use std::cmp::Ordering;

use crate::swim::member::record::MemberUpdate;

/// A membership delta plus its dissemination bookkeeping.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingUpdate {
    pub update: MemberUpdate,
    pub sent_count: u32,
}

impl PendingUpdate {
    /// Fresh entry with `sent_count = 0`.
    pub fn new(update: MemberUpdate) -> Self {
        Self {
            update,
            sent_count: 0,
        }
    }

    /// Bump the send counter. Used after the queue emits this entry on
    /// an outgoing datagram.
    pub fn record_sent(&mut self) {
        self.sent_count = self.sent_count.saturating_add(1);
    }
}

/// Ordering: **lower `sent_count` is "greater"** so a `BinaryHeap`
/// (max-heap) pops the least-disseminated entry first. Tie-breaks on
/// `(node_id, incarnation)` keep the order deterministic for tests.
impl Ord for PendingUpdate {
    fn cmp(&self, other: &Self) -> Ordering {
        // Max-heap semantics: the "greatest" entry pops first.
        // - Lower `sent_count` should pop first → reverse that field.
        // - Alphabetically earlier `node_id` should pop first on ties
        //   → reverse that field too.
        // - Higher `incarnation` pops first on further ties (fresher
        //   rumour wins) → natural order.
        other
            .sent_count
            .cmp(&self.sent_count)
            .then_with(|| {
                other
                    .update
                    .node_id
                    .as_str()
                    .cmp(self.update.node_id.as_str())
            })
            .then_with(|| self.update.incarnation.cmp(&other.update.incarnation))
    }
}

impl PartialOrd for PendingUpdate {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::swim::incarnation::Incarnation;
    use crate::swim::member::MemberState;
    use nodedb_types::NodeId;
    use std::collections::BinaryHeap;

    fn upd(id: &str, inc: u64) -> MemberUpdate {
        MemberUpdate {
            node_id: NodeId::new(id),
            addr: "127.0.0.1:7000".to_string(),
            state: MemberState::Alive,
            incarnation: Incarnation::new(inc),
        }
    }

    #[test]
    fn record_sent_increments() {
        let mut p = PendingUpdate::new(upd("n1", 0));
        assert_eq!(p.sent_count, 0);
        p.record_sent();
        p.record_sent();
        assert_eq!(p.sent_count, 2);
    }

    #[test]
    fn record_sent_saturates() {
        let mut p = PendingUpdate {
            update: upd("n1", 0),
            sent_count: u32::MAX,
        };
        p.record_sent();
        assert_eq!(p.sent_count, u32::MAX);
    }

    #[test]
    fn binary_heap_pops_least_sent_first() {
        let mut heap = BinaryHeap::new();
        heap.push(PendingUpdate {
            update: upd("a", 0),
            sent_count: 3,
        });
        heap.push(PendingUpdate {
            update: upd("b", 0),
            sent_count: 0,
        });
        heap.push(PendingUpdate {
            update: upd("c", 0),
            sent_count: 1,
        });
        assert_eq!(heap.pop().unwrap().update.node_id.as_str(), "b");
        assert_eq!(heap.pop().unwrap().update.node_id.as_str(), "c");
        assert_eq!(heap.pop().unwrap().update.node_id.as_str(), "a");
    }

    #[test]
    fn tie_break_is_deterministic() {
        let mut heap = BinaryHeap::new();
        heap.push(PendingUpdate::new(upd("b", 1)));
        heap.push(PendingUpdate::new(upd("a", 1)));
        // Same sent_count; alphabetical tie-break pops "a" first.
        assert_eq!(heap.pop().unwrap().update.node_id.as_str(), "a");
        assert_eq!(heap.pop().unwrap().update.node_id.as_str(), "b");
    }
}
