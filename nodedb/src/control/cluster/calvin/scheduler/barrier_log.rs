// SPDX-License-Identifier: BUSL-1.1

//! The dependent-read barrier events of one txn on one vShard, folded in the
//! order the vShard's data-group log holds them.
//!
//! Two kinds of entry reach a barrier through the log: a passive vShard's
//! read result, and the barrier's timeout. Every replica of the vShard
//! applies the log in one order, so every replica folds the same events into
//! the same [`BarrierLog`]:
//!
//! - A read result below the timeout entry counts. One above it does not.
//! - The first read result of a passive vShard counts. A later copy, from a
//!   re-proposal, does not.
//!
//! [`BarrierLog::outcome`] is then the same on every replica: the barrier
//! completes when every passive vShard's result counts, and it times out
//! when the timeout entry came first.

use std::collections::{BTreeMap, BTreeSet};

use nodedb_physical::physical_plan::meta::PassiveReadKeyId;
use nodedb_types::Value;

/// Read values received from passive vShards, keyed by passive vShard.
/// `BTreeMap` for determinism.
pub type ReceivedReads = BTreeMap<u32, Vec<(PassiveReadKeyId, Value)>>;

/// One barrier entry of the data-group log.
#[derive(Debug, Clone, PartialEq)]
pub enum BarrierEvent {
    /// A passive vShard's read values.
    Read {
        passive_vshard: u32,
        values: Vec<(PassiveReadKeyId, Value)>,
    },
    /// The barrier's timeout.
    Timeout,
}

/// Where a barrier stands once its log is folded.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BarrierOutcome {
    /// Every passive vShard's result came before any timeout.
    Complete,
    /// The timeout came before a passive vShard's result.
    TimedOut,
    /// A passive vShard's result is still missing, and no timeout came.
    Waiting,
}

/// The barrier events of one txn, folded in log order.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct BarrierLog {
    reads: ReceivedReads,
    timed_out: bool,
}

impl BarrierLog {
    /// Whether folding `event` changes this log: a timeout, or the first
    /// result of a passive vShard, before any timeout.
    pub fn accepts(&self, event: &BarrierEvent) -> bool {
        if self.timed_out {
            return false;
        }
        match event {
            BarrierEvent::Read { passive_vshard, .. } => !self.reads.contains_key(passive_vshard),
            BarrierEvent::Timeout => true,
        }
    }

    /// Fold `event`, the next barrier entry of the log.
    pub fn note(&mut self, event: BarrierEvent) {
        if !self.accepts(&event) {
            return;
        }
        match event {
            BarrierEvent::Read {
                passive_vshard,
                values,
            } => {
                self.reads.insert(passive_vshard, values);
            }
            BarrierEvent::Timeout => self.timed_out = true,
        }
    }

    /// Fold `later`, the events the log holds after every event of this one.
    pub fn extend(&mut self, later: BarrierLog) {
        if self.timed_out {
            return;
        }
        for (passive_vshard, values) in later.reads {
            self.reads.entry(passive_vshard).or_insert(values);
        }
        self.timed_out = later.timed_out;
    }

    /// The events this log holds: one per counted result, one for a timeout.
    pub fn event_count(&self) -> usize {
        self.reads.len() + usize::from(self.timed_out)
    }

    /// Where a barrier that waits for every vShard of `passive` stands.
    pub fn outcome(&self, passive: &BTreeSet<u32>) -> BarrierOutcome {
        if passive.iter().all(|vshard| self.reads.contains_key(vshard)) {
            BarrierOutcome::Complete
        } else if self.timed_out {
            BarrierOutcome::TimedOut
        } else {
            BarrierOutcome::Waiting
        }
    }

    /// The vShards of `passive` whose result has not counted.
    pub fn missing(&self, passive: &BTreeSet<u32>) -> Vec<u32> {
        passive
            .iter()
            .copied()
            .filter(|vshard| !self.reads.contains_key(vshard))
            .collect()
    }

    /// Every counted read value, keyed by the row it names. `BTreeMap`
    /// iterates in one order on every replica.
    pub fn injected_reads(&self) -> BTreeMap<PassiveReadKeyId, Value> {
        self.reads
            .values()
            .flatten()
            .map(|(key_id, value)| (key_id.clone(), value.clone()))
            .collect()
    }

    /// The log as the bytes a stored barrier row holds.
    pub fn to_bytes(&self) -> crate::Result<Vec<u8>> {
        let stored = StoredBarrierLog {
            reads: self
                .reads
                .iter()
                .map(|(vshard, values)| (*vshard, values.clone()))
                .collect(),
            timed_out: self.timed_out,
        };
        zerompk::to_msgpack_vec(&stored).map_err(|e| crate::Error::Serialization {
            format: "msgpack".into(),
            detail: format!("encode calvin barrier log: {e}"),
        })
    }

    /// The log a stored barrier row holds.
    pub fn from_bytes(bytes: &[u8]) -> crate::Result<Self> {
        let stored: StoredBarrierLog =
            zerompk::from_msgpack(bytes).map_err(|e| crate::Error::Serialization {
                format: "msgpack".into(),
                detail: format!("decode calvin barrier log: {e}"),
            })?;
        Ok(Self {
            reads: stored.reads.into_iter().collect(),
            timed_out: stored.timed_out,
        })
    }
}

/// The stored form of a [`BarrierLog`].
#[derive(zerompk::ToMessagePack, zerompk::FromMessagePack)]
#[msgpack(map)]
struct StoredBarrierLog {
    reads: Vec<(u32, Vec<(PassiveReadKeyId, Value)>)>,
    timed_out: bool,
}

#[cfg(test)]
mod tests {
    use nodedb_types::QualifiedCollection;

    use super::*;

    fn read(passive_vshard: u32, value: i64) -> BarrierEvent {
        BarrierEvent::Read {
            passive_vshard,
            values: vec![(
                PassiveReadKeyId::surrogate(
                    QualifiedCollection::from_stored(format!("c{passive_vshard}")),
                    1,
                ),
                Value::Integer(value),
            )],
        }
    }

    fn passive(vshards: &[u32]) -> BTreeSet<u32> {
        vshards.iter().copied().collect()
    }

    /// Every result before the timeout completes the barrier: the timeout
    /// that follows changes nothing.
    #[test]
    fn results_before_the_timeout_complete_the_barrier() {
        let mut log = BarrierLog::default();
        log.note(read(1, 10));
        log.note(read(2, 20));
        log.note(BarrierEvent::Timeout);
        assert_eq!(log.outcome(&passive(&[1, 2])), BarrierOutcome::Complete);
        assert_eq!(log.injected_reads().len(), 2);
    }

    /// A result above the timeout does not count, so the barrier times out.
    #[test]
    fn a_result_after_the_timeout_does_not_count() {
        let mut log = BarrierLog::default();
        log.note(read(1, 10));
        log.note(BarrierEvent::Timeout);
        log.note(read(2, 20));
        assert_eq!(log.outcome(&passive(&[1, 2])), BarrierOutcome::TimedOut);
        assert_eq!(log.missing(&passive(&[1, 2])), vec![2]);
    }

    /// A barrier missing a result with no timeout waits.
    #[test]
    fn a_missing_result_waits() {
        let mut log = BarrierLog::default();
        log.note(read(1, 10));
        assert_eq!(log.outcome(&passive(&[1, 2])), BarrierOutcome::Waiting);
    }

    /// The first copy of a passive vShard's result counts. A re-proposed
    /// copy does not replace it.
    #[test]
    fn the_first_copy_of_a_result_counts() {
        let mut log = BarrierLog::default();
        log.note(read(1, 10));
        assert!(!log.accepts(&read(1, 99)));
        log.note(read(1, 99));
        let values: Vec<Value> = log.injected_reads().into_values().collect();
        assert_eq!(values, vec![Value::Integer(10)]);
    }

    /// Two logs folded one after the other give the outcome one log of
    /// every event gives.
    #[test]
    fn extending_with_later_events_keeps_log_order() {
        let mut earlier = BarrierLog::default();
        earlier.note(read(1, 10));
        let mut later = BarrierLog::default();
        later.note(BarrierEvent::Timeout);
        later.note(read(2, 20));
        earlier.extend(later);
        assert_eq!(earlier.outcome(&passive(&[1, 2])), BarrierOutcome::TimedOut);

        let mut timed_out = BarrierLog::default();
        timed_out.note(BarrierEvent::Timeout);
        let mut late_reads = BarrierLog::default();
        late_reads.note(read(1, 10));
        timed_out.extend(late_reads);
        assert_eq!(timed_out.outcome(&passive(&[1])), BarrierOutcome::TimedOut);
    }

    /// A stored log decodes to the log it was encoded from.
    #[test]
    fn a_stored_log_round_trips() {
        let mut log = BarrierLog::default();
        log.note(read(2, 20));
        log.note(read(1, 10));
        log.note(BarrierEvent::Timeout);
        let bytes = log.to_bytes().expect("encode");
        assert_eq!(BarrierLog::from_bytes(&bytes).expect("decode"), log);
    }

    /// The event count is what the log holds: counted results and a timeout.
    #[test]
    fn event_count_counts_held_events() {
        let mut log = BarrierLog::default();
        assert_eq!(log.event_count(), 0);
        log.note(read(1, 10));
        log.note(read(1, 11));
        assert_eq!(log.event_count(), 1);
        log.note(BarrierEvent::Timeout);
        assert_eq!(log.event_count(), 2);
    }
}
