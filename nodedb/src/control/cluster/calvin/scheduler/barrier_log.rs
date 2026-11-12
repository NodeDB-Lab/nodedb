// SPDX-License-Identifier: BUSL-1.1

//! The dependent-read barrier events of one txn on one vShard, folded in the
//! order the vShard's data-group log holds them.
//!
//! Three kinds of entry reach a barrier through the log: a passive vShard's
//! read result or the refusal of its failed read, another participant's
//! `RETURNING` sequence changes, and the barrier's timeout. Every replica of
//! the vShard applies the log in one order, so every replica folds the same
//! events into the same [`BarrierLog`]:
//!
//! - A result below the timeout entry counts. One above it does not.
//! - The first read result or refusal of a passive vShard counts, and the
//!   first sequence changes of each `(plan, vShard)`. A later copy, from a
//!   re-proposal or a new leader, does not.
//!
//! [`BarrierLog::outcome`] is then the same on every replica: the read
//! barrier completes when every passive vShard's result counts, is refused
//! when a passive vShard's refusal counts, and times out when the timeout
//! entry came first. A `RETURNING` evaluation reads the counted sequence
//! changes the same way.

use std::collections::{BTreeMap, BTreeSet};

use nodedb_physical::physical_plan::meta::PassiveReadKeyId;
use nodedb_types::Value;

use crate::control::wal_replication::types::{FrozenSequenceEffect, PlanSequenceEffects};

/// Read values received from passive vShards, keyed by passive vShard.
/// `BTreeMap` for determinism.
pub type ReceivedReads = BTreeMap<u32, Vec<(PassiveReadKeyId, Value)>>;

/// Sequence changes received from other participants, keyed by
/// `(plan index, source vShard)`. `BTreeMap` iterates them in the order the
/// statements ran.
pub type ReceivedSequences = BTreeMap<(u32, u32), Vec<FrozenSequenceEffect>>;

/// One barrier entry of the data-group log.
#[derive(Debug, Clone, PartialEq)]
pub enum BarrierEvent {
    /// A passive vShard's read values.
    Read {
        passive_vshard: u32,
        values: Vec<(PassiveReadKeyId, Value)>,
    },
    /// A passive vShard's read failed. Its own vote carries the error.
    ReadRefused { passive_vshard: u32 },
    /// The sequence changes of plans another participant froze.
    Sequences {
        source_vshard: u32,
        plans: Vec<PlanSequenceEffects>,
    },
    /// The barrier's timeout.
    Timeout,
}

/// Where a barrier stands once its log is folded.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BarrierOutcome {
    /// Every passive vShard's result came before any timeout.
    Complete,
    /// A passive vShard's refusal came before any timeout: the txn cannot
    /// commit.
    Refused,
    /// The timeout came before a passive vShard's result.
    TimedOut,
    /// A passive vShard's result is still missing, and no timeout came.
    Waiting,
}

/// The barrier events of one txn, folded in log order.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct BarrierLog {
    reads: ReceivedReads,
    /// The passive vShards whose read failed.
    refused_reads: BTreeSet<u32>,
    sequences: ReceivedSequences,
    /// The `(plan index, source vShard)` plans whose participant refused.
    refused: BTreeSet<(u32, u32)>,
    timed_out: bool,
}

impl BarrierLog {
    /// Whether folding `event` changes this log, before any timeout: a
    /// timeout, the first result of a passive vShard, or the first changes
    /// of a `(plan, vShard)`.
    pub fn accepts(&self, event: &BarrierEvent) -> bool {
        if self.timed_out {
            return false;
        }
        match event {
            BarrierEvent::Read { passive_vshard, .. }
            | BarrierEvent::ReadRefused { passive_vshard } => !self.holds_read(*passive_vshard),
            BarrierEvent::Sequences {
                source_vshard,
                plans,
            } => plans
                .iter()
                .any(|plan| !self.holds_plan((plan.plan_index, *source_vshard))),
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
            BarrierEvent::ReadRefused { passive_vshard } => {
                self.refused_reads.insert(passive_vshard);
            }
            BarrierEvent::Sequences {
                source_vshard,
                plans,
            } => {
                for plan in plans {
                    self.note_plan((plan.plan_index, source_vshard), plan.refused, plan.effects);
                }
            }
            BarrierEvent::Timeout => self.timed_out = true,
        }
    }

    /// Whether the log counted the read result or the refusal of
    /// `passive_vshard`.
    fn holds_read(&self, passive_vshard: u32) -> bool {
        self.reads.contains_key(&passive_vshard) || self.refused_reads.contains(&passive_vshard)
    }

    /// Whether the log counted the changes or the refusal of `key`.
    fn holds_plan(&self, key: (u32, u32)) -> bool {
        self.sequences.contains_key(&key) || self.refused.contains(&key)
    }

    /// Count the changes or the refusal of `key`, unless the log counted
    /// either already.
    fn note_plan(&mut self, key: (u32, u32), refused: bool, effects: Vec<FrozenSequenceEffect>) {
        if self.holds_plan(key) {
            return;
        }
        if refused {
            self.refused.insert(key);
        } else {
            self.sequences.insert(key, effects);
        }
    }

    /// Fold `later`, the events the log holds after every event of this one.
    pub fn extend(&mut self, later: BarrierLog) {
        if self.timed_out {
            return;
        }
        for (passive_vshard, values) in later.reads {
            if !self.holds_read(passive_vshard) {
                self.reads.insert(passive_vshard, values);
            }
        }
        for passive_vshard in later.refused_reads {
            if !self.holds_read(passive_vshard) {
                self.refused_reads.insert(passive_vshard);
            }
        }
        for (key, effects) in later.sequences {
            self.note_plan(key, false, effects);
        }
        for key in later.refused {
            self.note_plan(key, true, Vec::new());
        }
        self.timed_out = later.timed_out;
    }

    /// The events this log holds: one per counted result, one per counted
    /// plan's sequence changes, one for a timeout.
    pub fn event_count(&self) -> usize {
        self.reads.len()
            + self.refused_reads.len()
            + self.sequences.len()
            + self.refused.len()
            + usize::from(self.timed_out)
    }

    /// Whether the timeout entry is in the log: no later result counts.
    pub fn timed_out(&self) -> bool {
        self.timed_out
    }

    /// Whether the participant of plan `plan_index` on `source_vshard`
    /// refused before any timeout.
    pub fn sequence_refused(&self, plan_index: u32, source_vshard: u32) -> bool {
        self.refused.contains(&(plan_index, source_vshard))
    }

    /// The counted sequence changes of plan `plan_index` on
    /// `source_vshard`, when they arrived before any timeout.
    pub fn sequence_effects(
        &self,
        plan_index: u32,
        source_vshard: u32,
    ) -> Option<&[FrozenSequenceEffect]> {
        self.sequences
            .get(&(plan_index, source_vshard))
            .map(Vec::as_slice)
    }

    /// Where a barrier that waits for every vShard of `passive` stands.
    pub fn outcome(&self, passive: &BTreeSet<u32>) -> BarrierOutcome {
        if passive.iter().all(|vshard| self.reads.contains_key(vshard)) {
            BarrierOutcome::Complete
        } else if passive
            .iter()
            .any(|vshard| self.refused_reads.contains(vshard))
        {
            BarrierOutcome::Refused
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
            .filter(|vshard| !self.holds_read(*vshard))
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
            refused_reads: self.refused_reads.iter().copied().collect(),
            sequences: self
                .sequences
                .iter()
                .map(|((plan_index, vshard), effects)| (*plan_index, *vshard, effects.clone()))
                .collect(),
            refused: self.refused.iter().copied().collect(),
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
            refused_reads: stored.refused_reads.into_iter().collect(),
            sequences: stored
                .sequences
                .into_iter()
                .map(|(plan_index, vshard, effects)| ((plan_index, vshard), effects))
                .collect(),
            refused: stored.refused.into_iter().collect(),
            timed_out: stored.timed_out,
        })
    }
}

/// The stored form of a [`BarrierLog`].
#[derive(zerompk::ToMessagePack, zerompk::FromMessagePack)]
#[msgpack(map)]
struct StoredBarrierLog {
    reads: Vec<(u32, Vec<(PassiveReadKeyId, Value)>)>,
    /// Each passive vShard whose read failed.
    refused_reads: Vec<u32>,
    /// `(plan index, source vShard, changes)`.
    sequences: Vec<(u32, u32, Vec<FrozenSequenceEffect>)>,
    /// `(plan index, source vShard)` of each refused plan.
    refused: Vec<(u32, u32)>,
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
                    QualifiedCollection::from_stored(
                        nodedb_types::DatabaseId::DEFAULT,
                        format!("c{passive_vshard}"),
                    )
                    .expect("bare name"),
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

    /// A refused read before the timeout refuses the barrier at once, and
    /// a later result of the same vShard does not replace it. A refusal
    /// after the timeout does not count.
    #[test]
    fn a_refused_read_refuses_the_barrier_once() {
        let refused = |passive_vshard| BarrierEvent::ReadRefused { passive_vshard };
        let mut log = BarrierLog::default();
        log.note(read(1, 10));
        log.note(refused(2));
        assert!(!log.accepts(&read(2, 20)));
        log.note(read(2, 20));
        assert_eq!(log.outcome(&passive(&[1, 2])), BarrierOutcome::Refused);
        assert!(log.missing(&passive(&[1, 2])).is_empty());
        assert_eq!(log.event_count(), 2);
        let bytes = log.to_bytes().expect("encode");
        assert_eq!(BarrierLog::from_bytes(&bytes).expect("decode"), log);

        let mut late = BarrierLog::default();
        late.note(BarrierEvent::Timeout);
        late.note(refused(2));
        assert_eq!(late.outcome(&passive(&[2])), BarrierOutcome::TimedOut);
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

    fn sequences(source_vshard: u32, plan_index: u32, value: i64) -> BarrierEvent {
        BarrierEvent::Sequences {
            source_vshard,
            plans: vec![PlanSequenceEffects {
                plan_index,
                effects: vec![FrozenSequenceEffect::Nextval {
                    name: "s".into(),
                    value,
                }],
                refused: false,
            }],
        }
    }

    /// The first changes of a `(plan, vShard)` count. A later copy does
    /// not replace them, and none counts after the timeout.
    #[test]
    fn the_first_sequence_changes_of_a_plan_count_before_the_timeout() {
        let mut log = BarrierLog::default();
        log.note(sequences(3, 0, 7));
        assert!(!log.accepts(&sequences(3, 0, 9)));
        log.note(sequences(3, 0, 9));
        assert!(log.accepts(&sequences(4, 0, 9)), "another source vShard");
        assert_eq!(
            log.sequence_effects(0, 3),
            Some(
                [FrozenSequenceEffect::Nextval {
                    name: "s".into(),
                    value: 7,
                }]
                .as_slice()
            )
        );
        log.note(BarrierEvent::Timeout);
        log.note(sequences(3, 1, 8));
        assert!(log.timed_out());
        assert_eq!(log.sequence_effects(1, 3), None);
        assert_eq!(log.event_count(), 2);
    }

    /// A plan's refusal counts like its changes: the first of the two
    /// stays, and the log stores it.
    #[test]
    fn a_refusal_counts_once_and_round_trips() {
        let refusal = BarrierEvent::Sequences {
            source_vshard: 3,
            plans: vec![PlanSequenceEffects {
                plan_index: 0,
                effects: Vec::new(),
                refused: true,
            }],
        };
        let mut log = BarrierLog::default();
        log.note(refusal.clone());
        assert!(!log.accepts(&refusal));
        assert!(!log.accepts(&sequences(3, 0, 7)), "the refusal came first");
        assert!(log.sequence_refused(0, 3));
        assert_eq!(log.sequence_effects(0, 3), None);
        let bytes = log.to_bytes().expect("encode");
        assert_eq!(BarrierLog::from_bytes(&bytes).expect("decode"), log);
    }

    /// A stored log decodes to the log it was encoded from.
    #[test]
    fn a_stored_log_round_trips() {
        let mut log = BarrierLog::default();
        log.note(read(2, 20));
        log.note(sequences(5, 1, 3));
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
