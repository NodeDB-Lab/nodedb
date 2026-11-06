// SPDX-License-Identifier: BUSL-1.1

//! Proposal keys of the committed entries this node handed to its apply
//! loop, per group, for the window a propose waiter can exist in.
//!
//! A data-group snapshot carries the keys at or below its index. The follower
//! that installs it never applies those entries, and the keys are the only
//! way it learns which proposals they were.

use std::collections::{HashMap, VecDeque};
use std::time::{Duration, Instant};

use crate::control::distributed_applier::proposal_ledger::PROPOSAL_LEDGER_CAPACITY;

/// One committed entry's key.
#[derive(Debug, Clone, Copy)]
struct CommittedKey {
    log_index: u64,
    key: u64,
    at: Instant,
}

/// One group's recent keys, oldest first, and where they are complete.
#[derive(Debug, Default)]
struct GroupKeys {
    keys: VecDeque<CommittedKey>,
    /// First index this process received. Keys below it were never seen here.
    first_noted: u64,
    /// Highest index whose key the count bound dropped.
    cap_dropped_through: u64,
}

/// The keys a snapshot carries, and the lowest index from which they are
/// complete. `complete_from` is `0` when every key in the window is present.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct CarriedKeys {
    pub keys: Vec<(u64, u64)>,
    pub complete_from: u64,
}

/// Recent committed keys per group, oldest first.
///
/// Bounded by age (the waiter window) and by count per group. The count bound
/// is the proposal ledger's capacity. A key older than the window serves no
/// waiter. A key the count bound drops leaves a gap, which
/// [`CarriedKeys::complete_from`] reports.
#[derive(Debug)]
pub(super) struct CommittedKeys {
    groups: HashMap<u64, GroupKeys>,
    window: Duration,
}

impl CommittedKeys {
    pub fn new(window: Duration) -> Self {
        Self {
            groups: HashMap::new(),
            window,
        }
    }

    pub fn set_window(&mut self, window: Duration) {
        self.window = window;
    }

    /// Record that entry `log_index` of `group_id` committed with proposal
    /// `key`. Key `0` names no proposal and is not kept.
    pub fn note(&mut self, group_id: u64, log_index: u64, key: u64, now: Instant) {
        let group = self.groups.entry(group_id).or_default();
        if group.first_noted == 0 {
            group.first_noted = log_index;
        }
        if key == 0 {
            return;
        }
        group.keys.push_back(CommittedKey {
            log_index,
            key,
            at: now,
        });
        while group
            .keys
            .front()
            .is_some_and(|oldest| now.saturating_duration_since(oldest.at) > self.window)
        {
            group.keys.pop_front();
        }
        while group.keys.len() > PROPOSAL_LEDGER_CAPACITY {
            if let Some(dropped) = group.keys.pop_front() {
                group.cap_dropped_through = group.cap_dropped_through.max(dropped.log_index);
            }
        }
    }

    /// Forget the key of `log_index`: the entry concluded with no effect, so
    /// a later copy of its proposal still applies.
    pub fn forget(&mut self, group_id: u64, log_index: u64) {
        let Some(group) = self.groups.get_mut(&group_id) else {
            return;
        };
        if let Ok(at) = group.keys.binary_search_by_key(&log_index, |k| k.log_index) {
            group.keys.remove(at);
        }
    }

    /// `group_id`'s keys at or below `through`, committed within the window,
    /// and the lowest index from which they are complete.
    pub fn through(&self, group_id: u64, through: u64, now: Instant) -> CarriedKeys {
        let Some(group) = self.groups.get(&group_id) else {
            // Nothing of the group was seen here: no index is known.
            return CarriedKeys {
                keys: Vec::new(),
                complete_from: through.saturating_add(1),
            };
        };
        let keys = group
            .keys
            .iter()
            .filter(|entry| {
                entry.log_index <= through && now.saturating_duration_since(entry.at) <= self.window
            })
            .map(|entry| (entry.log_index, entry.key))
            .collect();
        let from = group
            .first_noted
            .max(group.cap_dropped_through.saturating_add(1));
        // Complete from the group's first index is complete.
        let complete_from = if from <= 1 { 0 } else { from };
        CarriedKeys {
            keys,
            complete_from,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_forgotten_key_is_not_carried() {
        let mut keys = CommittedKeys::new(Duration::from_secs(30));
        let start = Instant::now();
        keys.note(1, 4, 0xa, start);
        keys.note(1, 5, 0xb, start);
        keys.forget(1, 4);
        keys.forget(2, 4);
        keys.forget(1, 9);
        assert_eq!(keys.through(1, 9, start).keys, vec![(5, 0xb)]);
    }

    #[test]
    fn keys_at_or_below_the_index_within_the_window_are_returned() {
        let window = Duration::from_secs(30);
        let mut keys = CommittedKeys::new(window);
        let start = Instant::now();
        keys.note(1, 4, 0xa, start);
        keys.note(1, 5, 0, start);
        keys.note(1, 6, 0xb, start);
        keys.note(2, 3, 0xc, start);
        keys.note(1, 9, 0xd, start);

        assert_eq!(keys.through(1, 6, start).keys, vec![(4, 0xa), (6, 0xb)]);
        assert_eq!(keys.through(2, 6, start).keys, vec![(3, 0xc)]);
        assert!(keys.through(1, 6, start + window * 2).keys.is_empty());
    }

    /// Keys from a group's first index are complete. A process that first saw
    /// the group at a later index, or a count bound that dropped keys, leaves
    /// the keys complete only above the gap.
    #[test]
    fn complete_from_names_the_gap() {
        let window = Duration::from_secs(30);
        let start = Instant::now();

        let mut keys = CommittedKeys::new(window);
        keys.note(1, 1, 0xa, start);
        assert_eq!(keys.through(1, 1, start).complete_from, 0);

        keys.note(2, 40, 0xb, start);
        assert_eq!(keys.through(2, 50, start).complete_from, 40);

        assert_eq!(keys.through(3, 50, start).complete_from, 51);

        let mut capped = CommittedKeys::new(window);
        let last = PROPOSAL_LEDGER_CAPACITY as u64 + 3;
        for index in 1..=last {
            capped.note(1, index, index, start);
        }
        assert_eq!(capped.through(1, last, start).complete_from, 4);
    }

    #[test]
    fn keys_past_the_window_are_evicted_on_the_next_note() {
        let window = Duration::from_secs(30);
        let mut keys = CommittedKeys::new(window);
        let start = Instant::now();
        keys.note(1, 1, 0xa, start);
        keys.note(1, 2, 0xb, start + window * 2);
        assert_eq!(keys.groups.get(&1).map(|group| group.keys.len()), Some(1));
    }
}
