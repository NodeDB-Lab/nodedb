// SPDX-License-Identifier: BUSL-1.1

//! The metadata leader's lease table.
//!
//! One table exists per leadership term. It records, for each node that
//! renewed with this leader, when its lease ends and what its last report
//! covered. It also records the floors: per group, the highest index of any
//! authorization change a barrier registered. A lease is granted only to a
//! report that covers every floor.
//!
//! A barrier releases once, for every target, each node holding an unexpired
//! lease reported coverage of it. Leases granted by an earlier leader are not
//! in the table. They end within one lease duration of this leader taking
//! over, so a barrier also waits until then.
//!
//! The table is pure: callers pass the clock, so every rule is testable.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use nodedb_cluster::GroupCoverage;

/// What a node's last renewal reported and when its lease ends.
#[derive(Debug, Default)]
struct HolderRecord {
    /// End of the lease this leader granted, if any.
    expires_at: Option<Instant>,
    /// Coverage by group, from the last renewal.
    coverage: HashMap<u64, u64>,
}

impl HolderRecord {
    fn covers(&self, group_id: u64, index: u64) -> bool {
        self.coverage
            .get(&group_id)
            .is_some_and(|through| *through >= index)
    }
}

/// The answer to a renewal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RenewDecision {
    Granted,
    Withheld,
}

/// Where a barrier stands.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BarrierState {
    /// No node can plan against state older than the targets.
    Released,
    /// Waiting for a report or an expiry. Nothing changes on its own before
    /// the instant named, except a renewal.
    Waiting { until: Instant },
    /// The floors of this term are not loaded yet.
    NotReady,
}

/// The lease table of one leadership term.
#[derive(Debug)]
pub struct LeaseTable {
    term: u64,
    leader_since: Instant,
    floors_ready: bool,
    floors: HashMap<u64, u64>,
    holders: HashMap<u64, HolderRecord>,
}

impl LeaseTable {
    /// A table for `term`, whose leadership this node observed at `now`.
    pub fn new(term: u64, now: Instant) -> Self {
        Self {
            term,
            leader_since: now,
            floors_ready: false,
            floors: HashMap::new(),
            holders: HashMap::new(),
        }
    }

    pub fn term(&self) -> u64 {
        self.term
    }

    pub fn floors_ready(&self) -> bool {
        self.floors_ready
    }

    /// Load the floors this term starts from: an index per group at or above
    /// every change acknowledged before the term.
    pub fn load_floors(&mut self, floors: &[GroupCoverage]) {
        self.raise_floors(floors);
        self.floors_ready = true;
    }

    /// Raise the floors to cover `targets`.
    pub fn raise_floors(&mut self, targets: &[GroupCoverage]) {
        for target in targets {
            let floor = self.floors.entry(target.group_id).or_insert(0);
            *floor = (*floor).max(target.through);
        }
    }

    /// Record a renewal from `node_id` and decide on its lease.
    pub fn renew(
        &mut self,
        node_id: u64,
        coverage: &[GroupCoverage],
        now: Instant,
        lease: Duration,
    ) -> RenewDecision {
        let record = self.holders.entry(node_id).or_default();
        record.coverage = coverage
            .iter()
            .map(|report| (report.group_id, report.through))
            .collect();
        let covered = self
            .floors
            .iter()
            .all(|(group_id, floor)| record.covers(*group_id, *floor));
        if !self.floors_ready || !covered {
            return RenewDecision::Withheld;
        }
        record.expires_at = Some(now + lease);
        RenewDecision::Granted
    }

    /// Every group whose floor `coverage` does not reach, as
    /// `(group_id, floor, reported)`. `reported` is `None` for a group the
    /// report omits.
    pub fn shortfall(&self, coverage: &[GroupCoverage]) -> Vec<(u64, u64, Option<u64>)> {
        let mut short: Vec<(u64, u64, Option<u64>)> = self
            .floors
            .iter()
            .filter_map(|(group_id, floor)| {
                let reported = coverage
                    .iter()
                    .find(|report| report.group_id == *group_id)
                    .map(|report| report.through);
                (reported.is_none_or(|through| through < *floor))
                    .then_some((*group_id, *floor, reported))
            })
            .collect();
        short.sort_unstable();
        short
    }

    /// Where a barrier on `targets` stands at `now`.
    pub fn barrier(
        &self,
        targets: &[GroupCoverage],
        now: Instant,
        lease: Duration,
    ) -> BarrierState {
        if !self.floors_ready {
            return BarrierState::NotReady;
        }
        let mut until: Option<Instant> = None;
        let mut wait_for = |instant: Instant| {
            until = Some(until.map_or(instant, |current: Instant| current.min(instant)));
        };
        let earlier_leases_end = self.leader_since + lease;
        if now < earlier_leases_end {
            wait_for(earlier_leases_end);
        }
        for record in self.holders.values() {
            let Some(expires_at) = record.expires_at.filter(|end| *end > now) else {
                continue;
            };
            let covered = targets
                .iter()
                .all(|target| record.covers(target.group_id, target.through));
            if !covered {
                wait_for(expires_at);
            }
        }
        match until {
            Some(until) => BarrierState::Waiting { until },
            None => BarrierState::Released,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const LEASE: Duration = Duration::from_millis(150);

    fn cover(group_id: u64, through: u64) -> GroupCoverage {
        GroupCoverage { group_id, through }
    }

    /// A table past the window of earlier leaders' leases.
    fn settled_table(start: Instant) -> LeaseTable {
        let mut table = LeaseTable::new(4, start);
        table.load_floors(&[cover(0, 10)]);
        table
    }

    #[test]
    fn nothing_is_granted_or_released_before_the_floors_load() {
        let now = Instant::now();
        let mut table = LeaseTable::new(1, now);
        assert_eq!(
            table.renew(2, &[cover(0, 99)], now, LEASE),
            RenewDecision::Withheld
        );
        assert_eq!(table.barrier(&[], now, LEASE), BarrierState::NotReady);
    }

    #[test]
    fn the_shortfall_names_each_uncovered_floor() {
        let mut table = settled_table(Instant::now());
        table.raise_floors(&[cover(7, 19)]);
        assert_eq!(
            table.shortfall(&[cover(0, 10), cover(7, 12)]),
            vec![(7, 19, Some(12))]
        );
        assert_eq!(table.shortfall(&[cover(7, 19)]), vec![(0, 10, None)]);
        assert!(table.shortfall(&[cover(0, 10), cover(7, 19)]).is_empty());
    }

    #[test]
    fn a_report_below_a_floor_is_withheld() {
        let start = Instant::now();
        let mut table = settled_table(start);
        assert_eq!(
            table.renew(2, &[cover(0, 9)], start, LEASE),
            RenewDecision::Withheld
        );
        assert_eq!(
            table.renew(2, &[cover(0, 10)], start, LEASE),
            RenewDecision::Granted
        );
        // A group the report omits counts as uncovered.
        table.raise_floors(&[cover(5, 1)]);
        assert_eq!(
            table.renew(2, &[cover(0, 10)], start, LEASE),
            RenewDecision::Withheld
        );
    }

    #[test]
    fn a_barrier_waits_out_the_leases_of_earlier_leaders() {
        let start = Instant::now();
        let table = settled_table(start);
        assert_eq!(
            table.barrier(&[cover(0, 5)], start, LEASE),
            BarrierState::Waiting {
                until: start + LEASE
            }
        );
        assert_eq!(
            table.barrier(&[cover(0, 5)], start + LEASE, LEASE),
            BarrierState::Released
        );
    }

    #[test]
    fn a_barrier_releases_on_coverage_or_expiry() {
        let start = Instant::now();
        let mut table = settled_table(start);
        let now = start + LEASE;
        assert_eq!(
            table.renew(2, &[cover(0, 10)], now, LEASE),
            RenewDecision::Granted
        );
        let target = [cover(0, 12)];
        table.raise_floors(&target);
        // Node 2 holds a lease and has not covered index 12.
        assert_eq!(
            table.barrier(&target, now, LEASE),
            BarrierState::Waiting { until: now + LEASE }
        );
        // Its renewal below the new floor is withheld, and its lease is not
        // extended.
        assert_eq!(
            table.renew(2, &[cover(0, 11)], now, LEASE),
            RenewDecision::Withheld
        );
        // Covering the target releases the barrier at once.
        assert_eq!(
            table.renew(2, &[cover(0, 12)], now, LEASE),
            RenewDecision::Granted
        );
        assert_eq!(table.barrier(&target, now, LEASE), BarrierState::Released);

        // A node that never covers releases the barrier when its lease ends.
        let later = [cover(0, 20)];
        table.raise_floors(&later);
        assert_eq!(
            table.barrier(&later, now, LEASE),
            BarrierState::Waiting { until: now + LEASE }
        );
        assert_eq!(
            table.barrier(&later, now + LEASE, LEASE),
            BarrierState::Released
        );
        // Once expired, it gets no lease back without covering the floor.
        assert_eq!(
            table.renew(2, &[cover(0, 12)], now + LEASE, LEASE),
            RenewDecision::Withheld
        );
    }
}
