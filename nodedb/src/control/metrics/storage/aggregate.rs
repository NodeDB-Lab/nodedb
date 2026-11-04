// SPDX-License-Identifier: BUSL-1.1

//! Merge node shares into cluster totals.
//!
//! Each Raft group's bytes come from one share. A leadership move can put a
//! group in two shares, the old leader's and the new one's. The share with
//! the higher term wins, and a tie takes the newer sample. A group no share
//! names counts zero until its new leader samples.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use super::shares::{StorageShare, TenantTotals, add_totals};

/// The last share of every peer, until it ages out or the peer leaves.
#[derive(Debug, Default)]
pub struct PeerReports {
    reports: HashMap<u64, StorageShare>,
}

impl PeerReports {
    /// Keep `share` as the last report of `node_id`.
    pub fn record(&mut self, node_id: u64, share: StorageShare) {
        self.reports.insert(node_id, share);
    }

    /// Drop every report of a peer outside `members` (a short list), and every report whose
    /// sample is older than `ttl` at `now`.
    pub fn retain(&mut self, members: &[u64], now: Instant, ttl: Duration) {
        self.reports
            .retain(|node_id, share| members.contains(node_id) && is_fresh(share, now, ttl));
    }

    /// Whether a report of `node_id` is held.
    pub fn holds(&self, node_id: u64) -> bool {
        self.reports.contains_key(&node_id)
    }

    /// Every held share.
    pub fn shares(&self) -> impl Iterator<Item = &StorageShare> {
        self.reports.values()
    }
}

/// Whether the sample of `share` is at most `ttl` old at `now`.
pub fn is_fresh(share: &StorageShare, now: Instant, ttl: Duration) -> bool {
    now.saturating_duration_since(share.sampled_at) <= ttl
}

/// The cluster totals of `shares`: per group, the share with the highest
/// term, then the newest sample.
pub fn cluster_totals<'a>(shares: impl IntoIterator<Item = &'a StorageShare>) -> TenantTotals {
    let mut chosen: HashMap<u64, (u64, Instant, &'a TenantTotals)> = HashMap::new();
    for share in shares {
        for (group_id, group) in &share.groups {
            let candidate = (group.term, share.sampled_at, &group.totals);
            let outranked = chosen.get(group_id).is_some_and(|(term, sampled_at, _)| {
                (*term, *sampled_at) >= (group.term, share.sampled_at)
            });
            if !outranked {
                chosen.insert(*group_id, candidate);
            }
        }
    }
    let mut out = TenantTotals::new();
    for (_, _, totals) in chosen.values() {
        add_totals(&mut out, totals);
    }
    out
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use nodedb_types::{DatabaseId, TenantId};

    use super::*;
    use crate::control::metrics::storage::shares::GroupTotals;

    /// A share sampled at `at` of `(group, term, bytes)` rows, all bytes in
    /// database 1, tenant 1.
    fn share(at: Instant, rows: &[(u64, u64, u64)]) -> StorageShare {
        let groups: BTreeMap<u64, GroupTotals> = rows
            .iter()
            .map(|(group_id, term, bytes)| {
                let mut totals = TenantTotals::new();
                totals.insert(key(), *bytes);
                (
                    *group_id,
                    GroupTotals {
                        term: *term,
                        totals,
                    },
                )
            })
            .collect();
        StorageShare {
            groups,
            led_vshards: Vec::new(),
            sampled_at: at,
        }
    }

    fn key() -> (DatabaseId, TenantId) {
        (DatabaseId::new(1), TenantId::new(1))
    }

    fn bytes(totals: &TenantTotals) -> u64 {
        totals.get(&key()).copied().unwrap_or(0)
    }

    /// Disjoint groups sum.
    #[test]
    fn disjoint_groups_sum() {
        let now = Instant::now();
        let a = share(now, &[(1, 1, 100)]);
        let b = share(now, &[(2, 1, 30)]);
        assert_eq!(bytes(&cluster_totals([&a, &b])), 130);
    }

    /// A group in two shares counts once, from the higher term.
    #[test]
    fn the_highest_term_wins_per_group() {
        let now = Instant::now();
        let old_leader = share(now + Duration::from_secs(1), &[(1, 3, 100), (2, 3, 7)]);
        let new_leader = share(now, &[(1, 4, 120)]);
        let totals = cluster_totals([&old_leader, &new_leader]);
        assert_eq!(
            bytes(&totals),
            127,
            "group 1 from term 4, group 2 from term 3"
        );
        let reversed = cluster_totals([&new_leader, &old_leader]);
        assert_eq!(bytes(&reversed), 127, "the order of shares does not matter");
    }

    /// At equal terms the newer sample wins.
    #[test]
    fn a_tie_takes_the_newer_sample() {
        let now = Instant::now();
        let older = share(now, &[(1, 5, 10)]);
        let newer = share(now + Duration::from_secs(2), &[(1, 5, 11)]);
        assert_eq!(bytes(&cluster_totals([&older, &newer])), 11);
        assert_eq!(bytes(&cluster_totals([&newer, &older])), 11);
    }

    /// A led group with no bytes at a higher term zeroes the group.
    #[test]
    fn an_empty_group_at_a_higher_term_wins() {
        let now = Instant::now();
        let old_leader = share(now, &[(1, 2, 50)]);
        let mut new_leader = share(now, &[]);
        new_leader.groups.insert(
            1,
            GroupTotals {
                term: 3,
                totals: TenantTotals::new(),
            },
        );
        assert_eq!(bytes(&cluster_totals([&old_leader, &new_leader])), 0);
    }

    /// A node alone totals its own share.
    #[test]
    fn a_single_node_totals_its_own_share() {
        let local = share(Instant::now(), &[(0, 0, 42)]);
        assert_eq!(cluster_totals([&local]), local.totals());
    }

    /// A report ages out after the TTL, and a peer that left drops at once.
    #[test]
    fn stale_and_departed_reports_drop() {
        let start = Instant::now();
        let ttl = Duration::from_secs(90);
        let mut reports = PeerReports::default();
        reports.record(2, share(start, &[(1, 1, 10)]));
        reports.record(3, share(start + Duration::from_secs(60), &[(2, 1, 20)]));
        let members = [2, 3];

        reports.retain(&members, start + ttl, ttl);
        assert!(reports.holds(2) && reports.holds(3));

        reports.retain(&members, start + ttl + Duration::from_secs(1), ttl);
        assert!(!reports.holds(2), "the report of node 2 is past the TTL");
        assert!(reports.holds(3));
        let shares: Vec<&StorageShare> = reports.shares().collect();
        assert_eq!(bytes(&cluster_totals(shares)), 20);

        let only_two = [2];
        reports.retain(&only_two, start + Duration::from_secs(61), ttl);
        assert!(!reports.holds(3), "node 3 left the topology");
    }
}
