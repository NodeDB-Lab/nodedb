// SPDX-License-Identifier: BUSL-1.1

//! The storage round: merge this node's share with every peer's into the
//! cluster total, and publish it.
//!
//! A round runs after each local sample. It asks every peer that can lead a
//! group, keeps each answer as that peer's last report, and drops a report
//! three sample periods old or from a peer that left the topology. Each Raft
//! group then counts once, from the report with the highest term.
//!
//! Totals publish once a complete round runs: a fresh local share and a
//! report from every active peer. From then on every round publishes, so a
//! peer that stops answering ages out of the total.

use std::time::{Duration, Instant};

use crate::control::state::SharedState;

use super::aggregate::{PeerReports, cluster_totals, is_fresh};
use super::peers::{Membership, fetch_peer_shares, membership};
use super::publish::publish_cluster_totals;
use super::shares::{StorageShare, TenantTotals};

/// The longest a round waits for one peer.
const PEER_TIMEOUT: Duration = Duration::from_secs(5);

/// A report counts for this many sample periods after its sample.
const REPORT_TTL_PERIODS: u32 = 3;

/// The peers `membership` asks.
fn asked(membership: &Membership) -> &[u64] {
    match membership {
        Membership::Peers { asked, .. } => asked,
        Membership::Alone | Membership::Unknown => &[],
    }
}

/// The merged result of one round.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoundOutcome {
    pub totals: TenantTotals,
    /// Whether the local share and every active peer's report counted.
    pub complete: bool,
}

/// The aggregator a storage loop owns across rounds.
#[derive(Debug)]
pub struct StorageAggregator {
    period: Duration,
    peers: PeerReports,
}

impl StorageAggregator {
    /// An aggregator for rounds every `period`.
    pub fn new(period: Duration) -> Self {
        Self {
            period,
            peers: PeerReports::default(),
        }
    }

    /// How old a report can be and still count.
    fn ttl(&self) -> Duration {
        self.period.saturating_mul(REPORT_TTL_PERIODS)
    }

    /// The wait for one peer: [`PEER_TIMEOUT`], or one period when shorter.
    fn peer_timeout(&self) -> Duration {
        PEER_TIMEOUT.min(self.period)
    }

    /// Run one round against `state` and publish its totals. No lock is held
    /// across the peer requests.
    pub async fn run_round(&mut self, state: &SharedState) {
        let membership = membership(state);
        let answers = fetch_peer_shares(state, asked(&membership), self.peer_timeout()).await;
        let local = state.storage_shares.latest();
        let outcome = self.merge(&membership, answers, local.as_deref(), Instant::now());
        if outcome.complete || state.cluster_storage_totals.has_total() {
            publish_cluster_totals(state, outcome.totals, outcome.complete);
        }
    }

    /// Fold `answers` into the held reports and merge them with `local`.
    fn merge(
        &mut self,
        membership: &Membership,
        answers: Vec<(u64, crate::Result<Option<StorageShare>>)>,
        local: Option<&StorageShare>,
        now: Instant,
    ) -> RoundOutcome {
        for (peer, answer) in answers {
            match answer {
                Ok(Some(share)) => self.peers.record(peer, share),
                Ok(None) => tracing::debug!(peer, "storage share: the peer has not sampled yet"),
                Err(error) => {
                    crate::diag::storage_share_peer_skipped(peer, &error);
                    tracing::warn!(peer, %error, "storage share: the round skips a peer");
                }
            }
        }
        let ttl = self.ttl();
        self.peers.retain(asked(membership), now, ttl);

        let local = local.filter(|share| is_fresh(share, now, ttl));
        let peers_reported = match membership {
            Membership::Alone => true,
            Membership::Peers { required, .. } => {
                required.iter().all(|peer| self.peers.holds(*peer))
            }
            Membership::Unknown => false,
        };
        RoundOutcome {
            totals: cluster_totals(local.into_iter().chain(self.peers.shares())),
            complete: local.is_some() && peers_reported,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use nodedb_types::{DatabaseId, TenantId};

    use super::*;
    use crate::control::metrics::storage::shares::GroupTotals;

    const PERIOD: Duration = Duration::from_secs(30);

    fn key() -> (DatabaseId, TenantId) {
        (DatabaseId::new(1), TenantId::new(1))
    }

    /// A share sampled at `at` of `(group, term, bytes)` rows.
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

    fn peers(asked: &[u64], required: &[u64]) -> Membership {
        Membership::Peers {
            asked: asked.to_vec(),
            required: required.to_vec(),
        }
    }

    fn bytes(outcome: &RoundOutcome) -> u64 {
        outcome.totals.get(&key()).copied().unwrap_or(0)
    }

    /// A node alone totals its own share, and the round is complete.
    #[test]
    fn a_single_node_equals_its_local_share() {
        let now = Instant::now();
        let local = share(now, &[(0, 0, 77)]);
        let mut aggregator = StorageAggregator::new(PERIOD);
        let outcome = aggregator.merge(&Membership::Alone, Vec::new(), Some(&local), now);
        assert!(outcome.complete);
        assert_eq!(outcome.totals, local.totals());
    }

    /// Per group, the highest term wins across the local share and peers.
    #[test]
    fn the_highest_term_wins_across_local_and_peers() {
        let now = Instant::now();
        let local = share(now, &[(1, 4, 120)]);
        let answers = vec![
            (2, Ok(Some(share(now, &[(1, 3, 100), (2, 3, 9)])))),
            (3, Ok(Some(share(now, &[(3, 1, 1)])))),
        ];
        let mut aggregator = StorageAggregator::new(PERIOD);
        let outcome = aggregator.merge(&peers(&[2, 3], &[2, 3]), answers, Some(&local), now);
        assert!(outcome.complete);
        assert_eq!(bytes(&outcome), 130, "group 1 counts once, from term 4");
    }

    /// An erroring peer is skipped: the others count, and the round is
    /// incomplete until it reports.
    #[test]
    fn an_erroring_peer_is_skipped() {
        let now = Instant::now();
        let local = share(now, &[(1, 1, 10)]);
        let answers = vec![
            (2, Ok(Some(share(now, &[(2, 1, 20)])))),
            (
                3,
                Err(crate::Error::Dispatch {
                    detail: "unreachable".into(),
                }),
            ),
        ];
        let mut aggregator = StorageAggregator::new(PERIOD);
        let outcome = aggregator.merge(&peers(&[2, 3], &[2, 3]), answers, Some(&local), now);
        assert!(!outcome.complete);
        assert_eq!(bytes(&outcome), 30);
    }

    /// A peer's last report holds through a failed round and drops after
    /// three periods.
    #[test]
    fn a_stale_peer_drops_after_the_ttl() {
        let start = Instant::now();
        let membership = peers(&[2], &[2]);
        let mut aggregator = StorageAggregator::new(PERIOD);
        let answers = vec![(2, Ok(Some(share(start, &[(2, 1, 20)]))))];
        let local = share(start, &[(1, 1, 10)]);
        let outcome = aggregator.merge(&membership, answers, Some(&local), start);
        assert_eq!(bytes(&outcome), 30);

        let failed = || {
            vec![(
                2,
                Err(crate::Error::Dispatch {
                    detail: "timeout".into(),
                }),
            )]
        };
        let later = start + PERIOD * 3;
        let local = share(later, &[(1, 1, 10)]);
        let outcome = aggregator.merge(&membership, failed(), Some(&local), later);
        assert!(outcome.complete, "the held report is still fresh");
        assert_eq!(bytes(&outcome), 30);

        let past = later + Duration::from_secs(1);
        let local = share(past, &[(1, 1, 10)]);
        let outcome = aggregator.merge(&membership, failed(), Some(&local), past);
        assert!(!outcome.complete);
        assert_eq!(bytes(&outcome), 10, "the stale report no longer counts");
    }

    /// A report of a peer that left the topology drops at once.
    #[test]
    fn a_departed_peer_drops() {
        let now = Instant::now();
        let mut aggregator = StorageAggregator::new(PERIOD);
        let answers = vec![(2, Ok(Some(share(now, &[(2, 1, 20)]))))];
        aggregator.merge(&peers(&[2], &[2]), answers, None, now);
        let local = share(now, &[(1, 1, 10)]);
        let outcome = aggregator.merge(&Membership::Alone, Vec::new(), Some(&local), now);
        assert_eq!(bytes(&outcome), 10);
    }

    /// A stale local share does not count, and the round is incomplete.
    #[test]
    fn a_stale_local_share_does_not_count() {
        let start = Instant::now();
        let local = share(start, &[(1, 1, 10)]);
        let mut aggregator = StorageAggregator::new(PERIOD);
        let now = start + PERIOD * 3 + Duration::from_secs(1);
        let outcome = aggregator.merge(&Membership::Alone, Vec::new(), Some(&local), now);
        assert!(!outcome.complete);
        assert!(outcome.totals.is_empty());
    }

    /// Routing with unknown peers never completes a round.
    #[test]
    fn unknown_membership_is_incomplete() {
        let now = Instant::now();
        let local = share(now, &[(1, 1, 10)]);
        let mut aggregator = StorageAggregator::new(PERIOD);
        let outcome = aggregator.merge(&Membership::Unknown, Vec::new(), Some(&local), now);
        assert!(!outcome.complete);
        assert_eq!(bytes(&outcome), 10);
    }

    #[test]
    fn the_peer_timeout_never_exceeds_the_period() {
        assert_eq!(StorageAggregator::new(PERIOD).peer_timeout(), PEER_TIMEOUT);
        let short = Duration::from_millis(200);
        assert_eq!(StorageAggregator::new(short).peer_timeout(), short);
    }
}
