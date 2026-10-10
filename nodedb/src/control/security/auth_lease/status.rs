// SPDX-License-Identifier: BUSL-1.1

//! This node's lease state, as `/healthz` and the metrics endpoint report it.
//!
//! Boot waits for the first lease before the gates open. A node that later
//! loses its lease refuses every permission-checked statement until it
//! renews. It still serves every other path, so readiness reports it as
//! degraded, with the reason, for as long as it holds no valid lease.

use std::fmt::Write as _;
use std::time::{Duration, Instant};

use crate::control::security::auth_fence::AuthorizationFence;
use crate::control::state::SharedState;

use super::leadership::sole_voter_term;

/// Whether this node can plan permission-checked statements now.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LeaseStatus {
    /// A single node: no lease exists and none is needed.
    NotRequired,
    /// The lease is valid for `remaining`.
    Valid { remaining: Duration },
    /// This node leads the metadata group as its only voter and holds a
    /// pinned lease, which has no expiry (see [`super::table`]).
    SoleVoter,
    /// No valid lease. `expired_for` is how long ago the last one ended, or
    /// `None` when no lease was ever granted.
    Invalid { expired_for: Option<Duration> },
}

impl LeaseStatus {
    /// Whether this node can plan permission-checked statements.
    pub fn admits_planning(&self) -> bool {
        !matches!(self, Self::Invalid { .. })
    }
}

/// The lease state of this node at `now`.
pub fn lease_status(state: &SharedState, now: Instant) -> LeaseStatus {
    lease_status_of(
        &state.authorization_fence,
        || sole_voter_term(state),
        state.node_id,
        now,
    )
}

/// The lease state of node `node_id` at `now`.
///
/// `sole_voter_term` returns the term in which this node leads the metadata
/// group as its only voter. It runs only when the bounded lease is not
/// valid, so the planning path reads the Raft status only after a lapse.
pub(crate) fn lease_status_of(
    fence: &AuthorizationFence,
    sole_voter_term: impl FnOnce() -> Option<u64>,
    node_id: u64,
    now: Instant,
) -> LeaseStatus {
    if fence.timing().is_none() {
        return LeaseStatus::NotRequired;
    }
    let valid_until = fence.holder().valid_until();
    if let Some(until) = valid_until.filter(|until| now < *until) {
        return LeaseStatus::Valid {
            remaining: until - now,
        };
    }
    let pinned = sole_voter_term().is_some_and(|term| {
        fence
            .leader()
            .is_some_and(|service| service.holds_pinned_lease(term, node_id))
    });
    if pinned {
        return LeaseStatus::SoleVoter;
    }
    LeaseStatus::Invalid {
        expired_for: valid_until.map(|until| now.saturating_duration_since(until)),
    }
}

/// Wait until this node can plan permission-checked statements, or refuse
/// once `timeout` passes.
///
/// `timeout` is a hard deadline on the first lease grant, not the metadata
/// group's stall bound. The two waits end differently: the stall bound resets
/// every time the group applies an entry, and this one never does, so a caller
/// that has no reason of its own passes its own constant rather than the one
/// the group uses.
pub async fn await_planning_admitted(state: &SharedState, timeout: Duration) -> crate::Result<()> {
    if planning_admitted_within(state, timeout).await {
        return Ok(());
    }
    Err(crate::Error::AuthorizationStateBehind {
        detail: format!(
            "no authorization lease was granted within {timeout:?}; last renewal: {}",
            state.authorization_fence.holder().last_attempt()
        ),
    })
}

/// Wait at most `within` until this node can plan permission-checked
/// statements, and return whether it can. Each renewal round wakes the
/// wait, so it ends as soon as a round grants a lease.
pub async fn planning_admitted_within(state: &SharedState, within: Duration) -> bool {
    admitted_within(
        &state.authorization_fence,
        || sole_voter_term(state),
        state.node_id,
        within,
    )
    .await
}

/// [`planning_admitted_within`] for node `node_id` behind `fence`.
pub(crate) async fn admitted_within(
    fence: &AuthorizationFence,
    sole_voter_term: impl Fn() -> Option<u64>,
    node_id: u64,
    within: Duration,
) -> bool {
    let admitted =
        || lease_status_of(fence, &sole_voter_term, node_id, Instant::now()).admits_planning();
    if admitted() {
        return true;
    }
    let deadline = tokio::time::Instant::now() + within;
    // Subscribed before the next check, so no round between the two is missed.
    let mut rounds = fence.holder().subscribe_rounds();
    loop {
        if admitted() {
            return true;
        }
        if tokio::time::Instant::now() >= deadline {
            return false;
        }
        tokio::select! {
            changed = rounds.changed() => {
                // The holder lives as long as `fence`, so its sender does too.
                // Without one, only the deadline is left to wait for.
                if changed.is_err() {
                    tokio::time::sleep_until(deadline).await;
                }
            }
            _ = tokio::time::sleep_until(deadline) => {}
        }
    }
}

/// Append the lease gauges. A single node holds no lease and emits none.
///
/// - `nodedb_authorization_lease_valid`: 1 while the lease is valid, else 0.
/// - `nodedb_authorization_lease_remaining_seconds`: time left on the lease,
///   0 without one, `+Inf` for a pinned lease.
pub fn render_prometheus(state: &SharedState, out: &mut String) {
    render_status(lease_status(state, Instant::now()), out);
}

fn render_status(status: LeaseStatus, out: &mut String) {
    let (valid, remaining) = match status {
        LeaseStatus::NotRequired => return,
        LeaseStatus::Valid { remaining } => (1, remaining.as_secs_f64()),
        LeaseStatus::SoleVoter => (1, f64::INFINITY),
        LeaseStatus::Invalid { .. } => (0, 0.0),
    };
    let _ = writeln!(
        out,
        "# HELP nodedb_authorization_lease_valid Whether this node holds a valid \
         authorization lease and can plan permission-checked statements\n\
         # TYPE nodedb_authorization_lease_valid gauge\n\
         nodedb_authorization_lease_valid {valid}\n\
         # HELP nodedb_authorization_lease_remaining_seconds Time left on this \
         node's authorization lease\n\
         # TYPE nodedb_authorization_lease_remaining_seconds gauge\n\
         nodedb_authorization_lease_remaining_seconds {}",
        prometheus_float(remaining)
    );
}

/// A gauge value in the Prometheus text format, which spells infinity `+Inf`.
fn prometheus_float(value: f64) -> String {
    if value.is_infinite() {
        "+Inf".to_string()
    } else {
        value.to_string()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Weak};

    use nodedb_cluster::GroupCoverage;

    use super::*;
    use crate::control::security::auth_lease::table::RenewDecision;
    use crate::control::security::auth_lease::{LeaderLeaseService, LeaseTiming};
    use crate::control::security::permission_tree::SourceIndex;

    const NODE: u64 = 1;
    const TERM: u64 = 3;

    fn timing() -> LeaseTiming {
        LeaseTiming::from_raft(Duration::from_millis(1000), Duration::from_millis(100))
            .expect("timing")
    }

    fn fence() -> AuthorizationFence {
        AuthorizationFence::new(Arc::new(SourceIndex::default()))
    }

    /// A fence whose leader service granted this node's lease at `granted_at`
    /// in `TERM`, pinned when `sole_voter` holds, as one renewal round does.
    fn fence_granted_at(granted_at: Instant, sole_voter: bool) -> AuthorizationFence {
        let fence = fence();
        let timing = timing();
        assert!(fence.install_timing(timing));
        let service = Arc::new(LeaderLeaseService::new(Weak::new(), timing));
        let coverage = [GroupCoverage {
            group_id: 0,
            through: 10,
        }];
        service.edit_table(TERM, granted_at, |table| {
            table.load_floors(&coverage);
            table.observe_sole_voter(sole_voter);
            assert_eq!(
                table.renew(NODE, &coverage, granted_at, timing.lease),
                RenewDecision::Granted
            );
            if sole_voter {
                table.pin(NODE);
            }
        });
        assert!(fence.install_leader(service));
        fence
            .holder()
            .install(timing.holder_expiry(granted_at, timing.lease));
        fence
    }

    #[test]
    fn a_node_without_timing_needs_no_lease() {
        let fence = fence();
        let status = lease_status_of(&fence, || None, NODE, Instant::now());
        assert_eq!(status, LeaseStatus::NotRequired);
        let mut out = String::new();
        render_status(status, &mut out);
        assert!(out.is_empty());
    }

    #[test]
    fn a_lapsed_lease_is_invalid_and_reports_zero() {
        let fence = fence();
        assert!(fence.install_timing(timing()));
        let now = Instant::now();
        assert_eq!(
            lease_status_of(&fence, || None, NODE, now),
            LeaseStatus::Invalid { expired_for: None }
        );

        fence.holder().install(now + Duration::from_secs(2));
        let status = lease_status_of(&fence, || None, NODE, now);
        assert_eq!(
            status,
            LeaseStatus::Valid {
                remaining: Duration::from_secs(2)
            }
        );
        let mut out = String::new();
        render_status(status, &mut out);
        assert!(out.contains("nodedb_authorization_lease_valid 1"));

        let later = now + Duration::from_secs(3);
        let status = lease_status_of(&fence, || None, NODE, later);
        assert_eq!(
            status,
            LeaseStatus::Invalid {
                expired_for: Some(Duration::from_secs(1))
            }
        );
        assert!(!status.admits_planning());
    }

    /// The renewal task of a sole-voter node is starved for a whole lease
    /// duration. Its bounded lease lapses, yet the node still plans: its
    /// lease is pinned, and every barrier waits for its coverage.
    #[test]
    fn a_starved_renewal_on_a_sole_voter_keeps_planning() {
        let granted_at = Instant::now();
        let fence = fence_granted_at(granted_at, true);
        let starved = granted_at + timing().lease;
        assert!(
            !fence.holder().is_valid_at(starved),
            "the bounded lease lapsed"
        );

        let status = lease_status_of(&fence, || Some(TERM), NODE, starved);
        assert_eq!(status, LeaseStatus::SoleVoter);
        assert!(status.admits_planning());
        let mut out = String::new();
        render_status(status, &mut out);
        assert!(out.contains("nodedb_authorization_lease_valid 1"));
        assert!(out.contains("nodedb_authorization_lease_remaining_seconds +Inf"));
    }

    /// A cluster node whose renewal lapses cannot confirm it holds the latest
    /// authorization state. It refuses, whatever its table once recorded.
    #[test]
    fn a_node_with_a_peer_voter_refuses_once_its_lease_lapses() {
        let granted_at = Instant::now();
        let starved = granted_at + timing().lease;

        // A lease granted while a peer voter existed was never pinned.
        let fence = fence_granted_at(granted_at, false);
        let status = lease_status_of(&fence, || None, NODE, starved);
        assert!(matches!(status, LeaseStatus::Invalid { .. }));
        assert!(!status.admits_planning());

        // A pinned lease stops counting the moment a peer voter joins.
        let fence = fence_granted_at(granted_at, true);
        let status = lease_status_of(&fence, || None, NODE, starved);
        assert!(!status.admits_planning());
    }

    /// A barrier that runs once a peer voter joined removes the pin. The node
    /// then refuses even after it is the only voter again, until a new grant.
    #[test]
    fn a_pin_removed_by_a_barrier_stays_removed() {
        let granted_at = Instant::now();
        let fence = fence_granted_at(granted_at, true);
        let starved = granted_at + timing().lease;
        fence
            .leader()
            .expect("leader service")
            .edit_table(TERM, starved, |table| table.observe_sole_voter(false));
        let status = lease_status_of(&fence, || Some(TERM), NODE, starved);
        assert!(!status.admits_planning());
    }

    /// A lease that lapsed while a round ran long admits planning as soon as
    /// that round grants it, well before the wait's bound.
    #[tokio::test]
    async fn a_lapsed_lease_admits_once_the_next_round_grants() {
        let fence = Arc::new(fence());
        assert!(fence.install_timing(timing()));
        let within = Duration::from_secs(10);
        let renewer = Arc::clone(&fence);
        let round = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            renewer
                .holder()
                .install(Instant::now() + Duration::from_secs(5));
            renewer.holder().record_attempt(
                crate::control::security::auth_lease::RenewAttempt::Granted { leader_id: NODE },
            );
        });

        let started = Instant::now();
        assert!(admitted_within(&fence, || None, NODE, within).await);
        assert!(started.elapsed() < within / 2, "woken by the round");
        round.await.expect("round");
    }

    /// Without a granting round the wait ends at the grace and refuses.
    #[tokio::test]
    async fn a_lapsed_lease_refuses_after_the_grace() {
        let fence = fence();
        assert!(fence.install_timing(timing()));
        let grace = Duration::from_millis(50);
        fence
            .holder()
            .record_attempt(crate::control::security::auth_lease::RenewAttempt::NoLeader);

        let started = Instant::now();
        assert!(!admitted_within(&fence, || None, NODE, grace).await);
        assert!(started.elapsed() >= grace);
    }

    /// A pin belongs to the leadership term that granted it.
    #[test]
    fn a_pin_from_an_earlier_term_does_not_count() {
        let granted_at = Instant::now();
        let fence = fence_granted_at(granted_at, true);
        let starved = granted_at + timing().lease;
        let status = lease_status_of(&fence, || Some(TERM + 1), NODE, starved);
        assert!(!status.admits_planning());
    }
}
