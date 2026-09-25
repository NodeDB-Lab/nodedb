// SPDX-License-Identifier: BUSL-1.1

//! Lease durations derived from the Raft timing.
//!
//! - **Lease:** the minimum election timeout. A leader that loses its
//!   quorum is replaced no sooner than that, the same bound Raft leader
//!   leases rely on.
//! - **Skew margin:** the heartbeat interval. The holder ends its lease this
//!   much before the leader does, so a holder clock that runs slow by up to
//!   the heartbeat-to-election ratio never outlives the leader's view.
//! - **Renewal:** every heartbeat interval. A holder that covers a change
//!   renews within one heartbeat, so an acknowledgement waits about one
//!   heartbeat when every node is healthy.

use std::time::{Duration, Instant};

/// Lease durations of one cluster.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LeaseTiming {
    /// How long a granted lease runs on the leader's clock.
    pub lease: Duration,
    /// How much earlier a holder ends its lease than the leader.
    pub skew_margin: Duration,
    /// How often a holder renews.
    pub renew_every: Duration,
}

impl LeaseTiming {
    /// Derive the timing from the Raft election timeout and heartbeat.
    pub fn from_raft(
        election_timeout_min: Duration,
        heartbeat_interval: Duration,
    ) -> crate::Result<Self> {
        if heartbeat_interval.is_zero() || heartbeat_interval >= election_timeout_min {
            return Err(crate::Error::Config {
                detail: format!(
                    "authorization lease: heartbeat interval {heartbeat_interval:?} must be \
                     non-zero and below the minimum election timeout {election_timeout_min:?}"
                ),
            });
        }
        Ok(Self {
            lease: election_timeout_min,
            skew_margin: heartbeat_interval,
            renew_every: heartbeat_interval,
        })
    }

    /// When a lease the leader granted for `granted`, requested at `sent_at`
    /// on the holder's clock, ends on the holder's clock.
    ///
    /// The leader starts its lease no earlier than it received the request,
    /// which is after `sent_at`. Ending at `sent_at + granted - skew_margin`
    /// therefore ends first on any holder clock that runs no slower than the
    /// margin allows.
    pub fn holder_expiry(&self, sent_at: Instant, granted: Duration) -> Instant {
        sent_at + granted.saturating_sub(self.skew_margin)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timing_follows_the_raft_configuration() {
        let timing = LeaseTiming::from_raft(Duration::from_millis(150), Duration::from_millis(50))
            .expect("timing");
        assert_eq!(timing.lease, Duration::from_millis(150));
        assert_eq!(timing.skew_margin, Duration::from_millis(50));
        assert_eq!(timing.renew_every, Duration::from_millis(50));
    }

    #[test]
    fn a_heartbeat_at_or_above_the_election_timeout_is_refused() {
        assert!(
            LeaseTiming::from_raft(Duration::from_millis(50), Duration::from_millis(50)).is_err()
        );
        assert!(LeaseTiming::from_raft(Duration::from_millis(50), Duration::ZERO).is_err());
    }

    /// The holder ends its lease a skew margin before the leader does, even
    /// when the leader granted at the very moment the request left.
    #[test]
    fn the_holder_expires_early_by_the_skew_margin() {
        let timing = LeaseTiming::from_raft(Duration::from_millis(150), Duration::from_millis(50))
            .expect("timing");
        let sent_at = Instant::now();
        let holder_end = timing.holder_expiry(sent_at, timing.lease);
        let leader_end = sent_at + timing.lease;
        assert_eq!(leader_end - holder_end, timing.skew_margin);
        // A holder clock slow by a third of the lease still ends first: 100ms
        // of holder time is at most 133ms of real time, inside 150ms.
        let slow_real_elapsed = (holder_end - sent_at).mul_f64(4.0 / 3.0);
        assert!(sent_at + slow_real_elapsed <= leader_end);
    }
}
