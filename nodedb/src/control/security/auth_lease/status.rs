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

/// Whether this node can plan permission-checked statements now.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LeaseStatus {
    /// A single node: no lease exists and none is needed.
    NotRequired,
    /// The lease is valid for `remaining`.
    Valid { remaining: Duration },
    /// No valid lease. `expired_for` is how long ago the last one ended, or
    /// `None` when no lease was ever granted.
    Invalid { expired_for: Option<Duration> },
}

/// The lease state at `now`.
pub fn lease_status(fence: &AuthorizationFence, now: Instant) -> LeaseStatus {
    if fence.timing().is_none() {
        return LeaseStatus::NotRequired;
    }
    match fence.holder().valid_until() {
        Some(until) if now < until => LeaseStatus::Valid {
            remaining: until - now,
        },
        Some(until) => LeaseStatus::Invalid {
            expired_for: Some(now.saturating_duration_since(until)),
        },
        None => LeaseStatus::Invalid { expired_for: None },
    }
}

/// Append the lease gauges. A single node holds no lease and emits none.
///
/// - `nodedb_authorization_lease_valid`: 1 while the lease is valid, else 0.
/// - `nodedb_authorization_lease_remaining_seconds`: time left on the lease,
///   0 without one.
pub fn render_prometheus(fence: &AuthorizationFence, out: &mut String) {
    let (valid, remaining) = match lease_status(fence, Instant::now()) {
        LeaseStatus::NotRequired => return,
        LeaseStatus::Valid { remaining } => (1, remaining),
        LeaseStatus::Invalid { .. } => (0, Duration::ZERO),
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
        remaining.as_secs_f64()
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::security::auth_lease::LeaseTiming;
    use crate::control::security::permission_tree::SourceIndex;

    fn timing() -> LeaseTiming {
        LeaseTiming::from_raft(Duration::from_millis(1000), Duration::from_millis(100))
            .expect("timing")
    }

    #[test]
    fn a_node_without_timing_needs_no_lease() {
        let fence = AuthorizationFence::new(std::sync::Arc::new(SourceIndex::default()));
        assert_eq!(
            lease_status(&fence, Instant::now()),
            LeaseStatus::NotRequired
        );
        let mut out = String::new();
        render_prometheus(&fence, &mut out);
        assert!(out.is_empty());
    }

    #[test]
    fn a_lapsed_lease_is_invalid_and_reports_zero() {
        let fence = AuthorizationFence::new(std::sync::Arc::new(SourceIndex::default()));
        assert!(fence.install_timing(timing()));
        let now = Instant::now();
        assert_eq!(
            lease_status(&fence, now),
            LeaseStatus::Invalid { expired_for: None }
        );

        fence.holder().install(now + Duration::from_secs(2));
        assert_eq!(
            lease_status(&fence, now),
            LeaseStatus::Valid {
                remaining: Duration::from_secs(2)
            }
        );
        let mut out = String::new();
        render_prometheus(&fence, &mut out);
        assert!(out.contains("nodedb_authorization_lease_valid 1"));

        let later = now + Duration::from_secs(3);
        assert_eq!(
            lease_status(&fence, later),
            LeaseStatus::Invalid {
                expired_for: Some(Duration::from_secs(1))
            }
        );
    }
}
