// SPDX-License-Identifier: BUSL-1.1

//! Where a node parks the tenant captures its cut barriers took, until the
//! database backup that asked for them collects them.
//!
//! Every replica of a group applies the same log, so every replica sees the
//! same first barrier of a request in that group. Only that first barrier
//! captures. A later barrier of the same request, a leader's second proposal
//! of it, sits above entries the first barrier already cut away, and never
//! captures.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

/// How long an uncollected capture, or the note of a request's first
/// barrier, stays on this node. A backup collects its captures within its
/// statement deadline, far below this.
const CAPTURE_TTL: Duration = Duration::from_secs(30 * 60);

/// One group's capture of a request's tenants.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum GroupCapture {
    /// `(tenant_id, TenantDataSnapshot msgpack)` per tenant, filtered to the
    /// group's vShards.
    Taken(Vec<(u64, Vec<u8>)>),
    /// The snapshot failed. The backup fails with this reason.
    Failed(String),
}

#[derive(Default)]
struct Registry {
    /// `(request, group)` whose first barrier this node applied.
    seen: HashMap<(u64, u64), Instant>,
    parked: HashMap<(u64, u64), (Instant, GroupCapture)>,
}

impl Registry {
    fn sweep(&mut self, now: Instant) {
        self.seen
            .retain(|_, at| now.saturating_duration_since(*at) < CAPTURE_TTL);
        self.parked
            .retain(|_, (at, _)| now.saturating_duration_since(*at) < CAPTURE_TTL);
    }
}

/// Every capture this node parked, by `(request, group)`.
#[derive(Default)]
pub struct CutCaptures {
    inner: Mutex<Registry>,
}

impl CutCaptures {
    pub fn new() -> Self {
        Self::default()
    }

    /// Note a barrier of `request` in `group`. `true` for the first one only.
    pub(crate) fn first_barrier(&self, request: u64, group: u64) -> bool {
        let now = Instant::now();
        let mut registry = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        registry.sweep(now);
        registry.seen.insert((request, group), now).is_none()
    }

    /// Park `capture` for `request` in `group`.
    pub(crate) fn park(&self, request: u64, group: u64, capture: GroupCapture) {
        let mut registry = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        registry
            .parked
            .insert((request, group), (Instant::now(), capture));
    }

    /// Remove and return every capture of `request` this node parked, by
    /// group.
    pub(crate) fn take_all(&self, request: u64) -> Vec<(u64, GroupCapture)> {
        let mut registry = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        let groups: Vec<u64> = registry
            .parked
            .keys()
            .filter(|(r, _)| *r == request)
            .map(|(_, group)| *group)
            .collect();
        groups
            .into_iter()
            .filter_map(|group| {
                registry
                    .parked
                    .remove(&(request, group))
                    .map(|(_, capture)| (group, capture))
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_the_first_barrier_of_a_request_captures() {
        let captures = CutCaptures::new();
        assert!(captures.first_barrier(7, 1));
        assert!(!captures.first_barrier(7, 1));
        assert!(captures.first_barrier(7, 2));
        assert!(captures.first_barrier(8, 1));
    }

    #[test]
    fn take_all_returns_one_request_and_removes_it() {
        let captures = CutCaptures::new();
        captures.park(7, 1, GroupCapture::Taken(vec![(1, vec![1])]));
        captures.park(7, 2, GroupCapture::Failed("x".into()));
        captures.park(8, 1, GroupCapture::Taken(Vec::new()));
        let mut taken = captures.take_all(7);
        taken.sort_by_key(|(group, _)| *group);
        assert_eq!(
            taken,
            vec![
                (1, GroupCapture::Taken(vec![(1, vec![1])])),
                (2, GroupCapture::Failed("x".into())),
            ]
        );
        assert!(captures.take_all(7).is_empty());
        assert_eq!(captures.take_all(8).len(), 1);
    }
}
