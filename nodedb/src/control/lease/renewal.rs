//! Descriptor lease renewal loop.
//!
//! Spawned once per cluster node at startup. Wakes every
//! `check_interval`, walks the local node's leases in
//! `metadata_cache.leases`, and re-acquires every lease whose
//! remaining time is below `threshold_pct` of the original
//! duration. Re-acquire goes through the standard
//! `SharedState::acquire_descriptor_lease` slow path, which
//! transparently forwards to the metadata-group leader if this
//! node isn't it.
//!
//! The loop holds `Weak<SharedState>` so process shutdown is not
//! blocked on its tokio task. On every tick it upgrades to a
//! strong reference, doing nothing if the upgrade fails.
//!
//! **Single-node clusters skip this loop entirely.** In single-node
//! mode there is no metadata raft handle, every `acquire_lease`
//! call writes straight into the local cache, and there is no
//! concurrent writer that could expire a lease behind the loop's
//! back. The `spawn` constructor returns `None` in that case so
//! the embedded usage path doesn't carry an idle tokio task.
//!
//! The DDL drain gate reads `metadata_cache.leases` to decide
//! when a `Put*` of a new descriptor version may commit. The
//! renewal loop is what keeps that map populated past initial
//! acquisition.

use std::sync::{Arc, Weak};
use std::time::Duration;

use nodedb_cluster::DescriptorId;
#[cfg(test)]
use nodedb_cluster::DescriptorLease;
use nodedb_types::config::tuning::ClusterTransportTuning;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};

use crate::control::state::SharedState;

/// Configuration extracted from `ClusterTransportTuning` at spawn
/// time. Captured so the loop has stable values for the duration
/// of its life — tuning hot-reload (if it ever lands) would need
/// to restart the loop.
#[derive(Debug, Clone, Copy)]
pub struct LeaseRenewalConfig {
    /// How often the loop wakes up to check for near-expiry leases.
    pub check_interval: Duration,
    /// The full lease duration assigned to renewed leases. Equal
    /// to `descriptor_lease_duration_secs` in seconds.
    pub full_duration: Duration,
    /// Renewal trigger threshold. A lease is re-acquired when
    /// `(expires_at - now) < (full_duration * threshold_pct / 100)`.
    /// Stored as a u8 0..=100, validated at construction.
    pub threshold_pct: u8,
}

impl LeaseRenewalConfig {
    pub fn from_tuning(tuning: &ClusterTransportTuning) -> Self {
        let threshold_pct = tuning.descriptor_lease_renewal_threshold_pct.min(100);
        Self {
            check_interval: Duration::from_secs(
                tuning.descriptor_lease_renewal_check_interval_secs.max(1),
            ),
            full_duration: Duration::from_secs(tuning.descriptor_lease_duration_secs.max(1)),
            threshold_pct,
        }
    }

    /// Compute the threshold in nanoseconds: leases with less than
    /// this much remaining time are eligible for renewal.
    pub(crate) fn threshold_remaining_ns(&self) -> u64 {
        let full_ns = self.full_duration.as_nanos() as u64;
        full_ns
            .saturating_mul(self.threshold_pct as u64)
            .saturating_div(100)
    }
}

/// Background task that keeps this node's descriptor leases fresh.
pub struct LeaseRenewalLoop {
    shared: Weak<SharedState>,
    config: LeaseRenewalConfig,
    shutdown_rx: watch::Receiver<bool>,
}

impl LeaseRenewalLoop {
    /// Spawn the renewal loop on the current tokio runtime. Returns
    /// `None` (and does not spawn anything) on single-node clusters
    /// where `metadata_raft` is not wired — see the module docstring.
    pub fn spawn(
        shared: Arc<SharedState>,
        tuning: &ClusterTransportTuning,
        shutdown_rx: watch::Receiver<bool>,
    ) -> Option<JoinHandle<()>> {
        if shared.metadata_raft.get().is_none() {
            debug!("descriptor lease renewal: skipping spawn (no metadata raft handle)");
            return None;
        }
        let config = LeaseRenewalConfig::from_tuning(tuning);
        info!(
            check_interval_secs = config.check_interval.as_secs(),
            full_duration_secs = config.full_duration.as_secs(),
            threshold_pct = config.threshold_pct,
            "descriptor lease renewal loop spawning"
        );
        let loop_handle = LeaseRenewalLoop {
            shared: Arc::downgrade(&shared),
            config,
            shutdown_rx,
        };
        Some(tokio::spawn(async move { loop_handle.run().await }))
    }

    async fn run(mut self) {
        let mut interval = tokio::time::interval(self.config.check_interval);
        // Skip the immediate first tick — at startup there are no
        // leases to renew yet, and the first real lease the planner
        // acquires won't be near expiry.
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        interval.tick().await;
        loop {
            tokio::select! {
                biased;
                _ = self.shutdown_rx.changed() => {
                    if *self.shutdown_rx.borrow() {
                        debug!("descriptor lease renewal loop shutting down");
                        return;
                    }
                }
                _ = interval.tick() => {
                    self.tick();
                }
            }
        }
    }

    /// One iteration: snapshot the near-expiry lease set under a
    /// short read lock, then re-acquire each one. Errors are
    /// logged at warn — the next tick retries automatically.
    ///
    /// **Why we use wall-clock nanoseconds, not `hlc_clock.peek()`**:
    /// `peek` returns the last HLC the clock observed, which may
    /// be frozen at the moment the lease was stamped if nothing
    /// else has advanced the clock since. We want "real time now"
    /// to compute `remaining = expires_at - now`, and the lease's
    /// `expires_at.wall_ns` was computed from real wall time when
    /// it was stamped. Comparing against `SystemTime::now()` keeps
    /// both sides of the subtraction in the same reference frame
    /// and avoids spuriously classifying leases as "not near
    /// expiry" just because the HLC hasn't ticked.
    fn tick(&self) {
        let Some(shared) = self.shared.upgrade() else {
            return;
        };
        let now_wall_ns = super::wall_now_ns();
        let near_expiry = collect_near_expiry(&shared, now_wall_ns, &self.config);
        if near_expiry.is_empty() {
            return;
        }
        debug!(
            count = near_expiry.len(),
            "descriptor lease renewal: re-acquiring near-expiry leases"
        );
        for (id, version) in near_expiry {
            // Bypass the fast path — we already know the lease is
            // near expiry and we WANT a fresh grant. Calling
            // `acquire_descriptor_lease` here would hit the
            // `expires_at > now` fast path and return the same
            // stale lease.
            if let Err(e) = super::propose::force_refresh_lease(
                &shared,
                id.clone(),
                version,
                self.config.full_duration,
            ) {
                warn!(
                    descriptor = ?id,
                    version,
                    error = %e,
                    "descriptor lease renewal: re-acquire failed"
                );
            }
        }
    }
}

/// Snapshot every lease in `(_, this_node_id)` whose remaining
/// time-to-expiry (relative to real wall-clock nanoseconds) is
/// below the configured threshold. Returns `(descriptor_id, version)`
/// pairs so the caller can release the read lock before issuing
/// the slow-path acquire that reacquires each one.
pub(crate) fn collect_near_expiry(
    shared: &SharedState,
    now_wall_ns: u64,
    config: &LeaseRenewalConfig,
) -> Vec<(DescriptorId, u64)> {
    let threshold_ns = config.threshold_remaining_ns();
    let cache = shared
        .metadata_cache
        .read()
        .unwrap_or_else(|p| p.into_inner());
    cache
        .leases
        .iter()
        .filter_map(|((id, node_id), lease)| {
            if *node_id != shared.node_id {
                return None;
            }
            let remaining_ns = lease.expires_at.wall_ns.saturating_sub(now_wall_ns);
            if remaining_ns < threshold_ns {
                Some((id.clone(), lease.version))
            } else {
                None
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_cluster::DescriptorKind;
    use nodedb_types::Hlc;
    use std::collections::HashMap;

    fn make_config(full_secs: u64, threshold_pct: u8) -> LeaseRenewalConfig {
        LeaseRenewalConfig {
            check_interval: Duration::from_secs(1),
            full_duration: Duration::from_secs(full_secs),
            threshold_pct,
        }
    }

    fn make_lease(node_id: u64, expires_wall_ns: u64) -> DescriptorLease {
        DescriptorLease {
            descriptor_id: DescriptorId::new(1, DescriptorKind::Collection, "x".to_string()),
            version: 1,
            node_id,
            expires_at: Hlc::new(expires_wall_ns, 0),
        }
    }

    #[test]
    fn threshold_remaining_ns_basic() {
        let config = make_config(100, 20);
        assert_eq!(config.threshold_remaining_ns(), 20 * 1_000_000_000);
    }

    #[test]
    fn threshold_remaining_ns_zero_pct() {
        let config = make_config(100, 0);
        assert_eq!(config.threshold_remaining_ns(), 0);
    }

    #[test]
    fn threshold_clamped_at_100() {
        let mut tuning = ClusterTransportTuning::default();
        tuning.descriptor_lease_renewal_threshold_pct = 250;
        let config = LeaseRenewalConfig::from_tuning(&tuning);
        assert_eq!(config.threshold_pct, 100);
    }

    /// Test the pure filtering logic of `collect_near_expiry` by
    /// walking a `HashMap<(DescriptorId, u64), DescriptorLease>`
    /// directly. We can't easily construct a full `SharedState`
    /// in a unit test, so this helper mirrors the real one but
    /// takes the cache inputs directly.
    fn collect_near_expiry_pure(
        leases: &HashMap<(DescriptorId, u64), DescriptorLease>,
        self_node_id: u64,
        now_wall_ns: u64,
        config: &LeaseRenewalConfig,
    ) -> Vec<(DescriptorId, u64)> {
        let threshold_ns = config.threshold_remaining_ns();
        leases
            .iter()
            .filter_map(|((id, node_id), lease)| {
                if *node_id != self_node_id {
                    return None;
                }
                let remaining_ns = lease.expires_at.wall_ns.saturating_sub(now_wall_ns);
                if remaining_ns < threshold_ns {
                    Some((id.clone(), lease.version))
                } else {
                    None
                }
            })
            .collect()
    }

    #[test]
    fn picks_only_near_expiry_self_leases() {
        let now_ns = 1_000_000_000_000u64;
        // 100s lease, 20% threshold = renew when < 20s remaining.
        let config = make_config(100, 20);

        // Lease 1: this node, 50s remaining → NOT near expiry.
        // Lease 2: this node, 10s remaining → near expiry, picked.
        // Lease 3: OTHER node, 10s remaining → skipped (wrong node).
        // Lease 4: this node, already expired → near expiry, picked.
        let mut leases = HashMap::new();
        let id_a = DescriptorId::new(1, DescriptorKind::Collection, "a".to_string());
        let id_b = DescriptorId::new(1, DescriptorKind::Collection, "b".to_string());
        let id_c = DescriptorId::new(1, DescriptorKind::Collection, "c".to_string());
        let id_d = DescriptorId::new(1, DescriptorKind::Collection, "d".to_string());
        leases.insert(
            (id_a.clone(), 1),
            make_lease(1, now_ns + 50 * 1_000_000_000),
        );
        leases.insert(
            (id_b.clone(), 1),
            make_lease(1, now_ns + 10 * 1_000_000_000),
        );
        leases.insert(
            (id_c.clone(), 2),
            make_lease(2, now_ns + 10 * 1_000_000_000),
        );
        leases.insert((id_d.clone(), 1), make_lease(1, now_ns - 1_000_000_000));

        let picked = collect_near_expiry_pure(&leases, 1, now_ns, &config);
        let names: Vec<String> = picked.iter().map(|(id, _)| id.name.clone()).collect();
        assert_eq!(picked.len(), 2);
        assert!(names.contains(&"b".to_string()));
        assert!(names.contains(&"d".to_string()));
        assert!(!names.contains(&"a".to_string()));
        assert!(!names.contains(&"c".to_string()));
    }

    #[test]
    fn empty_cache_returns_empty_vec() {
        let leases = HashMap::new();
        let config = make_config(100, 20);
        let picked = collect_near_expiry_pure(&leases, 1, 0, &config);
        assert!(picked.is_empty());
    }
}
