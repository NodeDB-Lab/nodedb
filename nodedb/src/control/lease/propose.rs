//! `acquire_lease` — synchronous propose-and-wait helper for
//! descriptor leases. Mirrors `metadata_proposer::propose_catalog_entry`.

use std::time::Duration;

use nodedb_cluster::{DescriptorId, DescriptorLease, MetadataEntry};
use nodedb_types::Hlc;

use crate::control::state::SharedState;
use crate::error::Error;

/// Default lease duration when callers don't pass an explicit value.
/// Matches `ClusterTransportTuning::descriptor_lease_duration_secs`.
pub const DEFAULT_LEASE_DURATION: Duration = Duration::from_secs(300);

/// Compute the HLC at which a lease granted at `now` for the given
/// duration should expire. Pure function so it can be unit-tested
/// without spinning up a cluster.
///
/// HLC arithmetic: we only advance the wall-clock component. The
/// logical counter resets to 0 on the synthetic future timestamp
/// because it represents a "this is the earliest moment a real HLC
/// could observe past expiry" sentinel, not a real causal event.
pub fn compute_expires_at(now: Hlc, duration: Duration) -> Hlc {
    let delta_ns: u64 = duration.as_nanos().try_into().unwrap_or(u64::MAX);
    Hlc::new(now.wall_ns.saturating_add(delta_ns), 0)
}

/// Acquire (or re-confirm) a lease on `descriptor_id` at the given
/// `version`, valid for `duration` from the moment this call returns.
///
/// **Fast path**: if `MetadataCache.leases` already contains a
/// non-expired lease for `(descriptor_id, this_node_id)` whose
/// `version >= version`, return it immediately without any raft
/// round-trip. The planner will hit this on every query after the
/// first one in a 5-minute window.
///
/// **Slow path**: build a `DescriptorLease`, wrap it in
/// `MetadataEntry::DescriptorLeaseGrant`, encode via `zerompk`,
/// propose through the metadata raft group, block on the applied
/// index watcher, then re-read the cache and return the lease.
///
/// **Single-node fallback**: if no metadata raft handle is wired
/// (single-node Origin), write the lease directly into the local
/// `MetadataCache.leases` map and return. This matches
/// `propose_catalog_entry`'s `Ok(0)` sentinel pattern — every node
/// that lacks cluster mode has the same in-memory cache, just
/// updated locally.
pub fn acquire_lease(
    shared: &SharedState,
    descriptor_id: DescriptorId,
    version: u64,
    duration: Duration,
) -> Result<DescriptorLease, Error> {
    let now = shared.hlc_clock.now();
    let cache_key = (descriptor_id.clone(), shared.node_id);

    // Fast path: existing non-expired lease covers this version.
    {
        let cache = shared
            .metadata_cache
            .read()
            .unwrap_or_else(|p| p.into_inner());
        if let Some(existing) = cache.leases.get(&cache_key)
            && existing.version >= version
            && existing.expires_at > now
        {
            return Ok(existing.clone());
        }
    }

    force_refresh_lease(shared, descriptor_id, version, duration)
}

/// Unconditionally propose a fresh lease grant, skipping the
/// "existing lease still valid" fast path. Used by the renewal
/// loop, which has already decided the current lease is near
/// expiry and must be refreshed even though it hasn't technically
/// expired yet.
///
/// The single-node fallback and the cluster propose path are
/// identical to [`acquire_lease`]; the only difference is that
/// this function always stamps a new `expires_at = now + duration`.
pub fn force_refresh_lease(
    shared: &SharedState,
    descriptor_id: DescriptorId,
    version: u64,
    duration: Duration,
) -> Result<DescriptorLease, Error> {
    // Drain gate: reject new acquires at versions being drained
    // by an in-flight DDL. The caller is supposed to retry with
    // the bumped version once the DDL commits and publishes the
    // new descriptor_version.
    //
    // Wall-clock comparison, not `hlc_clock.peek()`, for the
    // same reason the renewal loop uses wall clock: peek is
    // frozen between HLC-advancing events, and the drain's
    // `expires_at` is a real wall timestamp.
    let now_wall_ns = super::wall_now_ns();
    if shared
        .lease_drain
        .is_draining(&descriptor_id, version, now_wall_ns)
    {
        return Err(Error::Config {
            detail: format!(
                "descriptor lease drain in progress: \
                 {descriptor_id:?} at version {version}"
            ),
        });
    }

    let now = shared.hlc_clock.now();
    let cache_key = (descriptor_id.clone(), shared.node_id);
    let expires_at = compute_expires_at(now, duration);
    let lease = DescriptorLease {
        descriptor_id,
        version,
        node_id: shared.node_id,
        expires_at,
    };

    // Single-node / no-cluster fallback: write straight into the
    // local cache. The cache is shared with the rest of the process
    // via `Arc<RwLock<_>>` so subsequent reads see it immediately.
    if shared.metadata_raft.get().is_none() {
        install_into_local_cache(shared, &lease);
        return Ok(lease);
    }

    // Cluster path: encode + propose + block on apply via the
    // shared `propose_and_wait` helper.
    let entry = MetadataEntry::DescriptorLeaseGrant(lease.clone());
    super::propose_and_wait(shared, &entry, "grant")?;

    // Re-read the cache. Under normal conditions the apply path
    // already installed the lease before `wait_for` returned, so
    // this read is just confirmation. If for some reason the lease
    // is missing (race with cluster shutdown, lost commit), return
    // the in-memory copy we proposed — every committed lease at the
    // applied index is by definition durable.
    {
        let cache = shared
            .metadata_cache
            .read()
            .unwrap_or_else(|p| p.into_inner());
        if let Some(installed) = cache.leases.get(&cache_key) {
            return Ok(installed.clone());
        }
    }
    Ok(lease)
}

/// Install a lease directly into the in-memory cache. Used by the
/// single-node fallback only — the cluster path goes through the
/// raft applier, which calls `MetadataCache::apply` on every node.
fn install_into_local_cache(shared: &SharedState, lease: &DescriptorLease) {
    let mut cache = shared
        .metadata_cache
        .write()
        .unwrap_or_else(|p| p.into_inner());
    cache
        .leases
        .insert((lease.descriptor_id.clone(), lease.node_id), lease.clone());
    if lease.expires_at > cache.last_applied_hlc {
        cache.last_applied_hlc = lease.expires_at;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compute_expires_at_advances_wall_clock() {
        let now = Hlc::new(1_000_000_000, 5);
        let expires = compute_expires_at(now, Duration::from_secs(300));
        assert_eq!(expires.wall_ns, 1_000_000_000 + 300 * 1_000_000_000);
        assert_eq!(expires.logical, 0);
        assert!(expires > now);
    }

    #[test]
    fn compute_expires_at_zero_duration_is_strictly_greater_than_zero_hlc() {
        let now = Hlc::new(0, 0);
        let expires = compute_expires_at(now, Duration::from_secs(0));
        assert_eq!(expires, Hlc::new(0, 0));
    }

    #[test]
    fn compute_expires_at_saturates_on_overflow() {
        let now = Hlc::new(u64::MAX - 100, 0);
        let expires = compute_expires_at(now, Duration::from_secs(u64::MAX));
        assert_eq!(expires.wall_ns, u64::MAX);
    }
}
