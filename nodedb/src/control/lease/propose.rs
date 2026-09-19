// SPDX-License-Identifier: BUSL-1.1

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
/// Admission is linearized under `lease_admission_gate`, but the gate is never
/// held while proposing or waiting for raft. A slow-path caller reserves its
/// exact descriptor version under the gate before releasing it; that reservation
/// is visible to a drain that applies while the grant is in flight.
pub fn acquire_lease(
    shared: &SharedState,
    descriptor_id: DescriptorId,
    version: u64,
    duration: Duration,
) -> Result<DescriptorLease, Error> {
    {
        let _admission_gate = shared
            .lease_admission_gate
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        ensure_not_draining(shared, &descriptor_id, version)?;

        let now = shared.hlc_clock.now();
        let cache_key = (descriptor_id.clone(), shared.node_id);
        let cache = shared
            .metadata_cache
            .read()
            .unwrap_or_else(|p| p.into_inner());
        // A cached metadata lease itself keeps a drain from clearing, so it
        // needs no temporary reservation after the gate is released.
        if let Some(existing) = cache.leases.get(&cache_key)
            && existing.version >= version
            && existing.expires_at > now
        {
            return Ok(existing.clone());
        }

        // Keep this reservation live until the grant path has either installed
        // a metadata lease or returned an error.
        shared.lease_refcount.increment(&descriptor_id, version);
    }

    let result = acquire_lease_after_admission(shared, descriptor_id.clone(), version, duration);
    shared.lease_refcount.decrement(&descriptor_id, version);
    result
}

/// Acquire a descriptor lease after plan admission has already checked drain
/// state and reserved a refcount for `descriptor_id`. This helper deliberately
/// takes neither the admission gate nor another drain snapshot: the existing
/// reservation is the linearized admission record while its raft grant is in
/// flight.
pub(crate) fn acquire_lease_after_admission(
    shared: &SharedState,
    descriptor_id: DescriptorId,
    version: u64,
    duration: Duration,
) -> Result<DescriptorLease, Error> {
    // This gate is intentionally independent from admission: it serializes
    // first-holder and version-upgrade grants while raft applies metadata.
    let _grant_gate = shared
        .lease_grant_gate
        .lock()
        .unwrap_or_else(|poison| poison.into_inner());
    let now = shared.hlc_clock.now();
    let cache_key = (descriptor_id.clone(), shared.node_id);
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

    refresh_lease_after_admission(shared, descriptor_id, version, duration)
}

/// Reject an acquisition covered by an active descriptor drain.
///
/// A drain is a short barrier the DDL path runs before committing the next
/// `descriptor_version`, so an acquisition that lands inside it is not a
/// configuration fault — it is the same transient schema-change race the
/// planner already reports as [`Error::RetryableSchemaChanged`]. Typing it
/// that way is what lets the statement-level retry budget absorb the drain
/// instead of surfacing it to the client. The decision itself is unchanged:
/// this still fails closed, before any lease is granted.
pub(crate) fn ensure_not_draining(
    shared: &SharedState,
    descriptor_id: &DescriptorId,
    version: u64,
) -> Result<(), Error> {
    if shared.lease_drain.is_draining(descriptor_id, version) {
        return Err(drain_in_progress_error(descriptor_id, version));
    }
    Ok(())
}

/// Build the retryable error for an acquisition covered by an active drain.
///
/// The full descriptor identity and the requested version stay in the message
/// so a retry-budget exhaustion is still diagnosable from the client error.
fn drain_in_progress_error(descriptor_id: &DescriptorId, version: u64) -> Error {
    Error::RetryableSchemaChanged {
        descriptor: format!(
            "{descriptor_id:?} at version {version} (descriptor lease drain in progress)"
        ),
    }
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
    {
        let _admission_gate = shared
            .lease_admission_gate
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        ensure_not_draining(shared, &descriptor_id, version)?;
        // The existing metadata lease plus this reservation keeps drain safe
        // until the renewal's raft grant has completed.
        shared.lease_refcount.increment(&descriptor_id, version);
    }

    // Force refresh bypasses the cache fast path, but serializes its raw
    // proposal with all first-holder and version-upgrade grants.
    let result = {
        let _grant_gate = shared
            .lease_grant_gate
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        refresh_lease_after_admission(shared, descriptor_id.clone(), version, duration)
    };
    shared.lease_refcount.decrement(&descriptor_id, version);
    result
}

/// Unconditionally refresh after the caller has already linearized admission.
/// This raw helper must not lock the admission gate or re-check drain state:
/// doing either while its raft operation is in flight would reintroduce the
/// drain/applier deadlock.
fn refresh_lease_after_admission(
    shared: &SharedState,
    descriptor_id: DescriptorId,
    version: u64,
    duration: Duration,
) -> Result<DescriptorLease, Error> {
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
    let entry = if shared
        .cluster_version_view()
        .can_activate_feature(crate::control::rolling_upgrade::LEASE_FENCING_VERSION)
    {
        MetadataEntry::DescriptorLeaseGrantFenced {
            lease: lease.clone(),
            holder_incarnation: shared.node_incarnation,
        }
    } else {
        MetadataEntry::DescriptorLeaseGrant(lease.clone())
    };
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
    fn drain_rejection_is_typed_retryable_and_keeps_descriptor_identity() {
        use nodedb_cluster::DescriptorKind;

        let descriptor = DescriptorId::new(0, 1, DescriptorKind::Collection, "orders".to_string());
        match drain_in_progress_error(&descriptor, 7) {
            Error::RetryableSchemaChanged { descriptor: detail } => {
                assert!(
                    detail.contains("orders"),
                    "descriptor identity lost: {detail}"
                );
                assert!(detail.contains("version 7"), "version lost: {detail}");
            }
            other => panic!("expected RetryableSchemaChanged, got {other:?}"),
        }
    }

    #[test]
    fn compute_expires_at_saturates_on_overflow() {
        let now = Hlc::new(u64::MAX - 100, 0);
        let expires = compute_expires_at(now, Duration::from_secs(u64::MAX));
        assert_eq!(expires.wall_ns, u64::MAX);
    }
}
