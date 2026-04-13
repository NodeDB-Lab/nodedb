//! Release all leases held by this node on SIGTERM.
//!
//! On a clean shutdown the process walks
//! `metadata_cache.leases`, collects every entry whose
//! `node_id == self.node_id`, and emits a single batched
//! `DescriptorLeaseRelease` raft entry. This lets any in-flight
//! DDL drain immediately instead of waiting for the 5-minute
//! lease TTL.
//!
//! The helper is **best-effort**: on failure (timeout, leader
//! unavailable, etc.) it logs and returns. Leases then drain via
//! TTL, which is the same behavior as a crashed process. This is
//! a latency optimization on the happy-path shutdown, not a
//! correctness requirement.
//!
//! This helper runs INSIDE the tokio runtime, BEFORE
//! `shutdown_tx` flips. Called from `main.rs`'s Ctrl+C handler.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use nodedb_cluster::DescriptorId;
use tokio::time::timeout;

use crate::control::state::SharedState;

/// Maximum wall-clock time the shutdown helper will block on
/// the release round-trip. Bounds the Ctrl+C path so a wedged
/// cluster can't prevent process exit.
pub const DEFAULT_SHUTDOWN_RELEASE_TIMEOUT: Duration = Duration::from_secs(2);

/// Release every lease this node currently holds in the
/// metadata cache. No-op in single-node mode (no metadata raft
/// handle) and on empty lease sets.
///
/// Returns once the release raft entry has been applied locally
/// (via `release_descriptor_leases`'s internal wait), or once
/// the deadline is reached. Errors are logged; this function
/// never propagates failure because it runs on the shutdown
/// path and the caller has nowhere useful to propagate to.
pub async fn release_all_local_leases(shared: Arc<SharedState>, deadline: Duration) {
    let descriptor_ids = collect_local_descriptor_ids(&shared);
    if descriptor_ids.is_empty() {
        tracing::debug!("shutdown release: no local leases to release");
        return;
    }
    let count = descriptor_ids.len();

    // Bound the release call by `deadline`. `release_descriptor_leases`
    // is sync and uses `block_in_place + wait_for` internally, so
    // we run it via `spawn_blocking` to release the async runtime.
    let release_shared = Arc::clone(&shared);
    let release_task = tokio::task::spawn_blocking(move || {
        release_shared.release_descriptor_leases(descriptor_ids)
    });

    match timeout(deadline, release_task).await {
        Ok(Ok(Ok(()))) => {
            tracing::info!(count, "shutdown release: released {count} local leases");
        }
        Ok(Ok(Err(e))) => {
            tracing::warn!(
                error = %e,
                count,
                "shutdown release: propose failed, leases will drain via TTL"
            );
        }
        Ok(Err(join_err)) => {
            tracing::warn!(
                error = %join_err,
                "shutdown release: spawn_blocking task panicked"
            );
        }
        Err(_) => {
            tracing::warn!(
                count,
                deadline = ?deadline,
                "shutdown release: deadline exceeded, leases will drain via TTL"
            );
        }
    }
}

/// Snapshot the local lease set, keeping only unique descriptor
/// ids where `node_id == shared.node_id`. Returns an owned `Vec`
/// so the caller can cross await points without holding the
/// metadata cache read lock.
fn collect_local_descriptor_ids(shared: &SharedState) -> Vec<DescriptorId> {
    let cache = shared
        .metadata_cache
        .read()
        .unwrap_or_else(|p| p.into_inner());
    let mut seen = HashSet::new();
    let mut out = Vec::new();
    for ((id, node_id), _) in cache.leases.iter() {
        if *node_id != shared.node_id {
            continue;
        }
        if seen.insert(id.clone()) {
            out.push(id.clone());
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_cluster::{DescriptorKind, DescriptorLease};
    use nodedb_types::Hlc;

    fn make_lease(node_id: u64, name: &str) -> DescriptorLease {
        DescriptorLease {
            descriptor_id: DescriptorId::new(1, DescriptorKind::Collection, name.to_string()),
            version: 1,
            node_id,
            expires_at: Hlc::new(u64::MAX, 0),
        }
    }

    #[test]
    fn collect_filters_by_node_id_and_dedupes() {
        // Pure test of the filter logic against a raw
        // MetadataCache — construction of a full SharedState is
        // not feasible in a unit test. We exercise the predicate
        // by walking a HashMap directly.
        let mut map: std::collections::HashMap<(DescriptorId, u64), DescriptorLease> =
            std::collections::HashMap::new();
        let a = DescriptorId::new(1, DescriptorKind::Collection, "a".to_string());
        let b = DescriptorId::new(1, DescriptorKind::Collection, "b".to_string());
        map.insert((a.clone(), 1), make_lease(1, "a"));
        map.insert((b.clone(), 1), make_lease(1, "b"));
        map.insert((a.clone(), 2), make_lease(2, "a")); // other node

        let self_node_id = 1u64;
        let mut seen = HashSet::new();
        let mut out = Vec::new();
        for ((id, node_id), _) in map.iter() {
            if *node_id != self_node_id {
                continue;
            }
            if seen.insert(id.clone()) {
                out.push(id.clone());
            }
        }
        assert_eq!(out.len(), 2);
        let names: std::collections::HashSet<_> = out.iter().map(|i| i.name.clone()).collect();
        assert!(names.contains("a"));
        assert!(names.contains("b"));
    }
}
