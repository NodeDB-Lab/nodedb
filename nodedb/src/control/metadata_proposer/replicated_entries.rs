// SPDX-License-Identifier: BUSL-1.1

//! Metadata entries that replicate node-local registries: surrogate
//! allocation and Lite sync producers.
//!
//! Each one proposes the entry and waits for its commit on this node. In
//! single-node mode (no `metadata_raft` installed) each returns `Ok(0)`: the
//! local write already persisted the state.

use nodedb_cluster::{METADATA_GROUP_ID, MetadataEntry, encode_entry};

use crate::control::state::SharedState;
use crate::error::Error;

use super::timeouts::DEFAULT_PROPOSE_TIMEOUT;

/// Propose `entry` and wait until this node reaches its log index. `label`
/// names the entry in the errors. Returns `Ok(0)` when no cluster runs.
fn propose_and_wait(
    shared: &SharedState,
    entry: &MetadataEntry,
    label: &str,
) -> Result<u64, Error> {
    let Some(handle) = shared.metadata_raft.get() else {
        return Ok(0);
    };
    let raw = encode_entry(entry).map_err(|e| Error::Config {
        detail: format!("{label} encode: {e}"),
    })?;

    let log_index = handle.propose(raw)?;

    let watcher = shared.applied_index_watcher(METADATA_GROUP_ID);
    let outcome =
        tokio::task::block_in_place(|| watcher.wait_for(log_index, DEFAULT_PROPOSE_TIMEOUT));
    if !outcome.is_reached() {
        return Err(Error::Config {
            detail: format!("{label} propose timed out waiting for log index {log_index}"),
        });
    }

    Ok(log_index)
}

/// Propose a surrogate high-watermark advance to the metadata Raft group
/// and wait for it to be applied locally.
///
/// In single-node / no-cluster mode (no `metadata_raft` installed),
/// returns `Ok(0)` immediately — the WAL-only path on `SharedState` is
/// still sufficient. In cluster mode this is called by the leader-side
/// flush path instead of (or in addition to) the local WAL record, so
/// every follower's `SurrogateRegistry` advances to the same hwm via the
/// Raft commit.
///
/// `hwm` is the highest surrogate that has been issued so far on this
/// node. Followers apply the entry by calling
/// `SurrogateRegistry::restore_hwm(hwm)` (idempotent, monotonic).
pub fn propose_surrogate_hwm(shared: &SharedState, hwm: u32) -> Result<u64, Error> {
    propose_and_wait(
        shared,
        &MetadataEntry::SurrogateAlloc { hwm },
        "surrogate_alloc",
    )
}

/// Propose a HiLo surrogate batch reservation to the metadata Raft group
/// and wait for the commit (returns the assigned log index).
///
/// In single-node / no-cluster mode (no `metadata_raft` installed),
/// returns `Ok(0)` immediately — single-node uses the local `alloc_one`
/// path and never reaches here. Kept as a safety guard only.
///
/// The carved `[start, end)` range is NOT decided here: it is computed
/// at apply time on every node by advancing the global watermark in
/// identical log order (see `MetadataEntry::SurrogateReserve`). The
/// caller therefore cannot learn the range from this commit-wait alone
/// — `wait_for` returns on COMMIT, before the apply handler runs. The
/// owning node's apply handler fires an explicit completion signal
/// (`SurrogateAssigner::complete_reservation`) that the caller awaits
/// separately to learn the range.
///
/// `node_id` + `request_id` identify this node's specific in-flight
/// reservation so the apply handler routes the batch + signal back to it.
pub fn propose_surrogate_reserve(
    shared: &SharedState,
    node_id: u64,
    request_id: u64,
    batch_size: u32,
) -> Result<u64, Error> {
    propose_and_wait(
        shared,
        &MetadataEntry::SurrogateReserve {
            node_id,
            request_id,
            batch_size,
        },
        "surrogate_reserve",
    )
}

/// Propose a Lite client registration through the metadata Raft group and
/// wait for it to be applied locally.
///
/// In single-node / no-cluster mode (no `metadata_raft` installed),
/// returns `Ok(0)` immediately — the local registry write already persisted
/// the state. In cluster mode every follower applies the entry via
/// `SyncProducerRegistry::apply_register` so the `(producer_id, epoch)` pair
/// agrees on all nodes and survives leader failover.
pub fn propose_sync_producer_register(
    shared: &SharedState,
    lite_id: &str,
    producer_id: u64,
    tenant_id: u64,
    user_id: u64,
    epoch: u64,
    created_ms: i64,
) -> Result<u64, Error> {
    propose_and_wait(
        shared,
        &MetadataEntry::SyncProducerRegister {
            lite_id: lite_id.to_owned(),
            producer_id,
            tenant_id,
            user_id,
            epoch,
            created_ms,
        },
        "sync_producer_register",
    )
}

/// Propose a Lite client epoch fence through the metadata Raft group and
/// wait for it to be applied locally.
///
/// In single-node / no-cluster mode (no `metadata_raft` installed),
/// returns `Ok(0)` immediately — the local registry write already persisted
/// the state. In cluster mode every follower applies the entry via
/// `SyncProducerRegistry::apply_fence` (max-wins) so the epoch advance
/// survives leader failover.
pub fn propose_sync_producer_fence(
    shared: &SharedState,
    lite_id: &str,
    new_epoch: u64,
) -> Result<u64, Error> {
    propose_and_wait(
        shared,
        &MetadataEntry::SyncProducerFence {
            lite_id: lite_id.to_owned(),
            new_epoch,
        },
        "sync_producer_fence",
    )
}

/// Propose ownership of one Loro peer id through the metadata Raft group and
/// wait for it to be applied locally.
///
/// In single-node / no-cluster mode (no `metadata_raft` installed), returns
/// `Ok(0)` immediately — the local registry write already persisted the
/// ownership. In cluster mode the caller must re-read the owner after this
/// returns: the apply is lowest-producer-id-wins, so a node that lost a race it
/// did not know it was in learns the real owner only once the entry lands.
pub fn propose_sync_peer_bind(
    shared: &SharedState,
    binding: &crate::control::security::catalog::sync_producer::PeerBindingKey,
    producer_id: u64,
    bound_ms: i64,
) -> Result<u64, Error> {
    propose_and_wait(
        shared,
        &MetadataEntry::SyncPeerBind {
            database_id: binding.database_id,
            tenant_id: binding.tenant_id,
            collection: binding.collection.clone(),
            peer_id: binding.peer_id,
            producer_id,
            bound_ms,
        },
        "sync_peer_bind",
    )
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use nodedb_cluster::AppliedIndexWatcher;

    #[test]
    fn watcher_helper_returns_reached_on_past_target() {
        let w = AppliedIndexWatcher::new();
        w.bump(10);
        assert!(w.wait_for(5, Duration::from_millis(1)).is_reached());
    }
}
