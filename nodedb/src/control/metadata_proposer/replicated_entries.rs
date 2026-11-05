// SPDX-License-Identifier: BUSL-1.1

//! Metadata entries that replicate node-local registries: surrogate and
//! database-id allocation, Lite sync producers, and the system cursors of
//! the Event Plane lanes.
//!
//! Each one proposes the entry and awaits its commit on this node, on any
//! runtime flavor. Every node runs a metadata group, a one-node cluster
//! included.

use nodedb_cluster::{MetadataEntry, encode_entry};

use crate::control::catalog_entry::CatalogEntry;
use crate::control::state::SharedState;
use crate::error::Error;
use crate::event::cdc::consumer_group::OffsetCommit;

use super::wait::await_metadata_apply;

/// Propose `entry` and wait until this node reaches its log index, until the
/// statement deadline. `label` names the entry in the errors.
async fn propose_and_wait(
    shared: &SharedState,
    entry: &MetadataEntry,
    label: &str,
) -> Result<u64, Error> {
    let handle = shared.metadata_raft_handle()?;
    let raw = encode_entry(entry).map_err(|e| Error::Config {
        detail: format!("{label} encode: {e}"),
    })?;

    let log_index = handle.propose_async(raw).await?;
    await_metadata_apply(shared, log_index).await
}

/// Propose a cluster restore point at watermark `hlc` and wait until this
/// node reaches its log index, which is the point's id.
pub async fn propose_restore_point(
    shared: &SharedState,
    hlc: u64,
    created_at_ms: u64,
) -> Result<u64, Error> {
    propose_and_wait(
        shared,
        &MetadataEntry::RestorePoint { hlc, created_at_ms },
        "restore_point",
    )
    .await
}

/// Propose a surrogate high-watermark advance to the metadata Raft group
/// and wait for it to be applied locally.
///
/// The leader-side flush path calls this in addition to the local WAL
/// record, so every follower's `SurrogateRegistry` advances to the same hwm
/// via the Raft commit.
///
/// `hwm` is the highest surrogate that has been issued so far on this
/// node. Followers apply the entry by calling
/// `SurrogateRegistry::restore_hwm(hwm)` (idempotent, monotonic).
pub async fn propose_surrogate_hwm(shared: &SharedState, hwm: u32) -> Result<u64, Error> {
    propose_and_wait(
        shared,
        &MetadataEntry::SurrogateAlloc { hwm },
        "surrogate_alloc",
    )
    .await
}

/// Propose a HiLo surrogate batch reservation to the metadata Raft group
/// and wait for the commit (returns the assigned log index).
///
/// The carved `[start, end)` range is NOT decided here: it is computed
/// at apply time on every node by advancing the global watermark in
/// identical log order (see `MetadataEntry::SurrogateReserve`). The
/// caller therefore cannot learn the range from this commit-wait alone
/// — the wait returns on COMMIT, before the apply handler runs. The
/// owning node's apply handler fires an explicit completion signal
/// (`SurrogateAssigner::complete_reservation`) that the caller awaits
/// separately to learn the range.
///
/// `node_id` + `request_id` identify this node's specific in-flight
/// reservation so the apply handler routes the batch + signal back to it.
pub async fn propose_surrogate_reserve(
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
    .await
}

/// Propose a Lite client registration through the metadata Raft group and
/// wait for it to be applied locally.
///
/// Every follower applies the entry via
/// `SyncProducerRegistry::apply_register` so the `(producer_id, epoch)` pair
/// agrees on all nodes and survives leader failover.
pub async fn propose_sync_producer_register(
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
    .await
}

/// Propose a Lite client epoch fence through the metadata Raft group and
/// wait for it to be applied locally.
///
/// Every follower applies the entry via
/// `SyncProducerRegistry::apply_fence` (max-wins) so the epoch advance
/// survives leader failover.
pub async fn propose_sync_producer_fence(
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
    .await
}

/// Propose ownership of one Loro peer id through the metadata Raft group and
/// wait for it to be applied locally.
///
/// The caller must re-read the owner after this returns: the apply is lowest-producer-id-wins, so a node that lost a race it
/// did not know it was in learns the real owner only once the entry lands.
pub async fn propose_sync_peer_bind(
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
    .await
}

/// Propose a database-id reservation for `node_id` and wait until this node
/// applied it. The applier carves the id and hands it to the registry under
/// `request_id`.
pub async fn propose_database_id_reserve(
    shared: &SharedState,
    node_id: u64,
    request_id: u64,
) -> Result<u64, Error> {
    propose_and_wait(
        shared,
        &MetadataEntry::DatabaseIdReserve {
            node_id,
            request_id,
        },
        "database_id_reserve",
    )
    .await
}

/// Propose a consumer offset commit and wait until this node applied it.
///
/// Every node raises each offset to the highest one committed, so commits
/// converge in any order. A commit stamps no descriptor version, so it takes
/// no DDL preparation lease. A node that dies holding that lease then never
/// stalls another node's cursor.
///
/// The entry carries the running statement's audit context, if any: a user
/// `COMMIT OFFSET` is audited, a system cursor is not. A cursor is reader
/// state, so a mirror takes it and no mirror check runs here.
pub async fn propose_cursor_commit(
    shared: &SharedState,
    commit: OffsetCommit,
) -> Result<u64, Error> {
    propose_cursor_commit_audited(
        shared,
        commit,
        crate::control::server::shared::session::audit_context::current(),
    )
    .await
}

/// [`propose_cursor_commit`] with the audit context of the statement that
/// issued the commit, for a commit a transaction buffered until COMMIT.
pub async fn propose_cursor_commit_audited(
    shared: &SharedState,
    commit: OffsetCommit,
    audit: Option<crate::control::server::shared::session::audit_context::AuditCtx>,
) -> Result<u64, Error> {
    let entry = super::catalog::catalog_ddl_entry_with(
        &CatalogEntry::CommitConsumerOffsets(Box::new(commit)),
        audit,
    )?;
    propose_and_wait(shared, &entry, "cursor_commit").await
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use nodedb_cluster::AppliedIndexWatcher;

    #[tokio::test]
    async fn watcher_helper_returns_reached_on_past_target() {
        let w = AppliedIndexWatcher::new();
        w.bump(10);
        let deadline = Instant::now() + Duration::from_millis(1);
        assert!(w.wait_for_async(5, deadline).await.is_reached());
    }
}
