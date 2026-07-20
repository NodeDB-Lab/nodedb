// SPDX-License-Identifier: BUSL-1.1

//! Synchronous `propose-and-wait-for-local-apply` helper for
//! replicated catalog DDL.
//!
//! The sole entry point pgwire DDL handlers use to write a
//! [`CatalogEntry`] through the metadata raft group (group 0). It is
//! deliberately sync — pgwire DDL handlers are not async, and
//! `tokio::task::block_in_place`-style wrapping keeps the blocking
//! wait from starving the tokio runtime.
//!
//! Semantics:
//!
//! 1. If no cluster is configured (`shared.metadata_raft` not
//!    installed), returns `Ok(0)` immediately. The caller's legacy
//!    single-node direct-write path stays authoritative.
//! 2. If this node is the metadata-group leader, proposes the
//!    entry, blocks until its local applied watermark reaches the
//!    assigned log index (5s default timeout), and returns the
//!    log index on success.
//! 3. If this node is NOT the leader, returns
//!    `Error::Config { detail: "metadata propose: not leader ..." }`.
//!    Gateway-side redirection will make this transparent.

use std::sync::{Arc, Weak};
use std::time::Duration;

use nodedb_cluster::{METADATA_GROUP_ID, MetadataEntry, WaitOutcome, encode_entry};

#[cfg(test)]
use nodedb_cluster::AppliedIndexWatcher;

use crate::control::catalog_entry::{self, CatalogEntry};
use crate::control::state::SharedState;
use crate::error::Error;

/// Default upper bound on how long a single
/// `propose_catalog_entry` call will block before returning an
/// error.
pub const DEFAULT_PROPOSE_TIMEOUT: Duration = Duration::from_secs(5);

/// Default upper bound on how long a DDL drain will wait for
/// prior-version leases to release before giving up. Must be at
/// least `ClusterTransportTuning::descriptor_lease_duration_secs`
/// so an existing lease gets at least one full lifetime to
/// expire naturally. 35 seconds matches the 300s lease duration
/// plus a 30-second grace minus the typical 5-minute default
/// cut down for test budget — in production
/// `propose_catalog_entry_with_drain_timeout` can pass a longer
/// value if an operator is willing to wait.
pub const DEFAULT_DRAIN_TIMEOUT: Duration = Duration::from_secs(35);

/// Type-erased handle for proposing to the metadata raft group.
///
/// The apply watermark for the metadata group lives on
/// [`SharedState::applied_index_watcher`] (keyed by
/// [`nodedb_cluster::METADATA_GROUP_ID`]); callers of [`Self::propose`]
/// look it up there rather than receiving it through this handle.
pub trait MetadataRaftHandle: Send + Sync {
    /// Propose a raw encoded `MetadataEntry` to the metadata group.
    /// Returns its assigned log index on success.
    fn propose(&self, bytes: Vec<u8>) -> Result<u64, Error>;
}

/// Concrete impl wrapping `nodedb_cluster::RaftLoop`.
///
/// Holds the loop weakly: this handle lives on `SharedState`, which is
/// itself kept alive transitively by the `RaftLoop`, so a strong
/// reference here would close a cycle that pins both forever and blocks
/// clean shutdown. The loop is kept alive by its own spawned tasks;
/// `upgrade` therefore succeeds throughout normal operation and only
/// fails once the loop has been dropped on shutdown.
pub struct RaftLoopProposerHandle {
    raft_loop: Weak<
        nodedb_cluster::RaftLoop<
            crate::control::cluster::SpscCommitApplier,
            crate::control::LocalPlanExecutor,
        >,
    >,
}

impl RaftLoopProposerHandle {
    pub fn new(
        raft_loop: Arc<
            nodedb_cluster::RaftLoop<
                crate::control::cluster::SpscCommitApplier,
                crate::control::LocalPlanExecutor,
            >,
        >,
    ) -> Self {
        Self {
            raft_loop: Arc::downgrade(&raft_loop),
        }
    }
}

impl MetadataRaftHandle for RaftLoopProposerHandle {
    fn propose(&self, bytes: Vec<u8>) -> Result<u64, Error> {
        // The cluster crate's `propose_to_metadata_group_via_leader`
        // is async because it may need to forward to the metadata
        // leader over QUIC. The trait method is sync because every
        // caller (catalog DDL handlers, lease grant/release helpers)
        // is itself sync but runs inside a tokio task. Wrap in
        // `block_in_place` + the current runtime's `block_on` so the
        // forwarding QUIC round-trip drives without starving the
        // raft tick that produces the leader_hint.
        // `upgrade` fails only once the raft loop has been dropped on
        // shutdown; a request racing shutdown then fails cleanly with a
        // typed error instead of panicking.
        let raft_loop = self.raft_loop.upgrade().ok_or_else(|| Error::Config {
            detail: "metadata propose: cluster not running".into(),
        })?;
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current()
                .block_on(raft_loop.propose_to_metadata_group_via_leader(bytes))
        })
        .map_err(|e| Error::Config {
            detail: format!("metadata propose: {e}"),
        })
    }
}

/// Propose a `CatalogEntry` and block until the local applied-index
/// watcher confirms the entry has been applied on this node.
///
/// In single-node / no-cluster mode, returns `Ok(0)` immediately so
/// the caller can fall back to the legacy direct-write path.
pub fn propose_catalog_entry(shared: &SharedState, entry: &CatalogEntry) -> Result<u64, Error> {
    propose_catalog_entry_with_timeout(shared, entry, DEFAULT_PROPOSE_TIMEOUT)
}

/// Same as [`propose_catalog_entry`] but with an explicit timeout.
pub fn propose_catalog_entry_with_timeout(
    shared: &SharedState,
    entry: &CatalogEntry,
    timeout: Duration,
) -> Result<u64, Error> {
    let Some(handle) = shared.metadata_raft.get() else {
        return Ok(0);
    };

    if matches!(entry, CatalogEntry::PutTenantWithAdmin { .. }) {
        let versions = shared.cluster_version_view();
        let required = crate::control::rolling_upgrade::TENANT_ADMIN_ATOMIC_VERSION;
        if !versions.can_activate_feature(required) {
            return Err(Error::VersionCompat {
                detail: format!(
                    "CREATE TENANT requires every cluster node at wire version {required} or \
                     newer (minimum observed {})",
                    versions.min_version
                ),
            });
        }
    }

    // Rolling-upgrade gate: until every node in the cluster reports
    // at least `DISTRIBUTED_CATALOG_VERSION`, fall back to the legacy
    // direct-write path on the originating node. Mixing the
    // replicated and direct paths during a partial upgrade would
    // diverge catalog state across nodes — see
    // `control/rolling_upgrade.rs`.
    {
        let vs = shared.cluster_version_view();
        if !vs.can_activate_feature(crate::control::rolling_upgrade::DISTRIBUTED_CATALOG_VERSION) {
            tracing::warn!(
                min_version = vs.min_version,
                required = crate::control::rolling_upgrade::DISTRIBUTED_CATALOG_VERSION,
                "metadata propose: cluster in compat mode (mixed-version), \
                 falling back to legacy direct-write path"
            );
            return Ok(0);
        }
    }

    // Drain for Put* variants that carry descriptor_version.
    // Leases acquired at plan time are refcounted and held
    // through execute; when the last in-flight query using a
    // descriptor completes, its `QueryLeaseScope` drops and the
    // refcount hits zero, releasing the lease. Drain is what
    // makes this an actual barrier: the proposer waits for all
    // prior-version leases to release before committing the new
    // `Put*`, giving long-running in-flight queries a bounded
    // window (DEFAULT_DRAIN_TIMEOUT) to finish.
    if let Some((descriptor_id, prior_version)) =
        crate::control::lease::descriptor_id_and_prior_version(entry, shared)
        && prior_version > 0
    {
        crate::control::lease::drain_for_ddl(
            shared,
            descriptor_id,
            prior_version,
            DEFAULT_DRAIN_TIMEOUT,
        )?;
    }

    // Freeze the descriptor_version / constraint_version /
    // modification_hlc HERE, at propose time, so the value is computed
    // exactly once from this node's local catalog (`prior + 1`) and
    // then replicated verbatim inside the entry. Every node applies the
    // frozen value without re-deriving it, which makes replay-from-log
    // on restart and re-delivery during learner catch-up idempotent —
    // the divergence that a per-node apply-time stamp produced is gone.
    //
    // Gated on the same rolling-upgrade flag the apply path used to
    // gate on: only stamp once every node can activate descriptor
    // versioning; otherwise leave the entry's sentinel version `0`
    // (downstream resolvers treat `0` as `1`). Older nodes in a
    // mixed-version cluster lack the stamp logic, so a stamped value
    // would not be reproduced symmetrically there.
    let stamped_owned;
    let entry: &CatalogEntry = if shared
        .cluster_version_view()
        .can_activate_feature(crate::control::rolling_upgrade::DESCRIPTOR_VERSIONING_VERSION)
    {
        stamped_owned = catalog_entry::descriptor_stamp::stamp(
            entry.clone(),
            &shared.hlc_clock,
            shared.credentials.catalog(),
        );
        &stamped_owned
    } else {
        entry
    };

    let payload = catalog_entry::encode(entry)?;

    // DDL transaction buffer: if a transactional DDL session is
    // active on this thread (BEGIN ... COMMIT), buffer the payload
    // instead of proposing immediately. The buffered entries will
    // be proposed as a single MetadataEntry::Batch at COMMIT time.
    if crate::control::server::shared::session::ddl_buffer::try_buffer(payload.clone()) {
        return Ok(0);
    }

    // Attach J.4 audit context when the pgwire statement boundary
    // installed one. Internal callers (descriptor lease grant/release,
    // drain proposer) run outside that scope and emit the plain
    // `CatalogDdl` variant — they have no SQL text to log.
    let metadata_entry = match crate::control::server::shared::session::audit_context::current() {
        Some(ctx) => MetadataEntry::CatalogDdlAudited {
            payload,
            auth_user_id: ctx.auth_user_id,
            auth_user_name: ctx.auth_user_name,
            sql_text: ctx.sql_text,
        },
        None => MetadataEntry::CatalogDdl { payload },
    };
    let raw = encode_entry(&metadata_entry).map_err(|e| Error::Config {
        detail: format!("metadata entry encode: {e}"),
    })?;

    let log_index = handle.propose(raw)?;

    let watcher = shared.applied_index_watcher(METADATA_GROUP_ID);
    // `wait_for` blocks the calling thread on a Condvar. When the
    // caller is already inside a tokio task (pgwire handlers always
    // are), parking the worker without telling tokio starves every
    // other task that lands on it — including the raft tick that
    // would otherwise bump the watcher. Wrap the blocking section
    // in `block_in_place` so tokio reassigns a fresh worker.
    let outcome = tokio::task::block_in_place(|| watcher.wait_for(log_index, timeout));
    match outcome {
        WaitOutcome::Reached => Ok(log_index),
        WaitOutcome::TimedOut => Err(Error::Config {
            detail: format!(
                "metadata propose timed out after {:?} waiting for log index {} (current: {})",
                timeout,
                log_index,
                watcher.current()
            ),
        }),
        WaitOutcome::GroupGone => Err(Error::Config {
            detail: "metadata group no longer hosted on this node".into(),
        }),
    }
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
    let Some(handle) = shared.metadata_raft.get() else {
        return Ok(0);
    };

    let entry = MetadataEntry::SurrogateAlloc { hwm };
    let raw = encode_entry(&entry).map_err(|e| Error::Config {
        detail: format!("surrogate_alloc encode: {e}"),
    })?;

    let log_index = handle.propose(raw)?;

    let watcher = shared.applied_index_watcher(METADATA_GROUP_ID);
    let outcome =
        tokio::task::block_in_place(|| watcher.wait_for(log_index, DEFAULT_PROPOSE_TIMEOUT));
    if !outcome.is_reached() {
        return Err(Error::Config {
            detail: format!("surrogate_alloc propose timed out waiting for log index {log_index}"),
        });
    }

    Ok(log_index)
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
    let Some(handle) = shared.metadata_raft.get() else {
        return Ok(0);
    };

    let entry = MetadataEntry::SurrogateReserve {
        node_id,
        request_id,
        batch_size,
    };
    let raw = encode_entry(&entry).map_err(|e| Error::Config {
        detail: format!("surrogate_reserve encode: {e}"),
    })?;

    let log_index = handle.propose(raw)?;

    let watcher = shared.applied_index_watcher(METADATA_GROUP_ID);
    let outcome =
        tokio::task::block_in_place(|| watcher.wait_for(log_index, DEFAULT_PROPOSE_TIMEOUT));
    if !outcome.is_reached() {
        return Err(Error::Config {
            detail: format!(
                "surrogate_reserve propose timed out waiting for log index {log_index}"
            ),
        });
    }

    Ok(log_index)
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
    epoch: u64,
    created_ms: i64,
) -> Result<u64, Error> {
    let Some(handle) = shared.metadata_raft.get() else {
        return Ok(0);
    };

    let entry = MetadataEntry::SyncProducerRegister {
        lite_id: lite_id.to_owned(),
        producer_id,
        tenant_id,
        epoch,
        created_ms,
    };
    let raw = encode_entry(&entry).map_err(|e| Error::Config {
        detail: format!("sync_producer_register encode: {e}"),
    })?;

    let log_index = handle.propose(raw)?;

    let watcher = shared.applied_index_watcher(METADATA_GROUP_ID);
    let outcome =
        tokio::task::block_in_place(|| watcher.wait_for(log_index, DEFAULT_PROPOSE_TIMEOUT));
    if !outcome.is_reached() {
        return Err(Error::Config {
            detail: format!(
                "sync_producer_register propose timed out waiting for log index {log_index}"
            ),
        });
    }

    Ok(log_index)
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
    let Some(handle) = shared.metadata_raft.get() else {
        return Ok(0);
    };

    let entry = MetadataEntry::SyncProducerFence {
        lite_id: lite_id.to_owned(),
        new_epoch,
    };
    let raw = encode_entry(&entry).map_err(|e| Error::Config {
        detail: format!("sync_producer_fence encode: {e}"),
    })?;

    let log_index = handle.propose(raw)?;

    let watcher = shared.applied_index_watcher(METADATA_GROUP_ID);
    let outcome =
        tokio::task::block_in_place(|| watcher.wait_for(log_index, DEFAULT_PROPOSE_TIMEOUT));
    if !outcome.is_reached() {
        return Err(Error::Config {
            detail: format!(
                "sync_producer_fence propose timed out waiting for log index {log_index}"
            ),
        });
    }

    Ok(log_index)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn watcher_helper_returns_reached_on_past_target() {
        let w = AppliedIndexWatcher::new();
        w.bump(10);
        assert!(w.wait_for(5, Duration::from_millis(1)).is_reached());
    }
}
