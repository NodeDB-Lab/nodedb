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

use std::sync::{Arc, OnceLock};
use std::time::Duration;

use nodedb_cluster::{MetadataEntry, encode_entry};

use crate::control::catalog_entry::{self, CatalogEntry};
use crate::control::cluster::applied_index_watcher::AppliedIndexWatcher;
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
pub trait MetadataRaftHandle: Send + Sync {
    /// Propose a raw encoded `MetadataEntry` to the metadata group.
    /// Returns its assigned log index on success.
    fn propose(&self, bytes: Vec<u8>) -> Result<u64, Error>;

    /// The applied-index watcher backing this handle.
    fn watcher(&self) -> Arc<AppliedIndexWatcher>;
}

/// Concrete impl wrapping `nodedb_cluster::RaftLoop`.
pub struct RaftLoopProposerHandle {
    raft_loop: Arc<
        nodedb_cluster::RaftLoop<
            crate::control::cluster::SpscCommitApplier,
            crate::control::LocalPlanExecutor,
        >,
    >,
    watcher: OnceLock<Arc<AppliedIndexWatcher>>,
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
            raft_loop,
            watcher: OnceLock::new(),
        }
    }

    pub fn with_watcher(self, watcher: Arc<AppliedIndexWatcher>) -> Self {
        let _ = self.watcher.set(watcher);
        self
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
        let raft_loop = self.raft_loop.clone();
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current()
                .block_on(raft_loop.propose_to_metadata_group_via_leader(bytes))
        })
        .map_err(|e| Error::Config {
            detail: format!("metadata propose: {e}"),
        })
    }

    fn watcher(&self) -> Arc<AppliedIndexWatcher> {
        self.watcher
            .get()
            .cloned()
            .unwrap_or_else(|| Arc::new(AppliedIndexWatcher::new()))
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

    let payload = catalog_entry::encode(entry)?;

    // DDL transaction buffer: if a transactional DDL session is
    // active on this thread (BEGIN ... COMMIT), buffer the payload
    // instead of proposing immediately. The buffered entries will
    // be proposed as a single MetadataEntry::Batch at COMMIT time.
    if crate::control::server::pgwire::session::ddl_buffer::try_buffer(payload.clone()) {
        return Ok(0);
    }

    // Attach J.4 audit context when the pgwire statement boundary
    // installed one. Internal callers (descriptor lease grant/release,
    // drain proposer) run outside that scope and emit the plain
    // `CatalogDdl` variant — they have no SQL text to log.
    let metadata_entry = match crate::control::server::pgwire::session::audit_context::current() {
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

    let watcher = shared.applied_index_watcher();
    // `wait_for` blocks the calling thread on a Condvar. When the
    // caller is already inside a tokio task (pgwire handlers always
    // are), parking the worker without telling tokio starves every
    // other task that lands on it — including the raft tick that
    // would otherwise bump the watcher. Wrap the blocking section
    // in `block_in_place` so tokio reassigns a fresh worker.
    let timed_out = tokio::task::block_in_place(|| !watcher.wait_for(log_index, timeout));
    if timed_out {
        return Err(Error::Config {
            detail: format!(
                "metadata propose timed out after {:?} waiting for log index {} (current: {})",
                timeout,
                log_index,
                watcher.current()
            ),
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
    let Some(handle) = shared.metadata_raft.get() else {
        return Ok(0);
    };

    let entry = MetadataEntry::SurrogateAlloc { hwm };
    let raw = encode_entry(&entry).map_err(|e| Error::Config {
        detail: format!("surrogate_alloc encode: {e}"),
    })?;

    let log_index = handle.propose(raw)?;

    let watcher = shared.applied_index_watcher();
    let timed_out =
        tokio::task::block_in_place(|| !watcher.wait_for(log_index, DEFAULT_PROPOSE_TIMEOUT));
    if timed_out {
        return Err(Error::Config {
            detail: format!("surrogate_alloc propose timed out waiting for log index {log_index}"),
        });
    }

    Ok(log_index)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn watcher_helper_returns_true_on_past_target() {
        let w = AppliedIndexWatcher::new();
        w.bump(10);
        assert!(w.wait_for(5, Duration::from_millis(1)));
    }
}
