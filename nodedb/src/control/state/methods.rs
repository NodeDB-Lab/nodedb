// SPDX-License-Identifier: BUSL-1.1

//! SharedState impl methods: quota, audit, polling, memory estimates.

use std::sync::{Arc, Mutex};

use tracing::warn;

use crate::control::security::request_scope::AuthStores;
use crate::control::security::tenant::QuotaCheck;
use crate::types::TenantId;

use super::SharedState;

impl SharedState {
    /// Bundle of the auth-adjacent stores every `RequestAuthScope`
    /// construction needs (`scope_grants`, `quota_manager`), for callers
    /// that build a scope from `&SharedState` directly rather than pulling
    /// the two fields separately.
    pub fn auth_stores(&self) -> AuthStores<'_> {
        AuthStores::new(&self.scope_grants, &self.quota_manager, &self.risk_scorer)
    }

    /// Sequenced Raft proposer for ordinary writes. `start_raft` installs it
    /// before any listener opens.
    pub(crate) fn async_raft_proposer(
        &self,
    ) -> crate::Result<&Arc<crate::control::wal_replication::AsyncRaftProposer>> {
        self.async_raft_proposer_pair
            .get()
            .map(|pair| &pair.sequenced)
            .ok_or_else(|| raft_not_started("the async raft proposer"))
    }

    /// Raw proposer used only while a CRDT admission holds the vShard sequence.
    /// Ordinary writes must use [`Self::async_raft_proposer`].
    pub(in crate::control) fn raw_async_raft_proposer(
        &self,
    ) -> crate::Result<&Arc<crate::control::wal_replication::AsyncRaftProposer>> {
        self.async_raft_proposer_pair
            .get()
            .map(|pair| &pair.raw)
            .ok_or_else(|| raft_not_started("the raw async raft proposer"))
    }

    /// The propose phase alone: it returns once the data-group leader holds
    /// the entry in its log, with the wait for this node's apply of it. It
    /// takes no vShard admission slot. Calvin bookkeeping entries, which no
    /// write admission orders, propose through it.
    pub(in crate::control) fn async_raft_submit(
        &self,
    ) -> crate::Result<&Arc<crate::control::wal_replication::AsyncRaftSubmit>> {
        self.async_raft_proposer_pair
            .get()
            .map(|pair| &pair.submit)
            .ok_or_else(|| raft_not_started("the async raft submit"))
    }

    /// Handle for proposing to the metadata Raft group. `start_raft`
    /// installs it before any listener opens.
    pub fn metadata_raft_handle(
        &self,
    ) -> crate::Result<&Arc<dyn crate::control::metadata_proposer::MetadataRaftHandle>> {
        self.metadata_raft
            .get()
            .ok_or_else(|| raft_not_started("the metadata raft handle"))
    }

    /// Synchronous data-group Raft proposer. `start_raft` installs it before
    /// any listener opens.
    pub(crate) fn sync_raft_proposer(
        &self,
    ) -> crate::Result<&Arc<crate::control::wal_replication::RaftProposer>> {
        self.raft_proposer
            .get()
            .ok_or_else(|| raft_not_started("the raft proposer"))
    }

    /// The gateway `install_gateway` installs before any listener opens.
    pub fn installed_gateway(&self) -> crate::Result<&Arc<crate::control::gateway::Gateway>> {
        self.gateway.get().ok_or_else(|| crate::Error::Internal {
            detail: "the gateway is not installed: install_gateway has not run on this node".into(),
        })
    }

    /// Install the Raft proposal handles atomically during cluster startup.
    pub(in crate::control) fn install_async_raft_proposer_pair(
        &self,
        sequenced: Arc<crate::control::wal_replication::AsyncRaftProposer>,
        raw: Arc<crate::control::wal_replication::AsyncRaftProposer>,
        submit: Arc<crate::control::wal_replication::AsyncRaftSubmit>,
    ) -> crate::Result<()> {
        self.async_raft_proposer_pair
            .set(super::proposer_pair::AsyncRaftProposerPair {
                sequenced,
                raw,
                submit,
            })
            .map_err(|_| crate::Error::Internal {
                detail: "async raft proposer already installed".into(),
            })
    }

    /// Recover a strong `Arc<SharedState>` from `&self` by upgrading the
    /// gateway's `Weak` back-reference. Always succeeds on a booted node;
    /// returns a typed error while racing teardown, or before `install_gateway`
    /// runs.
    pub(crate) fn self_arc(&self) -> crate::Result<Arc<SharedState>> {
        self.installed_gateway()?.shared()
    }

    /// Whether this node is the leader of the metadata Raft group.
    ///
    /// Reuses the installed `raft_status_fn` snapshot (set by `start_raft`),
    /// looks up the metadata group (`METADATA_GROUP_ID == 0`), and reports
    /// whether its role string is `"Leader"`. Returns `false` before
    /// `start_raft` installs `raft_status_fn`.
    ///
    /// Not unit-tested in isolation: it requires a live `raft_status_fn`, so
    /// it is exercised by the cluster-level constraint-reconcile test.
    pub fn is_metadata_leader(&self) -> bool {
        let Some(status_fn) = self.raft_status_fn.get() else {
            return false;
        };
        status_fn()
            .into_iter()
            .any(|g| g.group_id == nodedb_cluster::METADATA_GROUP_ID && g.role == "Leader")
    }

    /// Whether this node runs work that must happen exactly once
    /// cluster-wide: the metadata-group leader does it. A one-node cluster
    /// leads its own metadata group.
    pub fn is_singleton_worker(&self) -> bool {
        self.is_metadata_leader()
    }

    /// Snapshot the configured global quota ceiling.
    ///
    /// Callers (notably `ALTER DATABASE … SET QUOTA`) pass the result to
    /// `SystemCatalog::put_database_quota` so the sum-of-quotas check runs
    /// against the live ceiling. A poisoned lock falls back to
    /// `GlobalQuotaCeiling::default()` (all zeros = no enforcement) so a
    /// poisoned lock never silently rejects valid quotas; the upstream poison
    /// will surface elsewhere with a real diagnostic.
    pub fn quota_ceiling_snapshot(&self) -> crate::control::security::catalog::GlobalQuotaCeiling {
        match self.quota_ceiling.read() {
            Ok(g) => g.clone(),
            Err(p) => p.into_inner().clone(),
        }
    }

    /// Replace the global quota ceiling. Called once at startup after the
    /// server config is parsed; future `ALTER SYSTEM` paths can also call this.
    pub fn set_quota_ceiling(
        &self,
        ceiling: crate::control::security::catalog::GlobalQuotaCeiling,
    ) {
        match self.quota_ceiling.write() {
            Ok(mut g) => *g = ceiling,
            Err(p) => *p.into_inner() = ceiling,
        }
    }

    /// Allocate the next unique request ID for this node.
    ///
    /// All callers that dispatch to the local Data Plane and register a waiter
    /// in `self.tracker` MUST obtain their IDs here. Using per-source counters
    /// that start at the same value causes `RequestTracker::register` to
    /// silently overwrite a prior registration, dropping its response channel
    /// and causing the original waiter to observe a "channel closed" error.
    #[inline]
    pub fn next_request_id(&self) -> crate::types::RequestId {
        crate::types::RequestId::new(
            self.request_id_counter
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed),
        )
    }

    /// Allocate the next unique distributed-shuffle ID for this node.
    ///
    /// Every coordinator-driven shuffle join allocates one id here. The id
    /// scopes the per-part producer inboxes and consumer barriers on each
    /// part-owner node, so two concurrent shuffles never alias each other's
    /// staged frames. Distinct from `next_request_id` so the shuffle and SPSC
    /// request keyspaces are independent.
    #[inline]
    pub fn next_shuffle_id(&self) -> u64 {
        self.shuffle_id_counter
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    }

    /// Commit time, in wall milliseconds, of the WAL state below `lsn`: every
    /// record under `lsn` committed by then.
    ///
    /// `lsn` is an exclusive bound, as `wal.next_lsn()` and [`Self::ms_to_lsn`]
    /// return. `None` when no time anchor covers `lsn - 1` yet.
    pub fn ms_to_lsn_inverse(&self, lsn: nodedb_types::Lsn) -> Option<i64> {
        let last = lsn.as_u64().checked_sub(1)?;
        let ns = self.wal.time_anchors().commit_ns_of(last)?;
        i64::try_from(ns / 1_000_000).ok()
    }

    /// The exclusive LSN bound of the WAL state committed by the end of
    /// millisecond `wall_ms`: every record under it committed by then.
    ///
    /// A time before the oldest retained anchor is an error, never the WAL
    /// frontier. A time past the newest anchor resolves to that anchor, since
    /// the records after it have not committed.
    pub fn ms_to_lsn(&self, wall_ms: i64) -> Result<nodedb_types::Lsn, nodedb_types::LsnTimeError> {
        self.wal
            .time_anchors()
            .lsn_at_or_before_ms(wall_ms)
            .map(|last| nodedb_types::Lsn::new(last.saturating_add(1)))
    }

    /// Shared HTTP client reused by every outbound emitter. Cloning the
    /// Arc is cheap — the client itself owns a connection pool, DNS
    /// resolver, and TLS session cache that every caller benefits from.
    pub fn http_client(&self) -> &std::sync::Arc<reqwest::Client> {
        &self.http_client
    }

    /// Cluster-wide version view derived on demand from the live
    /// `cluster_topology` snapshot. Replaces the old
    /// `cluster_version_state` shadow map — every call walks the
    /// live topology under a short read guard, so version updates
    /// from joins / leaves are observed immediately.
    ///
    /// Returns `ClusterVersionView::single_node()` when no topology
    /// handle is wired: callers that gate on a cluster-wide minimum
    /// treat this as "all nodes run the local build".
    pub fn cluster_version_view(&self) -> crate::control::rolling_upgrade::ClusterVersionView {
        let Some(topology) = &self.cluster_topology else {
            return crate::control::rolling_upgrade::ClusterVersionView::single_node();
        };
        let guard = topology.read().unwrap_or_else(|p| p.into_inner());
        crate::control::rolling_upgrade::compute_from_topology(&guard)
    }

    /// Shared handle to a Raft group's apply watermark watcher.
    ///
    /// Lazily creates the watcher if it does not yet exist so a
    /// proposer can register its waiter before the first apply on a
    /// brand-new group. Used by
    /// [`crate::control::metadata_proposer::propose_catalog_entry_async`]
    /// (with `nodedb_cluster::METADATA_GROUP_ID`) and by the
    /// descriptor-lease drain path. Distributed-write commit
    /// waiting goes through `propose_tracker` directly because it
    /// also needs SPSC dispatch coupling, but the underlying apply
    /// watermark for any data group can be read from the same
    /// registry.
    pub fn applied_index_watcher(
        &self,
        group_id: u64,
    ) -> std::sync::Arc<nodedb_cluster::AppliedIndexWatcher> {
        self.group_watchers.get_or_create(group_id)
    }

    /// Shared handle to the entire per-group apply watermark
    /// registry. Use this when you need to operate on multiple
    /// groups (e.g. test harnesses asserting full cluster
    /// convergence).
    pub fn group_watchers(&self) -> std::sync::Arc<nodedb_cluster::GroupAppliedWatchers> {
        self.group_watchers.clone()
    }

    /// Maximum SPSC ring buffer utilization across all cores (0-100).
    pub fn max_spsc_utilization(&self) -> u8 {
        match self.dispatcher.lock() {
            Ok(d) => d.max_utilization(),
            Err(p) => p.into_inner().max_utilization(),
        }
    }

    /// Get the idle session timeout in seconds (0 = no timeout).
    pub fn idle_timeout_secs(&self) -> u64 {
        self.idle_timeout_secs
    }

    /// Get the absolute session lifetime in seconds (0 = disabled).
    pub fn session_absolute_timeout_secs(&self) -> u64 {
        self.session_absolute_timeout_secs
    }

    /// Override the idle and absolute session-timeout fields. Intended for
    /// tests, which build `SharedState` with both set to `0` (disabled) and
    /// need a short idle window to exercise the listener watchdog. Requires
    /// `&mut`, so it must be called before the state is wrapped in an `Arc`.
    pub fn set_session_timeouts_for_test(&mut self, idle_secs: u64, absolute_secs: u64) {
        self.idle_timeout_secs = idle_secs;
        self.session_absolute_timeout_secs = absolute_secs;
    }

    /// Access to timeseries partition registries.
    pub fn timeseries_registries(
        &self,
    ) -> Option<
        &Mutex<
            std::collections::HashMap<
                String,
                crate::engine::timeseries::partition_registry::PartitionRegistry,
            >,
        >,
    > {
        self.ts_partition_registries.as_ref()
    }

    /// Check tenant quota before dispatching a request. Returns Ok if allowed.
    pub fn check_tenant_quota(&self, tenant_id: TenantId) -> crate::Result<()> {
        let tenants = match self.tenants.lock() {
            Ok(t) => t,
            Err(poisoned) => {
                warn!("tenant isolation mutex poisoned, recovering");
                poisoned.into_inner()
            }
        };
        match tenants.check(tenant_id) {
            QuotaCheck::Allowed => Ok(()),
            QuotaCheck::MemoryExceeded { used, limit } => Err(crate::Error::MemoryExhausted {
                engine: format!("tenant {tenant_id}: {used}/{limit} bytes"),
            }),
            QuotaCheck::ConcurrencyExceeded { active, limit } => Err(crate::Error::BadRequest {
                detail: format!("tenant {tenant_id}: {active}/{limit} concurrent requests"),
            }),
            QuotaCheck::RateLimited { qps, limit } => Err(crate::Error::BadRequest {
                detail: format!("tenant {tenant_id}: rate limited ({qps}/{limit} qps)"),
            }),
            QuotaCheck::StorageExceeded { used, limit } => Err(crate::Error::BadRequest {
                detail: format!("tenant {tenant_id}: storage quota ({used}/{limit} bytes)"),
            }),
        }
    }

    /// Record request start for tenant quota tracking.
    pub(super) fn tenant_request_start(&self, tenant_id: TenantId) {
        match self.tenants.lock() {
            Ok(mut t) => t.request_start(tenant_id),
            Err(poisoned) => poisoned.into_inner().request_start(tenant_id),
        }
    }

    /// Record request end for tenant quota tracking.
    pub(super) fn tenant_request_end(&self, tenant_id: TenantId) {
        match self.tenants.lock() {
            Ok(mut t) => t.request_end(tenant_id),
            Err(poisoned) => poisoned.into_inner().request_end(tenant_id),
        }
    }

    /// Check if a tenant can open a new connection.
    pub fn check_tenant_connection(&self, tenant_id: TenantId) -> crate::Result<()> {
        let tenants = match self.tenants.lock() {
            Ok(t) => t,
            Err(poisoned) => {
                warn!("tenant isolation mutex poisoned, recovering");
                poisoned.into_inner()
            }
        };
        match tenants.check_connection(tenant_id) {
            QuotaCheck::Allowed => Ok(()),
            QuotaCheck::ConcurrencyExceeded { active, limit } => Err(crate::Error::BadRequest {
                detail: format!("tenant {tenant_id}: too many connections ({active}/{limit})"),
            }),
            other => Err(crate::Error::BadRequest {
                detail: format!("tenant {tenant_id}: connection rejected ({other:?})"),
            }),
        }
    }

    /// Record a new connection for a tenant.
    pub fn tenant_connection_start(&self, tenant_id: TenantId) {
        match self.tenants.lock() {
            Ok(mut t) => t.connection_start(tenant_id),
            Err(poisoned) => poisoned.into_inner().connection_start(tenant_id),
        }
    }

    /// Record a connection close for a tenant.
    pub fn tenant_connection_end(&self, tenant_id: TenantId) {
        match self.tenants.lock() {
            Ok(mut t) => t.connection_end(tenant_id),
            Err(poisoned) => poisoned.into_inner().connection_end(tenant_id),
        }
    }

    /// Poll responses from all Data Plane cores and route them to waiting sessions.
    /// Returns the number of responses routed — callers use this for adaptive
    /// backoff (zero ⇒ idle, sleep longer; non-zero ⇒ active, stay hot).
    pub fn poll_and_route_responses(&self) -> usize {
        let responses = match self.dispatcher.lock() {
            Ok(mut d) => d.poll_responses(),
            Err(poisoned) => {
                warn!("dispatcher mutex poisoned, recovering");
                poisoned.into_inner().poll_responses()
            }
        };
        let count = responses.len();
        for resp in responses {
            if !self.tracker.complete(resp) {
                warn!("response for unknown or cancelled request");
            }
        }
        count
    }
}

/// The error for a Raft handle read before `start_raft` installed it.
fn raft_not_started(what: &str) -> crate::Error {
    crate::Error::Internal {
        detail: format!("{what} is not installed: start_raft has not run on this node"),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    /// A poisoned write-HLC map must still report the recorded mark.
    ///
    /// Reading `0` from a poisoned lock silently disables the restore
    /// staleness gate: the gate refuses an envelope whose watermark is older
    /// than this value, and every watermark clears a mark of `0`. A restore
    /// then overwrites newer writes with no error.
    #[test]
    fn a_poisoned_write_hlc_map_still_reports_the_recorded_mark() {
        let map: Arc<Mutex<HashMap<u64, u64>>> = Arc::new(Mutex::new(HashMap::new()));
        map.lock().expect("fresh lock").insert(7, 4_242);

        let poisoner = Arc::clone(&map);
        let _ = std::thread::spawn(move || {
            let _guard = poisoner.lock().expect("acquire before panicking");
            panic!("poison the write-HLC map");
        })
        .join();
        assert!(
            map.lock().is_err(),
            "the map must be poisoned for this test"
        );

        let recovered = map
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .get(&7)
            .copied()
            .unwrap_or(0);
        assert_eq!(
            recovered, 4_242,
            "a poisoned lock must not erase the tenant's high-water mark"
        );
    }
}
