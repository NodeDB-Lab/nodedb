// SPDX-License-Identifier: BUSL-1.1

//! SharedState impl methods: quota, audit, polling, memory estimates.

use std::sync::Mutex;

use tracing::warn;

use crate::control::security::tenant::QuotaCheck;
use crate::types::TenantId;

use super::SharedState;

impl SharedState {
    /// Whether this node is the leader of the metadata Raft group.
    ///
    /// Reuses the installed `raft_status_fn` snapshot (set by `start_raft`),
    /// looks up the metadata group (`METADATA_GROUP_ID == 0`), and reports
    /// whether its role string is `"Leader"`. Returns `false` in single-node
    /// mode, where `raft_status_fn` is never installed — there is no metadata
    /// Raft group to lead.
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
    /// server config is parsed; future `ALTER SYSTEM` paths may also call this.
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

    /// Convert an LSN back to wall-clock milliseconds using the anchor map.
    ///
    /// Used by the clone resolver to compute `effective_source_ms` for the
    /// engine's `system_as_of_ms` field. Returns `None` when the anchor map
    /// has no entries that bracket the requested LSN (engine then uses
    /// latest-version behaviour — safe for tests where anchors are absent).
    ///
    /// A poisoned lock is recovered via `into_inner()` (consistent with the
    /// other `lsn_ms_map` accessors below) and logged so that bitemporal
    /// reads do not silently degrade to "latest" on the back of a panic in
    /// an unrelated writer.
    pub fn ms_to_lsn_inverse(&self, lsn: nodedb_types::Lsn) -> Option<i64> {
        let map = match self.lsn_ms_map.lock() {
            Ok(g) => g,
            Err(poisoned) => {
                warn!(
                    lsn = %lsn,
                    "lsn_ms_map poisoned in ms_to_lsn_inverse — recovering inner state; \
                     bitemporal reads may use a stale anchor view until the next anchor is recorded"
                );
                poisoned.into_inner()
            }
        };
        nodedb_types::temporal::lsn_to_ms(&map, lsn).ok()
    }

    /// Convert wall-clock milliseconds to the nearest LSN using the anchor map.
    ///
    /// When the map is populated (WAL anchor records have been replayed), this
    /// performs linear interpolation between surrounding anchors.  When the map
    /// is empty (no anchors yet, or testing) the current WAL frontier is
    /// returned as the best available approximation.
    pub fn ms_to_lsn(&self, wall_ms: i64) -> nodedb_types::Lsn {
        let map = match self.lsn_ms_map.lock() {
            Ok(g) => g,
            Err(poisoned) => {
                warn!(
                    wall_ms = wall_ms,
                    "lsn_ms_map poisoned in ms_to_lsn — recovering inner state"
                );
                poisoned.into_inner()
            }
        };
        // Walk anchors to find the pair that brackets wall_ms.
        let anchors = map.anchors();
        if anchors.is_empty() {
            // No anchor data yet.  A very small wall_ms (well before any
            // plausible server-start epoch) predates all recorded LSNs;
            // return LSN 0 so the clone predation check fires correctly.
            // Any other value maps to the current frontier.
            const EPOCH_THRESHOLD_MS: i64 = 1_000_000; // ~16 minutes after Unix epoch
            if wall_ms < EPOCH_THRESHOLD_MS {
                return nodedb_types::Lsn::new(0);
            }
            return self.wal.next_lsn();
        }
        // Binary search by wall_ms to find the bracketing pair.
        match anchors.binary_search_by_key(&wall_ms, |a| a.wall_ms) {
            Ok(idx) => nodedb_types::Lsn::new(anchors[idx].lsn),
            Err(0) => nodedb_types::Lsn::new(anchors[0].lsn),
            Err(idx) if idx >= anchors.len() => {
                nodedb_types::Lsn::new(anchors[anchors.len() - 1].lsn)
            }
            Err(idx) => {
                // Interpolate between anchors[idx-1] and anchors[idx].
                let lo = anchors[idx - 1];
                let hi = anchors[idx];
                let ms_span = (hi.wall_ms - lo.wall_ms).max(1) as u64;
                let lsn_span = hi.lsn.saturating_sub(lo.lsn);
                let ms_delta = (wall_ms - lo.wall_ms).max(0) as u64;
                let lsn = lo.lsn + (lsn_span * ms_delta / ms_span);
                nodedb_types::Lsn::new(lsn)
            }
        }
    }

    /// Record a new WAL anchor into the LSN↔ms map.
    ///
    /// Called when an `LsnMsAnchor` WAL record is replayed at startup or
    /// emitted by the WAL writer.  Non-monotonic anchors are silently ignored
    /// (the WAL guarantees monotonicity; a violation here means a partially
    /// replayed or corrupt segment and must not panic).
    pub fn push_lsn_ms_anchor(&self, lsn: u64, wall_ms: i64) {
        if let Ok(mut map) = self.lsn_ms_map.lock() {
            let anchor = nodedb_types::temporal::LsnMsAnchor::new(lsn, wall_ms);
            // Silently ignore non-monotonic anchors — they arrive during WAL
            // replay of partially-written segments and must not crash the server.
            let _ = map.push(anchor);
        }
    }

    /// Advance the per-tenant observed write-HLC high-water to the current
    /// HLC wall time. Idempotent and monotonic: no-op if a larger value is
    /// already recorded. Callers MUST invoke this only after a successful
    /// dispatch; "success" is defined as `Response.status == Status::Ok`
    /// (and, for `Result<Response>` callers, `Result::Ok` as well). A
    /// poisoned lock is silently ignored — the high-water is best-effort
    /// and the RESTORE staleness gate treats missing entries as zero.
    pub fn advance_tenant_write_hlc(&self, tenant_id: u64) {
        let wall = self.hlc_clock.now().wall_ns;
        if let Ok(mut map) = self.tenant_write_hlc.lock() {
            let entry = map.entry(tenant_id).or_insert(0);
            if wall > *entry {
                *entry = wall;
            }
        }
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
    /// Returns `ClusterVersionView::single_node()` when no
    /// topology handle is installed (single-node mode): callers
    /// that gate on a cluster-wide minimum treat this as "all
    /// nodes run the local build", which is the correct behavior
    /// for a solo node.
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
    /// [`crate::control::metadata_proposer::propose_catalog_entry`]
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
    pub fn tenant_request_start(&self, tenant_id: TenantId) {
        match self.tenants.lock() {
            Ok(mut t) => t.request_start(tenant_id),
            Err(poisoned) => poisoned.into_inner().request_start(tenant_id),
        }
    }

    /// Record request end for tenant quota tracking.
    pub fn tenant_request_end(&self, tenant_id: TenantId) {
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
