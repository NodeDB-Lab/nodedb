//! SharedState impl methods: quota, audit, polling, memory estimates.

use std::sync::{Arc, Mutex};

use tracing::{error, warn};

use crate::control::security::tenant::QuotaCheck;
use crate::types::TenantId;

use super::SharedState;

impl SharedState {
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

    /// Advance the per-tenant observed write-HLC high-water to the current
    /// HLC wall time. Idempotent and monotonic: no-op if a larger value is
    /// already recorded. Callers MUST invoke this only after a successful
    /// dispatch; "success" is defined as `Response.status == Status::Ok`
    /// (and, for `Result<Response>` callers, `Result::Ok` as well). A
    /// poisoned lock is silently ignored — the high-water is best-effort
    /// and the RESTORE staleness gate treats missing entries as zero.
    pub fn advance_tenant_write_hlc(&self, tenant_id: u32) {
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

    /// Shared handle to the metadata applied-index watcher.
    ///
    /// Used by [`crate::control::metadata_proposer::propose_catalog_entry`]
    /// to block until a freshly-proposed metadata entry has been
    /// applied locally.
    pub fn applied_index_watcher(
        &self,
    ) -> std::sync::Arc<crate::control::cluster::applied_index_watcher::AppliedIndexWatcher> {
        self.metadata_applied_index_watcher.clone()
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

    /// Reset per-second rate counters. Called by a 1-second timer.
    pub fn reset_tenant_rate_counters(&self) {
        match self.tenants.lock() {
            Ok(mut t) => t.reset_rate_counters(),
            Err(poisoned) => poisoned.into_inner().reset_rate_counters(),
        }
    }

    /// Record an audit event (best-effort).
    ///
    /// Writes to both the in-memory cache and the durable audit WAL (if available).
    /// On audit WAL failure, logs an error but does not propagate it. Use
    /// [`audit_record_strict`] when the caller must abort on audit failure
    /// (e.g. data-modifying DDL where the accounting standard requires atomic
    /// audit + data durability).
    pub fn audit_record(
        &self,
        event: crate::control::security::audit::AuditEvent,
        tenant_id: Option<crate::types::TenantId>,
        source: &str,
        detail: &str,
    ) {
        if let Err(e) = self.audit_record_strict(event, tenant_id, source, detail) {
            error!(error = %e, "audit WAL write failed — entry recorded in-memory only");
        }
    }

    /// Record an audit event with strict durability.
    ///
    /// Returns an error if the durable audit WAL write fails. Callers that
    /// guard data mutations MUST use this and abort on failure — the accounting
    /// standard requires: "If the audit write fails, the data write also fails."
    pub fn audit_record_strict(
        &self,
        event: crate::control::security::audit::AuditEvent,
        tenant_id: Option<crate::types::TenantId>,
        source: &str,
        detail: &str,
    ) -> crate::Result<()> {
        let entry = match self.audit.lock() {
            Ok(mut log) => {
                log.record(event, tenant_id, source, detail);
                log.all().back().cloned()
            }
            Err(poisoned) => {
                warn!("audit log mutex poisoned, recovering");
                let mut log = poisoned.into_inner();
                log.record(event, tenant_id, source, detail);
                log.all().back().cloned()
            }
        };

        // Write to durable audit WAL — failure is a hard error.
        if let Some(ref entry) = entry {
            let bytes =
                zerompk::to_msgpack_vec(entry).map_err(|e| crate::Error::Serialization {
                    format: "msgpack".into(),
                    detail: format!("audit entry serialization failed: {e}"),
                })?;
            let data_lsn = self.wal.next_lsn().as_u64();
            self.wal.append_audit_durable(&bytes, data_lsn)?;
        }
        Ok(())
    }

    /// Update per-tenant memory estimates.
    pub fn update_tenant_memory_estimates(&self) {
        let total_allocated = tikv_jemalloc_ctl::stats::allocated::read().unwrap_or(0) as u64;

        let mut tenants = match self.tenants.lock() {
            Ok(t) => t,
            Err(p) => p.into_inner(),
        };

        let tenant_requests: Vec<(crate::types::TenantId, u64)> = {
            let users = self.credentials.list_user_details();
            let mut seen = std::collections::HashSet::new();
            let mut result = Vec::new();
            for user in &users {
                if seen.insert(user.tenant_id) {
                    let total = tenants
                        .usage(user.tenant_id)
                        .map_or(0, |u| u.total_requests);
                    result.push((user.tenant_id, total));
                }
            }
            result
        };

        let total_reqs: u64 = tenant_requests.iter().map(|(_, r)| *r).sum();
        if total_reqs == 0 {
            return;
        }

        for (tid, reqs) in &tenant_requests {
            let proportion = *reqs as f64 / total_reqs as f64;
            let estimated_bytes = (total_allocated as f64 * proportion) as u64;
            tenants.update_memory(*tid, estimated_bytes);
        }
    }

    /// Flush in-memory audit entries to the persistent catalog.
    pub fn flush_audit_log(&self) {
        let entries = match self.audit.lock() {
            Ok(mut log) => log.drain_for_persistence(),
            Err(poisoned) => {
                warn!("audit log mutex poisoned during flush, recovering");
                poisoned.into_inner().drain_for_persistence()
            }
        };

        if entries.is_empty() {
            return;
        }

        if let Some(catalog) = self.credentials.catalog() {
            let stored: Vec<crate::control::security::catalog::StoredAuditEntry> = entries
                .iter()
                .map(|e| crate::control::security::catalog::StoredAuditEntry {
                    seq: e.seq,
                    timestamp_us: e.timestamp_us,
                    event: format!("{:?}", e.event),
                    tenant_id: e.tenant_id.map(|t| t.as_u32()),
                    source: e.source.clone(),
                    detail: e.detail.clone(),
                    prev_hash: e.prev_hash.clone(),
                })
                .collect();

            if let Err(e) = catalog.append_audit_entries(&stored) {
                warn!(error = %e, count = stored.len(), "failed to persist audit entries");
                if let Ok(mut log) = self.audit.lock() {
                    for entry in entries {
                        log.record(entry.event, entry.tenant_id, &entry.source, &entry.detail);
                    }
                }
            } else {
                tracing::debug!(count = stored.len(), "flushed audit entries to catalog");

                if self.audit_retention_days > 0 {
                    let retention_us = self.audit_retention_days as u64 * 86400 * 1_000_000;
                    let now_us = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_micros() as u64;
                    let cutoff = now_us.saturating_sub(retention_us);
                    match catalog.prune_audit_before(cutoff) {
                        Ok(0) => {}
                        Ok(n) => tracing::info!(
                            pruned = n,
                            days = self.audit_retention_days,
                            "pruned old audit entries"
                        ),
                        Err(e) => warn!(error = %e, "failed to prune old audit entries"),
                    }
                }
            }
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

    /// Acquire (or re-confirm) a descriptor lease at the given
    /// version, valid for `duration` from now. This is the public
    /// API the planner and tests use to obtain a lease before reading
    /// a descriptor.
    ///
    /// Fast path returns immediately if a non-expired lease at the
    /// requested version (or higher) is already held by this node.
    /// Slow path proposes a `MetadataEntry::DescriptorLeaseGrant`
    /// through the metadata raft group and blocks on the local
    /// applied watermark. Single-node fallback writes directly to
    /// the in-memory cache. See
    /// [`crate::control::lease::propose::acquire_lease`] for the
    /// full semantics.
    pub fn acquire_descriptor_lease(
        &self,
        descriptor_id: nodedb_cluster::DescriptorId,
        version: u64,
        duration: std::time::Duration,
    ) -> crate::Result<nodedb_cluster::DescriptorLease> {
        crate::control::lease::acquire_lease(self, descriptor_id, version, duration)
    }

    /// Release every lease this node currently holds against any
    /// of `descriptor_ids`. Used on `SIGTERM` drain and by tests.
    /// Empty input is a no-op.
    pub fn release_descriptor_leases(
        &self,
        descriptor_ids: Vec<nodedb_cluster::DescriptorId>,
    ) -> crate::Result<()> {
        crate::control::lease::release_leases(self, descriptor_ids)
    }

    /// Acquire the descriptor leases needed to execute a plan
    /// that reads the descriptors in `version_set`. Returns a
    /// [`crate::control::lease::QueryLeaseScope`] whose drop
    /// decrements each refcount and triggers a background
    /// release for any descriptor whose count hits zero.
    ///
    /// This is called by the pgwire handler AFTER planning
    /// (fresh or cache hit) and held through the query's
    /// execute phase. Multiple concurrent queries that share
    /// a descriptor all pay a single raft acquire (on the
    /// first-holder call) and a single raft release (when the
    /// last holder drops its scope).
    ///
    /// Errors while acquiring (drain in progress, NotLeader,
    /// etc.) are logged at warn and the affected descriptor is
    /// NOT added to the returned scope — the query proceeds
    /// without a lease on that one descriptor. This matches
    /// the best-effort semantics of the original in-adapter
    /// acquire: a transient lease failure must not break user
    /// queries.
    pub fn acquire_plan_lease_scope(
        self: &Arc<Self>,
        version_set: &crate::control::planner::descriptor_set::DescriptorVersionSet,
    ) -> crate::control::lease::QueryLeaseScope {
        use crate::control::lease::{DEFAULT_LEASE_DURATION, QueryLeaseScope};
        if version_set.is_empty() {
            return QueryLeaseScope::empty();
        }
        let mut held_ids = Vec::with_capacity(version_set.len());
        for (id, version) in version_set.iter() {
            let count_after = self.lease_refcount.increment(id);
            // Add the decrement-on-drop obligation to the
            // scope BEFORE the raft acquire so any early
            // return / panic still releases the refcount.
            held_ids.push(id.clone());
            if count_after == 1 {
                // First holder on this node pays the raft
                // round-trip.
                let acquire_result: crate::Result<nodedb_cluster::DescriptorLease> =
                    self.acquire_descriptor_lease(id.clone(), version, DEFAULT_LEASE_DURATION);
                if let Err(e) = acquire_result {
                    let msg = e.to_string();
                    // Drain-in-progress is caught by the
                    // planner's RetryableSchemaChanged path;
                    // by the time we get here the plan was
                    // already committed (cache hit or fresh
                    // plan), so a late drain observation is
                    // best treated as a warning. The handler
                    // layer's retry loop would have caught it
                    // earlier in the fresh-plan path.
                    if !msg.contains("drain in progress") {
                        tracing::warn!(
                            error = %msg,
                            descriptor = ?id,
                            version,
                            "acquire_plan_lease_scope: first-holder acquire failed"
                        );
                    }
                }
            }
        }
        QueryLeaseScope::new(held_ids, self)
    }

    /// Look up a single lease by `(descriptor_id, this_node_id)`,
    /// filtering expired records. Used by tests and by the planner
    /// to short-circuit when a fresh lease already exists. Returns
    /// `None` if absent or past expiry.
    pub fn lookup_lease_for_self(
        &self,
        descriptor_id: &nodedb_cluster::DescriptorId,
    ) -> Option<nodedb_cluster::DescriptorLease> {
        let now = self.hlc_clock.peek();
        let cache = self
            .metadata_cache
            .read()
            .unwrap_or_else(|p| p.into_inner());
        cache
            .leases
            .get(&(descriptor_id.clone(), self.node_id))
            .filter(|l| l.expires_at > now)
            .cloned()
    }
}
