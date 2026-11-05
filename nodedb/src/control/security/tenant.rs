// SPDX-License-Identifier: BUSL-1.1

//! Tenant isolation enforcement.
//!
//! Tenant data MUST be logically isolated in identifiers,
//! WAL streams, quotas, and cache accounting. Query planning and vector
//! prefilter bitmaps MUST be tenant-scoped by construction.

use std::collections::HashMap;

use crate::types::TenantId;

/// Per-tenant resource quotas.
///
/// Live projection of the tenant's persisted `QuotaRecord`, installed by the
/// quota post-apply hook. Zero on any ceiling means "no limit", matching the
/// record it comes from.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct TenantQuota {
    /// Maximum memory budget in bytes (across all engines).
    pub max_memory_bytes: u64,
    /// Maximum storage budget in bytes (L1 + L2).
    pub max_storage_bytes: u64,
    /// Maximum concurrent requests.
    pub max_concurrent_requests: u32,
    /// Maximum queries per second.
    pub max_qps: u32,
    /// Maximum vector dimensions allowed.
    pub max_vector_dim: u32,
    /// Maximum graph traversal depth.
    pub max_graph_depth: u32,
    /// Maximum active connections per tenant (0 = unlimited).
    #[serde(default)]
    pub max_connections: u32,
    /// Per-tenant override for the dropped-collection retention window,
    /// in days. `None` means "inherit the system-wide default set in
    /// `server.retention.deactivated_collection_retention_days`".
    /// Set via
    /// `ALTER TENANT <id> SET QUOTA deactivated_collection_retention_days = <n>`.
    #[serde(default)]
    pub deactivated_collection_retention_days: Option<u32>,
}

impl Default for TenantQuota {
    fn default() -> Self {
        Self {
            max_memory_bytes: 1024 * 1024 * 1024,       // 1 GiB
            max_storage_bytes: 10 * 1024 * 1024 * 1024, // 10 GiB
            max_concurrent_requests: 100,
            max_qps: 1000,
            max_vector_dim: 4096,
            max_graph_depth: 10,
            max_connections: 0, // Unlimited by default.
            deactivated_collection_retention_days: None,
        }
    }
}

impl From<&nodedb_types::QuotaRecord> for TenantQuota {
    /// Project a persisted quota record onto the live enforcement view.
    ///
    /// Every ceiling carries over verbatim, so a zero field keeps the record's
    /// "no limit" meaning here as well.
    fn from(record: &nodedb_types::QuotaRecord) -> Self {
        Self {
            max_memory_bytes: record.max_memory_bytes,
            max_storage_bytes: record.max_storage_bytes,
            max_concurrent_requests: record.max_concurrent_requests,
            max_qps: record.max_qps,
            max_vector_dim: record.max_vector_dim,
            max_graph_depth: record.max_graph_depth,
            max_connections: record.max_connections,
            deactivated_collection_retention_days: record.deactivated_collection_retention_days,
        }
    }
}

/// Runtime usage counters for a tenant.
#[derive(Debug, Clone, Default)]
pub struct TenantUsage {
    /// Current memory consumption in bytes.
    pub memory_bytes: u64,
    /// Current storage consumption in bytes.
    pub storage_bytes: u64,
    /// Current in-flight requests.
    pub active_requests: u32,
    /// Requests in the current second window.
    pub requests_this_second: u32,
    /// Total requests served.
    pub total_requests: u64,
    /// Total requests rejected due to quota.
    pub rejected_requests: u64,
    /// Current active connections.
    pub active_connections: u32,
}

/// Quota check result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QuotaCheck {
    /// Request is within quota.
    Allowed,
    /// Request exceeds memory quota.
    MemoryExceeded { used: u64, limit: u64 },
    /// Too many concurrent requests.
    ConcurrencyExceeded { active: u32, limit: u32 },
    /// Rate limit exceeded.
    RateLimited { qps: u32, limit: u32 },
}

impl QuotaCheck {
    pub fn is_allowed(&self) -> bool {
        matches!(self, QuotaCheck::Allowed)
    }
}

/// Tenant isolation manager.
///
/// Enforces per-tenant quotas and tracks resource usage.
/// Lives on the Control Plane (Send + Sync).
#[derive(Debug)]
pub struct TenantIsolation {
    quotas: HashMap<TenantId, TenantQuota>,
    usage: HashMap<TenantId, TenantUsage>,
    /// Default quota applied to tenants without explicit config.
    default_quota: TenantQuota,
    /// Monotonic high-water-mark of every tenant id observed, used to
    /// allocate fresh ids without a catalog (the embedded / unit path).
    /// The catalog's durable `tenant_id_hwm` is authoritative when one
    /// is wired up; this mirror only serves the no-catalog fallback.
    id_hwm: u64,
}

impl TenantIsolation {
    pub fn new(default_quota: TenantQuota) -> Self {
        Self {
            quotas: HashMap::new(),
            usage: HashMap::new(),
            default_quota,
            id_hwm: 0,
        }
    }

    /// Set quota for a specific tenant.
    pub fn set_quota(&mut self, tenant_id: TenantId, quota: TenantQuota) {
        self.id_hwm = self.id_hwm.max(tenant_id.as_u64());
        self.quotas.insert(tenant_id, quota);
    }

    /// Allocate the next tenant id from the in-memory high-water-mark,
    /// skipping the reserved system (`0`) and bootstrap (`1`) slots.
    /// Used only when no catalog is wired up; with a catalog,
    /// `SystemCatalog::allocate_tenant_id` is the durable authority.
    pub fn allocate_tenant_id(&mut self) -> u64 {
        let next = self
            .id_hwm
            .max(crate::control::security::catalog::tenant_id_hwm::FIRST_USER_TENANT_ID - 1)
            + 1;
        self.id_hwm = next;
        next
    }

    /// Whether a tenant already has an explicit quota record.
    pub fn has_quota(&self, tenant_id: TenantId) -> bool {
        self.quotas.contains_key(&tenant_id)
    }

    /// Remove the quota record (and any usage counters) for a tenant.
    pub fn remove_quota(&mut self, tenant_id: TenantId) {
        self.quotas.remove(&tenant_id);
        self.usage.remove(&tenant_id);
    }

    /// Drop the explicit quota record so the tenant falls back to the default.
    /// Usage counters stay, because the tenant itself still exists.
    pub fn clear_quota(&mut self, tenant_id: TenantId) {
        self.quotas.remove(&tenant_id);
    }

    /// Get quota for a tenant (falls back to default).
    pub fn quota(&self, tenant_id: TenantId) -> &TenantQuota {
        self.quotas.get(&tenant_id).unwrap_or(&self.default_quota)
    }

    /// Check if a request from the tenant is within quota.
    pub fn check(&self, tenant_id: TenantId) -> QuotaCheck {
        let quota = self.quota(tenant_id);
        let usage = self.usage.get(&tenant_id);

        let usage = match usage {
            Some(u) => u,
            None => return QuotaCheck::Allowed, // No usage yet.
        };

        // A zero ceiling means "no limit" on every dimension, the same
        // convention `check_connection` and the persisted record use.
        if quota.max_memory_bytes > 0 && usage.memory_bytes > quota.max_memory_bytes {
            return QuotaCheck::MemoryExceeded {
                used: usage.memory_bytes,
                limit: quota.max_memory_bytes,
            };
        }
        if quota.max_concurrent_requests > 0
            && usage.active_requests >= quota.max_concurrent_requests
        {
            return QuotaCheck::ConcurrencyExceeded {
                active: usage.active_requests,
                limit: quota.max_concurrent_requests,
            };
        }
        if quota.max_qps > 0 && usage.requests_this_second >= quota.max_qps {
            return QuotaCheck::RateLimited {
                qps: usage.requests_this_second,
                limit: quota.max_qps,
            };
        }

        QuotaCheck::Allowed
    }

    /// Record a new request from a tenant.
    pub fn request_start(&mut self, tenant_id: TenantId) {
        let usage = self.usage.entry(tenant_id).or_default();
        usage.active_requests += 1;
        usage.requests_this_second += 1;
        usage.total_requests += 1;
    }

    /// Record request completion.
    pub fn request_end(&mut self, tenant_id: TenantId) {
        if let Some(usage) = self.usage.get_mut(&tenant_id) {
            usage.active_requests = usage.active_requests.saturating_sub(1);
        }
    }

    /// Record a rejected request.
    pub fn request_rejected(&mut self, tenant_id: TenantId) {
        let usage = self.usage.entry(tenant_id).or_default();
        usage.rejected_requests += 1;
    }

    /// Update memory usage for a tenant.
    pub fn update_memory(&mut self, tenant_id: TenantId, bytes: u64) {
        let usage = self.usage.entry(tenant_id).or_default();
        usage.memory_bytes = bytes;
    }

    /// Update storage usage for a tenant.
    pub fn update_storage(&mut self, tenant_id: TenantId, bytes: u64) {
        let usage = self.usage.entry(tenant_id).or_default();
        usage.storage_bytes = bytes;
    }

    /// Reset per-second rate counters (called once per second by a timer).
    pub fn reset_rate_counters(&mut self) {
        for usage in self.usage.values_mut() {
            usage.requests_this_second = 0;
        }
    }

    /// Check if a new connection is allowed for this tenant.
    pub fn check_connection(&self, tenant_id: TenantId) -> QuotaCheck {
        let quota = self.quota(tenant_id);
        if quota.max_connections == 0 {
            return QuotaCheck::Allowed; // Unlimited.
        }
        let usage = match self.usage.get(&tenant_id) {
            Some(u) => u,
            None => return QuotaCheck::Allowed,
        };
        if usage.active_connections >= quota.max_connections {
            QuotaCheck::ConcurrencyExceeded {
                active: usage.active_connections,
                limit: quota.max_connections,
            }
        } else {
            QuotaCheck::Allowed
        }
    }

    /// Record a new connection.
    pub fn connection_start(&mut self, tenant_id: TenantId) {
        let usage = self.usage.entry(tenant_id).or_default();
        usage.active_connections += 1;
    }

    /// Record a connection close.
    pub fn connection_end(&mut self, tenant_id: TenantId) {
        if let Some(usage) = self.usage.get_mut(&tenant_id) {
            usage.active_connections = usage.active_connections.saturating_sub(1);
        }
    }

    /// Get usage stats for a tenant.
    pub fn usage(&self, tenant_id: TenantId) -> Option<&TenantUsage> {
        self.usage.get(&tenant_id)
    }

    pub fn tenant_count(&self) -> usize {
        self.usage.len()
    }

    /// Snapshot current usage + quota into a [`TenantQuotaMetrics`] for the HTTP
    /// metrics endpoint. Couples to `control::metrics::tenant` — both must stay
    /// in sync with the quota/usage field names.
    pub fn snapshot_metrics(
        &self,
        tenant_id: TenantId,
    ) -> crate::control::metrics::tenant::TenantQuotaMetrics {
        let quota = self.quota(tenant_id);
        let usage = self.usage.get(&tenant_id);
        let (mem_used, stor_used, qps, conns) = match usage {
            Some(u) => (
                u.memory_bytes,
                u.storage_bytes,
                u.requests_this_second as u64,
                u.active_connections as u64,
            ),
            None => (0, 0, 0, 0),
        };
        crate::control::metrics::tenant::TenantQuotaMetrics {
            tenant_id: tenant_id.as_u64(),
            memory_bytes_used: mem_used,
            memory_bytes_limit: quota.max_memory_bytes,
            storage_bytes_used: stor_used,
            storage_bytes_limit: quota.max_storage_bytes,
            qps_current: qps,
            qps_limit: quota.max_qps as u64,
            connections_active: conns,
            connections_limit: quota.max_connections as u64,
        }
    }

    /// Iterate over all tenants that have recorded usage statistics.
    ///
    /// Returns `(tenant_id, usage, quota)` tuples. Tenants without a custom
    /// quota receive the default quota. Iteration order is unspecified.
    pub fn iter_usage(&self) -> impl Iterator<Item = (TenantId, &TenantUsage, &TenantQuota)> {
        self.usage.iter().map(move |(&tid, usage)| {
            let quota = self.quotas.get(&tid).unwrap_or(&self.default_quota);
            (tid, usage, quota)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn t(id: u64) -> TenantId {
        TenantId::new(id)
    }

    #[test]
    fn default_quota_applied() {
        let isolation = TenantIsolation::new(TenantQuota::default());
        let quota = isolation.quota(t(1));
        assert_eq!(quota.max_concurrent_requests, 100);
    }

    #[test]
    fn custom_quota_overrides_default() {
        let mut isolation = TenantIsolation::new(TenantQuota::default());
        isolation.set_quota(
            t(1),
            TenantQuota {
                max_concurrent_requests: 50,
                ..Default::default()
            },
        );
        assert_eq!(isolation.quota(t(1)).max_concurrent_requests, 50);
        assert_eq!(isolation.quota(t(2)).max_concurrent_requests, 100); // default
    }

    #[test]
    fn quota_check_allowed() {
        let isolation = TenantIsolation::new(TenantQuota::default());
        assert!(isolation.check(t(1)).is_allowed());
    }

    #[test]
    fn quota_check_concurrency_exceeded() {
        let mut isolation = TenantIsolation::new(TenantQuota {
            max_concurrent_requests: 2,
            ..Default::default()
        });

        isolation.request_start(t(1));
        isolation.request_start(t(1));
        assert_eq!(
            isolation.check(t(1)),
            QuotaCheck::ConcurrencyExceeded {
                active: 2,
                limit: 2,
            }
        );

        isolation.request_end(t(1));
        assert!(isolation.check(t(1)).is_allowed());
    }

    #[test]
    fn zero_ceilings_mean_no_limit() {
        let mut isolation = TenantIsolation::new(TenantQuota::default());
        isolation.set_quota(t(1), TenantQuota::from(&nodedb_types::QuotaRecord::DEFAULT));
        isolation.update_memory(t(1), u64::MAX);
        isolation.update_storage(t(1), u64::MAX);
        for _ in 0..1000 {
            isolation.request_start(t(1));
        }
        assert!(
            isolation.check(t(1)).is_allowed(),
            "an all-zero record must impose no ceiling at all"
        );
    }

    #[test]
    fn quota_record_projects_every_dimension() {
        let record = nodedb_types::QuotaRecord {
            max_memory_bytes: 1 << 30,
            max_storage_bytes: 1 << 34,
            max_qps: 250,
            max_connections: 7,
            max_concurrent_requests: 64,
            max_vector_dim: 1536,
            max_graph_depth: 8,
            deactivated_collection_retention_days: Some(14),
            ..nodedb_types::QuotaRecord::DEFAULT
        };
        let quota = TenantQuota::from(&record);
        assert_eq!(quota.max_memory_bytes, 1 << 30);
        assert_eq!(quota.max_storage_bytes, 1 << 34);
        assert_eq!(quota.max_qps, 250);
        assert_eq!(quota.max_connections, 7);
        assert_eq!(quota.max_concurrent_requests, 64);
        assert_eq!(quota.max_vector_dim, 1536);
        assert_eq!(quota.max_graph_depth, 8);
        assert_eq!(quota.deactivated_collection_retention_days, Some(14));
    }

    #[test]
    fn quota_check_rate_limited() {
        let mut isolation = TenantIsolation::new(TenantQuota {
            max_qps: 3,
            ..Default::default()
        });

        for _ in 0..3 {
            isolation.request_start(t(1));
            isolation.request_end(t(1));
        }

        assert_eq!(
            isolation.check(t(1)),
            QuotaCheck::RateLimited { qps: 3, limit: 3 }
        );

        isolation.reset_rate_counters();
        assert!(isolation.check(t(1)).is_allowed());
    }

    #[test]
    fn quota_check_memory_exceeded() {
        let mut isolation = TenantIsolation::new(TenantQuota {
            max_memory_bytes: 1000,
            ..Default::default()
        });

        isolation.update_memory(t(1), 1001);
        assert!(matches!(
            isolation.check(t(1)),
            QuotaCheck::MemoryExceeded { .. }
        ));
    }

    #[test]
    fn request_rejected_tracking() {
        let mut isolation = TenantIsolation::new(TenantQuota::default());
        isolation.request_rejected(t(1));
        isolation.request_rejected(t(1));
        assert_eq!(isolation.usage(t(1)).unwrap().rejected_requests, 2);
    }

    #[test]
    fn multi_tenant_isolation() {
        let mut isolation = TenantIsolation::new(TenantQuota {
            max_concurrent_requests: 1,
            ..Default::default()
        });

        isolation.request_start(t(1));
        // Tenant 1 is at limit.
        assert!(!isolation.check(t(1)).is_allowed());
        // Tenant 2 is unaffected.
        assert!(isolation.check(t(2)).is_allowed());
    }

    // Fresh-boot single-tenant INSERT must not trip the default memory quota.
    //
    // The 1-second `update_tenant_memory_estimates` background loop on
    // `SharedState` reads `tikv_jemalloc_ctl::stats::allocated::read()` and
    // proportions the result across active tenants by `total_requests`. With
    // one tenant doing all the work, that tenant inherits 100% of the process
    // allocation. The default `max_memory_bytes` cap must stay large enough
    // that a typical fresh-boot footprint never trips it — otherwise routine
    // INSERTs against an empty collection start failing with `MemoryExceeded`
    // on a quiet single-tenant deployment.

    const MIB: u64 = 1024 * 1024;

    #[test]
    fn default_cap_clears_typical_fresh_boot_footprint() {
        // Single-tenant attribution at ~113 MiB (typical post-boot jemalloc
        // footprint on a 2-core data plane) must clear the default cap.
        let mut iso = TenantIsolation::new(TenantQuota::default());
        iso.request_start(t(1));
        iso.request_end(t(1));
        iso.update_memory(t(1), 113 * MIB);

        let check = iso.check(t(1));
        assert!(
            check.is_allowed(),
            "default tenant quota must clear a 113 MiB fresh-boot footprint; got {check:?}"
        );
    }

    #[test]
    fn default_cap_clears_quarter_gibibyte_footprint() {
        // 256 MiB attributed to a single tenant — still well under the cap.
        let mut iso = TenantIsolation::new(TenantQuota::default());
        iso.request_start(t(1));
        iso.request_end(t(1));
        iso.update_memory(t(1), 256 * MIB);
        assert!(
            iso.check(t(1)).is_allowed(),
            "default tenant quota must clear a 256 MiB attribution; got {:?}",
            iso.check(t(1))
        );
    }

    #[test]
    fn default_cap_remains_at_least_one_gib() {
        // Pin the lower bound of the default memory cap. Lowering this value
        // would re-introduce the class of fresh-boot quota failures: the
        // proportional estimator hands the active tenant ~all of the process
        // jemalloc footprint, so the cap must stay above typical post-boot
        // allocation. A tighter envelope must come from deriving the cap from
        // `server.memory_limit`, not from shrinking the universal default.
        let q = TenantQuota::default();
        assert!(
            q.max_memory_bytes >= 1024 * MIB,
            "default tenant memory cap must remain >= 1 GiB; got {} bytes",
            q.max_memory_bytes
        );
    }
}
