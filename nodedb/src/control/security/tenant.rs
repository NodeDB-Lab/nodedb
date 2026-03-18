//! Tenant isolation enforcement.
//!
//! TDD §14.3: Tenant data MUST be logically isolated in identifiers,
//! WAL streams, quotas, and cache accounting. Query planning and vector
//! prefilter bitmaps MUST be tenant-scoped by construction.

use std::collections::HashMap;

use crate::types::TenantId;

/// Per-tenant resource quotas.
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
    /// Request exceeds storage quota.
    StorageExceeded { used: u64, limit: u64 },
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
}

impl TenantIsolation {
    pub fn new(default_quota: TenantQuota) -> Self {
        Self {
            quotas: HashMap::new(),
            usage: HashMap::new(),
            default_quota,
        }
    }

    /// Set quota for a specific tenant.
    pub fn set_quota(&mut self, tenant_id: TenantId, quota: TenantQuota) {
        self.quotas.insert(tenant_id, quota);
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

        if usage.memory_bytes > quota.max_memory_bytes {
            return QuotaCheck::MemoryExceeded {
                used: usage.memory_bytes,
                limit: quota.max_memory_bytes,
            };
        }
        if usage.storage_bytes > quota.max_storage_bytes {
            return QuotaCheck::StorageExceeded {
                used: usage.storage_bytes,
                limit: quota.max_storage_bytes,
            };
        }
        if usage.active_requests >= quota.max_concurrent_requests {
            return QuotaCheck::ConcurrencyExceeded {
                active: usage.active_requests,
                limit: quota.max_concurrent_requests,
            };
        }
        if usage.requests_this_second >= quota.max_qps {
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
}

#[cfg(test)]
mod tests {
    use super::*;

    fn t(id: u32) -> TenantId {
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
}
