//! SharedState impl methods: quota, audit, polling, memory estimates.

use std::sync::Mutex;

use tracing::warn;

use crate::control::security::tenant::QuotaCheck;
use crate::types::TenantId;

use super::SharedState;

impl SharedState {
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

    /// Reset per-second rate counters. Called by a 1-second timer.
    pub fn reset_tenant_rate_counters(&self) {
        match self.tenants.lock() {
            Ok(mut t) => t.reset_rate_counters(),
            Err(poisoned) => poisoned.into_inner().reset_rate_counters(),
        }
    }

    /// Record an audit event.
    ///
    /// Writes to both the in-memory cache and the durable audit WAL (if available).
    /// The audit WAL write is synchronous — the entry is fsynced before return.
    pub fn audit_record(
        &self,
        event: crate::control::security::audit::AuditEvent,
        tenant_id: Option<crate::types::TenantId>,
        source: &str,
        detail: &str,
    ) {
        let entry = match self.audit.lock() {
            Ok(mut log) => {
                log.record(event, tenant_id, source, detail);
                // Clone the last entry for WAL persistence.
                log.all().back().cloned()
            }
            Err(poisoned) => {
                warn!("audit log mutex poisoned, recovering");
                let mut log = poisoned.into_inner();
                log.record(event, tenant_id, source, detail);
                log.all().back().cloned()
            }
        };

        // Write to durable audit WAL if available.
        if let Some(ref entry) = entry
            && let Ok(bytes) = rmp_serde::to_vec_named(entry)
        {
            let data_lsn = self.wal.next_lsn().as_u64();
            if let Err(e) = self.wal.append_audit_durable(&bytes, data_lsn) {
                warn!(error = %e, "failed to write audit entry to durable WAL");
            }
        }
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
    pub fn poll_and_route_responses(&self) {
        let responses = match self.dispatcher.lock() {
            Ok(mut d) => d.poll_responses(),
            Err(poisoned) => {
                warn!("dispatcher mutex poisoned, recovering");
                poisoned.into_inner().poll_responses()
            }
        };
        for resp in responses {
            if !self.tracker.complete(resp) {
                warn!("response for unknown or cancelled request");
            }
        }
    }
}
