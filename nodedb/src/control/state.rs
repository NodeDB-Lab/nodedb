use std::sync::{Arc, Mutex, RwLock};

use tracing::warn;

use crate::bridge::dispatch::Dispatcher;
use crate::control::request_tracker::RequestTracker;
use crate::control::security::apikey::ApiKeyStore;
use crate::control::security::audit::AuditLog;
use crate::control::security::credential::CredentialStore;
use crate::control::security::permission::PermissionStore;
use crate::control::security::role::RoleStore;
use crate::control::security::tenant::{QuotaCheck, TenantIsolation, TenantQuota};
use crate::types::TenantId;
use crate::wal::WalManager;

/// Shared state accessible by all Control Plane sessions.
///
/// This is the glue that connects TCP sessions to the Data Plane via the
/// Dispatcher/SPSC bridge, and to the WAL for the write path.
///
/// All fields are `Send + Sync` — safe for sharing across Tokio tasks.
pub struct SharedState {
    /// Routes requests to Data Plane cores via SPSC.
    pub dispatcher: Mutex<Dispatcher>,

    /// Tracks in-flight requests and routes responses back to sessions.
    pub tracker: RequestTracker,

    /// Write-ahead log for durability.
    pub wal: Arc<WalManager>,

    /// Credential store for user authentication.
    pub credentials: Arc<CredentialStore>,

    /// Audit log for security-relevant events.
    pub audit: Mutex<AuditLog>,

    /// API key store.
    pub api_keys: ApiKeyStore,

    /// Custom role store (inheritance, CRUD).
    pub roles: RoleStore,

    /// Collection-level permission grants + ownership.
    pub permissions: PermissionStore,

    /// Per-tenant quota enforcement.
    pub tenants: Mutex<TenantIsolation>,

    /// Audit retention in days (0 = keep forever).
    audit_retention_days: u32,

    /// Idle session timeout in seconds (0 = no timeout).
    idle_timeout_secs: u64,

    /// Cluster topology (None in single-node mode).
    pub cluster_topology: Option<Arc<RwLock<nodedb_cluster::ClusterTopology>>>,

    /// Cluster routing table (None in single-node mode).
    pub cluster_routing: Option<Arc<RwLock<nodedb_cluster::RoutingTable>>>,

    /// Cluster transport for forwarding requests (None in single-node mode).
    pub cluster_transport: Option<Arc<nodedb_cluster::NexarTransport>>,

    /// This node's ID (0 in single-node mode).
    pub node_id: u64,

    /// Propose tracker for distributed writes (None in single-node mode).
    pub propose_tracker: Option<Arc<crate::control::wal_replication::ProposeTracker>>,

    /// Raft propose function — wraps RaftLoop::propose (None in single-node mode).
    /// Signature: (vshard_id, data) → Result<(group_id, log_index)>
    pub raft_proposer: Option<Arc<crate::control::wal_replication::RaftProposer>>,

    /// Query Raft group statuses for observability (None in single-node mode).
    pub raft_status_fn: Option<Arc<dyn Fn() -> Vec<nodedb_cluster::GroupStatus> + Send + Sync>>,

    /// Migration tracker for observability (None in single-node mode).
    pub migration_tracker: Option<Arc<nodedb_cluster::MigrationTracker>>,
}

impl SharedState {
    /// Create shared state with in-memory credential store (for tests).
    pub fn new(dispatcher: Dispatcher, wal: Arc<WalManager>) -> Arc<Self> {
        Arc::new(Self {
            dispatcher: Mutex::new(dispatcher),
            tracker: RequestTracker::new(),
            wal,
            credentials: Arc::new(CredentialStore::new()),
            audit: Mutex::new(AuditLog::new(10_000)),
            api_keys: ApiKeyStore::new(),
            roles: RoleStore::new(),
            permissions: PermissionStore::new(),
            tenants: Mutex::new(TenantIsolation::new(TenantQuota::default())),
            cluster_topology: None,
            cluster_routing: None,
            cluster_transport: None,
            node_id: 0,
            propose_tracker: None,
            raft_proposer: None,
            raft_status_fn: None,
            migration_tracker: None,
            audit_retention_days: 0,
            idle_timeout_secs: 0,
        })
    }

    /// Create shared state with persistent credential store (for production).
    pub fn open(
        dispatcher: Dispatcher,
        wal: Arc<WalManager>,
        catalog_path: &std::path::Path,
        auth_config: &crate::config::auth::AuthConfig,
    ) -> crate::Result<Arc<Self>> {
        let mut credentials = CredentialStore::open(catalog_path)?;
        credentials.set_lockout_policy(
            auth_config.max_failed_logins,
            auth_config.lockout_duration_secs,
            auth_config.password_expiry_days,
        );

        let api_keys = ApiKeyStore::new();
        let roles = RoleStore::new();
        let permissions = PermissionStore::new();
        let mut audit_start_seq = 1u64;
        if let Some(catalog) = credentials.catalog() {
            api_keys.load_from(catalog)?;
            roles.load_from(catalog)?;
            permissions.load_from(catalog)?;
            let max_seq = catalog.load_audit_max_seq()?;
            if max_seq > 0 {
                audit_start_seq = max_seq + 1;
            }
        }

        let mut audit_log = AuditLog::new(10_000);
        audit_log.set_next_seq(audit_start_seq);

        Ok(Arc::new(Self {
            dispatcher: Mutex::new(dispatcher),
            tracker: RequestTracker::new(),
            wal,
            credentials: Arc::new(credentials),
            audit: Mutex::new(audit_log),
            api_keys,
            roles,
            permissions,
            tenants: Mutex::new(TenantIsolation::new(TenantQuota::default())),
            cluster_topology: None,
            cluster_routing: None,
            cluster_transport: None,
            node_id: 0,
            propose_tracker: None,
            raft_proposer: None,
            raft_status_fn: None,
            migration_tracker: None,
            audit_retention_days: auth_config.audit_retention_days,
            idle_timeout_secs: auth_config.idle_timeout_secs,
        }))
    }

    /// Get the idle session timeout in seconds (0 = no timeout).
    pub fn idle_timeout_secs(&self) -> u64 {
        self.idle_timeout_secs
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
    pub fn audit_record(
        &self,
        event: crate::control::security::audit::AuditEvent,
        tenant_id: Option<crate::types::TenantId>,
        source: &str,
        detail: &str,
    ) {
        match self.audit.lock() {
            Ok(mut log) => {
                log.record(event, tenant_id, source, detail);
            }
            Err(poisoned) => {
                warn!("audit log mutex poisoned, recovering");
                poisoned
                    .into_inner()
                    .record(event, tenant_id, source, detail);
            }
        }
    }

    /// Update per-tenant memory estimates.
    ///
    /// Called periodically (e.g. every 30 seconds). Uses jemalloc stats
    /// to estimate global memory, then distributes proportionally across
    /// tenants based on their request activity.
    pub fn update_tenant_memory_estimates(&self) {
        // Get total allocated from jemalloc.
        let total_allocated = tikv_jemalloc_ctl::stats::allocated::read().unwrap_or(0) as u64;

        let mut tenants = match self.tenants.lock() {
            Ok(t) => t,
            Err(p) => p.into_inner(),
        };

        // Distribute proportionally by total_requests per tenant.
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
    /// Called periodically (e.g. every 10 seconds) by a background task.
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
                // Re-insert entries so they're not lost.
                if let Ok(mut log) = self.audit.lock() {
                    for entry in entries {
                        log.record(entry.event, entry.tenant_id, &entry.source, &entry.detail);
                    }
                }
            } else {
                tracing::debug!(count = stored.len(), "flushed audit entries to catalog");

                // Prune old entries based on retention policy.
                if self.audit_retention_days > 0 {
                    let retention_us = self.audit_retention_days as u64 * 86400 * 1_000_000; // days → microseconds
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
    ///
    /// This should be called periodically from a background Tokio task.
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
