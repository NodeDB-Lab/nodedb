use std::sync::{Arc, Mutex};

use tracing::warn;

use crate::bridge::dispatch::Dispatcher;
use crate::control::request_tracker::RequestTracker;
use crate::control::security::audit::AuditLog;
use crate::control::security::credential::CredentialStore;
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

    /// Per-tenant quota enforcement.
    pub tenants: Mutex<TenantIsolation>,
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
            tenants: Mutex::new(TenantIsolation::new(TenantQuota::default())),
        })
    }

    /// Create shared state with persistent credential store (for production).
    pub fn open(
        dispatcher: Dispatcher,
        wal: Arc<WalManager>,
        catalog_path: &std::path::Path,
    ) -> crate::Result<Arc<Self>> {
        let credentials = CredentialStore::open(catalog_path)?;
        Ok(Arc::new(Self {
            dispatcher: Mutex::new(dispatcher),
            tracker: RequestTracker::new(),
            wal,
            credentials: Arc::new(credentials),
            audit: Mutex::new(AuditLog::new(10_000)),
            tenants: Mutex::new(TenantIsolation::new(TenantQuota::default())),
        }))
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
