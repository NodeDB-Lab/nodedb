//! `SessionHandleStore` — fingerprint-bound, rate-limited, observable session
//! handle resolver. See `super::mod` for the design rationale.

use std::collections::HashMap;
use std::sync::RwLock;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use super::fingerprint::{ClientFingerprint, FingerprintMode};
use super::miss_tracker::{PerConnectionSpikeDetector, SpikeOutcome, TenantMissCounters};
use super::rate_limit::{PerConnectionRateLimiter, RateLimitDecision};
use crate::control::security::audit::AuditEvent;
use crate::control::security::auth_context::AuthContext;
use crate::types::TenantId;

/// Default rate-limit budget per connection (20/min).
const DEFAULT_RATE_LIMIT_MAX: u32 = 20;
const DEFAULT_RATE_LIMIT_WINDOW: Duration = Duration::from_secs(60);
/// Default spike threshold — tuned to fire once a connection has clearly
/// stopped looking like a healthy caller (many misses, sustained).
const DEFAULT_MISS_SPIKE_THRESHOLD: u32 = 10;
const DEFAULT_MISS_SPIKE_WINDOW: Duration = Duration::from_secs(60);

struct CachedSession {
    auth_context: AuthContext,
    /// Fingerprint of the client that created the handle. Compared against
    /// every resolver's caller fingerprint under `FingerprintMode`.
    captured_fingerprint: ClientFingerprint,
    expires_at: u64,
}

/// Result type for resolver callers who need to distinguish a hard rate-limit
/// rejection (must close connection) from an ordinary miss (can continue).
#[derive(Debug)]
pub enum ResolveOutcome {
    /// Handle resolved and fingerprint matched.
    Resolved(Box<AuthContext>),
    /// Handle unknown, expired, or fingerprint-rejected. Caller should
    /// treat as "no session", fall back to base identity.
    Miss,
    /// Connection has exceeded its per-connection rate budget. Caller
    /// must return a fatal pgwire error and close the connection.
    RateLimited,
}

pub struct SessionHandleStore {
    sessions: RwLock<HashMap<String, CachedSession>>,
    default_ttl_secs: u64,
    fingerprint_mode: FingerprintMode,
    rate_limiter: PerConnectionRateLimiter,
    miss_counters: TenantMissCounters,
    spike_detector: PerConnectionSpikeDetector,
    /// Audit sink — emits events to the main `AuditLog` once wired by
    /// `SharedState`. A no-op hook is used in tests.
    audit_hook: RwLock<Box<dyn Fn(AuditEvent) + Send + Sync>>,
}

impl SessionHandleStore {
    pub fn new(default_ttl_secs: u64) -> Self {
        Self {
            sessions: RwLock::new(HashMap::new()),
            default_ttl_secs,
            fingerprint_mode: FingerprintMode::default(),
            rate_limiter: PerConnectionRateLimiter::new(
                DEFAULT_RATE_LIMIT_MAX,
                DEFAULT_RATE_LIMIT_WINDOW,
            ),
            miss_counters: TenantMissCounters::default(),
            spike_detector: PerConnectionSpikeDetector::new(
                DEFAULT_MISS_SPIKE_THRESHOLD,
                DEFAULT_MISS_SPIKE_WINDOW,
            ),
            audit_hook: RwLock::new(Box::new(|_| ())),
        }
    }

    /// Builder: override fingerprint mode (default `Subnet`).
    pub fn with_fingerprint_mode(mut self, mode: FingerprintMode) -> Self {
        self.fingerprint_mode = mode;
        self
    }

    /// Builder: override rate-limit budget.
    pub fn with_rate_limit(mut self, max_attempts: u32, window: Duration) -> Self {
        self.rate_limiter = PerConnectionRateLimiter::new(max_attempts, window);
        self
    }

    /// Builder: override miss-spike detection threshold/window.
    pub fn with_miss_spike_threshold(mut self, threshold: u32, window: Duration) -> Self {
        self.spike_detector = PerConnectionSpikeDetector::new(threshold, window);
        self
    }

    /// Build a store from the public `[auth.session]` TOML configuration.
    pub fn from_config(config: &crate::config::auth::SessionHandleConfig) -> Self {
        Self::new(config.ttl_secs)
            .with_fingerprint_mode(config.fingerprint_mode.into())
            .with_rate_limit(
                config.resolve_attempts_per_window,
                Duration::from_secs(config.rate_limit_window_secs),
            )
            .with_miss_spike_threshold(
                config.miss_spike_threshold,
                Duration::from_secs(config.miss_spike_window_secs),
            )
    }

    /// Install an audit sink. Called at `SharedState` wire-up time.
    pub fn set_audit_hook<F>(&self, hook: F)
    where
        F: Fn(AuditEvent) + Send + Sync + 'static,
    {
        let mut slot = self.audit_hook.write().unwrap_or_else(|p| p.into_inner());
        *slot = Box::new(hook);
    }

    /// Create a handle bound to the captured client fingerprint.
    pub fn create(
        &self,
        auth_context: AuthContext,
        captured_fingerprint: ClientFingerprint,
    ) -> String {
        let now = now_secs();
        let cached = CachedSession {
            auth_context,
            captured_fingerprint,
            expires_at: now + self.default_ttl_secs,
        };

        let mut sessions = self.sessions.write().unwrap_or_else(|p| p.into_inner());
        let handle = loop {
            let candidate = generate_handle();
            if !sessions.contains_key(&candidate) {
                break candidate;
            }
        };
        sessions.insert(handle.clone(), cached);

        // Lazy cleanup of expired handles (bounded per call).
        let expired: Vec<String> = sessions
            .iter()
            .filter(|(_, s)| now >= s.expires_at)
            .take(100)
            .map(|(k, _)| k.clone())
            .collect();
        for key in expired {
            sessions.remove(&key);
        }

        handle
    }

    /// Resolve a handle with full hygiene: per-connection rate limiting,
    /// fingerprint binding, miss counting, spike detection.
    ///
    /// `conn_key` is a stable identifier for the resolving connection
    /// (e.g. the pgwire peer `SocketAddr` as a string). `caller_fingerprint`
    /// is the current caller's `(tenant_id, ip)`.
    pub fn resolve(
        &self,
        handle: &str,
        conn_key: &str,
        caller_fingerprint: &ClientFingerprint,
    ) -> ResolveOutcome {
        if self.rate_limiter.admit(conn_key) == RateLimitDecision::Denied {
            return ResolveOutcome::RateLimited;
        }

        let now = now_secs();
        let sessions = self.sessions.read().unwrap_or_else(|p| p.into_inner());
        let Some(cached) = sessions.get(handle) else {
            drop(sessions);
            self.record_miss(conn_key, caller_fingerprint.tenant_id);
            return ResolveOutcome::Miss;
        };
        if now >= cached.expires_at {
            drop(sessions);
            self.record_miss(conn_key, caller_fingerprint.tenant_id);
            return ResolveOutcome::Miss;
        }
        if !cached
            .captured_fingerprint
            .matches(caller_fingerprint, self.fingerprint_mode)
        {
            let auth_ctx_tenant = cached.captured_fingerprint.tenant_id;
            drop(sessions);
            self.emit_audit(AuditEvent::SessionHandleFingerprintMismatch);
            // Miss counted against the caller's tenant — they probed a
            // session that isn't theirs.
            self.record_miss(conn_key, caller_fingerprint.tenant_id);
            // Also bump the captured tenant so ops see both sides in Grafana.
            if auth_ctx_tenant != caller_fingerprint.tenant_id {
                self.miss_counters.increment(auth_ctx_tenant);
            }
            return ResolveOutcome::Miss;
        }

        ResolveOutcome::Resolved(Box::new(cached.auth_context.clone()))
    }

    fn record_miss(&self, conn_key: &str, tenant_id: TenantId) {
        self.miss_counters.increment(tenant_id);
        if self.spike_detector.record(conn_key) == SpikeOutcome::Fired {
            self.emit_audit(AuditEvent::SessionHandleResolveMissSpike);
        }
    }

    fn emit_audit(&self, event: AuditEvent) {
        let hook = self.audit_hook.read().unwrap_or_else(|p| p.into_inner());
        (hook)(event);
    }

    /// Invalidate a handle. Returns true if the handle existed.
    pub fn invalidate(&self, handle: &str) -> bool {
        let mut sessions = self.sessions.write().unwrap_or_else(|p| p.into_inner());
        sessions.remove(handle).is_some()
    }

    /// Forget per-connection rate-limit and spike state on disconnect.
    pub fn forget_connection(&self, conn_key: &str) {
        self.rate_limiter.forget(conn_key);
        self.spike_detector.forget(conn_key);
    }

    /// Number of active (non-expired) handles.
    pub fn count(&self) -> usize {
        let now = now_secs();
        let sessions = self.sessions.read().unwrap_or_else(|p| p.into_inner());
        sessions.values().filter(|s| now < s.expires_at).count()
    }

    /// Age of the oldest active session handle in seconds.
    pub fn oldest_age_secs(&self) -> u64 {
        let now = now_secs();
        let sessions = self.sessions.read().unwrap_or_else(|p| p.into_inner());
        sessions
            .values()
            .filter(|s| now < s.expires_at)
            .map(|s| now.saturating_sub(s.expires_at.saturating_sub(self.default_ttl_secs)))
            .max()
            .unwrap_or(0)
    }

    /// Prometheus-scrape accessor for `session_handle.resolve_miss_total{tenant=...}`.
    pub fn miss_total_for_tenant(&self, tenant_id: TenantId) -> u64 {
        self.miss_counters.get(tenant_id)
    }
}

impl Default for SessionHandleStore {
    fn default() -> Self {
        Self::new(3600)
    }
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn generate_handle() -> String {
    crate::control::security::random::generate_tagged_random_hex("nds_")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::security::auth_context::generate_session_id;
    use crate::control::security::identity::{AuthMethod, AuthenticatedIdentity, Role};
    use std::net::{IpAddr, Ipv4Addr};
    use std::sync::{Arc, Mutex};

    fn ctx(tenant: u32, user: &str) -> AuthContext {
        let identity = AuthenticatedIdentity {
            user_id: 1,
            username: user.into(),
            tenant_id: TenantId::new(tenant),
            auth_method: AuthMethod::ApiKey,
            roles: vec![Role::ReadWrite],
            is_superuser: false,
        };
        AuthContext::from_identity(&identity, generate_session_id())
    }

    fn fp(tenant: u32, a: u8, b: u8, c: u8, d: u8) -> ClientFingerprint {
        ClientFingerprint::new(TenantId::new(tenant), IpAddr::V4(Ipv4Addr::new(a, b, c, d)))
    }

    #[test]
    fn create_then_resolve_with_matching_fingerprint() {
        let store = SessionHandleStore::new(3600);
        let origin = fp(1, 10, 0, 0, 5);
        let handle = store.create(ctx(1, "alice"), origin);
        match store.resolve(&handle, "c1", &origin) {
            ResolveOutcome::Resolved(ctx) => assert_eq!(ctx.username, "alice"),
            other => panic!("expected Resolved, got {other:?}"),
        }
    }

    #[test]
    fn unknown_handle_is_miss() {
        let store = SessionHandleStore::new(3600);
        match store.resolve("nds_nonexistent", "c1", &fp(1, 10, 0, 0, 5)) {
            ResolveOutcome::Miss => {}
            other => panic!("expected Miss, got {other:?}"),
        }
    }

    #[test]
    fn expired_handle_is_miss() {
        let store = SessionHandleStore::new(0);
        let origin = fp(1, 10, 0, 0, 5);
        let handle = store.create(ctx(1, "alice"), origin);
        matches!(store.resolve(&handle, "c1", &origin), ResolveOutcome::Miss);
    }

    #[test]
    fn invalidate_removes_handle() {
        let store = SessionHandleStore::new(3600);
        let origin = fp(1, 10, 0, 0, 5);
        let handle = store.create(ctx(1, "alice"), origin);
        assert!(store.invalidate(&handle));
        matches!(store.resolve(&handle, "c1", &origin), ResolveOutcome::Miss);
    }

    #[test]
    fn fingerprint_mismatch_emits_audit_and_counts_as_miss() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let store = SessionHandleStore::new(3600).with_fingerprint_mode(FingerprintMode::Strict);
        let sink = Arc::clone(&events);
        store.set_audit_hook(move |e| sink.lock().unwrap().push(e));

        let handle = store.create(ctx(1, "alice"), fp(1, 10, 0, 0, 5));
        matches!(
            store.resolve(&handle, "c1", &fp(1, 10, 0, 0, 6)),
            ResolveOutcome::Miss
        );

        let emitted = events.lock().unwrap();
        assert!(
            emitted
                .iter()
                .any(|e| matches!(e, AuditEvent::SessionHandleFingerprintMismatch))
        );
        assert!(store.miss_total_for_tenant(TenantId::new(1)) >= 1);
    }

    #[test]
    fn subnet_mode_tolerates_ipv4_24() {
        let store = SessionHandleStore::new(3600).with_fingerprint_mode(FingerprintMode::Subnet);
        let handle = store.create(ctx(1, "alice"), fp(1, 10, 0, 0, 5));
        matches!(
            store.resolve(&handle, "c1", &fp(1, 10, 0, 0, 99)),
            ResolveOutcome::Resolved(_)
        );
    }

    #[test]
    fn disabled_mode_skips_ip_check() {
        let store = SessionHandleStore::new(3600).with_fingerprint_mode(FingerprintMode::Disabled);
        let handle = store.create(ctx(1, "alice"), fp(1, 10, 0, 0, 5));
        matches!(
            store.resolve(&handle, "c1", &fp(1, 192, 168, 1, 1)),
            ResolveOutcome::Resolved(_)
        );
    }

    #[test]
    fn rate_limit_returns_rate_limited_after_threshold() {
        let store = SessionHandleStore::new(3600).with_rate_limit(3, Duration::from_secs(60));
        let caller = fp(1, 10, 0, 0, 5);
        for _ in 0..3 {
            let _ = store.resolve("nds_bogus", "c1", &caller);
        }
        matches!(
            store.resolve("nds_bogus", "c1", &caller),
            ResolveOutcome::RateLimited
        );
    }

    #[test]
    fn miss_spike_emits_audit_event_once() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let store = SessionHandleStore::new(3600)
            .with_rate_limit(1000, Duration::from_secs(60))
            .with_miss_spike_threshold(5, Duration::from_secs(60));
        let sink = Arc::clone(&events);
        store.set_audit_hook(move |e| sink.lock().unwrap().push(e));

        let caller = fp(1, 10, 0, 0, 5);
        for _ in 0..20 {
            let _ = store.resolve("nds_bogus", "c1", &caller);
        }

        let emitted = events.lock().unwrap();
        let spikes = emitted
            .iter()
            .filter(|e| matches!(e, AuditEvent::SessionHandleResolveMissSpike))
            .count();
        assert_eq!(spikes, 1, "expected exactly one spike event, got {spikes}");
    }

    #[test]
    fn miss_counter_tagged_by_tenant() {
        let store = SessionHandleStore::new(3600).with_rate_limit(1000, Duration::from_secs(60));
        for _ in 0..5 {
            let _ = store.resolve("nds_bogus", "c1", &fp(7, 10, 0, 0, 5));
        }
        for _ in 0..3 {
            let _ = store.resolve("nds_bogus", "c2", &fp(9, 10, 0, 0, 5));
        }
        assert_eq!(store.miss_total_for_tenant(TenantId::new(7)), 5);
        assert_eq!(store.miss_total_for_tenant(TenantId::new(9)), 3);
        assert_eq!(store.miss_total_for_tenant(TenantId::new(99)), 0);
    }

    #[test]
    fn hit_does_not_increment_miss_counter() {
        let store = SessionHandleStore::new(3600).with_fingerprint_mode(FingerprintMode::Disabled);
        let handle = store.create(ctx(1, "alice"), fp(1, 10, 0, 0, 5));
        for _ in 0..5 {
            let _ = store.resolve(&handle, "c1", &fp(1, 10, 0, 0, 5));
        }
        assert_eq!(store.miss_total_for_tenant(TenantId::new(1)), 0);
    }

    #[test]
    fn generated_handle_has_nds_prefix_and_128_bit_payload() {
        let h = generate_handle();
        assert!(h.starts_with("nds_"));
        let rest = h.strip_prefix("nds_").unwrap();
        assert_eq!(rest.len(), 32);
        assert!(rest.chars().all(|c| c.is_ascii_hexdigit()));
    }
}
