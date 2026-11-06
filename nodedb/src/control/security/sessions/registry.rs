// SPDX-License-Identifier: BUSL-1.1

//! Active session registry.
//!
//! Tracks every long-lived authenticated session with its kill signal and
//! credential version at bind time.  Provides:
//!
//! - Bounded capacity (`max_active_sessions`; 0 = unlimited).  Over-cap
//!   returns [`SessionCapExceeded`] — no LRU eviction of live sessions.
//! - Kill signals by user, tenant, API key, principal, IP and id (`kill.rs`).
//! - `list_all` for `SHOW SESSIONS`.

use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::{RwLock, atomic::AtomicU64};

use tokio::sync::watch;

use nodedb_types::DatabaseId;

use crate::control::security::time::now_secs;

/// Reason a session was terminated.
///
/// Exhaustive matches required — no `_ =>` arms anywhere.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KillReason {
    /// Initial state — session is alive.
    Alive,
    /// Session killed because the associated user was dropped.
    UserDropped,
    /// Session closed because the per-database idle timeout elapsed.
    IdleTimeout,
    /// Session closed because the OIDC token expired.
    TokenExpired,
    /// Session killed by an administrator via `KILL SESSION` DDL.
    AdminKill,
}

impl KillReason {
    /// The error a session reports to its client. `None` for a live session.
    pub fn as_error(self) -> Option<crate::Error> {
        match self {
            Self::Alive => None,
            Self::UserDropped => Some(crate::Error::SessionUserDropped),
            Self::IdleTimeout => Some(crate::Error::SessionIdleTimeout),
            Self::TokenExpired => Some(crate::Error::SessionTokenExpired),
            Self::AdminKill => Some(crate::Error::SessionKilledByAdmin),
        }
    }
}

/// The `SessionParams::auth_method` label of an OIDC bearer session.
pub const AUTH_METHOD_OIDC: &str = "oidc_bearer";

/// Error returned when `register` would exceed `max_active_sessions`.
#[derive(Debug, thiserror::Error)]
#[error("max_active_sessions ({cap}) exceeded — rejecting new login")]
pub struct SessionCapExceeded {
    pub cap: usize,
}

/// Parameters for registering a new session.
pub struct SessionParams {
    pub user_id: u64,
    pub username: String,
    pub db_user: String,
    pub peer_addr: String,
    pub protocol: String,
    pub auth_method: String,
    pub tenant_id: u64,
    /// Per-user credential version at the time of authentication.
    pub credential_version: u64,
    /// The database this session is bound to at login time (0 = unbound).
    pub current_database: Option<DatabaseId>,
    /// OIDC token expiry in milliseconds since epoch (0 = no token-bound expiry).
    pub token_expiry_ms: Option<u64>,
    /// The API key that authenticated the session.
    pub api_key_id: Option<String>,
}

/// A registered session.
pub(super) struct RegisteredSession {
    pub(super) user_id: u64,
    pub(super) db_user: String,
    pub(super) peer_addr: String,
    protocol: String,
    pub(super) auth_method: String,
    pub(super) tenant_id: u64,
    /// The API key that authenticated the session.
    pub(super) api_key_id: Option<String>,
    connected_at: u64,
    last_active: AtomicU64,
    /// Kill signal carrying the reason for termination.
    pub(super) kill_tx: watch::Sender<KillReason>,
    /// Credential version at bind time; used for `SHOW SESSIONS`.
    credential_version: u64,
    /// The database this session is currently bound to (0 = unbound).
    current_database: AtomicU64,
    /// Idle timeout in seconds for this session's database (0 = no timeout).
    idle_timeout_secs: AtomicU64,
    /// OIDC token expiry in milliseconds since epoch (0 = no token-bound expiry).
    token_expiry_ms: AtomicU64,
    /// Bytes received (decoded payload size).
    bytes_in: AtomicU64,
    /// Bytes sent (response payload size).
    bytes_out: AtomicU64,
    /// Short digest of the currently executing statement, if any.
    current_statement_digest: RwLock<Option<String>>,
}

impl RegisteredSession {
    pub(super) fn current_database(&self) -> DatabaseId {
        DatabaseId::new(self.current_database.load(Ordering::Relaxed))
    }
}

/// Thread-safe registry of active authenticated sessions.
pub struct SessionRegistry {
    /// session_id → registered session.
    pub(super) sessions: RwLock<HashMap<String, RegisteredSession>>,
    /// 0 = unlimited.
    max_sessions: usize,
}

impl SessionRegistry {
    /// Create an unbounded registry.
    pub fn new() -> Self {
        Self {
            sessions: RwLock::new(HashMap::new()),
            max_sessions: 0,
        }
    }

    /// Create a registry with a session cap.  0 = unlimited.
    pub fn with_cap(max_sessions: usize) -> Self {
        Self {
            sessions: RwLock::new(HashMap::new()),
            max_sessions,
        }
    }

    /// Register a new session.
    ///
    /// Returns a kill-signal receiver the session loop must check at each
    /// request boundary.  Returns [`SessionCapExceeded`] when the registry
    /// is full.
    pub fn register(
        &self,
        session_id: &str,
        params: &SessionParams,
    ) -> Result<watch::Receiver<KillReason>, SessionCapExceeded> {
        let now = now_secs();
        let (kill_tx, kill_rx) = watch::channel(KillReason::Alive);
        let current_database_u64 = params.current_database.map(|d| d.as_u64()).unwrap_or(0);
        let token_expiry = params.token_expiry_ms.unwrap_or(0);
        let entry = RegisteredSession {
            user_id: params.user_id,
            db_user: params.db_user.clone(),
            peer_addr: params.peer_addr.clone(),
            protocol: params.protocol.clone(),
            auth_method: params.auth_method.clone(),
            tenant_id: params.tenant_id,
            api_key_id: params.api_key_id.clone(),
            connected_at: now,
            last_active: AtomicU64::new(now),
            kill_tx,
            credential_version: params.credential_version,
            current_database: AtomicU64::new(current_database_u64),
            idle_timeout_secs: AtomicU64::new(0),
            token_expiry_ms: AtomicU64::new(token_expiry),
            bytes_in: AtomicU64::new(0),
            bytes_out: AtomicU64::new(0),
            current_statement_digest: RwLock::new(None),
        };

        let mut sessions = self.sessions.write().unwrap_or_else(|p| p.into_inner());
        if self.max_sessions > 0 && sessions.len() >= self.max_sessions {
            return Err(SessionCapExceeded {
                cap: self.max_sessions,
            });
        }
        sessions.insert(session_id.to_string(), entry);
        Ok(kill_rx)
    }

    /// Unregister a session on disconnect.
    pub fn unregister(&self, session_id: &str) {
        let mut sessions = self.sessions.write().unwrap_or_else(|p| p.into_inner());
        sessions.remove(session_id);
    }

    /// Look up a session's bound database without signalling kill_tx.
    /// Used by the KILL SESSION DDL to resolve `current_database` for the
    /// authority check before deciding whether to kill.
    pub fn lookup_session_database(&self, session_id: &str) -> Option<DatabaseId> {
        let sessions = self.sessions.read().unwrap_or_else(|p| p.into_inner());
        sessions
            .get(session_id)
            .map(|s| DatabaseId::new(s.current_database.load(Ordering::Relaxed)))
    }

    /// Count active sessions, optionally filtered by numeric user ID.
    pub fn count(&self, user_filter: Option<u64>) -> usize {
        let sessions = self.sessions.read().unwrap_or_else(|p| p.into_inner());
        match user_filter {
            Some(uid) => sessions.values().filter(|s| s.user_id == uid).count(),
            None => sessions.len(),
        }
    }

    /// Update last-active timestamp (called at each request boundary).
    pub fn touch(&self, session_id: &str) {
        let now = now_secs();
        let sessions = self.sessions.read().unwrap_or_else(|p| p.into_inner());
        if let Some(s) = sessions.get(session_id) {
            s.last_active.store(now, Ordering::Relaxed);
        }
    }

    /// Update last-active timestamp and increment byte counters.
    pub fn touch_with_bytes(&self, session_id: &str, bytes_in_delta: u64, bytes_out_delta: u64) {
        let now = now_secs();
        let sessions = self.sessions.read().unwrap_or_else(|p| p.into_inner());
        if let Some(s) = sessions.get(session_id) {
            s.last_active.store(now, Ordering::Relaxed);
            s.bytes_in.fetch_add(bytes_in_delta, Ordering::Relaxed);
            s.bytes_out.fetch_add(bytes_out_delta, Ordering::Relaxed);
        }
    }

    /// Set the current statement digest for a session.
    pub fn set_current_statement(&self, session_id: &str, digest: String) {
        let sessions = self.sessions.read().unwrap_or_else(|p| p.into_inner());
        if let Some(s) = sessions.get(session_id)
            && let Ok(mut g) = s.current_statement_digest.write()
        {
            *g = Some(digest);
        }
    }

    /// Clear the current statement digest (called after response is sent).
    pub fn clear_current_statement(&self, session_id: &str) {
        let sessions = self.sessions.read().unwrap_or_else(|p| p.into_inner());
        if let Some(s) = sessions.get(session_id)
            && let Ok(mut g) = s.current_statement_digest.write()
        {
            *g = None;
        }
    }

    /// Update the current database binding for a session.
    pub fn set_current_database(&self, session_id: &str, db_id: DatabaseId) {
        let sessions = self.sessions.read().unwrap_or_else(|p| p.into_inner());
        if let Some(s) = sessions.get(session_id) {
            s.current_database.store(db_id.as_u64(), Ordering::Relaxed);
        }
    }

    /// Update the OIDC token expiry for a session.
    pub fn set_token_expiry(&self, session_id: &str, exp_ms: u64) {
        let sessions = self.sessions.read().unwrap_or_else(|p| p.into_inner());
        if let Some(s) = sessions.get(session_id) {
            s.token_expiry_ms.store(exp_ms, Ordering::Relaxed);
        }
    }

    /// Update the idle timeout (seconds) cached for a session.
    pub fn set_idle_timeout_secs(&self, session_id: &str, secs: u64) {
        let sessions = self.sessions.read().unwrap_or_else(|p| p.into_inner());
        if let Some(s) = sessions.get(session_id) {
            s.idle_timeout_secs.store(secs, Ordering::Relaxed);
        }
    }

    /// Iterate over all sessions for the idle sweeper.
    ///
    /// Returns a snapshot of session info sufficient for sweep decisions.
    pub fn sweep_snapshot(&self) -> Vec<SweepEntry> {
        let sessions = self.sessions.read().unwrap_or_else(|p| p.into_inner());
        sessions
            .iter()
            .map(|(id, s)| SweepEntry {
                session_id: id.clone(),
                last_active_secs: s.last_active.load(Ordering::Relaxed),
                idle_timeout_secs: s.idle_timeout_secs.load(Ordering::Relaxed),
                token_expiry_ms: s.token_expiry_ms.load(Ordering::Relaxed),
                current_database: DatabaseId::new(s.current_database.load(Ordering::Relaxed)),
                tenant_id: s.tenant_id,
                db_user: s.db_user.clone(),
                busy: s
                    .current_statement_digest
                    .read()
                    .map_or(true, |digest| digest.is_some()),
            })
            .collect()
    }

    /// List all active sessions for `SHOW SESSIONS`.
    pub fn list_all(&self) -> Vec<SessionInfo> {
        let now_s = now_secs();
        let sessions = self.sessions.read().unwrap_or_else(|p| p.into_inner());
        sessions
            .iter()
            .map(|(id, s)| {
                let last_active = s.last_active.load(Ordering::Relaxed);
                let token_expiry_ms = s.token_expiry_ms.load(Ordering::Relaxed);
                let token_expires_in_seconds = if token_expiry_ms == 0 {
                    None
                } else {
                    let exp_secs = token_expiry_ms / 1000;
                    Some(exp_secs.saturating_sub(now_s))
                };
                let digest = s
                    .current_statement_digest
                    .read()
                    .ok()
                    .and_then(|g| g.clone());
                SessionInfo {
                    session_id: id.clone(),
                    user_id: s.user_id,
                    db_user: s.db_user.clone(),
                    auth_method: s.auth_method.clone(),
                    connected_at: s.connected_at,
                    last_active,
                    client_ip: s.peer_addr.clone(),
                    protocol: s.protocol.clone(),
                    tenant_id: s.tenant_id,
                    credential_version: s.credential_version,
                    current_database: DatabaseId::new(s.current_database.load(Ordering::Relaxed)),
                    idle_seconds: now_s.saturating_sub(last_active),
                    bytes_in: s.bytes_in.load(Ordering::Relaxed),
                    bytes_out: s.bytes_out.load(Ordering::Relaxed),
                    current_statement_digest: digest,
                    token_expires_in_seconds,
                }
            })
            .collect()
    }

    /// Returns the maximum session cap (0 = unlimited).
    pub fn cap(&self) -> usize {
        self.max_sessions
    }
}

/// Lightweight snapshot entry produced for the idle sweeper.
#[derive(Debug, Clone)]
pub struct SweepEntry {
    pub session_id: String,
    pub last_active_secs: u64,
    pub idle_timeout_secs: u64,
    pub token_expiry_ms: u64,
    pub current_database: DatabaseId,
    pub tenant_id: u64,
    pub db_user: String,
    /// The session runs a statement or serves a stream, so it is not idle.
    pub busy: bool,
}

impl From<SessionCapExceeded> for crate::Error {
    fn from(e: SessionCapExceeded) -> Self {
        crate::Error::SessionCapExceeded { cap: e.cap }
    }
}

impl Default for SessionRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Session info for `SHOW SESSIONS` output.
#[derive(Debug, Clone)]
pub struct SessionInfo {
    pub session_id: String,
    pub user_id: u64,
    pub db_user: String,
    pub auth_method: String,
    pub connected_at: u64,
    pub last_active: u64,
    pub client_ip: String,
    pub protocol: String,
    pub tenant_id: u64,
    pub credential_version: u64,
    /// The database this session is currently bound to.
    pub current_database: DatabaseId,
    /// Seconds since last activity.
    pub idle_seconds: u64,
    /// Bytes received from the client (decoded payload size).
    pub bytes_in: u64,
    /// Bytes sent to the client (response payload size).
    pub bytes_out: u64,
    /// Short digest of the currently executing statement, if any.
    pub current_statement_digest: Option<String>,
    /// Seconds until the OIDC token expires (None if not OIDC).
    pub token_expires_in_seconds: Option<u64>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn params(user_id: u64, addr: &str, proto: &str) -> SessionParams {
        SessionParams {
            user_id,
            username: format!("user_{user_id}"),
            db_user: format!("user_{user_id}"),
            peer_addr: addr.to_string(),
            protocol: proto.to_string(),
            auth_method: "password".to_string(),
            tenant_id: 1,
            credential_version: 0,
            current_database: None,
            token_expiry_ms: None,
            api_key_id: None,
        }
    }

    #[test]
    fn a_stored_user_kill_leaves_an_oidc_session_with_an_equal_number() {
        let reg = SessionRegistry::new();
        let stored = reg
            .register("s1", &params(7, "10.0.0.1:1", "native"))
            .unwrap();
        let mut external = params(7, "10.0.0.2:1", "native");
        external.auth_method = AUTH_METHOD_OIDC.to_string();
        let external = reg.register("s2", &external).unwrap();

        assert_eq!(
            reg.kill_sessions_for_stored_user(7, KillReason::UserDropped),
            1
        );

        assert_eq!(*stored.borrow(), KillReason::UserDropped);
        assert_eq!(*external.borrow(), KillReason::Alive);
    }

    #[test]
    fn an_oidc_user_kill_leaves_a_stored_user_with_an_equal_number() {
        let reg = SessionRegistry::new();
        let stored = reg
            .register("s1", &params(7, "10.0.0.1:1", "native"))
            .unwrap();
        let mut external = params(7, "10.0.0.2:1", "native");
        external.auth_method = AUTH_METHOD_OIDC.to_string();
        let external = reg.register("s2", &external).unwrap();

        assert_eq!(reg.kill_sessions_for_oidc_user(7, KillReason::AdminKill), 1);

        assert_eq!(*stored.borrow(), KillReason::Alive);
        assert_eq!(*external.borrow(), KillReason::AdminKill);
    }

    #[test]
    fn an_api_key_kill_ends_the_sessions_of_that_key_only() {
        let reg = SessionRegistry::new();
        let mut first = params(7, "10.0.0.1:1", "native");
        first.api_key_id = Some("key-a".to_string());
        let mut second = params(7, "10.0.0.1:2", "native");
        second.api_key_id = Some("key-b".to_string());
        let first = reg.register("s1", &first).unwrap();
        let second = reg.register("s2", &second).unwrap();

        assert_eq!(
            reg.kill_sessions_for_api_key("key-a", KillReason::AdminKill),
            1
        );

        assert_eq!(*first.borrow(), KillReason::AdminKill);
        assert_eq!(*second.borrow(), KillReason::Alive);
    }

    #[test]
    fn a_blacklist_kill_matches_the_set_the_entry_refuses() {
        let reg = SessionRegistry::new();
        let stored = reg
            .register("s1", &params(42, "10.0.0.1:1", "native"))
            .unwrap();
        let mut oidc = params(42, "10.0.0.2:1", "native");
        oidc.auth_method = AUTH_METHOD_OIDC.to_string();
        let oidc = reg.register("s2", &oidc).unwrap();
        // An OIDC username is its token's subject, so it can read as a number.
        let mut named = params(43, "10.0.0.3:1", "native");
        named.db_user = "42".to_string();
        let named = reg.register("s3", &named).unwrap();

        assert_eq!(
            reg.kill_sessions_for_blacklisted_user("alice", KillReason::AdminKill),
            0
        );
        assert_eq!(
            reg.kill_sessions_for_blacklisted_user("042", KillReason::AdminKill),
            0,
            "an entry key is the exact decimal form"
        );
        assert_eq!(*stored.borrow(), KillReason::Alive);

        assert_eq!(
            reg.kill_sessions_for_blacklisted_user("42", KillReason::AdminKill),
            2
        );
        assert_eq!(*stored.borrow(), KillReason::AdminKill);
        assert_eq!(*oidc.borrow(), KillReason::AdminKill);
        assert_eq!(*named.borrow(), KillReason::Alive);

        // A token with no `user_id` claim goes by its subject.
        let mut subject = params(0, "10.0.0.4:1", "native");
        subject.db_user = "sub-a".to_string();
        subject.auth_method = AUTH_METHOD_OIDC.to_string();
        let subject = reg.register("s4", &subject).unwrap();
        assert_eq!(
            reg.kill_sessions_for_blacklisted_user("sub-a", KillReason::AdminKill),
            1
        );
        assert_eq!(*subject.borrow(), KillReason::AdminKill);
    }

    #[test]
    fn a_tenant_in_database_kill_leaves_the_tenants_other_databases() {
        let reg = SessionRegistry::new();
        let mut moved = params(1, "10.0.0.1:1", "native");
        moved.current_database = Some(DatabaseId::new(70));
        let mut elsewhere = params(2, "10.0.0.1:2", "native");
        elsewhere.current_database = Some(DatabaseId::new(71));
        let moved = reg.register("s1", &moved).unwrap();
        let elsewhere = reg.register("s2", &elsewhere).unwrap();

        let killed =
            reg.kill_sessions_for_tenant_in_database(1, DatabaseId::new(70), KillReason::AdminKill);

        assert_eq!(killed, 1);
        assert_eq!(*moved.borrow(), KillReason::AdminKill);
        assert_eq!(*elsewhere.borrow(), KillReason::Alive);
    }

    #[test]
    fn an_ip_kill_compares_the_whole_ip() {
        let reg = SessionRegistry::new();
        let near = reg
            .register("s1", &params(1, "10.0.0.12:1", "native"))
            .unwrap();

        assert_eq!(
            reg.kill_sessions_for_ip(KillReason::AdminKill, |ip| ip == "10.0.0.1"),
            0
        );
        assert_eq!(*near.borrow(), KillReason::Alive);
    }

    #[test]
    fn active_sessions_register_unregister() {
        let reg = SessionRegistry::new();
        let rx = reg
            .register("s1", &params(42, "10.0.0.1:5000", "native"))
            .unwrap();
        assert!(!rx.has_changed().unwrap_or(false));
        assert_eq!(reg.count(None), 1);
        assert_eq!(reg.count(Some(42)), 1);
        reg.unregister("s1");
        assert_eq!(reg.count(None), 0);
    }

    #[test]
    fn max_active_sessions_over_cap_rejects() {
        let reg = SessionRegistry::with_cap(2);

        reg.register("s1", &params(1, "10.0.0.1:5000", "native"))
            .unwrap();
        reg.register("s2", &params(2, "10.0.0.2:5000", "native"))
            .unwrap();

        // Third registration must fail.
        let result = reg.register("s3", &params(3, "10.0.0.3:5000", "native"));
        assert!(
            result.is_err(),
            "over-cap registration must return SessionCapExceeded"
        );

        // Existing sessions still alive.
        assert_eq!(reg.count(None), 2);
    }

    #[test]
    fn session_hard_revoke_close() {
        let reg = SessionRegistry::new();
        let mut rx = reg
            .register("s1", &params(99, "10.0.0.1:5000", "native"))
            .unwrap();
        assert!(!rx.has_changed().unwrap_or(false));

        let killed = reg.kill_sessions_for_stored_user(99, KillReason::UserDropped);
        assert_eq!(killed, 1);
        assert!(rx.has_changed().unwrap_or(false));
        assert_eq!(*rx.borrow_and_update(), KillReason::UserDropped);
    }

    /// A tenant kill names sessions by their tenant, never by a user id that
    /// happens to hold the same number.
    #[test]
    fn a_tenant_kill_ends_the_tenants_sessions_only() {
        let reg = SessionRegistry::new();
        let mut in_tenant = params(5, "10.0.0.1:5000", "native");
        in_tenant.tenant_id = 7;
        let mut user_with_tenants_number = params(7, "10.0.0.1:5001", "native");
        user_with_tenants_number.tenant_id = 8;
        let in_tenant = reg.register("s1", &in_tenant).unwrap();
        let other = reg.register("s2", &user_with_tenants_number).unwrap();

        assert_eq!(reg.kill_sessions_for_tenant(7, KillReason::AdminKill), 1);

        assert_eq!(*in_tenant.borrow(), KillReason::AdminKill);
        assert_eq!(*other.borrow(), KillReason::Alive);
    }

    #[test]
    fn kill_by_ip() {
        let reg = SessionRegistry::new();
        let _rx1 = reg
            .register("s1", &params(1, "10.0.0.1:5000", "native"))
            .unwrap();
        let _rx2 = reg
            .register("s2", &params(2, "10.0.0.1:5001", "pgwire"))
            .unwrap();
        let _rx3 = reg
            .register("s3", &params(3, "192.168.1.1:5000", "http"))
            .unwrap();

        let killed = reg.kill_sessions_for_ip(KillReason::AdminKill, |ip| ip == "10.0.0.1");
        assert_eq!(killed, 2);
    }

    #[test]
    fn show_sessions_lists_active() {
        let reg = SessionRegistry::new();
        reg.register("sess-abc", &params(7, "127.0.0.1:1234", "pgwire"))
            .unwrap();

        let all = reg.list_all();
        assert_eq!(all.len(), 1);
        assert_eq!(all[0].session_id, "sess-abc");
        assert_eq!(all[0].user_id, 7);
        assert_eq!(all[0].protocol, "pgwire");
    }

    #[test]
    fn kill_session_by_id_returns_db() {
        let reg = SessionRegistry::new();
        let _rx = reg
            .register("s1", &params(5, "10.0.0.1:5000", "native"))
            .unwrap();

        let result = reg.kill_session_by_id("s1", KillReason::AdminKill);
        assert!(result.is_some());

        let not_found = reg.kill_session_by_id("does-not-exist", KillReason::AdminKill);
        assert!(not_found.is_none());
    }

    #[test]
    fn kill_session_by_id_unknown_returns_none() {
        let reg = SessionRegistry::new();
        assert!(
            reg.kill_session_by_id("ghost", KillReason::AdminKill)
                .is_none()
        );
    }
}
