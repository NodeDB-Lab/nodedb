// SPDX-License-Identifier: BUSL-1.1

//! One authenticated session's hold on the session registry.
//!
//! A session loop holds a [`SessionBinding`] for its whole life. The binding
//! carries the kill signal the loop races against its next read, and it brings
//! a session-lifetime identity up to date at each request boundary.

use std::sync::Arc;

use tokio::sync::watch;

use crate::control::security::auth_context::generate_session_id;
use crate::control::security::identity::{AuthMethod, AuthenticatedIdentity};
use crate::control::server::session_auth::identity::rebuild_session_identity;
use crate::control::state::SharedState;
use crate::types::DatabaseId;

use super::registry::{AUTH_METHOD_OIDC, KillReason, SessionParams, SessionRegistry};

/// The most characters of a statement `SHOW SESSIONS` shows.
const STATEMENT_TEXT_CHARS: usize = 256;

/// What a session binds with.
pub struct SessionBindRequest<'a> {
    /// The id `SHOW SESSIONS` and `KILL SESSION` name. `None` generates one.
    pub session_id: Option<String>,
    pub identity: &'a AuthenticatedIdentity,
    pub protocol: &'static str,
    pub peer_addr: &'a str,
    pub current_database: Option<DatabaseId>,
}

impl<'a> SessionBindRequest<'a> {
    pub fn new(
        identity: &'a AuthenticatedIdentity,
        protocol: &'static str,
        peer_addr: &'a str,
    ) -> Self {
        Self {
            session_id: None,
            identity,
            protocol,
            peer_addr,
            current_database: None,
        }
    }

    pub fn in_database(mut self, database: DatabaseId) -> Self {
        self.current_database = Some(database);
        self
    }
}

/// A registered session. Its drop removes the registry row.
pub struct SessionBinding {
    registry: Arc<SessionRegistry>,
    session_id: String,
    kill_rx: watch::Receiver<KillReason>,
    user_id: u64,
    /// The identity comes from token claims, so the credential store has no
    /// record of it.
    external: bool,
    /// The credential version the identity in force reflects. `None` until the
    /// first request, because the identity predates this binding.
    seen_version: Option<u64>,
}

impl std::fmt::Debug for SessionBinding {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SessionBinding")
            .field("session_id", &self.session_id)
            .field("user_id", &self.user_id)
            .finish_non_exhaustive()
    }
}

/// The `SessionParams::auth_method` label of `method`.
fn auth_method_label(method: &AuthMethod) -> &'static str {
    match method {
        AuthMethod::ScramSha256 => "scram_sha_256",
        AuthMethod::CleartextPassword => "cleartext_password",
        AuthMethod::ApiKey => "api_key",
        AuthMethod::Certificate => "certificate",
        AuthMethod::Trust => "trust",
        AuthMethod::OidcBearer => AUTH_METHOD_OIDC,
    }
}

/// Register a session, then confirm its identity still stands.
///
/// The order closes the race with a revoke. A revoke that lands after the
/// registration finds the row and signals it. A revoke that landed before the
/// registration shows in the re-read here, and the bind refuses.
pub fn bind_session(
    state: &SharedState,
    request: SessionBindRequest<'_>,
) -> crate::Result<SessionBinding> {
    let SessionBindRequest {
        session_id,
        identity,
        protocol,
        peer_addr,
        current_database,
    } = request;
    let session_id = match session_id {
        Some(session_id) => session_id,
        None => generate_session_id()?,
    };
    let external = matches!(identity.auth_method, AuthMethod::OidcBearer);
    let kill_rx = state.session_registry.register(
        &session_id,
        &SessionParams {
            user_id: identity.user_id,
            username: identity.username.clone(),
            db_user: identity.username.clone(),
            peer_addr: peer_addr.to_string(),
            protocol: protocol.to_string(),
            auth_method: auth_method_label(&identity.auth_method).to_string(),
            tenant_id: identity.tenant_id.as_u64(),
            credential_version: state.credentials.current_version(identity.user_id),
            current_database,
            token_expiry_ms: identity.token_expiry_ms,
            api_key_id: identity.api_key_id.clone(),
        },
    )?;
    // From here every refusal drops the binding, which removes the row.
    let binding = SessionBinding {
        registry: Arc::clone(&state.session_registry),
        session_id,
        kill_rx,
        user_id: identity.user_id,
        external,
        seen_version: None,
    };
    if !external && rebuild_session_identity(state, identity).is_none() {
        return Err(crate::Error::SessionUserDropped);
    }
    if let Some(database) = current_database {
        state
            .session_registry
            .set_idle_timeout_secs(&binding.session_id, state.idle_timeout_cache.get(database));
    }
    Ok(binding)
}

/// Wait for the kill of an optional binding. Pending forever when there is none,
/// which is the state of a connection before authentication.
pub async fn session_ended(binding: &mut Option<SessionBinding>) -> crate::Error {
    match binding {
        Some(binding) => binding.ended().await,
        None => std::future::pending().await,
    }
}

/// Wait for the kill on a receiver from [`SessionBinding::kill_signal`]. Pending
/// forever when there is none.
pub async fn kill_signal_ended(kill: &mut Option<watch::Receiver<KillReason>>) -> crate::Error {
    match kill {
        Some(kill) => wait_for_kill(kill).await,
        None => std::future::pending().await,
    }
}

async fn wait_for_kill(kill: &mut watch::Receiver<KillReason>) -> crate::Error {
    loop {
        if let Some(error) = kill.borrow_and_update().as_error() {
            return error;
        }
        if kill.changed().await.is_err() {
            // The row is gone, so nothing can signal this session again.
            return crate::Error::SessionKilledByAdmin;
        }
    }
}

impl SessionBinding {
    pub fn session_id(&self) -> &str {
        &self.session_id
    }

    pub fn user_id(&self) -> u64 {
        self.user_id
    }

    /// A receiver for a task that watches the kill apart from the session loop.
    pub fn kill_signal(&self) -> watch::Receiver<KillReason> {
        self.kill_rx.clone()
    }

    /// The error of a killed session.
    pub fn check(&self) -> crate::Result<()> {
        match self.kill_rx.borrow().as_error() {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    /// Wait for the kill. Cancel-safe.
    pub async fn ended(&mut self) -> crate::Error {
        wait_for_kill(&mut self.kill_rx).await
    }

    /// Check the kill and bring `identity` up to date with the credential store.
    ///
    /// Reads the version before it rebuilds, so a mutation during the rebuild
    /// leaves the version ahead and the next request rebuilds again. An identity
    /// that no longer resolves ends the session.
    pub fn refresh(
        &mut self,
        state: &SharedState,
        identity: &mut AuthenticatedIdentity,
    ) -> crate::Result<()> {
        self.check()?;
        if self.external {
            return Ok(());
        }
        // A key expires with no credential mutation, and a node that catches up
        // through a metadata image applies no revoke entry. So the key is read
        // on every request, and neither case depends on the version moving.
        let key_stands = identity
            .api_key_id
            .as_deref()
            .is_none_or(|key_id| state.api_keys.is_key_valid(key_id));
        let version = state.credentials.current_version(self.user_id);
        if key_stands && self.seen_version == Some(version) {
            return Ok(());
        }
        let rebuilt = key_stands
            .then(|| rebuild_session_identity(state, identity))
            .flatten();
        let Some(current) = rebuilt else {
            self.registry
                .kill_session_by_id(&self.session_id, KillReason::UserDropped);
            return Err(crate::Error::SessionUserDropped);
        };
        *identity = current;
        self.seen_version = Some(version);
        Ok(())
    }

    /// Mark activity and add to the byte counters.
    pub fn touch(&self, bytes_in: u64, bytes_out: u64) {
        self.registry
            .touch_with_bytes(&self.session_id, bytes_in, bytes_out);
    }

    /// Record the database the session runs in. The idle sweep reads that
    /// database's idle cap at its next tick.
    pub fn set_database(&self, database: DatabaseId) {
        self.registry
            .set_current_database(&self.session_id, database);
    }

    /// Mark the session busy with a statement or stream. The idle sweep skips a
    /// busy session, and `SHOW SESSIONS` shows `digest`.
    pub fn set_statement(&self, digest: &str) {
        self.registry
            .set_current_statement(&self.session_id, digest.to_string());
    }

    /// Mark the session busy with `sql`. `SHOW SESSIONS` shows the text the audit
    /// trail records for it, so a secret the audit text redacts stays hidden.
    ///
    /// The redaction reads one statement. A message that holds several shows a
    /// fixed label, because a secret past the first statement can escape it.
    pub fn set_statement_text(&self, sql: &str) {
        if sql.trim().trim_end_matches(';').contains(';') {
            self.set_statement("multi_statement");
            return;
        }
        let redacted = nodedb_sql::ddl_ast::parse::audit_text_for(sql);
        let text = redacted.as_deref().unwrap_or(sql);
        let shown: String = text.chars().take(STATEMENT_TEXT_CHARS).collect();
        self.registry.set_current_statement(&self.session_id, shown);
    }

    /// Move the session's token expiry, after a token refresh.
    pub fn set_token_expiry_ms(&self, expiry_ms: u64) {
        self.registry.set_token_expiry(&self.session_id, expiry_ms);
    }

    /// Clear the mark of [`Self::set_statement`] and mark activity.
    pub fn clear_statement(&self) {
        self.registry.clear_current_statement(&self.session_id);
        self.registry.touch(&self.session_id);
    }
}

impl Drop for SessionBinding {
    fn drop(&mut self) {
        self.registry.unregister(&self.session_id);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::bridge::dispatch::Dispatcher;
    use crate::control::security::identity::Role;
    use crate::control::server::session_auth::trust_identity;
    use crate::types::TenantId;
    use crate::wal::WalManager;

    fn state() -> (Arc<SharedState>, tempfile::TempDir) {
        let dir = tempfile::tempdir().expect("tempdir");
        let wal = Arc::new(WalManager::open_for_testing(&dir.path().join("t.wal")).expect("wal"));
        let (dispatcher, _) = Dispatcher::new(1, 64);
        (SharedState::new(dispatcher, wal).expect("state"), dir)
    }

    fn user(state: &SharedState, name: &str) -> AuthenticatedIdentity {
        state
            .credentials
            .create_user(name, "pw", TenantId::new(1), vec![Role::ReadOnly])
            .expect("create user");
        trust_identity(state, name).expect("identity")
    }

    fn bind(
        state: &SharedState,
        identity: &AuthenticatedIdentity,
    ) -> crate::Result<SessionBinding> {
        bind_session(
            state,
            SessionBindRequest::new(identity, "test", "127.0.0.1:1"),
        )
    }

    /// The revoke lands between authentication and the bind.
    #[test]
    fn a_bind_after_the_drop_refuses_and_leaves_no_row() {
        let (state, _dir) = state();
        let identity = user(&state, "late");
        state.credentials.drop_user("late").expect("drop");

        let refused = bind(&state, &identity);

        assert!(matches!(refused, Err(crate::Error::SessionUserDropped)));
        assert_eq!(state.session_registry.count(None), 0);
    }

    /// The name came back under a new id, so the old identity does not stand.
    #[test]
    fn a_bind_refuses_a_recreated_name() {
        let (state, _dir) = state();
        let identity = user(&state, "reborn");
        state.credentials.drop_user("reborn").expect("drop");
        user(&state, "reborn");

        assert!(matches!(
            bind(&state, &identity),
            Err(crate::Error::SessionUserDropped)
        ));
    }

    #[test]
    fn dropping_the_binding_removes_the_row() {
        let (state, _dir) = state();
        let identity = user(&state, "brief");
        let binding = bind(&state, &identity).expect("bind");
        assert_eq!(state.session_registry.count(None), 1);
        drop(binding);
        assert_eq!(state.session_registry.count(None), 0);
    }

    #[test]
    fn a_role_change_reaches_the_next_request() {
        let (state, _dir) = state();
        let mut identity = user(&state, "changing");
        let mut binding = bind(&state, &identity).expect("bind");
        binding
            .refresh(&state, &mut identity)
            .expect("first request");

        state
            .credentials
            .update_roles("changing", vec![Role::ReadWrite])
            .expect("update roles");
        binding
            .refresh(&state, &mut identity)
            .expect("next request");

        assert_eq!(identity.roles, vec![Role::ReadWrite]);
        assert!(binding.check().is_ok());
    }

    #[test]
    fn a_drop_ends_the_session_and_reports_the_user_dropped_error() {
        let (state, _dir) = state();
        let mut identity = user(&state, "gone");
        let mut binding = bind(&state, &identity).expect("bind");

        state.credentials.drop_user("gone").expect("drop");

        assert!(matches!(
            binding.refresh(&state, &mut identity),
            Err(crate::Error::SessionUserDropped)
        ));
    }

    /// A lookup miss that no kill signal covers still ends the session.
    #[test]
    fn a_refresh_that_finds_no_user_signals_the_kill() {
        let (state, _dir) = state();
        let mut identity = user(&state, "absent");
        let mut binding = bind(&state, &identity).expect("bind");
        identity.username = "not-in-the-store".to_string();

        assert!(binding.refresh(&state, &mut identity).is_err());
        assert_eq!(*binding.kill_signal().borrow(), KillReason::UserDropped);
    }

    fn shown_statement(state: &SharedState, binding: &SessionBinding) -> Option<String> {
        state
            .session_registry
            .list_all()
            .into_iter()
            .find(|session| session.session_id == binding.session_id())
            .and_then(|session| session.current_statement_digest)
    }

    #[test]
    fn show_sessions_never_shows_a_secret_the_audit_text_redacts() {
        let (state, _dir) = state();
        let identity = user(&state, "shown");
        let binding = bind(&state, &identity).expect("bind");

        binding.set_statement_text("SELECT 1");
        assert_eq!(
            shown_statement(&state, &binding).as_deref(),
            Some("SELECT 1")
        );

        binding.set_statement_text("ALTER USER shown SET PASSWORD 'hidden-pw'");
        let single = shown_statement(&state, &binding).expect("a statement shows");
        assert!(!single.contains("hidden-pw"), "{single}");

        binding.set_statement_text("SELECT 1; ALTER USER shown SET PASSWORD 'hidden-pw'");
        let several = shown_statement(&state, &binding).expect("a label shows");
        assert_eq!(several, "multi_statement");

        binding.clear_statement();
        assert_eq!(shown_statement(&state, &binding), None);
    }

    #[test]
    fn the_session_row_carries_the_identitys_token_expiry() {
        let (state, _dir) = state();
        let identity = user(&state, "bearer").with_token_expiry_ms(1);
        let _binding = bind(&state, &identity).expect("bind");

        let sweep = state.session_registry.sweep_snapshot();
        assert_eq!(sweep[0].token_expiry_ms, 1);
    }

    #[tokio::test]
    async fn ended_returns_the_reason_of_the_kill() {
        let (state, _dir) = state();
        let identity = user(&state, "watched");
        let mut binding = bind(&state, &identity).expect("bind");
        state
            .session_registry
            .kill_session_by_id(binding.session_id(), KillReason::IdleTimeout);

        assert!(matches!(
            binding.ended().await,
            crate::Error::SessionIdleTimeout
        ));
    }
}
