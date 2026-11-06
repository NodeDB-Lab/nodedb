// SPDX-License-Identifier: BUSL-1.1

//! Kill signals of [`SessionRegistry`], by target.
//!
//! Every function here sends on a watch under a read lock, so it does no I/O
//! and it is safe on the metadata apply path. A session row stays until the
//! session's own drop path removes it.

use std::net::SocketAddr;

use super::registry::{AUTH_METHOD_OIDC, KillReason, RegisteredSession, SessionRegistry};
use crate::control::security::blacklist::store::user_entry_id;

impl SessionRegistry {
    /// Signal every session `selects` accepts. Returns the number signalled.
    fn kill_where(
        &self,
        reason: KillReason,
        selects: impl Fn(&RegisteredSession) -> bool,
    ) -> usize {
        let sessions = self.sessions.read().unwrap_or_else(|p| p.into_inner());
        let mut killed = 0;
        for session in sessions.values().filter(|session| selects(session)) {
            let _ = session.kill_tx.send(reason);
            killed += 1;
        }
        killed
    }

    /// Kill all sessions of the credential-store user `user_id`.
    ///
    /// An OIDC session carries the `user_id` of its token claims. That number
    /// shares no id space with the credential store, so a session with an equal
    /// number belongs to a different principal and stays open.
    pub fn kill_sessions_for_stored_user(&self, user_id: u64, reason: KillReason) -> usize {
        self.kill_where(reason, |s| {
            s.user_id == user_id && s.auth_method != AUTH_METHOD_OIDC
        })
    }

    /// Kill all OIDC sessions whose token claims carry `user_id`. A
    /// credential-store user with an equal number stays connected.
    pub fn kill_sessions_for_oidc_user(&self, user_id: u64, reason: KillReason) -> usize {
        self.kill_where(reason, |s| {
            s.user_id == user_id && s.auth_method == AUTH_METHOD_OIDC
        })
    }

    /// Kill all sessions of a tenant.
    pub fn kill_sessions_for_tenant(&self, tenant_id: u64, reason: KillReason) -> usize {
        self.kill_where(reason, |s| s.tenant_id == tenant_id)
    }

    /// Kill the sessions of a tenant that run in `database`.
    pub fn kill_sessions_for_tenant_in_database(
        &self,
        tenant_id: u64,
        database: nodedb_types::DatabaseId,
        reason: KillReason,
    ) -> usize {
        self.kill_where(reason, |s| {
            s.tenant_id == tenant_id && s.current_database() == database
        })
    }

    /// Kill all sessions authenticated by the API key `key_id`.
    pub fn kill_sessions_for_api_key(&self, key_id: &str, reason: KillReason) -> usize {
        self.kill_where(reason, |s| s.api_key_id.as_deref() == Some(key_id))
    }

    /// Kill the sessions a blacklist entry for `blacklisted_id` refuses.
    ///
    /// `check_blacklist` compares the entry with [`user_entry_id`], which reads
    /// no auth method. So a number covers stored and OIDC principals alike, and
    /// this kills exactly that set.
    pub fn kill_sessions_for_blacklisted_user(
        &self,
        blacklisted_id: &str,
        reason: KillReason,
    ) -> usize {
        self.kill_where(reason, |s| {
            user_entry_id(s.user_id, &s.db_user) == blacklisted_id
        })
    }

    /// Kill all sessions whose peer IP `matches` accepts.
    ///
    /// A peer address that names no IP never matches. `matches` receives the
    /// bare IP text, with the port removed.
    pub fn kill_sessions_for_ip(
        &self,
        reason: KillReason,
        matches: impl Fn(&str) -> bool,
    ) -> usize {
        self.kill_where(reason, |s| {
            s.peer_addr
                .parse::<SocketAddr>()
                .is_ok_and(|peer| matches(&peer.ip().to_string()))
        })
    }

    /// Kill one session by id. Returns its bound database, or `None` when the
    /// session does not exist.
    pub fn kill_session_by_id(
        &self,
        session_id: &str,
        reason: KillReason,
    ) -> Option<nodedb_types::DatabaseId> {
        let sessions = self.sessions.read().unwrap_or_else(|p| p.into_inner());
        let session = sessions.get(session_id)?;
        let database = session.current_database();
        let _ = session.kill_tx.send(reason);
        Some(database)
    }
}
