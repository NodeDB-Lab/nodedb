//! Concurrent session store — keyed by socket address.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::RwLock;

use super::state::{PgSession, TransactionState};

/// Concurrent session store — keyed by socket address.
pub struct SessionStore {
    sessions: RwLock<HashMap<SocketAddr, PgSession>>,
}

impl Default for SessionStore {
    fn default() -> Self {
        Self::new()
    }
}

impl SessionStore {
    pub fn new() -> Self {
        Self {
            sessions: RwLock::new(HashMap::new()),
        }
    }

    /// Ensure a session exists for this address.
    pub fn ensure_session(&self, addr: SocketAddr) {
        let mut sessions = self.sessions.write().unwrap_or_else(|p| p.into_inner());
        sessions.entry(addr).or_insert_with(PgSession::new);
    }

    /// Remove a session (connection closed).
    pub fn remove(&self, addr: &SocketAddr) {
        let mut sessions = self.sessions.write().unwrap_or_else(|p| p.into_inner());
        sessions.remove(addr);
    }

    /// List all active sessions as (peer_address, transaction_state) pairs.
    pub fn all_sessions(&self) -> Vec<(String, String)> {
        let sessions = self.sessions.read().unwrap_or_else(|p| p.into_inner());
        sessions
            .iter()
            .map(|(addr, session)| {
                let tx = match session.tx_state {
                    TransactionState::Idle => "idle",
                    TransactionState::InBlock => "in_transaction",
                    TransactionState::Failed => "failed",
                };
                (addr.to_string(), tx.to_string())
            })
            .collect()
    }

    /// Number of active sessions.
    pub fn count(&self) -> usize {
        let sessions = self.sessions.read().unwrap_or_else(|p| p.into_inner());
        sessions.len()
    }

    /// Look up cached physical tasks for a SQL string in the
    /// session's plan cache. `current_version` maps each
    /// recorded descriptor id to its current persisted version
    /// (or `None` if dropped). The cache returns a hit only
    /// when every recorded `(id, version)` pair still matches.
    ///
    /// On a hit returns both the cached tasks and the
    /// `DescriptorVersionSet` they were built against — the
    /// caller passes the set into
    /// `SharedState::acquire_plan_lease_scope` so cache hits
    /// and fresh plans share the same lease-acquisition path.
    pub fn get_cached_plan<F>(
        &self,
        addr: &SocketAddr,
        sql: &str,
        current_version: F,
    ) -> Option<(
        Vec<crate::control::planner::physical::PhysicalTask>,
        crate::control::planner::descriptor_set::DescriptorVersionSet,
    )>
    where
        F: Fn(&nodedb_cluster::DescriptorId) -> Option<u64>,
    {
        let mut sessions = self.sessions.write().unwrap_or_else(|p| p.into_inner());
        sessions
            .get_mut(addr)
            .and_then(|s| s.plan_cache.get(sql, current_version))
    }

    /// Store compiled physical tasks in the session's plan
    /// cache along with the descriptor version set they were
    /// built against.
    pub fn put_cached_plan(
        &self,
        addr: &SocketAddr,
        sql: &str,
        tasks: Vec<crate::control::planner::physical::PhysicalTask>,
        versions: crate::control::planner::descriptor_set::DescriptorVersionSet,
    ) {
        let mut sessions = self.sessions.write().unwrap_or_else(|p| p.into_inner());
        if let Some(session) = sessions.get_mut(addr) {
            session.plan_cache.put(sql, tasks, versions);
        }
    }

    /// Access the session map with a read lock for use by other session submodules.
    pub(super) fn read_session<R>(
        &self,
        addr: &SocketAddr,
        f: impl FnOnce(&PgSession) -> R,
    ) -> Option<R> {
        let sessions = self.sessions.read().unwrap_or_else(|p| p.into_inner());
        sessions.get(addr).map(f)
    }

    /// Access the session map with a write lock for use by other session submodules.
    pub(super) fn write_session<R>(
        &self,
        addr: &SocketAddr,
        f: impl FnOnce(&mut PgSession) -> R,
    ) -> Option<R> {
        let mut sessions = self.sessions.write().unwrap_or_else(|p| p.into_inner());
        sessions.get_mut(addr).map(f)
    }
}
