// SPDX-License-Identifier: BUSL-1.1

//! Concurrent session store — keyed by socket address.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::RwLock;
use std::sync::atomic::Ordering::Relaxed;

use nodedb_types::DatabaseId;

use crate::types::TenantId;

use super::state::{ConnSession, TransactionState, now_unix_ms};

/// Concurrent session store — keyed by socket address.
pub struct SessionStore {
    sessions: RwLock<HashMap<SocketAddr, ConnSession>>,
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
        sessions.entry(addr).or_insert_with(ConnSession::new);
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
    /// On a hit returns the cached tasks, the
    /// `DescriptorVersionSet` they were built against, and the
    /// `OutputSchema` they were compiled with — the caller
    /// passes the version set into
    /// `SharedState::acquire_plan_lease_scope` so cache hits
    /// and fresh plans share the same lease-acquisition path.
    pub fn get_cached_plan<F>(
        &self,
        addr: &SocketAddr,
        sql: &str,
        current_version: F,
    ) -> Option<(
        Vec<nodedb_physical::physical_task::PhysicalTask>,
        crate::control::planner::descriptor_set::DescriptorVersionSet,
        crate::control::server::response_shape::schema::OutputSchema,
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
    /// cache along with the descriptor version set and output
    /// schema they were built against.
    pub fn put_cached_plan(
        &self,
        addr: &SocketAddr,
        sql: &str,
        tasks: Vec<nodedb_physical::physical_task::PhysicalTask>,
        versions: crate::control::planner::descriptor_set::DescriptorVersionSet,
        output_schema: crate::control::server::response_shape::schema::OutputSchema,
    ) {
        let mut sessions = self.sessions.write().unwrap_or_else(|p| p.into_inner());
        if let Some(session) = sessions.get_mut(addr) {
            session.plan_cache.put(sql, tasks, versions, output_schema);
        }
    }

    /// Retrieve the `current_database` for a connection, or `None` if the session
    /// does not exist or has not had a database bound yet.
    pub fn get_current_database(&self, addr: &SocketAddr) -> Option<DatabaseId> {
        let sessions = self.sessions.read().unwrap_or_else(|p| p.into_inner());
        sessions.get(addr)?.current_database
    }

    /// Bind a database to a session.  Called at pgwire startup once the database
    /// name from the StartupMessage has been resolved to a `DatabaseId`.
    pub fn set_current_database(&self, addr: &SocketAddr, db_id: DatabaseId) {
        let mut sessions = self.sessions.write().unwrap_or_else(|p| p.into_inner());
        if let Some(session) = sessions.get_mut(addr) {
            session.current_database = Some(db_id);
        }
    }

    /// Read the session's superuser tenant override, if any. Returns `None`
    /// when the session has never run `SET TENANT` (the common case).
    pub fn get_effective_tenant_id(&self, addr: &SocketAddr) -> Option<TenantId> {
        let sessions = self.sessions.read().unwrap_or_else(|p| p.into_inner());
        sessions.get(addr).and_then(|s| s.effective_tenant_id)
    }

    /// Install or clear the session's tenant override. Callers MUST have
    /// already verified the connection is a superuser and is not inside an
    /// active transaction — this method performs no policy checks.
    ///
    /// Invalidates the session's plan cache and SQL-level prepared statements
    /// so plans built against the prior tenant's catalog cannot be reused.
    pub fn set_effective_tenant_id(&self, addr: &SocketAddr, tenant: Option<TenantId>) {
        let mut sessions = self.sessions.write().unwrap_or_else(|p| p.into_inner());
        if let Some(session) = sessions.get_mut(addr) {
            session.effective_tenant_id = tenant;
            session.plan_cache.clear();
            session.prepared_stmts.clear();
        }
    }

    /// Read the identity resolved for queries on this connection, if any.
    /// Returns `None` when no query has resolved an identity yet (the session
    /// never issued a statement past auth) or the session does not exist.
    pub fn identity(
        &self,
        addr: &SocketAddr,
    ) -> Option<crate::control::security::identity::AuthenticatedIdentity> {
        let sessions = self.sessions.read().unwrap_or_else(|p| p.into_inner());
        sessions.get(addr).and_then(|s| s.identity.clone())
    }

    /// Stash the identity resolved for queries on this connection. Called from
    /// the per-query auth chokepoint (`resolve_identity`) so a connection torn
    /// down mid-transaction can reclaim its Data-Plane overlays without a live
    /// query. Overwrites any prior value — the identity in force for the most
    /// recent query is the one teardown must use. Creates the session entry if
    /// absent so the extended-query path (which resolves identity before
    /// `ensure_session`) still records it.
    pub fn set_identity(
        &self,
        addr: &SocketAddr,
        identity: crate::control::security::identity::AuthenticatedIdentity,
    ) {
        let mut sessions = self.sessions.write().unwrap_or_else(|p| p.into_inner());
        sessions
            .entry(*addr)
            .or_insert_with(ConnSession::new)
            .identity = Some(identity);
    }

    /// Record the start of a statement executing on this connection. Bumps the
    /// in-flight counter so the idle watchdog never closes a connection with a
    /// statement in progress. Creates the session entry if absent (mirrors
    /// `set_identity`) so the extended-query path — which can begin execution
    /// before `ensure_session` runs — still has its in-flight state tracked.
    pub fn begin_request(&self, addr: &SocketAddr) {
        let mut sessions = self.sessions.write().unwrap_or_else(|p| p.into_inner());
        sessions
            .entry(*addr)
            .or_insert_with(ConnSession::new)
            .in_flight
            .fetch_add(1, Relaxed);
    }

    /// Record the completion of a statement on this connection: decrement the
    /// in-flight counter (saturating — never underflows if the session was
    /// already removed by a concurrent teardown) and stamp last-activity to
    /// "now" so the idle window restarts from statement completion.
    pub fn end_request(&self, addr: &SocketAddr) {
        let mut sessions = self.sessions.write().unwrap_or_else(|p| p.into_inner());
        if let Some(session) = sessions.get_mut(addr) {
            // Exclusive write lock held: no concurrent mutator, so a
            // load-check-store is a safe saturating decrement.
            if session.in_flight.load(Relaxed) > 0 {
                session.in_flight.fetch_sub(1, Relaxed);
            }
            session.last_activity_ms.store(now_unix_ms(), Relaxed);
        }
    }

    /// Whether the connection at `addr` is eligible for idle timeout: the
    /// session exists, has zero statements in flight, and its last activity is
    /// at least `idle_ms` in the past relative to `now_ms`. Returns `false`
    /// when the session is missing — nothing to time out.
    pub fn idle_eligible(&self, addr: &SocketAddr, idle_ms: u64, now_ms: u64) -> bool {
        let sessions = self.sessions.read().unwrap_or_else(|p| p.into_inner());
        match sessions.get(addr) {
            Some(session) => {
                session.in_flight.load(Relaxed) == 0
                    && now_ms.saturating_sub(session.last_activity_ms.load(Relaxed)) >= idle_ms
            }
            None => false,
        }
    }

    /// Reset per-session state for a `USE DATABASE` switch:
    ///   1. Aborts any open transaction (discards tx_buffer, resets state to Idle).
    ///   2. Clears all SQL-level prepared statements.
    ///   3. Clears the wire-level plan cache.
    ///   4. Rebinds `current_database` to the new id.
    pub fn reset_for_database_switch(&self, addr: &SocketAddr, new_db: DatabaseId) {
        let mut sessions = self.sessions.write().unwrap_or_else(|p| p.into_inner());
        if let Some(session) = sessions.get_mut(addr) {
            // Abort open transaction.
            session.tx_state = TransactionState::Idle;
            session.tx_buffer.clear();
            session.tx_snapshot_lsn = None;
            session.tx_snapshot_epoch = None;
            session.tx_id = None;
            session.tx_vshards.clear();
            session.tx_read_set.clear();
            session.savepoints.clear();
            session.pending_offset_commits.clear();
            session.pending_notifies.clear();
            // Invalidate prepared statements and plan cache.
            session.prepared_stmts.clear();
            session.plan_cache.clear();
            // A USE DATABASE switch crosses out of any tenant override — the
            // new database may not exist (or have the same id) in the override
            // tenant, so the safe contract is to drop the override on switch.
            session.effective_tenant_id = None;
            // Rebind database.
            session.current_database = Some(new_db);
        }
    }

    /// Access the session map with a read lock for use by other session submodules.
    pub(super) fn read_session<R>(
        &self,
        addr: &SocketAddr,
        f: impl FnOnce(&ConnSession) -> R,
    ) -> Option<R> {
        let sessions = self.sessions.read().unwrap_or_else(|p| p.into_inner());
        sessions.get(addr).map(f)
    }

    /// Access the session map with a write lock for use by other session submodules.
    pub(super) fn write_session<R>(
        &self,
        addr: &SocketAddr,
        f: impl FnOnce(&mut ConnSession) -> R,
    ) -> Option<R> {
        let mut sessions = self.sessions.write().unwrap_or_else(|p| p.into_inner());
        sessions.get_mut(addr).map(f)
    }
}
