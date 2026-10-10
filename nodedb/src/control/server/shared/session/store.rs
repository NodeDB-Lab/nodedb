// SPDX-License-Identifier: BUSL-1.1

//! Concurrent session store keyed by collision-free session identities.

use std::collections::HashMap;
use std::sync::RwLock;
use std::sync::atomic::Ordering::Relaxed;

use nodedb_types::DatabaseId;

use crate::types::TenantId;

use super::connection::{ConnectionId, ConnectionMetadata, ConnectionRegistrationError, SessionId};
use super::state::{ConnSession, TransactionState, now_unix_ms};

struct SessionEntry {
    session: ConnSession,
    metadata: Option<ConnectionMetadata>,
}

impl SessionEntry {
    fn legacy() -> Self {
        Self {
            session: ConnSession::new(),
            metadata: None,
        }
    }

    fn connection(metadata: ConnectionMetadata) -> Self {
        Self {
            session: ConnSession::new(),
            metadata: Some(metadata),
        }
    }
}

/// Concurrent session store with typed connection registrations and legacy
/// socket-address compatibility.
pub struct SessionStore {
    sessions: RwLock<HashMap<SessionId, SessionEntry>>,
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

    /// Preserve the legacy address-keyed session behavior.
    pub fn ensure_session(&self, addr: std::net::SocketAddr) {
        let mut sessions = self.sessions.write().unwrap_or_else(|p| p.into_inner());
        sessions
            .entry(SessionId::from(addr))
            .or_insert_with(SessionEntry::legacy);
    }

    /// Reset mutable state while preserving immutable typed-connection metadata.
    ///
    /// The admission permit carries over: it is connection capacity, not
    /// session state, and the connection stays open across the reset. Dropping
    /// it here frees the database and tenant slots of a live connection and
    /// lets `DISCARD ALL` walk past `max_connections`.
    pub fn reset_session(&self, id: impl Into<SessionId>) {
        self.write_session(id, |session| {
            let admission_permit = session.admission_permit.take();
            *session = ConnSession::new();
            session.admission_permit = admission_permit;
        });
    }

    /// Park a connection's scoped admission permit on its session.
    ///
    /// Returns `false` when no session is registered under `id`; the permit is
    /// then dropped, releasing the slots it holds. Any permit already parked is
    /// replaced and released.
    pub fn set_admission_permit(
        &self,
        id: impl Into<SessionId>,
        permit: crate::control::server::admission::ScopedConnectionPermit,
    ) -> bool {
        self.write_session(id, move |session| {
            session.admission_permit = Some(permit);
        })
        .is_some()
    }

    /// Register one immutable typed connection. Existing registrations are
    /// never overwritten, including registrations with matching endpoints.
    pub fn register_connection(
        &self,
        id: ConnectionId,
        metadata: ConnectionMetadata,
    ) -> Result<(), ConnectionRegistrationError> {
        let mut sessions = self.sessions.write().unwrap_or_else(|p| p.into_inner());
        let key = SessionId::from(id);
        if sessions.contains_key(&key) {
            return Err(ConnectionRegistrationError::Duplicate(id));
        }
        sessions.insert(key, SessionEntry::connection(metadata));
        Ok(())
    }

    /// Return immutable accept-time metadata for a registered connection.
    pub fn connection_metadata(&self, id: ConnectionId) -> Option<ConnectionMetadata> {
        let sessions = self.sessions.read().unwrap_or_else(|p| p.into_inner());
        sessions
            .get(&SessionId::from(id))
            .and_then(|entry| entry.metadata)
    }

    /// Snapshot registered typed connections and their immutable metadata.
    pub fn connection_snapshot(&self) -> Vec<(ConnectionId, ConnectionMetadata)> {
        let sessions = self.sessions.read().unwrap_or_else(|p| p.into_inner());
        sessions
            .iter()
            .filter_map(|(key, entry)| match (key, entry.metadata) {
                (SessionId::Connection(id), Some(metadata)) => Some((*id, metadata)),
                _ => None,
            })
            .collect()
    }

    /// Snapshot only exact typed connections for administrative display.
    pub fn connection_snapshot_with_state(
        &self,
    ) -> Vec<(ConnectionId, ConnectionMetadata, TransactionState)> {
        let sessions = self
            .sessions
            .read()
            .unwrap_or_else(|poison| poison.into_inner());
        let mut snapshot: Vec<_> = sessions
            .iter()
            .filter_map(|(key, entry)| match (key, entry.metadata) {
                (SessionId::Connection(id), Some(metadata)) => {
                    Some((*id, metadata, entry.session.tx_state))
                }
                _ => None,
            })
            .collect();
        snapshot.sort_unstable_by_key(|(id, _, _)| *id);
        snapshot
    }

    /// Remove a session (connection closed).
    pub fn remove(&self, id: impl Into<SessionId>) {
        let mut sessions = self.sessions.write().unwrap_or_else(|p| p.into_inner());
        sessions.remove(&id.into());
    }

    pub fn all_sessions(&self) -> Vec<(String, String)> {
        let sessions = self.sessions.read().unwrap_or_else(|p| p.into_inner());
        sessions
            .iter()
            .map(|(id, entry)| {
                let label = match id {
                    SessionId::Connection(id) => format!("connection:{id}"),
                    SessionId::LegacySocket(addr) => addr.to_string(),
                };
                let tx = match entry.session.tx_state {
                    TransactionState::Idle => "idle",
                    TransactionState::InBlock => "in_transaction",
                    TransactionState::Failed => "failed",
                };
                (label, tx.to_string())
            })
            .collect()
    }

    pub fn count(&self) -> usize {
        let sessions = self.sessions.read().unwrap_or_else(|p| p.into_inner());
        sessions.len()
    }

    pub fn get_cached_plan<F>(
        &self,
        id: impl Into<SessionId>,
        sql: &str,
        current_version: F,
        current_permission_tree_version: u64,
        current_rls_version: u64,
    ) -> Option<(
        Vec<nodedb_physical::physical_task::PhysicalTask>,
        crate::control::planner::descriptor_set::DescriptorVersionSet,
        crate::control::server::response_shape::schema::OutputSchema,
    )>
    where
        F: Fn(&nodedb_cluster::DescriptorId) -> Option<u64>,
    {
        let mut sessions = self.sessions.write().unwrap_or_else(|p| p.into_inner());
        sessions.get_mut(&id.into()).and_then(|entry| {
            entry.session.plan_cache.get(
                sql,
                current_version,
                current_permission_tree_version,
                current_rls_version,
            )
        })
    }

    pub fn put_cached_plan(
        &self,
        id: impl Into<SessionId>,
        sql: &str,
        tasks: Vec<nodedb_physical::physical_task::PhysicalTask>,
        versions: crate::control::planner::descriptor_set::DescriptorVersionSet,
        output_schema: crate::control::server::response_shape::schema::OutputSchema,
    ) {
        let mut sessions = self.sessions.write().unwrap_or_else(|p| p.into_inner());
        if let Some(entry) = sessions.get_mut(&id.into()) {
            entry
                .session
                .plan_cache
                .put(sql, tasks, versions, output_schema);
        }
    }

    pub fn get_current_database(&self, id: impl Into<SessionId>) -> Option<DatabaseId> {
        let sessions = self.sessions.read().unwrap_or_else(|p| p.into_inner());
        sessions.get(&id.into())?.session.current_database
    }

    pub fn set_current_database(&self, id: impl Into<SessionId>, db_id: DatabaseId) {
        self.write_session(id, |session| session.current_database = Some(db_id));
    }

    pub fn get_effective_tenant_id(&self, id: impl Into<SessionId>) -> Option<TenantId> {
        let sessions = self.sessions.read().unwrap_or_else(|p| p.into_inner());
        sessions
            .get(&id.into())
            .and_then(|entry| entry.session.effective_tenant_id)
    }

    pub fn set_effective_tenant_id(&self, id: impl Into<SessionId>, tenant: Option<TenantId>) {
        self.write_session(id, |session| {
            session.effective_tenant_id = tenant;
            session.plan_cache.clear();
            session.prepared_stmts.clear();
        });
    }

    pub fn identity(
        &self,
        id: impl Into<SessionId>,
    ) -> Option<crate::control::security::identity::AuthenticatedIdentity> {
        let sessions = self.sessions.read().unwrap_or_else(|p| p.into_inner());
        sessions
            .get(&id.into())
            .and_then(|entry| entry.session.identity.clone())
    }

    pub fn set_identity(
        &self,
        id: impl Into<SessionId>,
        identity: crate::control::security::identity::AuthenticatedIdentity,
    ) {
        let mut sessions = self.sessions.write().unwrap_or_else(|p| p.into_inner());
        sessions
            .entry(id.into())
            .or_insert_with(SessionEntry::legacy)
            .session
            .identity = Some(identity);
    }

    pub fn begin_request(&self, id: impl Into<SessionId>) {
        let mut sessions = self.sessions.write().unwrap_or_else(|p| p.into_inner());
        sessions
            .entry(id.into())
            .or_insert_with(SessionEntry::legacy)
            .session
            .in_flight
            .fetch_add(1, Relaxed);
    }

    pub fn end_request(&self, id: impl Into<SessionId>) {
        self.write_session(id, |session| {
            if session.in_flight.load(Relaxed) > 0 {
                session.in_flight.fetch_sub(1, Relaxed);
            }
            session.last_activity_ms.store(now_unix_ms(), Relaxed);
        });
    }

    pub fn idle_eligible(&self, id: impl Into<SessionId>, idle_ms: u64, now_ms: u64) -> bool {
        self.read_session(id, |session| {
            session.in_flight.load(Relaxed) == 0
                && now_ms.saturating_sub(session.last_activity_ms.load(Relaxed)) >= idle_ms
        })
        .unwrap_or(false)
    }

    pub fn reset_for_database_switch(&self, id: impl Into<SessionId>, new_db: DatabaseId) {
        self.write_session(id, |session| {
            session.tx_state = TransactionState::Idle;
            debug_assert_eq!(session.tx_buffer.len(), session.tx_lease_scopes.len());
            session.tx_buffer.clear();
            session.tx_lease_scopes.clear();
            session.tx_body_tasks.clear();
            session.tx_ts_preview_rejected.clear();
            session.tx_snapshot_epoch = None;
            session.tx_id = None;
            session.tx_vshards.clear();
            session.tx_read_set.clear();
            session.tx_reservation_vshards.clear();
            session.tx_reservation_owner = None;
            session.savepoints.clear();
            session.pending_offset_commits.clear();
            session.pending_field_inference.clear();
            session.pending_notifies.clear();
            session.pending_publishes.clear();
            // Cursors can retain rows from the previous database, including
            // WITH HOLD cursors, so no cursor can survive a database switch.
            session.cursors.clear();
            // LIVE subscriptions are bound to the database selected when they
            // are created. Retaining them across USE DATABASE will deliver
            // events from the previous database on the new session binding.
            session.live_subscriptions.clear();
            session.prepared_stmts.clear();
            session.plan_cache.clear();
            session.effective_tenant_id = None;
            session.current_database = Some(new_db);
        });
    }

    pub(super) fn read_session<R>(
        &self,
        id: impl Into<SessionId>,
        f: impl FnOnce(&ConnSession) -> R,
    ) -> Option<R> {
        let sessions = self.sessions.read().unwrap_or_else(|p| p.into_inner());
        sessions.get(&id.into()).map(|entry| f(&entry.session))
    }

    pub(super) fn write_session<R>(
        &self,
        id: impl Into<SessionId>,
        f: impl FnOnce(&mut ConnSession) -> R,
    ) -> Option<R> {
        let mut sessions = self.sessions.write().unwrap_or_else(|p| p.into_inner());
        sessions
            .get_mut(&id.into())
            .map(|entry| f(&mut entry.session))
    }

    /// Count of this session's own currently-held lease-scope entries for
    /// `descriptor` at a version `<= up_to_version`. A session with no open
    /// transaction, or none registered under `id`, holds none. Used by the
    /// DDL drain to exclude the requesting transaction's own hold from what
    /// it waits on — a session cannot conflict with itself.
    pub(super) fn own_lease_hold_count(
        &self,
        id: impl Into<SessionId>,
        descriptor: &nodedb_cluster::DescriptorId,
        up_to_version: u64,
    ) -> u32 {
        self.read_session(id, |session| {
            session
                .tx_lease_scopes
                .iter()
                .flatten()
                .flat_map(|scope| scope.descriptor_versions().iter())
                .filter(|(held, version)| held == descriptor && *version <= up_to_version)
                .count() as u32
        })
        .unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn address(port: u16) -> std::net::SocketAddr {
        std::net::SocketAddr::from(([127, 0, 0, 1], port))
    }

    #[test]
    fn session_cleanup() {
        let store = SessionStore::new();
        let addr: std::net::SocketAddr = "127.0.0.1:5000".parse().unwrap();
        store.ensure_session(addr);
        assert_eq!(store.count(), 1);

        store.remove(addr);
        assert_eq!(store.count(), 0);
    }

    fn task() -> nodedb_physical::physical_task::PhysicalTask {
        nodedb_physical::physical_task::PhysicalTask {
            tenant_id: TenantId::new(1),
            database_id: DatabaseId::DEFAULT,
            vshard_id: crate::types::VShardId::new(1),
            plan: nodedb_physical::physical_plan::PhysicalPlan::Meta(
                nodedb_physical::physical_plan::MetaOp::WalAppend {
                    payload: Vec::new(),
                },
            ),
            post_set_op: nodedb_physical::physical_task::PostSetOp::None,
            txn_id: None,
        }
    }

    #[test]
    fn rollback_and_database_switch_clear_aligned_lease_holders() {
        use std::sync::Arc;

        use crate::control::lease::QueryLeaseScope;

        let store = SessionStore::new();
        let addr: std::net::SocketAddr = "127.0.0.1:6011".parse().expect("address");
        store.ensure_session(addr);
        let scope = Arc::new(QueryLeaseScope::empty());
        store.begin(addr, 0).expect("begin");
        assert!(store.buffer_write(addr, task()));
        assert!(store.attach_tx_lease_scope_since(addr, 0, Arc::clone(&scope)));
        store.rollback(addr).expect("rollback");
        store.read_session(addr, |session| {
            assert!(session.tx_buffer.is_empty());
            assert!(session.tx_lease_scopes.is_empty());
            assert_eq!(session.tx_buffer.len(), session.tx_lease_scopes.len());
        });

        store.begin(addr, 0).expect("begin");
        assert!(store.buffer_write(addr, task()));
        assert!(store.attach_tx_lease_scope_since(addr, 0, scope));
        store.reset_for_database_switch(addr, DatabaseId::new(2));
        store.read_session(addr, |session| {
            assert!(session.tx_buffer.is_empty());
            assert!(session.tx_lease_scopes.is_empty());
            assert_eq!(session.tx_buffer.len(), session.tx_lease_scopes.len());
        });
    }

    #[test]
    fn database_switch_clears_all_cursors() {
        let store = SessionStore::new();
        let session = address(4998);
        store.ensure_session(session);
        store.declare_cursor(
            session,
            "previous_database_cursor".into(),
            vec!["old row".into()],
            false,
            true,
        );

        store.reset_for_database_switch(session, DatabaseId::new(2));

        assert!(matches!(
            store.fetch_cursor(session, "previous_database_cursor", 1),
            Err(crate::Error::BadRequest { detail }) if detail == "cursor \"previous_database_cursor\" does not exist"
        ));
    }

    #[test]
    fn typed_connections_with_same_peer_are_isolated() {
        let store = SessionStore::new();
        let peer = address(4000);
        let first = ConnectionId::new(1).unwrap();
        let second = ConnectionId::new(2).unwrap();
        let first_metadata = ConnectionMetadata {
            peer_addr: peer,
            local_addr: address(5432),
        };
        let second_metadata = ConnectionMetadata {
            peer_addr: peer,
            local_addr: address(6432),
        };
        store.register_connection(first, first_metadata).unwrap();
        store.register_connection(second, second_metadata).unwrap();

        store.set_current_database(first, DatabaseId::new(1));
        store.set_current_database(second, DatabaseId::new(2));
        assert_eq!(store.get_current_database(first), Some(DatabaseId::new(1)));
        assert_eq!(store.get_current_database(second), Some(DatabaseId::new(2)));
        assert_eq!(store.connection_metadata(first), Some(first_metadata));
        assert_eq!(store.connection_metadata(second), Some(second_metadata));

        store.remove(first);
        assert_eq!(store.get_current_database(first), None);
        assert_eq!(store.get_current_database(second), Some(DatabaseId::new(2)));
    }

    #[test]
    fn duplicate_typed_connection_is_rejected_without_overwrite() {
        let store = SessionStore::new();
        let id = ConnectionId::new(7).unwrap();
        let first = ConnectionMetadata {
            peer_addr: address(4001),
            local_addr: address(5432),
        };
        let second = ConnectionMetadata {
            peer_addr: address(4002),
            local_addr: address(6432),
        };
        store.register_connection(id, first).unwrap();
        assert_eq!(
            store.register_connection(id, second),
            Err(ConnectionRegistrationError::Duplicate(id))
        );
        assert_eq!(store.connection_metadata(id), Some(first));
    }

    #[test]
    fn typed_snapshot_excludes_legacy_and_sorts_by_connection_id() {
        let store = SessionStore::new();
        store.ensure_session(address(4999));
        let later = ConnectionId::new(9).unwrap_or_else(|_| unreachable!());
        let earlier = ConnectionId::new(2).unwrap_or_else(|_| unreachable!());
        let metadata = ConnectionMetadata {
            peer_addr: address(4004),
            local_addr: address(5432),
        };
        assert!(store.register_connection(later, metadata).is_ok());
        assert!(store.register_connection(earlier, metadata).is_ok());
        let snapshot = store.connection_snapshot_with_state();
        assert_eq!(snapshot.len(), 2);
        assert_eq!(snapshot[0].0, earlier);
        assert_eq!(snapshot[1].0, later);
    }

    #[test]
    fn legacy_address_session_is_distinct_from_typed_connection() {
        let store = SessionStore::new();
        let peer = address(4003);
        let id = ConnectionId::new(9).unwrap();
        store.ensure_session(peer);
        store
            .register_connection(
                id,
                ConnectionMetadata {
                    peer_addr: peer,
                    local_addr: address(5432),
                },
            )
            .unwrap();
        store.set_current_database(peer, DatabaseId::new(3));
        store.set_current_database(id, DatabaseId::new(4));
        assert_eq!(store.get_current_database(peer), Some(DatabaseId::new(3)));
        assert_eq!(store.get_current_database(id), Some(DatabaseId::new(4)));
    }
}
