// SPDX-License-Identifier: BUSL-1.1

//! An owned transaction block for work the database initiates itself.

use crate::control::server::shared::session::{DmlTxnCtx, SessionId, SessionStore};
use crate::control::state::SharedState;

/// A private session sitting inside a transaction block, owned by the caller.
///
/// Client transactions live on a connection; a trigger or event action has no
/// connection, so it brings its own session. The store is private to this
/// scope, so the fixed session identity never collides with another scope's.
pub struct SystemTxnScope {
    sessions: SessionStore,
    session_id: SessionId,
}

impl SystemTxnScope {
    /// Open a transaction block on a fresh private session.
    pub fn begin(state: &SharedState) -> Result<Self, crate::Error> {
        let sessions = SessionStore::new();
        let addr = std::net::SocketAddr::from(([0, 0, 0, 0], 0));
        // `begin` reports success without entering the block when the session
        // does not exist yet, so the session must be created first.
        sessions.ensure_session(addr);
        let session_id = SessionId::from(addr);

        let snapshot_lsn = {
            let next = state.wal.next_lsn();
            crate::types::Lsn::new(next.as_u64().saturating_sub(1))
        };
        let snapshot_epoch = state
            .calvin
            .last_applied_epoch
            .load(std::sync::atomic::Ordering::Acquire);

        // Deliberately no `ddl_buffer::activate()`, unlike the client BEGIN
        // path. That buffer is scoped to a client connection future, and a
        // system transaction runs on the Event Plane outside any such scope.
        // A system action carries DML, so DDL buffering has nothing to do
        // here and any DDL it did contain proposes through the normal path.
        sessions
            .begin(session_id, snapshot_lsn, snapshot_epoch)
            .map_err(|detail| crate::Error::BadRequest {
                detail: detail.to_owned(),
            })?;

        Ok(Self {
            sessions,
            session_id,
        })
    }

    /// Borrow the DML routing context for this scope's session.
    pub fn ctx(&self) -> DmlTxnCtx<'_> {
        DmlTxnCtx {
            sessions: &self.sessions,
            session_id: self.session_id,
        }
    }

    pub fn sessions(&self) -> &SessionStore {
        &self.sessions
    }

    pub fn session_id(&self) -> SessionId {
        self.session_id
    }
}
