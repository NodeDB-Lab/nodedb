// SPDX-License-Identifier: BUSL-1.1

//! An owned transaction block for work the database initiates itself.

use crate::control::server::shared::session::{DmlTxnCtx, SessionId, SessionStore};
use crate::control::state::SharedState;

/// A private session sitting inside a transaction block, owned by the caller.
///
/// Client transactions live on a connection; a trigger or event action has no
/// connection, so it brings its own session. The store is private to this
/// scope, so the fixed session identity never collides with another scope's.
///
/// Open it inside `conn_scope::scoped_system_txn`. Its COMMIT and ROLLBACK
/// drain the DDL buffer of the connection scope they run in.
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

        let snapshot_epoch = state
            .calvin
            .last_applied_epoch
            .load(std::sync::atomic::Ordering::Acquire);

        // DDL buffers into the slots `conn_scope::scoped_system_txn` installs
        // around the transaction, and COMMIT applies it with the writes.
        crate::control::server::shared::session::ddl_buffer::activate();
        sessions
            .begin(session_id, snapshot_epoch)
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
