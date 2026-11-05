// SPDX-License-Identifier: BUSL-1.1

//! RAII in-flight guard for pgwire statement execution.
//!
//! Constructed at the top of a query handler and dropped when the statement
//! finishes (including early return, error, or panic-unwind). On construction
//! it bumps the connection's in-flight counter; on drop it decrements it and
//! stamps last-activity to "now". The listener watchdog reads that state to
//! decide idle eligibility, so this guard is what guarantees a long-running
//! statement is never idle-killed and that the idle window only starts once a
//! statement has actually completed.

use crate::control::server::shared::session::{SessionId, SessionStore};

/// Marks a statement as in flight for the lifetime of the guard.
pub(crate) struct InFlightGuard<'a> {
    sessions: &'a SessionStore,
    session_id: SessionId,
}

impl<'a> InFlightGuard<'a> {
    /// Begin a request: increment the connection's in-flight counter and show
    /// `statement_kind` in `SHOW SESSIONS`.
    pub(crate) fn new(
        sessions: &'a SessionStore,
        session_id: SessionId,
        statement_kind: &str,
    ) -> Self {
        sessions.begin_request(session_id);
        if let Some(binding) = sessions.binding(session_id) {
            binding.set_statement(statement_kind);
        }
        Self {
            sessions,
            session_id,
        }
    }
}

impl Drop for InFlightGuard<'_> {
    fn drop(&mut self) {
        // Decrement in-flight and stamp last-activity on every exit path.
        self.sessions.end_request(self.session_id);
        if let Some(binding) = self.sessions.binding(self.session_id) {
            if let Some(database) = self.sessions.get_current_database(self.session_id) {
                binding.set_database(database);
            }
            binding.clear_statement();
        }
    }
}
