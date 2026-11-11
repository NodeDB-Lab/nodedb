// SPDX-License-Identifier: BUSL-1.1

//! Per-session NOTICE queue.
//!
//! Response shapers (e.g. `payload_to_response`) push warnings here while
//! they format payloads; the wire handler drains them after each query so
//! the client receives them as pgwire `NoticeResponse` messages.

use super::connection::SessionId;
use super::store::SessionStore;

impl SessionStore {
    /// Queue a NOTICE message for delivery after the current query.
    pub fn push_notice(&self, addr: impl Into<SessionId>, message: String) {
        self.write_session(addr, |session| {
            session.pending_notices.push(message);
        });
    }

    /// How many NOTICE messages wait for delivery on a connection.
    pub fn pending_notice_count(&self, addr: impl Into<SessionId>) -> usize {
        self.write_session(addr, |session| session.pending_notices.len())
            .unwrap_or_default()
    }

    /// Drop the NOTICE messages queued after the first `keep`: those of a
    /// statement attempt that runs again.
    pub fn truncate_pending_notices(&self, addr: impl Into<SessionId>, keep: usize) {
        self.write_session(addr, |session| session.pending_notices.truncate(keep));
    }

    /// Drain all pending NOTICE messages for a connection.
    pub fn drain_notices(&self, addr: impl Into<SessionId>) -> Vec<String> {
        self.write_session(addr, |session| std::mem::take(&mut session.pending_notices))
            .unwrap_or_default()
    }
}
