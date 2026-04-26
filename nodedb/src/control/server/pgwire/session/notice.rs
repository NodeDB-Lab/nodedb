//! Per-session NOTICE queue.
//!
//! Response shapers (e.g. `payload_to_response`) push warnings here while
//! they format payloads; the wire handler drains them after each query so
//! the client receives them as pgwire `NoticeResponse` messages.

use std::net::SocketAddr;

use super::store::SessionStore;

impl SessionStore {
    /// Queue a NOTICE message for delivery after the current query.
    pub fn push_notice(&self, addr: &SocketAddr, message: String) {
        self.write_session(addr, |session| {
            session.pending_notices.push(message);
        });
    }

    /// Drain all pending NOTICE messages for a connection.
    pub fn drain_notices(&self, addr: &SocketAddr) -> Vec<String> {
        self.write_session(addr, |session| std::mem::take(&mut session.pending_notices))
            .unwrap_or_default()
    }
}
