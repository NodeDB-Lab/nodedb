// SPDX-License-Identifier: BUSL-1.1

//! Close this node's QUIC endpoint.
//!
//! Dropping every handle of an endpoint does not release its UDP socket. The
//! endpoint's driver task owns the socket, and it runs until every connection
//! is gone. A peer that keeps its connection alive keeps the socket bound
//! until the idle timeout, so a node that stops and starts again on the same
//! address fails to bind. Closing the endpoint ends every connection at once:
//! peers see the close immediately, and the driver releases the socket once
//! the last handle drops.

use std::time::Duration;

use super::transport::NexarTransport;

/// The QUIC application close code a node sends when it shuts down.
const SHUTDOWN_CLOSE_CODE: u32 = 0;

impl NexarTransport {
    /// Close every connection, refuse new ones, and wait until the peers
    /// acknowledged the close or `timeout` passed. Returns `false` when
    /// `timeout` passed first; the connections are closed either way.
    pub async fn close(&self, timeout: Duration) -> bool {
        let endpoint = self.listener.endpoint();
        endpoint.close(
            quinn::VarInt::from_u32(SHUTDOWN_CLOSE_CODE),
            b"node shutdown",
        );
        // The cached connections are closed: a send after this fails at once
        // instead of reusing one.
        self.peers
            .write()
            .unwrap_or_else(|p| p.into_inner())
            .clear();
        tokio::time::timeout(timeout, endpoint.wait_idle())
            .await
            .is_ok()
    }
}
