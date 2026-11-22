// SPDX-License-Identifier: BUSL-1.1

//! The ordered stream a request belongs to.
//!
//! Requests keep strict FIFO order per `(connection, vShard)` only. The
//! Control Plane names the stream. The Data Plane compares streams and
//! never allocates one.

use crate::control::server::shared::session::ConnectionId;

/// The ordered stream of a [`super::Request`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum RequestStream {
    /// A request of one client connection. Its id comes from the one
    /// process-wide allocator, so ids of every protocol are distinct.
    Connection(ConnectionId),
    /// A request no client connection orders: Raft apply, Calvin, replay,
    /// maintenance, the Event Plane, a background loop, or a leg another
    /// node forwarded.
    Internal,
}

impl RequestStream {
    /// The connection of a client request. `None` for internal work.
    pub fn connection(self) -> Option<ConnectionId> {
        match self {
            RequestStream::Connection(id) => Some(id),
            RequestStream::Internal => None,
        }
    }
}
