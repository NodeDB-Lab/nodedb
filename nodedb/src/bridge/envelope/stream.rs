// SPDX-License-Identifier: BUSL-1.1

//! The ordered stream a request belongs to.
//!
//! Requests keep strict FIFO order per `(connection, vShard)` only. The
//! Control Plane names the stream. The Data Plane compares streams and
//! never allocates one.

use crate::control::server::shared::session::ConnectionId;

/// One client connection, named by the node that accepted it.
///
/// A connection id is unique per process only. A request forwarded to
/// another node keeps its origin node, so two nodes' equal ids stay apart.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ClientStream {
    /// The node that accepted the connection.
    pub origin_node: u64,
    /// The connection id on the origin node.
    pub id: ConnectionId,
}

/// The ordered stream of a [`super::Request`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum RequestStream {
    /// A request of one client connection, on this node or forwarded from
    /// the node that accepted it.
    Connection(ClientStream),
    /// A request no client connection orders: Raft apply, Calvin, replay,
    /// maintenance, the Event Plane, or a background loop.
    Internal,
}

impl RequestStream {
    /// The stream of connection `id`, accepted by node `origin_node`.
    pub fn client(origin_node: u64, id: ConnectionId) -> Self {
        RequestStream::Connection(ClientStream { origin_node, id })
    }

    /// The connection of a client request. `None` for internal work.
    pub fn connection(self) -> Option<ClientStream> {
        match self {
            RequestStream::Connection(client) => Some(client),
            RequestStream::Internal => None,
        }
    }
}
