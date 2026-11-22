// SPDX-License-Identifier: BUSL-1.1

//! Immutable identity captured for one accepted pgwire connection.

use std::net::SocketAddr;

use crate::control::server::shared::session::ConnectionId;

/// Immutable pgwire connection identity. Addresses are metadata only; all
/// session state is keyed by `id`. The id comes from the process-wide
/// allocator on `SharedState`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct PgConnectionContext {
    pub(crate) id: ConnectionId,
    pub(crate) peer_addr: SocketAddr,
    pub(crate) local_addr: SocketAddr,
}
