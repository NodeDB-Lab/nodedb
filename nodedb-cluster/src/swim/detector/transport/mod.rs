//! SWIM transport abstraction.
//!
//! The detector talks to the network exclusively through the [`Transport`]
//! trait. Two production-facing impls exist:
//!
//! 1. [`in_memory::InMemoryTransport`] — a tokio-mpsc fabric used by every
//!    unit test. Supports per-edge drop and partition injection so tests
//!    can deterministically simulate unreachable peers.
//! 2. [`udp::UdpTransport`] — the real wire-level transport that binds a
//!    `tokio::net::UdpSocket` and framing-encodes every datagram via
//!    [`crate::swim::wire::encode`].
//!
//! The trait is `Send + Sync` and its methods are `async`. Errors are
//! typed [`SwimError`] variants so callers never see raw `io::Error`.

pub mod in_memory;
pub mod udp;

use std::net::SocketAddr;

use async_trait::async_trait;

use crate::swim::error::SwimError;
use crate::swim::wire::SwimMessage;

pub use in_memory::{InMemoryTransport, TransportFabric};
pub use udp::UdpTransport;

/// Abstract SWIM transport. Implementations may be unreliable (UDP-like);
/// the detector assumes nothing about ordering or delivery guarantees.
#[async_trait]
pub trait Transport: Send + Sync {
    /// Send a single SWIM datagram to `to`. Errors indicate the transport
    /// itself is broken, not that the peer is unreachable — an unreachable
    /// peer is modelled as a silent drop.
    async fn send(&self, to: SocketAddr, msg: SwimMessage) -> Result<(), SwimError>;

    /// Block until the next inbound datagram is available. Returns
    /// [`SwimError::TransportClosed`] when the transport is shut down.
    async fn recv(&self) -> Result<(SocketAddr, SwimMessage), SwimError>;

    /// The local bind address — returned so callers can include it in
    /// outgoing messages without plumbing the address through separately.
    fn local_addr(&self) -> SocketAddr;
}
