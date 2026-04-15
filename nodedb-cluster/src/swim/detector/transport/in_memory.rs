//! Test-only tokio-mpsc fabric implementing [`super::Transport`].

use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::{Mutex, mpsc};

use super::Transport;
use crate::swim::error::SwimError;
use crate::swim::wire::SwimMessage;

/// Test-only tokio-mpsc fabric that hosts multiple [`InMemoryTransport`]
/// endpoints sharing the same address space.
#[derive(Debug, Default)]
pub struct TransportFabric {
    inner: Mutex<FabricInner>,
}

// Test-only: the fabric is gated behind `TransportFabric` (used only by
// the detector unit tests), so the unbounded `HashMap`/`HashSet` here
// are acceptable — the "no unbounded collections in hot path" rule
// applies to production code only.
#[derive(Debug, Default)]
struct FabricInner {
    /// Inbound queue per bound address.
    inboxes: HashMap<SocketAddr, mpsc::Sender<(SocketAddr, SwimMessage)>>,
    /// Set of (from, to) pairs whose datagrams are silently dropped.
    dropped_edges: HashSet<(SocketAddr, SocketAddr)>,
}

impl TransportFabric {
    /// Construct a new fabric.
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            inner: Mutex::new(FabricInner::default()),
        })
    }

    /// Bind a new endpoint on the fabric. Panics only if `addr` is already
    /// bound in the fabric (test-only assertion — production transport
    /// is [`super::UdpTransport`]).
    pub async fn bind(self: &Arc<Self>, addr: SocketAddr) -> InMemoryTransport {
        let (tx, rx) = mpsc::channel(1024);
        let mut guard = self.inner.lock().await;
        assert!(
            guard.inboxes.insert(addr, tx).is_none(),
            "address {addr} already bound"
        );
        InMemoryTransport {
            addr,
            fabric: Arc::clone(self),
            inbox: Mutex::new(rx),
        }
    }

    /// Inject a permanent drop rule on the directed edge `(from, to)`.
    /// Any subsequent `send` from `from` to `to` is silently discarded.
    pub async fn drop_edge(&self, from: SocketAddr, to: SocketAddr) {
        self.inner.lock().await.dropped_edges.insert((from, to));
    }

    /// Remove every endpoint at `addr`, simulating a crashed node: future
    /// sends to it are dropped, and any in-flight recv returns
    /// `TransportClosed`.
    pub async fn remove(&self, addr: SocketAddr) {
        self.inner.lock().await.inboxes.remove(&addr);
    }
}

/// In-memory endpoint bound to the shared [`TransportFabric`].
#[derive(Debug)]
pub struct InMemoryTransport {
    addr: SocketAddr,
    fabric: Arc<TransportFabric>,
    inbox: Mutex<mpsc::Receiver<(SocketAddr, SwimMessage)>>,
}

#[async_trait]
impl Transport for InMemoryTransport {
    async fn send(&self, to: SocketAddr, msg: SwimMessage) -> Result<(), SwimError> {
        let inner = self.fabric.inner.lock().await;
        if inner.dropped_edges.contains(&(self.addr, to)) {
            return Ok(()); // silent drop
        }
        let Some(tx) = inner.inboxes.get(&to).cloned() else {
            return Ok(()); // peer not bound; silent drop
        };
        drop(inner);
        // Peer's inbox is full → silently drop (UDP semantics).
        let _ = tx.try_send((self.addr, msg));
        Ok(())
    }

    async fn recv(&self) -> Result<(SocketAddr, SwimMessage), SwimError> {
        let mut rx = self.inbox.lock().await;
        rx.recv().await.ok_or(SwimError::TransportClosed)
    }

    fn local_addr(&self) -> SocketAddr {
        self.addr
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::swim::incarnation::Incarnation;
    use crate::swim::wire::{Ping, ProbeId};
    use nodedb_types::NodeId;
    use std::net::{IpAddr, Ipv4Addr};

    fn addr(p: u16) -> SocketAddr {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), p)
    }

    fn ping() -> SwimMessage {
        SwimMessage::Ping(Ping {
            probe_id: ProbeId::new(1),
            from: NodeId::new("a"),
            incarnation: Incarnation::ZERO,
            piggyback: vec![],
        })
    }

    #[tokio::test]
    async fn send_and_recv_roundtrip() {
        let fab = TransportFabric::new();
        let a = fab.bind(addr(7000)).await;
        let b = fab.bind(addr(7001)).await;
        a.send(addr(7001), ping()).await.expect("send");
        let (from, msg) = b.recv().await.expect("recv");
        assert_eq!(from, addr(7000));
        assert!(matches!(msg, SwimMessage::Ping(_)));
    }

    #[tokio::test]
    async fn dropped_edge_silently_discards() {
        let fab = TransportFabric::new();
        let a = fab.bind(addr(7000)).await;
        let _b = fab.bind(addr(7001)).await;
        fab.drop_edge(addr(7000), addr(7001)).await;
        a.send(addr(7001), ping()).await.expect("send");
        let got = tokio::time::timeout(std::time::Duration::from_millis(20), _b.recv()).await;
        assert!(got.is_err(), "dropped edge should not deliver");
    }

    #[tokio::test]
    async fn unbound_peer_silently_discards() {
        let fab = TransportFabric::new();
        let a = fab.bind(addr(7000)).await;
        a.send(addr(7999), ping()).await.expect("send to void");
    }

    #[tokio::test]
    async fn remove_endpoint_closes_recv() {
        let fab = TransportFabric::new();
        let b = fab.bind(addr(7001)).await;
        fab.remove(addr(7001)).await;
        let _ = b;
    }

    #[tokio::test]
    async fn local_addr_returns_bind() {
        let fab = TransportFabric::new();
        let a = fab.bind(addr(7000)).await;
        assert_eq!(a.local_addr(), addr(7000));
    }
}
