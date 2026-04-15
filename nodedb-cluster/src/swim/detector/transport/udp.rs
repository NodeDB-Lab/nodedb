//! Real UDP transport for SWIM.
//!
//! Binds a single `tokio::net::UdpSocket` and implements [`super::Transport`]
//! by framing every outbound `SwimMessage` with the zerompk wire codec
//! and parsing every inbound datagram the same way. Malformed inbound
//! bytes surface as [`SwimError::Decode`] but do not close the socket —
//! the failure detector's recv loop treats non-`TransportClosed` errors
//! as transient and keeps running.
//!
//! Datagram size is capped by [`RECV_BUF_BYTES`], which must be large
//! enough to hold a zerompk-encoded `SwimMessage` at the configured
//! `max_piggyback` budget. 64 KiB is the IPv4 UDP maximum and is
//! comfortably larger than any realistic SWIM payload.

use std::net::SocketAddr;
use std::sync::Arc;

use async_trait::async_trait;
use tokio::net::UdpSocket;
use tokio::sync::Mutex;

use super::Transport;
use crate::swim::error::SwimError;
use crate::swim::wire::{self, SwimMessage};

/// IPv4 UDP maximum datagram size. zerompk-encoded SWIM messages with a
/// 6-entry piggyback fit in well under 2 KiB, so 64 KiB is abundant.
pub const RECV_BUF_BYTES: usize = 65_536;

/// SWIM datagram transport backed by a real UDP socket.
#[derive(Debug)]
pub struct UdpTransport {
    socket: Arc<UdpSocket>,
    local_addr: SocketAddr,
    /// Serializes access to the recv buffer. `recv_from` takes `&self`
    /// on `UdpSocket`, but we need a reusable buffer without allocating
    /// 64 KiB per datagram; the mutex guards that buffer.
    recv_buf: Mutex<Vec<u8>>,
}

impl UdpTransport {
    /// Bind to `addr`. Passing `127.0.0.1:0` picks an ephemeral port,
    /// which [`UdpTransport::local_addr`] then reports.
    pub async fn bind(addr: SocketAddr) -> Result<Self, SwimError> {
        let socket = UdpSocket::bind(addr).await.map_err(|e| SwimError::Encode {
            detail: format!("udp bind {addr}: {e}"),
        })?;
        let local_addr = socket.local_addr().map_err(|e| SwimError::Encode {
            detail: format!("udp local_addr: {e}"),
        })?;
        Ok(Self {
            socket: Arc::new(socket),
            local_addr,
            recv_buf: Mutex::new(vec![0u8; RECV_BUF_BYTES]),
        })
    }
}

#[async_trait]
impl Transport for UdpTransport {
    async fn send(&self, to: SocketAddr, msg: SwimMessage) -> Result<(), SwimError> {
        let bytes = wire::encode(&msg)?;
        // UDP send_to is atomic per datagram; partial sends aren't a
        // thing, so we only have to handle the error case.
        self.socket
            .send_to(&bytes, to)
            .await
            .map(|_| ())
            .map_err(|e| SwimError::Encode {
                detail: format!("udp send_to {to}: {e}"),
            })
    }

    async fn recv(&self) -> Result<(SocketAddr, SwimMessage), SwimError> {
        let mut buf = self.recv_buf.lock().await;
        let (n, from) =
            self.socket
                .recv_from(&mut buf[..])
                .await
                .map_err(|e| SwimError::Decode {
                    detail: format!("udp recv_from: {e}"),
                })?;
        let msg = wire::decode(&buf[..n])?;
        Ok((from, msg))
    }

    fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::swim::incarnation::Incarnation;
    use crate::swim::wire::{Ack, Ping, ProbeId};
    use nodedb_types::NodeId;
    use std::net::{IpAddr, Ipv4Addr};
    use std::time::Duration;

    fn any_loopback() -> SocketAddr {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0)
    }

    #[tokio::test]
    async fn bind_and_report_local_addr() {
        let t = UdpTransport::bind(any_loopback()).await.expect("bind");
        assert_eq!(t.local_addr().ip(), IpAddr::V4(Ipv4Addr::LOCALHOST));
        assert_ne!(t.local_addr().port(), 0);
    }

    #[tokio::test]
    async fn send_and_recv_roundtrip_between_real_sockets() {
        let a = UdpTransport::bind(any_loopback()).await.expect("bind a");
        let b = UdpTransport::bind(any_loopback()).await.expect("bind b");
        let ping = SwimMessage::Ping(Ping {
            probe_id: ProbeId::new(42),
            from: NodeId::new("a"),
            incarnation: Incarnation::new(3),
            piggyback: vec![],
        });
        a.send(b.local_addr(), ping.clone()).await.expect("send");
        let (from, msg) = tokio::time::timeout(Duration::from_secs(1), b.recv())
            .await
            .expect("recv timed out")
            .expect("recv ok");
        assert_eq!(from, a.local_addr());
        assert_eq!(msg, ping);
    }

    #[tokio::test]
    async fn recv_decodes_ack_variant() {
        let a = UdpTransport::bind(any_loopback()).await.expect("bind a");
        let b = UdpTransport::bind(any_loopback()).await.expect("bind b");
        let ack = SwimMessage::Ack(Ack {
            probe_id: ProbeId::new(1),
            from: NodeId::new("b"),
            incarnation: Incarnation::new(7),
            piggyback: vec![],
        });
        a.send(b.local_addr(), ack.clone()).await.expect("send");
        let (_from, msg) = tokio::time::timeout(Duration::from_secs(1), b.recv())
            .await
            .expect("recv timed out")
            .expect("recv ok");
        assert_eq!(msg, ack);
    }

    #[tokio::test]
    async fn decode_error_on_garbage_datagram() {
        // Bind one socket, send raw garbage to it from a second
        // loopback socket, then call `recv` through the transport
        // wrapper. The malformed bytes must surface as SwimError::Decode
        // rather than close the socket.
        let victim = UdpTransport::bind(any_loopback()).await.expect("bind");
        let sender = tokio::net::UdpSocket::bind(any_loopback())
            .await
            .expect("sender bind");
        sender
            .send_to(&[0xff_u8; 8], victim.local_addr())
            .await
            .expect("send garbage");
        let err = tokio::time::timeout(Duration::from_secs(1), victim.recv())
            .await
            .expect("recv timed out")
            .expect_err("decode should fail");
        assert!(matches!(err, SwimError::Decode { .. }));

        // Follow up with a valid datagram to prove the socket still
        // works after a bad one.
        let sender_transport = UdpTransport::bind(any_loopback()).await.expect("bind");
        let ping = SwimMessage::Ping(Ping {
            probe_id: ProbeId::new(1),
            from: NodeId::new("x"),
            incarnation: Incarnation::ZERO,
            piggyback: vec![],
        });
        sender_transport
            .send(victim.local_addr(), ping.clone())
            .await
            .expect("send valid");
        let (_from, msg) = tokio::time::timeout(Duration::from_secs(1), victim.recv())
            .await
            .expect("recv timed out")
            .expect("recv ok");
        assert_eq!(msg, ping);
    }
}
