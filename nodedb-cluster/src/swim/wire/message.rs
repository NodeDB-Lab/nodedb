//! Top-level SWIM datagram enum.
//!
//! `SwimMessage` is the single type every transport sends and receives.
//! zerompk encodes it as a length-2 MessagePack array `[VariantName,
//! payload]`, where `VariantName` is the Rust variant identifier
//! verbatim (`Ping`, `PingReq`, `Ack`, `Nack`). The variant name strings
//! are part of the wire contract — renaming them breaks compatibility.

use serde::{Deserialize, Serialize};

use super::probe::{Ack, Nack, Ping, PingReq};
use crate::swim::member::record::MemberUpdate;

/// The four datagram types SWIM exchanges over the wire.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub enum SwimMessage {
    Ping(Ping),
    PingReq(PingReq),
    Ack(Ack),
    Nack(Nack),
}

impl SwimMessage {
    /// Mutable borrow of the piggyback slot, independent of variant.
    /// Used by the dissemination queue (E-δ) to stamp outgoing deltas
    /// without caring which message type it is stamping onto.
    pub fn piggyback_mut(&mut self) -> &mut Vec<MemberUpdate> {
        match self {
            SwimMessage::Ping(m) => &mut m.piggyback,
            SwimMessage::PingReq(m) => &mut m.piggyback,
            SwimMessage::Ack(m) => &mut m.piggyback,
            SwimMessage::Nack(m) => &mut m.piggyback,
        }
    }

    /// Read-only borrow of the piggyback slot.
    pub fn piggyback(&self) -> &[MemberUpdate] {
        match self {
            SwimMessage::Ping(m) => &m.piggyback,
            SwimMessage::PingReq(m) => &m.piggyback,
            SwimMessage::Ack(m) => &m.piggyback,
            SwimMessage::Nack(m) => &m.piggyback,
        }
    }

    /// Drop piggyback entries beyond `max`. Used before encoding to keep
    /// a datagram below the UDP MTU — the dissemination queue (E-δ) will
    /// decide which updates are highest-priority; this helper just
    /// enforces the upper bound.
    pub fn truncate_piggyback(&mut self, max: usize) {
        let slot = self.piggyback_mut();
        if slot.len() > max {
            slot.truncate(max);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::probe::{NackReason, ProbeId};
    use super::*;
    use crate::swim::incarnation::Incarnation;
    use crate::swim::member::MemberState;
    use nodedb_types::NodeId;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    fn mk_update(id: &str) -> MemberUpdate {
        MemberUpdate {
            node_id: NodeId::new(id),
            addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 7000).to_string(),
            state: MemberState::Alive,
            incarnation: Incarnation::ZERO,
        }
    }

    fn ping_with_piggyback(n: usize) -> SwimMessage {
        SwimMessage::Ping(Ping {
            probe_id: ProbeId::new(1),
            from: NodeId::new("a"),
            incarnation: Incarnation::new(2),
            piggyback: (0..n).map(|i| mk_update(&format!("n{i}"))).collect(),
        })
    }

    #[test]
    fn piggyback_accessor_returns_variant_slot() {
        let msg = ping_with_piggyback(3);
        assert_eq!(msg.piggyback().len(), 3);
    }

    #[test]
    fn truncate_bounds_piggyback() {
        let mut msg = ping_with_piggyback(10);
        msg.truncate_piggyback(4);
        assert_eq!(msg.piggyback().len(), 4);
    }

    #[test]
    fn truncate_is_noop_when_under_limit() {
        let mut msg = ping_with_piggyback(2);
        msg.truncate_piggyback(16);
        assert_eq!(msg.piggyback().len(), 2);
    }

    #[test]
    fn piggyback_mut_accessor_for_every_variant() {
        let mut variants: Vec<SwimMessage> = vec![
            ping_with_piggyback(0),
            SwimMessage::PingReq(PingReq {
                probe_id: ProbeId::ZERO,
                from: NodeId::new("a"),
                target: NodeId::new("b"),
                target_addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 7001).to_string(),
                piggyback: vec![],
            }),
            SwimMessage::Ack(Ack {
                probe_id: ProbeId::ZERO,
                from: NodeId::new("b"),
                incarnation: Incarnation::ZERO,
                piggyback: vec![],
            }),
            SwimMessage::Nack(Nack {
                probe_id: ProbeId::ZERO,
                from: NodeId::new("c"),
                reason: NackReason::TargetUnreachable,
                piggyback: vec![],
            }),
        ];
        for m in &mut variants {
            m.piggyback_mut().push(mk_update("extra"));
            assert_eq!(m.piggyback().len(), 1);
        }
    }
}
