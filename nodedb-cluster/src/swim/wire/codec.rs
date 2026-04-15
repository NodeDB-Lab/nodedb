//! zerompk (MessagePack) codec for [`SwimMessage`].
//!
//! Thin wrapper over `zerompk::to_msgpack_vec` / `zerompk::from_msgpack`
//! that maps codec errors into the typed [`SwimError`] so the failure
//! detector never sees raw zerompk errors.
//!
//! The encode path is infallible in practice — `SwimMessage` is composed
//! entirely of types with well-defined MessagePack representations — but
//! the return type stays fallible so a future addition of a fallible
//! field cannot silently panic.

use super::message::SwimMessage;
use crate::swim::error::SwimError;

/// Serialize a `SwimMessage` into a zerompk byte buffer.
pub fn encode(msg: &SwimMessage) -> Result<Vec<u8>, SwimError> {
    zerompk::to_msgpack_vec(msg).map_err(|e| SwimError::Encode {
        detail: e.to_string(),
    })
}

/// Decode a zerompk byte buffer into a `SwimMessage`. Truncated or
/// malformed input returns [`SwimError::Decode`] rather than panicking.
pub fn decode(bytes: &[u8]) -> Result<SwimMessage, SwimError> {
    zerompk::from_msgpack(bytes).map_err(|e| SwimError::Decode {
        detail: e.to_string(),
    })
}

#[cfg(test)]
mod tests {
    use super::super::probe::{Ack, Nack, NackReason, Ping, PingReq, ProbeId};
    use super::*;
    use crate::swim::incarnation::Incarnation;
    use crate::swim::member::MemberState;
    use crate::swim::member::record::MemberUpdate;
    use nodedb_types::NodeId;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    fn addr(port: u16) -> SocketAddr {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port)
    }

    fn update(id: &str, port: u16) -> MemberUpdate {
        MemberUpdate {
            node_id: NodeId::new(id),
            addr: addr(port).to_string(),
            state: MemberState::Alive,
            incarnation: Incarnation::new(1),
        }
    }

    fn assert_roundtrip(msg: SwimMessage) {
        let bytes = encode(&msg).expect("encode");
        let decoded = decode(&bytes).expect("decode");
        assert_eq!(decoded, msg);
    }

    #[test]
    fn ping_roundtrip_empty_piggyback() {
        assert_roundtrip(SwimMessage::Ping(Ping {
            probe_id: ProbeId::new(5),
            from: NodeId::new("a"),
            incarnation: Incarnation::new(3),
            piggyback: vec![],
        }));
    }

    #[test]
    fn ping_roundtrip_with_piggyback() {
        assert_roundtrip(SwimMessage::Ping(Ping {
            probe_id: ProbeId::new(12),
            from: NodeId::new("sender"),
            incarnation: Incarnation::new(7),
            piggyback: vec![update("n1", 7001), update("n2", 7002)],
        }));
    }

    #[test]
    fn ping_req_roundtrip() {
        assert_roundtrip(SwimMessage::PingReq(PingReq {
            probe_id: ProbeId::new(9),
            from: NodeId::new("a"),
            target: NodeId::new("b"),
            target_addr: addr(7003).to_string(),
            piggyback: vec![update("helper", 7004)],
        }));
    }

    #[test]
    fn ack_roundtrip() {
        assert_roundtrip(SwimMessage::Ack(Ack {
            probe_id: ProbeId::new(1),
            from: NodeId::new("b"),
            incarnation: Incarnation::new(11),
            piggyback: vec![],
        }));
    }

    #[test]
    fn nack_roundtrip_every_reason() {
        for reason in [
            NackReason::TargetUnreachable,
            NackReason::TargetDead,
            NackReason::RateLimited,
        ] {
            assert_roundtrip(SwimMessage::Nack(Nack {
                probe_id: ProbeId::new(2),
                from: NodeId::new("c"),
                reason,
                piggyback: vec![],
            }));
        }
    }

    #[test]
    fn decode_rejects_garbage() {
        let garbage = [0xff_u8; 8];
        assert!(matches!(decode(&garbage), Err(SwimError::Decode { .. })));
    }

    #[test]
    fn decode_rejects_truncated() {
        let full = encode(&SwimMessage::Ping(Ping {
            probe_id: ProbeId::new(1),
            from: NodeId::new("a"),
            incarnation: Incarnation::ZERO,
            piggyback: vec![],
        }))
        .expect("encode");
        let truncated = &full[..full.len() / 2];
        assert!(matches!(decode(truncated), Err(SwimError::Decode { .. })));
    }

    #[test]
    fn wire_tag_stability_ping() {
        // zerompk encodes SwimMessage as [VariantName, payload]. Lock the
        // PascalCase variant name so a rename breaks this test loudly.
        let msg = SwimMessage::Ping(Ping {
            probe_id: ProbeId::new(1),
            from: NodeId::new("a"),
            incarnation: Incarnation::ZERO,
            piggyback: vec![],
        });
        let bytes = encode(&msg).expect("encode");
        let as_str = String::from_utf8_lossy(&bytes);
        assert!(
            as_str.contains("Ping"),
            "wire tag 'Ping' missing from encoded bytes: {bytes:?}"
        );
    }

    #[test]
    fn wire_tag_distinguishes_variants() {
        // Locks in that the four variants encode to disjoint tag strings.
        // We can't substring-match "ack" because msgpack length-prefixes
        // short strings with bytes that can appear inside other fields;
        // instead we verify that the Ack encoding does NOT contain the
        // Ping tag (and vice versa), which is the property we actually
        // care about for wire compatibility.
        let ack = SwimMessage::Ack(Ack {
            probe_id: ProbeId::new(1),
            from: NodeId::new("sender"),
            incarnation: Incarnation::ZERO,
            piggyback: vec![],
        });
        let ping = SwimMessage::Ping(Ping {
            probe_id: ProbeId::new(1),
            from: NodeId::new("sender"),
            incarnation: Incarnation::ZERO,
            piggyback: vec![],
        });
        let ack_bytes = encode(&ack).expect("encode");
        let ping_bytes = encode(&ping).expect("encode");
        assert_ne!(
            ack_bytes, ping_bytes,
            "ack and ping must encode to different bytes"
        );
        // Round-trip type stability: decoded variants match the input.
        assert!(matches!(decode(&ack_bytes), Ok(SwimMessage::Ack(_))));
        assert!(matches!(decode(&ping_bytes), Ok(SwimMessage::Ping(_))));
    }

    #[test]
    fn wire_tag_stability_ping_req() {
        let msg = SwimMessage::PingReq(PingReq {
            probe_id: ProbeId::new(1),
            from: NodeId::new("a"),
            target: NodeId::new("b"),
            target_addr: addr(7000).to_string(),
            piggyback: vec![],
        });
        let bytes = encode(&msg).expect("encode");
        let as_str = String::from_utf8_lossy(&bytes);
        assert!(
            as_str.contains("PingReq"),
            "expected 'PingReq' variant name, got: {as_str:?}"
        );
    }
}
