//! A single membership entry — the (state, incarnation, addr) record the
//! failure detector keeps for every peer it has ever heard of, including
//! itself.

use std::net::SocketAddr;
use std::time::Instant;

use nodedb_types::NodeId;
use serde::{Deserialize, Serialize};

use super::super::incarnation::Incarnation;
use super::state::MemberState;

/// Per-node SWIM record.
///
/// `last_state_change` is a monotonic wall-clock instant captured whenever
/// the state or incarnation changes. It drives the suspicion timeout and
/// is deliberately not serialized — on the wire, only the durable triple
/// `(node_id, state, incarnation, addr)` is exchanged, and the receiver
/// stamps its own local instant on merge.
#[derive(Debug, Clone)]
pub struct Member {
    pub node_id: NodeId,
    pub addr: SocketAddr,
    pub state: MemberState,
    pub incarnation: Incarnation,
    pub last_state_change: Instant,
}

impl Member {
    /// Construct a freshly-learned `Alive` record at incarnation zero.
    pub fn new_alive(node_id: NodeId, addr: SocketAddr) -> Self {
        Self {
            node_id,
            addr,
            state: MemberState::Alive,
            incarnation: Incarnation::ZERO,
            last_state_change: Instant::now(),
        }
    }

    /// Durable triple used for rumour comparison: the pair
    /// `(incarnation, state.precedence())`. Lexicographic `Ord` on the
    /// resulting tuple implements the SWIM merge rule.
    pub fn rumour_key(&self) -> (Incarnation, u8) {
        (self.incarnation, self.state.precedence())
    }

    /// Shorthand for `self.state.is_reachable()`. Used by routing to
    /// compute the set of peers eligible for leader election, replication,
    /// and query dispatch.
    pub fn is_reachable(&self) -> bool {
        self.state.is_reachable()
    }
}

/// Serializable subset of a `Member` — everything except the monotonic
/// instant. Used as the wire payload for membership deltas.
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
pub struct MemberUpdate {
    pub node_id: NodeId,
    /// Socket address in string form (e.g. `"10.0.0.7:7000"`). Stored as a
    /// `String` on the wire because `std::net::SocketAddr` does not have a
    /// zerompk `ToMessagePack` impl. The receiver parses with
    /// [`MemberUpdate::parse_addr`].
    pub addr: String,
    pub state: MemberState,
    pub incarnation: Incarnation,
}

impl MemberUpdate {
    /// Parse [`Self::addr`] back into a `SocketAddr`. Returns `None` on
    /// malformed input — the caller treats an unparseable address as a
    /// bad rumour and drops it (never panics).
    pub fn parse_addr(&self) -> Option<SocketAddr> {
        self.addr.parse().ok()
    }
}

impl From<&Member> for MemberUpdate {
    fn from(m: &Member) -> Self {
        Self {
            node_id: m.node_id.clone(),
            addr: m.addr.to_string(),
            state: m.state,
            incarnation: m.incarnation,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr};

    fn addr() -> SocketAddr {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 7000)
    }

    #[test]
    fn new_alive_defaults() {
        let m = Member::new_alive(NodeId::new("n1"), addr());
        assert_eq!(m.state, MemberState::Alive);
        assert_eq!(m.incarnation, Incarnation::ZERO);
        assert!(m.is_reachable());
    }

    #[test]
    fn rumour_key_is_lex_order() {
        let older = (Incarnation::new(3), MemberState::Alive.precedence());
        let newer_inc = (Incarnation::new(4), MemberState::Alive.precedence());
        let same_inc_higher_state = (Incarnation::new(3), MemberState::Suspect.precedence());
        assert!(older < newer_inc);
        assert!(older < same_inc_higher_state);
        assert!(same_inc_higher_state < newer_inc);
    }

    #[test]
    fn update_roundtrip_via_from() {
        let m = Member::new_alive(NodeId::new("n7"), addr());
        let u = MemberUpdate::from(&m);
        assert_eq!(u.node_id, m.node_id);
        assert_eq!(u.addr, m.addr.to_string());
        assert_eq!(u.state, m.state);
        assert_eq!(u.incarnation, m.incarnation);
    }
}
