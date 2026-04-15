//! SWIM probe message structs.
//!
//! These are the four datagram types the failure detector exchanges over
//! the network once E-ε wires in a transport. They are pure data types
//! with `serde` derives — no I/O, no validation beyond what the type
//! system enforces.
//!
//! ## Message flow (reference)
//!
//! ```text
//!            ┌──────── Ping ───────┐
//! sender A ──┤                     ├── target B
//!            └──── Ack / timeout ──┘
//!                       │
//!                     (timeout)
//!                       ▼
//!            ┌──── PingReq ────┐
//! sender A ──┤                 ├── helper C ──── Ping ───► target B
//!            └─── Ack / Nack ──┘                           │
//!                                   ◄─── Ack / timeout ────┘
//! ```
//!
//! Every message carries a bounded `piggyback: Vec<MemberUpdate>` slot
//! used for gossip-style dissemination of membership deltas (E-δ). The
//! wire format reserves the slot now so later sub-batches don't need a
//! compatibility break.

use nodedb_types::NodeId;
use serde::{Deserialize, Serialize};

use crate::swim::incarnation::Incarnation;
use crate::swim::member::record::MemberUpdate;

/// Monotonic per-sender probe identifier. Used to correlate `Ack`/`Nack`
/// with the originating `Ping`/`PingReq`.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    PartialOrd,
    Ord,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct ProbeId(u64);

impl ProbeId {
    /// The smallest probe id. The first probe a sender emits after boot.
    pub const ZERO: ProbeId = ProbeId(0);

    /// Construct from the raw `u64`. Public for tests and decode paths.
    pub const fn new(v: u64) -> Self {
        Self(v)
    }

    /// Raw value.
    pub const fn get(self) -> u64 {
        self.0
    }

    /// Advance by one, saturating at `u64::MAX`. A sender that issued
    /// 2^64 probes without restart would freeze at the max — SWIM does
    /// not reuse probe ids within a single incarnation.
    pub fn bump(self) -> Self {
        ProbeId(self.0.saturating_add(1))
    }
}

/// Why a helper returned `Nack` instead of a forwarded `Ack`.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub enum NackReason {
    /// Helper tried to contact the target and did not receive an ack
    /// within its own probe timeout.
    TargetUnreachable,
    /// Helper already considers the target `Dead` or `Left`.
    TargetDead,
    /// Helper refused to forward the probe due to rate limiting.
    RateLimited,
}

/// Direct probe. Sender A asks target B "are you alive?".
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
pub struct Ping {
    pub probe_id: ProbeId,
    pub from: NodeId,
    /// Sender's current incarnation. Receiver uses this for merge logic.
    pub incarnation: Incarnation,
    pub piggyback: Vec<MemberUpdate>,
}

/// Indirect probe. Sender A asks helper C to probe target B on A's
/// behalf after A's direct ping to B timed out.
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
pub struct PingReq {
    pub probe_id: ProbeId,
    pub from: NodeId,
    pub target: NodeId,
    /// Target's last-known socket address in string form (e.g.
    /// `"10.0.0.7:7000"`). Stored as `String` because `SocketAddr` has no
    /// zerompk impl; the helper parses before connecting.
    pub target_addr: String,
    pub piggyback: Vec<MemberUpdate>,
}

/// Positive response to a `Ping` or a helper-forwarded `PingReq`.
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
pub struct Ack {
    pub probe_id: ProbeId,
    pub from: NodeId,
    /// Responder's incarnation at the moment of ack. If the responder
    /// refuted a self-`Suspect` rumour during this probe round, the
    /// bumped incarnation is propagated here.
    pub incarnation: Incarnation,
    pub piggyback: Vec<MemberUpdate>,
}

/// Negative response from a helper that could not ack on behalf of the
/// original target.
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
pub struct Nack {
    pub probe_id: ProbeId,
    pub from: NodeId,
    pub reason: NackReason,
    pub piggyback: Vec<MemberUpdate>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn probe_id_bump_is_monotonic() {
        assert_eq!(ProbeId::ZERO.bump(), ProbeId::new(1));
        assert_eq!(ProbeId::new(42).bump(), ProbeId::new(43));
    }

    #[test]
    fn probe_id_saturates_at_u64_max() {
        let max = ProbeId::new(u64::MAX);
        assert_eq!(max.bump(), max);
    }

    #[test]
    fn probe_id_total_order() {
        assert!(ProbeId::new(1) < ProbeId::new(2));
        assert!(ProbeId::ZERO < ProbeId::new(1));
    }

    #[test]
    fn nack_reason_equality() {
        assert_eq!(NackReason::TargetDead, NackReason::TargetDead);
        assert_ne!(NackReason::TargetDead, NackReason::RateLimited);
    }
}
