//! The four-valued SWIM member state machine.
//!
//! SWIM (with the Lifeguard refinement) tracks four distinct states per
//! peer, listed below in precedence order. When two updates with the same
//! incarnation disagree, the one with the higher-precedence state wins.
//!
//! | State     | Precedence | Meaning                                            |
//! |-----------|-----------:|----------------------------------------------------|
//! | `Alive`   | 0          | Peer responded to the most recent probe round.     |
//! | `Suspect` | 1          | Peer missed its direct + indirect probes; under a suspicion timer. |
//! | `Dead`    | 2          | Suspicion timer elapsed without a refutation; peer is confirmed failed. |
//! | `Left`    | 3          | Peer sent an explicit graceful-leave message.       |
//!
//! `Left` is the terminal state: once observed it cannot be reverted by
//! any subsequent rumour, regardless of incarnation. Every other transition
//! is legal as long as the incoming `(incarnation, state)` lexicographically
//! dominates the stored pair. See `swim::membership::merge` for the merge
//! rule; this file only defines the state enum and its precedence.

use serde::{Deserialize, Serialize};

/// Discrete SWIM member states.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub enum MemberState {
    /// Responding to probes.
    Alive,
    /// Missed probes; on a suspicion timer.
    Suspect,
    /// Confirmed failed.
    Dead,
    /// Gracefully left the cluster.
    Left,
}

impl MemberState {
    /// Precedence rank for the state. Higher values beat lower values when
    /// the incarnations of two competing updates are equal.
    pub const fn precedence(self) -> u8 {
        match self {
            MemberState::Alive => 0,
            MemberState::Suspect => 1,
            MemberState::Dead => 2,
            MemberState::Left => 3,
        }
    }

    /// `true` if the peer is currently considered reachable (routable) by
    /// the rest of the system. Only `Alive` counts.
    pub const fn is_reachable(self) -> bool {
        matches!(self, MemberState::Alive)
    }

    /// `true` if the peer has reached a terminal state from which it cannot
    /// recover within the current incarnation. `Left` is the only terminal
    /// state — `Dead` members may still be resurrected if the same node
    /// rejoins with a strictly higher incarnation.
    pub const fn is_terminal(self) -> bool {
        matches!(self, MemberState::Left)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn precedence_is_total_and_strict() {
        assert!(MemberState::Alive.precedence() < MemberState::Suspect.precedence());
        assert!(MemberState::Suspect.precedence() < MemberState::Dead.precedence());
        assert!(MemberState::Dead.precedence() < MemberState::Left.precedence());
    }

    #[test]
    fn only_alive_is_reachable() {
        assert!(MemberState::Alive.is_reachable());
        assert!(!MemberState::Suspect.is_reachable());
        assert!(!MemberState::Dead.is_reachable());
        assert!(!MemberState::Left.is_reachable());
    }

    #[test]
    fn only_left_is_terminal() {
        assert!(!MemberState::Alive.is_terminal());
        assert!(!MemberState::Suspect.is_terminal());
        assert!(!MemberState::Dead.is_terminal());
        assert!(MemberState::Left.is_terminal());
    }

    #[test]
    fn exhaustive_match_reminder() {
        // Compile-time guard: adding a new variant must break this match so
        // every call site (precedence, is_reachable, is_terminal, merge) is
        // updated in lockstep.
        fn _check(s: MemberState) {
            match s {
                MemberState::Alive
                | MemberState::Suspect
                | MemberState::Dead
                | MemberState::Left => {}
            }
        }
    }
}
