// SPDX-License-Identifier: BUSL-1.1

//! Owed sequencer entries: what the scheduler proposes, and how it tells
//! that an entry is applied.
//!
//! A proposal can be lost: a node that does not lead the sequencer group
//! refuses it, and a leader change can drop an appended entry. A lost vote
//! leaves every participant waiting for a verdict forever. So the scheduler
//! keeps each proposal in [`OwedEntries`] and proposes it again on the stall
//! tick until this node's completion registry shows its effect.

use std::collections::BTreeMap;

use nodedb_cluster::calvin::{AbortReason, ParticipantProgress, SequencerEntry};

use crate::control::cluster::calvin::scheduler::lock_manager::TxnId;
use crate::control::cluster::calvin::scheduler::metrics::sequencer_propose_kind;

/// A sequencer entry the scheduler proposes for one txn on its vShard.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SchedulerProposal {
    /// This vShard's commit vote. `Some(reason)` is an abort vote.
    Vote { abort: Option<AbortReason> },
    /// This vShard applied or dropped the txn.
    CompletionAck,
    /// The active executor saw its OLLP prediction drift.
    OllpMismatch,
    /// The txn's local plan routing failed for good.
    RoutingFailed { detail: String },
}

impl SchedulerProposal {
    /// The owed-entry kind this proposal fills.
    pub fn kind(&self) -> OwedKind {
        match self {
            Self::Vote { .. } => OwedKind::Vote,
            Self::CompletionAck => OwedKind::CompletionAck,
            Self::OllpMismatch => OwedKind::OllpMismatch,
            Self::RoutingFailed { .. } => OwedKind::RoutingFailed,
        }
    }

    /// The sequencer entry for `txn_id` proposed by `vshard`.
    pub fn entry(&self, txn_id: TxnId, vshard: u32) -> SequencerEntry {
        let (epoch, position) = (txn_id.epoch, txn_id.position);
        match self {
            Self::Vote { abort: None } => SequencerEntry::Vote {
                epoch,
                position,
                vshard,
                commit: true,
            },
            Self::Vote {
                abort: Some(reason),
            } => SequencerEntry::AbortVote {
                epoch,
                position,
                vshard,
                reason: *reason,
            },
            Self::CompletionAck => SequencerEntry::CompletionAck {
                epoch,
                position,
                vshard_id: vshard,
            },
            Self::OllpMismatch => SequencerEntry::OllpMismatch { epoch, position },
            Self::RoutingFailed { detail } => SequencerEntry::TxnRoutingFailed {
                epoch,
                position,
                detail: detail.clone(),
            },
        }
    }
}

/// The kind of an owed entry. A txn owes at most one entry of each kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum OwedKind {
    Vote,
    CompletionAck,
    OllpMismatch,
    RoutingFailed,
}

impl OwedKind {
    /// Short name for logs.
    pub fn label(self) -> &'static str {
        sequencer_propose_kind::LABELS[self.metric_index()]
    }

    /// Index into the `sequencer_propose_kind` metric labels.
    pub fn metric_index(self) -> usize {
        match self {
            Self::Vote => sequencer_propose_kind::VOTE,
            Self::CompletionAck => sequencer_propose_kind::COMPLETION_ACK,
            Self::OllpMismatch => sequencer_propose_kind::OLLP_MISMATCH,
            Self::RoutingFailed => sequencer_propose_kind::ROUTING_FAILED,
        }
    }

    /// Whether this node applied the entry of this kind.
    ///
    /// `progress` is the registry's view of the txn for the scheduler's
    /// vShard. `entry_seen` is whether the registry held an entry for the
    /// txn at an earlier check. The registry removes an entry only once the
    /// txn's outcome fired, and a txn with a fired outcome needs no entry of
    /// any kind. A missing entry that was never seen means this node has not
    /// seeded the txn, so the entry is still owed.
    ///
    /// A stored verdict also settles a vote: the verdict forms only once
    /// every participant's vote is in the tally.
    pub fn is_applied(self, progress: Option<ParticipantProgress>, entry_seen: bool) -> bool {
        let Some(p) = progress else {
            return entry_seen;
        };
        match self {
            Self::Vote => p.voted || p.has_verdict,
            Self::CompletionAck => p.acked,
            Self::OllpMismatch => p.mismatched,
            Self::RoutingFailed => p.routing_failed,
        }
    }
}

/// One owed sequencer entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OwedEntry {
    /// The msgpack-encoded `SequencerEntry`.
    pub bytes: Vec<u8>,
    /// The registry held an entry for the txn at some check.
    pub entry_seen: bool,
    /// The last proposal left this node after the previous sweep. The next
    /// sweep skips the entry once, which gives it one tick to apply.
    pub in_flight: bool,
}

/// Owed entries keyed by txn and kind, in deterministic order.
///
/// Bounded: it holds at most one entry per (txn, kind), and a txn owes at
/// most two kinds (a vote and a completion ack, or a single terminal
/// signal). An entry leaves once this node applies it. Entries pile up only
/// while no sequencer leader is reachable, and then the sequencer admits no
/// new txns either.
pub type OwedEntries = BTreeMap<(TxnId, OwedKind), OwedEntry>;

#[cfg(test)]
mod tests {
    use super::*;

    fn progress() -> ParticipantProgress {
        ParticipantProgress {
            voted: false,
            acked: false,
            has_verdict: false,
            mismatched: false,
            routing_failed: false,
        }
    }

    const ALL_KINDS: [OwedKind; 4] = [
        OwedKind::Vote,
        OwedKind::CompletionAck,
        OwedKind::OllpMismatch,
        OwedKind::RoutingFailed,
    ];

    #[test]
    fn nothing_is_applied_while_the_registry_shows_no_effect() {
        for kind in ALL_KINDS {
            assert!(!kind.is_applied(Some(progress()), true), "{kind:?}");
        }
    }

    #[test]
    fn each_kind_is_applied_once_its_own_effect_shows() {
        let voted = ParticipantProgress {
            voted: true,
            ..progress()
        };
        let acked = ParticipantProgress {
            acked: true,
            ..progress()
        };
        let mismatched = ParticipantProgress {
            mismatched: true,
            ..progress()
        };
        let routing_failed = ParticipantProgress {
            routing_failed: true,
            ..progress()
        };
        assert!(OwedKind::Vote.is_applied(Some(voted), false));
        assert!(!OwedKind::CompletionAck.is_applied(Some(voted), false));
        assert!(OwedKind::CompletionAck.is_applied(Some(acked), false));
        assert!(!OwedKind::Vote.is_applied(Some(acked), false));
        assert!(OwedKind::OllpMismatch.is_applied(Some(mismatched), false));
        assert!(OwedKind::RoutingFailed.is_applied(Some(routing_failed), false));
    }

    #[test]
    fn a_stored_verdict_settles_the_vote_only() {
        let decided = ParticipantProgress {
            has_verdict: true,
            ..progress()
        };
        assert!(OwedKind::Vote.is_applied(Some(decided), false));
        assert!(!OwedKind::CompletionAck.is_applied(Some(decided), false));
    }

    #[test]
    fn a_removed_entry_settles_every_kind_only_after_it_was_seen() {
        for kind in ALL_KINDS {
            assert!(kind.is_applied(None, true), "{kind:?}");
            assert!(!kind.is_applied(None, false), "{kind:?}");
        }
    }

    #[test]
    fn a_vote_proposal_carries_its_abort_reason() {
        let txn = TxnId::new(3, 4);
        assert!(matches!(
            SchedulerProposal::Vote { abort: None }.entry(txn, 9),
            SequencerEntry::Vote {
                epoch: 3,
                position: 4,
                vshard: 9,
                commit: true
            }
        ));
        assert!(matches!(
            SchedulerProposal::Vote {
                abort: Some(AbortReason::SerializationConflict)
            }
            .entry(txn, 9),
            SequencerEntry::AbortVote {
                epoch: 3,
                position: 4,
                vshard: 9,
                reason: AbortReason::SerializationConflict
            }
        ));
    }
}
