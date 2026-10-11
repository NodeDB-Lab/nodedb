// SPDX-License-Identifier: BUSL-1.1

//! One transaction's completion entry in the
//! [`super::completion::CalvinCompletionRegistry`].

use std::collections::{BTreeMap, BTreeSet};
use std::time::Instant;

use super::completion::{ParticipantVote, VerdictOutcome};
use super::completion_waiter::CompletionWaiter;

pub(crate) struct PendingCompletion {
    pub(crate) expected_participants: usize,
    pub(crate) acked_vshards: BTreeSet<u32>,
    /// The apply result each acked participant's `CompletionAck` carried,
    /// by vShard. A participant whose ack carried none has no entry.
    pub(crate) ack_results: BTreeMap<u32, Vec<u8>>,
    pub(crate) completion_tx: Option<CompletionWaiter>,
    /// Set when the multi-part transaction lost its parts
    /// (`SequencerEntry::TxnPartsAbandoned`). Its outcome is
    /// `Aborted { PartsLost }` without waiting for acks: no participant
    /// staged the whole transaction.
    pub(crate) abandoned: bool,
    /// Durable per-participant commit votes tallied from `SequencerEntry::Vote`
    /// and `SequencerEntry::AbortVote`, keyed by vshard so a re-proposed vote
    /// (retry) overwrites deterministically. Once complete, the leader
    /// aggregates the tally into the global `verdict` that gates each
    /// participant's flush/drop at the cross-shard commit barrier.
    pub(crate) votes: BTreeMap<u32, ParticipantVote>,
    /// The authoritative commit/abort verdict, applied from a replicated
    /// `SequencerEntry::Verdict` or `AbortVerdict`; `None` until applied. This
    /// is the durable barrier gate: a participant parked in `AwaitingVerdict`
    /// resumes into its flush (commit) or drop (abort) once it is set. It is
    /// also the ONLY authority for "already decided" — a post-failover leader
    /// re-proposes from `votes` until it is stored (see
    /// `drain_unproposed_verdicts`).
    pub(crate) verdict: Option<VerdictOutcome>,
    /// Dedup guard for the LOCAL emit path: set the first time the tally becomes
    /// complete so the verdict signal is emitted exactly once across vote
    /// re-proposals. Per-node, non-durable, never reset on failover — so it is
    /// NOT consulted by `drain_unproposed_verdicts` (which trusts only the
    /// durable `verdict`, letting a promoted leader re-propose a still-unstored one).
    pub(crate) verdict_proposed: bool,
    /// Queued for eviction as a terminal entry with no waiter
    /// (`completion_gc`).
    pub(crate) parked: bool,
    /// Set when this replica applied the `EpochBatch` that sequenced the
    /// txn (`seed_expected`). An entry without it is an orphan: the
    /// waiterless sweep evicts it once it outlives the window with no live
    /// waiter (`completion_gc`).
    pub(crate) sequenced: bool,
    /// When this entry was created.
    pub(crate) created: Instant,
}

impl PendingCompletion {
    pub(crate) fn new(created: Instant) -> Self {
        Self {
            expected_participants: 0,
            acked_vshards: BTreeSet::new(),
            ack_results: BTreeMap::new(),
            completion_tx: None,
            abandoned: false,
            votes: BTreeMap::new(),
            verdict: None,
            verdict_proposed: false,
            parked: false,
            sequenced: false,
            created,
        }
    }

    pub(crate) fn has_waiter(&self) -> bool {
        self.completion_tx.is_some()
    }

    /// Whether a waiter is registered and its receiver still listens.
    pub(crate) fn has_live_waiter(&self) -> bool {
        self.completion_tx
            .as_ref()
            .is_some_and(CompletionWaiter::is_live)
    }

    /// Whether the entry's outcome is decided: the verdict is stored and
    /// every expected participant acked, or the transaction lost its parts.
    ///
    /// Acks without a verdict are never terminal. Every participant acks
    /// only after it read the verdict.
    pub(crate) fn is_terminal(&self) -> bool {
        self.abandoned || (self.verdict.is_some() && self.is_complete())
    }

    /// Every acked participant's apply result, in vShard order.
    pub(crate) fn take_ack_results(&mut self) -> Vec<Vec<u8>> {
        std::mem::take(&mut self.ack_results)
            .into_values()
            .collect()
    }

    pub(crate) fn is_complete(&self) -> bool {
        // Require a KNOWN participant count (>0). The `expected_participants == 0`
        // default means "not yet seeded" — completion must not fire until the
        // count is known (via `note_assigned` on the leader, or `register_completion`
        // from the routed assignment on a remote coordinator). Without this guard a
        // replicated ack that races ahead of seeding, or a bare `register_completion`,
        // would spuriously report `Completed` with zero acks.
        self.expected_participants > 0 && self.acked_vshards.len() >= self.expected_participants
    }
}
