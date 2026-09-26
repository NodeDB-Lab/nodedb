// SPDX-License-Identifier: BUSL-1.1

//! Per-entry proposal identity gate: skip a second committed copy of a
//! proposal, and record every durably applied proposal in the ledger. See
//! [`crate::control::distributed_applier::proposal_ledger`].
//!
//! Entries finish out of order, so the gate also knows which proposals are
//! in flight. A second copy of an in-flight proposal waits until the first
//! copy concludes, then the ledger decides it like any other copy.

use std::collections::HashMap;

use crate::control::distributed_applier::proposal_ledger::{
    AppliedOutcome, PriorApply, ProposalLedger,
};
use crate::control::distributed_applier::propose_tracker::{AppliedWrite, ProposeTracker};

/// What applying one committed entry produced.
pub(super) enum EntryOutcome {
    /// The entry carries no durable state and no proposal to record: it
    /// neither advances nor breaks the applied prefix.
    Skipped,
    /// A second committed copy of a proposal this node already applied. Its
    /// first copy's outcome is durable, so it extends the prefix. The ledger
    /// already holds the proposal.
    Repeat,
    /// The entry was applied. `durable` says its outcome survives a restart.
    /// `result` is what its waiter received, when the apply produced one.
    Applied {
        durable: bool,
        result: Option<AppliedOutcome>,
    },
}

impl EntryOutcome {
    /// Whether the apply wrote the entry's rows. A refused or failed apply
    /// wrote none, so it raises no tenant write mark. A second copy writes
    /// nothing itself: its mark follows its first copy (see
    /// [`ProposalGate::prior_wrote_rows`]).
    pub fn wrote_rows(&self) -> bool {
        match self {
            Self::Skipped | Self::Repeat => false,
            Self::Applied {
                result: Some(outcome),
                ..
            } => outcome.is_ok(),
            // An apply with no waiter result reports its success as `durable`.
            Self::Applied {
                durable,
                result: None,
            } => *durable,
        }
    }
}

/// How a concluded entry moves its group's durable applied prefix.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum PrefixStep {
    /// Neither advances nor breaks the prefix.
    Neutral,
    /// Extends the prefix when `true`, breaks it when `false`.
    Record(bool),
}

/// The outcome a waiter received, in the form the ledger keeps. A refusal is
/// kept as its typed code; an error with no code keeps its message.
pub(super) fn ledger_outcome(result: &crate::Result<AppliedWrite>) -> AppliedOutcome {
    match result {
        Ok(applied) => Ok(applied.clone()),
        Err(crate::Error::DataPlane(code)) => Err(code.clone()),
        Err(other) => Err(crate::bridge::envelope::ErrorCode::Internal {
            detail: other.to_string(),
        }),
    }
}

/// The proposal ledger, plus the proposals whose apply has not concluded.
pub(super) struct ProposalGate {
    ledger: ProposalLedger,
    /// Keyed proposals started and not concluded, with their copy counts.
    in_flight: HashMap<u64, u32>,
}

impl ProposalGate {
    pub fn new(ledger: ProposalLedger) -> Self {
        Self {
            ledger,
            in_flight: HashMap::new(),
        }
    }

    /// Whether a copy of `proposal_key` is started and not concluded. Key `0`
    /// names no proposal and is never in flight.
    pub fn in_flight(&self, proposal_key: u64) -> bool {
        proposal_key != 0 && self.in_flight.contains_key(&proposal_key)
    }

    /// Whether the first copy of `proposal_key` applied on this node and
    /// wrote its rows. A key recovered from the WAL keeps no outcome, so it
    /// counts as written: a mark too high only refuses a restore that FORCE
    /// can override, and a mark too low lets a restore overwrite the write.
    pub fn prior_wrote_rows(&self, proposal_key: u64) -> bool {
        match self.ledger.prior(proposal_key) {
            Some(PriorApply::Outcome(outcome)) => outcome.is_ok(),
            Some(PriorApply::NoOutcome) => true,
            None => false,
        }
    }

    /// When `proposal_key` already applied on this node, resolve the entry's
    /// waiter with the first copy's outcome and return `true`: the caller
    /// skips the entry. The first copy's outcome is durable: the record that
    /// carries its key was durable before the floor passed it, or it applied
    /// in this process.
    pub fn skip_duplicate(
        &self,
        tracker: &ProposeTracker,
        group_id: u64,
        log_index: u64,
        proposal_key: u64,
    ) -> bool {
        let Some(prior) = self.ledger.prior(proposal_key) else {
            return false;
        };
        let result = match prior {
            PriorApply::Outcome(Ok(applied)) => Ok(applied.clone()),
            PriorApply::Outcome(Err(code)) => Err(crate::Error::DataPlane(code.clone())),
            PriorApply::NoOutcome => Ok(AppliedWrite::unversioned(Vec::new())),
        };
        tracing::debug!(
            group_id,
            log_index,
            proposal_key,
            "skipping a second committed copy of an applied proposal"
        );
        tracker.complete(group_id, log_index, proposal_key, result);
        true
    }

    /// Note that a copy of `proposal_key` started and has not concluded.
    pub fn open(&mut self, proposal_key: u64) {
        if proposal_key != 0 {
            *self.in_flight.entry(proposal_key).or_insert(0) += 1;
        }
    }

    /// Conclude an entry of `proposal_key` with `outcome`: note a durable
    /// apply in the ledger, close the copy `open` noted when `opened`, and
    /// return how the entry moves its group's prefix.
    ///
    /// No separate marker is written: the entry's own records carry its key,
    /// durable in the same write as its effect.
    pub fn conclude(
        &mut self,
        proposal_key: u64,
        opened: bool,
        outcome: EntryOutcome,
    ) -> PrefixStep {
        if opened
            && proposal_key != 0
            && let Some(copies) = self.in_flight.get_mut(&proposal_key)
        {
            *copies -= 1;
            if *copies == 0 {
                self.in_flight.remove(&proposal_key);
            }
        }
        match outcome {
            EntryOutcome::Skipped => PrefixStep::Neutral,
            EntryOutcome::Repeat => PrefixStep::Record(true),
            EntryOutcome::Applied { durable, result } => {
                if durable {
                    self.ledger.note(proposal_key, result);
                }
                PrefixStep::Record(durable)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::distributed_applier::proposal_ledger::PROPOSAL_LEDGER_CAPACITY;

    #[test]
    fn a_copy_of_an_in_flight_proposal_is_decided_once_the_first_concludes() {
        let tracker = ProposeTracker::new();
        let mut gate = ProposalGate::new(ProposalLedger::new(PROPOSAL_LEDGER_CAPACITY));
        gate.open(42);
        assert!(gate.in_flight(42));
        assert!(!gate.skip_duplicate(&tracker, 1, 9, 42));

        let step = gate.conclude(
            42,
            true,
            EntryOutcome::Applied {
                durable: true,
                result: Some(Ok(AppliedWrite::unversioned(Vec::new()))),
            },
        );
        assert_eq!(step, PrefixStep::Record(true));
        assert!(!gate.in_flight(42));
        assert!(gate.skip_duplicate(&tracker, 1, 9, 42));
    }

    #[test]
    fn a_failed_first_copy_leaves_the_second_copy_to_apply() {
        let tracker = ProposeTracker::new();
        let mut gate = ProposalGate::new(ProposalLedger::new(PROPOSAL_LEDGER_CAPACITY));
        gate.open(7);
        let step = gate.conclude(
            7,
            true,
            EntryOutcome::Applied {
                durable: false,
                result: None,
            },
        );
        assert_eq!(step, PrefixStep::Record(false));
        assert!(!gate.in_flight(7));
        assert!(!gate.skip_duplicate(&tracker, 1, 9, 7));
    }
}
