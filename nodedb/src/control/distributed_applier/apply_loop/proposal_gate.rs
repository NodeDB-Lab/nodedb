// SPDX-License-Identifier: BUSL-1.1

//! Per-entry proposal identity gate: skip a second committed copy of a
//! proposal, and record every durably applied proposal in the ledger. See
//! [`crate::control::distributed_applier::proposal_ledger`].

use crate::control::distributed_applier::applied_index::AppliedPrefix;
use crate::control::distributed_applier::proposal_ledger::{
    AppliedOutcome, PriorApply, ProposalLedger,
};
use crate::control::distributed_applier::propose_tracker::{AppliedWrite, ProposeTracker};

/// What applying one committed entry produced.
pub(super) enum EntryOutcome {
    /// The entry carries no durable state and no proposal to record: it
    /// neither advances nor breaks the applied prefix.
    Skipped,
    /// The entry was applied. `durable` says its outcome survives a restart.
    /// `result` is what its waiter received, when the apply produced one.
    Applied {
        durable: bool,
        result: Option<AppliedOutcome>,
    },
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

/// Per-batch state of the proposal gate.
pub(super) struct ProposalGate<'a> {
    pub ledger: &'a mut ProposalLedger,
    pub group_id: u64,
}

impl ProposalGate<'_> {
    /// When `proposal_key` already applied on this node, resolve the entry's
    /// waiter with the first copy's outcome, extend the prefix, and return
    /// `true`: the caller skips the entry. The first copy's outcome is
    /// durable: the record that carries its key was durable before the floor
    /// passed it, or it applied in this process.
    pub fn skip_duplicate(
        &self,
        tracker: &ProposeTracker,
        prefix: &mut AppliedPrefix,
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
            group_id = self.group_id,
            log_index,
            proposal_key,
            "skipping a second committed copy of an applied proposal"
        );
        tracker.complete(self.group_id, log_index, proposal_key, result);
        prefix.record(log_index, true);
        true
    }

    /// Record `outcome` for the entry at `log_index`: extend or break the
    /// prefix, and note a durable apply of a keyed proposal in the ledger.
    ///
    /// No separate marker is written: the entry's own records carry its key,
    /// durable in the same write as its effect.
    pub fn settle(
        &mut self,
        prefix: &mut AppliedPrefix,
        log_index: u64,
        proposal_key: u64,
        outcome: EntryOutcome,
    ) {
        let (durable, result) = match outcome {
            EntryOutcome::Skipped => {
                prefix.skip();
                return;
            }
            EntryOutcome::Applied { durable, result } => (durable, result),
        };
        if durable {
            self.ledger.note(proposal_key, result);
        }
        prefix.record(log_index, durable);
    }
}
