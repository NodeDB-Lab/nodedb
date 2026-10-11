// SPDX-License-Identifier: BUSL-1.1

//! The coordinator's side of a Calvin completion: what fires, and what it
//! carries.
//!
//! Every participant vShard proposes a `CompletionAck` to the sequencer log,
//! and every sequencer replica applies it, so a coordinator learns each
//! participant's apply result wherever that participant runs. A coordinator
//! that registers for a [`CompletionReport`] receives those results with the
//! outcome. A coordinator that registers for the outcome alone receives only
//! the [`AttemptOutcome`].

use tokio::sync::oneshot;

use super::completion::AttemptOutcome;

/// A completion as a coordinator that asked for its apply results receives
/// it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CompletionReport {
    pub outcome: AttemptOutcome,
    /// Each participant vShard's apply result, as its `CompletionAck`
    /// carried it, in vShard order. A participant whose ack carried none
    /// contributes none. Empty for every outcome but a completion.
    pub ack_results: Vec<Vec<u8>>,
}

/// The registered receiver of one transaction's terminal outcome.
pub(crate) enum CompletionWaiter {
    Outcome(oneshot::Sender<AttemptOutcome>),
    Report(oneshot::Sender<CompletionReport>),
}

impl CompletionWaiter {
    /// Whether the receiver still listens. A coordinator that gave up
    /// waiting dropped it.
    pub(crate) fn is_live(&self) -> bool {
        match self {
            Self::Outcome(tx) => !tx.is_closed(),
            Self::Report(tx) => !tx.is_closed(),
        }
    }

    /// Deliver `outcome` with `ack_results`. `false` when the receiver is
    /// gone.
    pub(crate) fn send(self, outcome: AttemptOutcome, ack_results: Vec<Vec<u8>>) -> bool {
        match self {
            Self::Outcome(tx) => tx.send(outcome).is_ok(),
            Self::Report(tx) => tx
                .send(CompletionReport {
                    outcome,
                    ack_results,
                })
                .is_ok(),
        }
    }
}
