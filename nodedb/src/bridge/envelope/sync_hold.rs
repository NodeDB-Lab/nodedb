// SPDX-License-Identifier: BUSL-1.1

//! Why the sync idempotency gate held a frame back without applying it.

use nodedb_types::sync::wire::AckStatus;

/// A gate verdict that applies nothing and is not a refusal.
///
/// The sender acts on each one differently, so each crosses the bridge by
/// name. None of them asks the sender to compensate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncHold {
    /// The frame's sequence is at or below the stream's mark: it applied
    /// under an earlier delivery.
    Duplicate,
    /// The frame's producer epoch is below the producer's floor.
    Fenced,
    /// The frame skipped sequences. `expected` is the next one the stream
    /// admits.
    Gap { expected: u64 },
}

impl SyncHold {
    /// The ack status a sender receives for this hold.
    pub fn ack_status(self) -> AckStatus {
        match self {
            Self::Duplicate => AckStatus::Duplicate,
            Self::Fenced => AckStatus::Fenced,
            Self::Gap { expected } => AckStatus::Gap { expected },
        }
    }
}

impl std::fmt::Display for SyncHold {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Duplicate => write!(f, "duplicate"),
            Self::Fenced => write!(f, "fenced producer epoch"),
            Self::Gap { expected } => write!(f, "sequence gap, expected {expected}"),
        }
    }
}
