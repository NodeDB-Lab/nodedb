// SPDX-License-Identifier: Apache-2.0

//! `AckStatus` — outcome of an idempotency check on the server side.

use serde::{Deserialize, Serialize};

/// Server-side outcome for an acknowledged ingest message.
///
/// Receivers can match on this to surface duplicate-detection and
/// gap-detection UX without special-casing the `applied_seq` value.
#[derive(
    Debug,
    Clone,
    Default,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub enum AckStatus {
    /// The message was applied for the first time.
    #[default]
    Applied,
    /// The message was a duplicate and was ignored (idempotent replay).
    Duplicate,
    /// The producer's epoch is older than the server's recorded epoch;
    /// the message was fenced to prevent stale-epoch writes.
    Fenced,
    /// A gap in the sequence was detected; `expected` is the next seq
    /// the server expected from this producer.
    Gap { expected: u64 },
    /// The message passed envelope validation and was admitted for
    /// processing, but has NOT been applied yet.
    ///
    /// Emitted by the session boundary before the durable apply runs. A sender
    /// must keep the message queued until a terminal status arrives — treating
    /// this as success would retire a write that may still be refused.
    Accepted,
    /// The message will never apply, and `reason` says why.
    ///
    /// Terminal, and the only status that is: a sender must compensate rather
    /// than re-send. It lives in this enum, rather than beside it as a separate
    /// `accepted` flag, so that a receiver matching on the status cannot read a
    /// refusal as an apply. The engine ack messages that also carry `accepted`
    /// and `reject_reason` derive both from this variant, so the two can never
    /// disagree.
    ///
    /// Contrast [`Self::Gap`], which is a *retryable* refusal: nothing applied,
    /// but the identical message at the same sequence is expected to succeed.
    Rejected { reason: String },
}
