// SPDX-License-Identifier: Apache-2.0

//! The compiled row-level-security WRITE predicate carried on a write plan.
//!
//! A bare `Vec<u8>` could not separate "no policy restricts this write" from
//! "the predicate was lost in transit". Both read as empty, and the second
//! silently disables the policy. Each value here names which case it is.
//!
//! No `Default`: a write plan cannot carry an unexplained empty check.

use serde::{Deserialize, Serialize};

/// What the Data Plane gate must do with one write's policy check.
///
/// Exhaustive, so a caller cannot handle the admit cases and forget the deny.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteGateDecision<'a> {
    /// Admit every row without evaluating anything.
    AdmitAll,
    /// Evaluate these compiled predicate bytes against each row image.
    Evaluate(&'a [u8]),
    /// Deny. The plan reached the gate before RLS injection ran over it.
    /// This is a bug in the write path, not a policy outcome.
    DenyNotInjected,
}

/// The write-policy check attached to one write operation.
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
pub enum RlsWriteCheck {
    /// Compiled predicate bytes, decoded as `Vec<ScanFilter>` and evaluated
    /// against each row image.
    Predicate(Vec<u8>),

    /// Injection ran and found no write policy for this identity here.
    NoPolicyApplies,

    /// Follower apply, or WAL replay after a crash.
    ///
    /// The authoring identity is absent, so re-deciding could deny a write the
    /// leader already committed and diverge the replicas. Admits on purpose.
    ///
    /// Only follower-apply and replay code may build this. Never build it where
    /// a live writing identity exists.
    AlreadyDecidedElsewhere,

    /// This request already ran the policy over this exact row image, with a
    /// live identity. That live identity separates it from
    /// [`RlsWriteCheck::AlreadyDecidedElsewhere`].
    ///
    /// Only build this where the earlier decision provably covered the same
    /// image. If the op can touch a row that check did not see, it is not this.
    DecidedEarlierInRequest,

    /// A collection NodeDB maintains for itself, which no user creates and no
    /// policy attaches to — rate-limit bookkeeping, for example.
    ///
    /// These never run injection, so they cannot claim `NoPolicyApplies`.
    SystemInternalCollection,

    /// Built, but injection has not run yet. Transient, Control Plane only.
    ///
    /// If it reaches the gate, the write is DENIED — a missed injection fails
    /// loudly instead of quietly disabling the policy.
    PendingInjection,
}

impl RlsWriteCheck {
    /// Build the check from the policy compiler's output.
    ///
    /// Empty bytes become [`RlsWriteCheck::NoPolicyApplies`], never an empty
    /// predicate, so "no policy" cannot later read as "predicate lost".
    ///
    /// Call this only from the RLS injection pass.
    pub fn from_injected(predicate_bytes: Vec<u8>) -> Self {
        if predicate_bytes.is_empty() {
            RlsWriteCheck::NoPolicyApplies
        } else {
            RlsWriteCheck::Predicate(predicate_bytes)
        }
    }

    /// Bypass for follower apply or WAL replay.
    ///
    /// Never use it to silence a compile error where a live writing identity
    /// exists — [`RlsWriteCheck::pending_injection`] fails closed there.
    pub fn already_decided_elsewhere() -> Self {
        RlsWriteCheck::AlreadyDecidedElsewhere
    }

    /// Bypass for rows this same request already admitted. Only when the
    /// earlier check covered the same row image.
    pub fn decided_earlier_in_request() -> Self {
        RlsWriteCheck::DecidedEarlierInRequest
    }

    /// Bypass for a write to one of NodeDB's own internal collections.
    pub fn system_internal_collection() -> Self {
        RlsWriteCheck::SystemInternalCollection
    }

    /// Placeholder between plan construction and injection.
    ///
    /// Safe wherever the right value is not yet known: it fails closed, and the
    /// dispatch boundary rejects any write plan still holding it.
    pub fn pending_injection() -> Self {
        RlsWriteCheck::PendingInjection
    }

    /// What the Data Plane gate must do with this check.
    pub fn decision(&self) -> WriteGateDecision<'_> {
        match self {
            RlsWriteCheck::Predicate(bytes) => WriteGateDecision::Evaluate(bytes),
            RlsWriteCheck::NoPolicyApplies
            | RlsWriteCheck::AlreadyDecidedElsewhere
            | RlsWriteCheck::DecidedEarlierInRequest
            | RlsWriteCheck::SystemInternalCollection => WriteGateDecision::AdmitAll,
            RlsWriteCheck::PendingInjection => WriteGateDecision::DenyNotInjected,
        }
    }

    /// True only when a compiled predicate is attached — "does a policy
    /// restrict this identity here". Not a gate decision; see
    /// [`RlsWriteCheck::decision`].
    pub fn has_predicate(&self) -> bool {
        matches!(self, RlsWriteCheck::Predicate(_))
    }

    /// True while the plan has not been through injection. The dispatch
    /// boundary uses it to refuse un-injected writes.
    pub fn is_pending_injection(&self) -> bool {
        matches!(self, RlsWriteCheck::PendingInjection)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_compiler_output_is_no_policy_not_an_empty_predicate() {
        assert_eq!(
            RlsWriteCheck::from_injected(Vec::new()),
            RlsWriteCheck::NoPolicyApplies
        );
        assert!(!RlsWriteCheck::from_injected(Vec::new()).has_predicate());
    }

    #[test]
    fn compiled_bytes_become_a_predicate_the_gate_evaluates() {
        let check = RlsWriteCheck::from_injected(vec![1, 2, 3]);
        assert!(check.has_predicate());
        assert_eq!(check.decision(), WriteGateDecision::Evaluate(&[1, 2, 3]));
    }

    #[test]
    fn every_bypass_admits_every_row() {
        for check in [
            RlsWriteCheck::NoPolicyApplies,
            RlsWriteCheck::already_decided_elsewhere(),
            RlsWriteCheck::decided_earlier_in_request(),
            RlsWriteCheck::system_internal_collection(),
        ] {
            assert_eq!(
                check.decision(),
                WriteGateDecision::AdmitAll,
                "{check:?} must admit"
            );
            assert!(!check.has_predicate(), "{check:?} carries no predicate");
        }
    }

    /// The whole point of the type: a plan that never went through injection
    /// must be denied, not admitted the way a bare empty `Vec<u8>` would be.
    #[test]
    fn an_uninjected_plan_is_denied_rather_than_admitted() {
        let check = RlsWriteCheck::pending_injection();
        assert_eq!(check.decision(), WriteGateDecision::DenyNotInjected);
        assert!(check.is_pending_injection());
        assert!(!check.has_predicate());
    }
}
