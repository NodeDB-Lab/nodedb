// SPDX-License-Identifier: Apache-2.0

//! Pre-validation: fast-reject before Raft round-trip.
//!
//! The full validation path is: agent delta → Raft proposal → leader validates
//! → commit or reject. This round-trip is expensive (network + consensus).
//!
//! Pre-validation offers an optional fast-reject: the Control Plane checks
//! the delta against the leader's current state BEFORE submitting to Raft.
//! If the delta would obviously fail (e.g., UNIQUE violation on a value that
//! already exists), it's rejected immediately — saving the Raft round-trip.
//!
//! Pre-validation is best-effort: it may have a stale view of the leader state,
//! so deltas that pass pre-validation might still fail at commit time. But
//! deltas that fail pre-validation would definitely fail at commit time.

use crate::row_lookup::RowLookup;
use crate::validator::{ProposedChange, ValidationOutcome, Validator};

/// Result of pre-validation.
#[derive(Debug)]
pub enum PreValidationResult {
    /// The change looks valid against current state — proceed to Raft.
    Proceed,
    /// The change would definitely fail — reject immediately.
    FastReject { constraint: String, reason: String },
}

/// Pre-validate a proposed change against the leader's current state.
///
/// This is called on the Control Plane before submitting to Raft.
/// It uses the same validator but doesn't touch the DLQ — that's only
/// for actual commit-time rejections.
pub fn pre_validate(
    validator: &Validator,
    state: &impl RowLookup,
    change: &ProposedChange,
    crs: &nodedb_crs::CrsContext<'_>,
) -> PreValidationResult {
    match validator.validate(state, change, crs) {
        ValidationOutcome::Accepted => PreValidationResult::Proceed,
        ValidationOutcome::Rejected(violations) => {
            let v = &violations[0];
            PreValidationResult::FastReject {
                constraint: v.constraint_name.clone(),
                reason: v.reason.clone(),
            }
        }
        // An unevaluable predicate (division/modulo by zero)
        // is rejected before it ever reaches Raft — same terminal outcome as a
        // violation, so the delta never commits.
        ValidationOutcome::EvalError {
            constraint_name,
            error,
        } => PreValidationResult::FastReject {
            constraint: constraint_name,
            reason: format!("CHECK predicate failed to evaluate: {error}"),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::constraint::ConstraintSet;
    use crate::state::CrdtState;
    use loro::LoroValue;

    #[test]
    fn pre_validate_fast_rejects_not_null() {
        let state = CrdtState::new(1).unwrap();
        let mut cs = ConstraintSet::new();
        cs.add_not_null("name_nn", "users", "name");
        let validator = Validator::new(cs, 10);

        let change = ProposedChange {
            collection: "users".into(),
            row_id: "u1".into(),
            surrogate: nodedb_types::Surrogate::ZERO,
            fields: vec![("email".into(), LoroValue::String("a@b.com".into()))],
        };

        match pre_validate(
            &validator,
            &state,
            &change,
            &nodedb_crs::CrsContext::builtin_only(),
        ) {
            PreValidationResult::FastReject { constraint, .. } => {
                assert_eq!(constraint, "name_nn");
            }
            _ => panic!("expected fast reject"),
        }
    }

    #[test]
    fn pre_validate_proceeds_when_valid() {
        let state = CrdtState::new(1).unwrap();
        let mut cs = ConstraintSet::new();
        cs.add_not_null("name_nn", "users", "name");
        let validator = Validator::new(cs, 10);

        let change = ProposedChange {
            collection: "users".into(),
            row_id: "u1".into(),
            surrogate: nodedb_types::Surrogate::ZERO,
            fields: vec![("name".into(), LoroValue::String("Alice".into()))],
        };

        assert!(matches!(
            pre_validate(
                &validator,
                &state,
                &change,
                &nodedb_crs::CrsContext::builtin_only()
            ),
            PreValidationResult::Proceed
        ));
    }
}
