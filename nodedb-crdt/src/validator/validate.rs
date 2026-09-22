// SPDX-License-Identifier: Apache-2.0

//! Basic validate() + validate_or_reject() entry points.

use crate::CrdtAuthContext;
use crate::error::{CrdtError, Result};
use crate::policy::PolicyResolution;
use crate::row_lookup::RowLookup;

use super::core::Validator;
use super::types::{ProposedChange, ValidationOutcome, Violation};

/// Convert the first violation into a [`CrdtError::ConstraintViolation`], appending
/// `suffix` (e.g. `"deferred for retry"`) to the reason when provided.
///
/// Returns `Ok(())` when `violations` is empty — this branch is unreachable in
/// practice (only reachable from `PolicyResolution` variants that are only ever
/// constructed from a non-empty violation list) but is kept as a safe fallback.
fn first_violation_err(
    violations: Vec<Violation>,
    collection: &str,
    suffix: Option<&str>,
) -> Result<()> {
    match violations.into_iter().next() {
        Some(v) => Err(CrdtError::ConstraintViolation {
            constraint: v.constraint_name,
            collection: collection.to_string(),
            detail: match suffix {
                Some(s) => format!("{} ({})", v.reason, s),
                None => v.reason,
            },
        }),
        None => Ok(()),
    }
}

impl Validator {
    /// Verify signing policy and replay sequence before candidate import.
    pub fn verify_delta_auth(
        &self,
        collection: &str,
        auth: &CrdtAuthContext,
        delta_bytes: &[u8],
    ) -> Result<()> {
        let is_signed = auth.delta_signature != [0u8; 32];
        if self.delta_signing_required(collection) && !is_signed {
            return Err(CrdtError::InvalidSignature {
                user_id: auth.user_id,
                detail: format!("collection `{collection}` requires signed deltas"),
            });
        }
        if !is_signed {
            return Ok(());
        }
        let verifier = self
            .delta_verifier
            .as_ref()
            .ok_or_else(|| CrdtError::InvalidSignature {
                user_id: auth.user_id,
                detail: "no delta verifier is configured".into(),
            })?;
        verifier
            .registry()
            .check_seq(auth.user_id, auth.device_id, auth.seq_no)?;
        verifier.verify(
            auth.user_id,
            auth.device_id,
            auth.seq_no,
            delta_bytes,
            &auth.delta_signature,
        )?;
        verifier
            .registry()
            .commit_seq(auth.user_id, auth.device_id, auth.seq_no)
    }

    /// Validate a proposed change against all applicable constraints.
    ///
    /// Returns `Accepted` if all constraints pass, or `Rejected` with
    /// detailed violation information.
    pub fn validate(&self, state: &impl RowLookup, change: &ProposedChange) -> ValidationOutcome {
        let constraints = self.constraints.for_collection(&change.collection);
        let mut violations = Vec::new();

        for constraint in constraints {
            match self.check_constraint(state, change, constraint) {
                Ok(Some(violation)) => violations.push(violation),
                Ok(None) => {}
                // A predicate that could not be evaluated (division/modulo by
                // zero) is a hard failure, not a violation to
                // be batched with the others — surface it immediately so it
                // bypasses conflict-policy resolution downstream.
                Err(error) => {
                    return ValidationOutcome::EvalError {
                        constraint_name: constraint.name.clone(),
                        error,
                    };
                }
            }
        }

        if violations.is_empty() {
            ValidationOutcome::Accepted
        } else {
            ValidationOutcome::Rejected(violations)
        }
    }

    /// Validate and apply declarative policy resolution.
    ///
    /// ## Replay protection
    ///
    /// When `auth.delta_signature` is non-zero, the following steps execute
    /// in this order to prevent replay attacks at minimum cost:
    ///
    /// 1. **Cheap seq_no check** — `seq_no > last_seen[(user_id, device_id)]`.
    ///    Fails fast before any HMAC computation.
    /// 2. **HMAC verification** — constant-time comparison prevents timing attacks.
    /// 3. **Atomic seq update** — `last_seen` advances only on success.
    ///
    /// For accepted changes, returns Ok(()).
    /// For violations, applies policy and:
    /// - If AutoResolved: returns Ok(())
    /// - If Deferred/Webhook/Escalate: returns appropriate error
    pub fn validate_or_reject(
        &mut self,
        state: &impl RowLookup,
        peer_id: u64,
        auth: CrdtAuthContext,
        change: &ProposedChange,
        delta_bytes: Vec<u8>,
    ) -> Result<()> {
        // Check auth expiry: agents that accumulated deltas offline must
        // re-authenticate before syncing.
        if auth.auth_expires_at > 0 {
            let now_ms = nodedb_types::clock::since_epoch()
                .unwrap_or_default()
                .as_millis() as u64;
            if now_ms > auth.auth_expires_at {
                return Err(CrdtError::AuthExpired {
                    user_id: auth.user_id,
                    expired_at: auth.auth_expires_at,
                });
            }
        }

        self.verify_delta_auth(&change.collection, &auth, &delta_bytes)?;

        let hlc_timestamp = nodedb_types::clock::since_epoch()
            .unwrap_or_default()
            .as_millis() as u64;

        match self.validate_with_policy(state, peer_id, auth, change, delta_bytes, hlc_timestamp)? {
            PolicyResolution::AutoResolved(_) => Ok(()),
            PolicyResolution::Deferred { violations, .. } => {
                // Violation was deferred for retry; return error to signal this.
                // The deferred entry was already enqueued by validate_with_policy.
                first_violation_err(violations, &change.collection, Some("deferred for retry"))
            }
            PolicyResolution::WebhookRequired { violations, .. } => {
                // Webhook decision required; return error.
                first_violation_err(violations, &change.collection, Some("webhook required"))
            }
            PolicyResolution::Escalate { violations } => {
                // Already enqueued to DLQ by validate_with_policy.
                first_violation_err(violations, &change.collection, None)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use loro::LoroValue;

    use super::*;
    use crate::constraint::ConstraintSet;
    use crate::dead_letter::CompensationHint;
    use crate::state::CrdtState;

    fn setup() -> (CrdtState, ConstraintSet) {
        let state = CrdtState::new(1).unwrap();
        let mut cs = ConstraintSet::new();
        cs.add_unique("users_email_unique", "users", "email");
        cs.add_not_null("users_name_nn", "users", "name");
        cs.add_foreign_key("posts_author_fk", "posts", "author_id", "users", "id");
        (state, cs)
    }

    #[test]
    fn valid_insert_accepted() {
        let (state, cs) = setup();
        let validator = Validator::new(cs, 100);

        let change = ProposedChange {
            collection: "users".into(),
            row_id: "u1".into(),
            surrogate: nodedb_types::Surrogate::ZERO,
            fields: vec![
                ("name".into(), LoroValue::String("Alice".into())),
                (
                    "email".into(),
                    LoroValue::String("alice@example.com".into()),
                ),
            ],
        };

        assert!(matches!(
            validator.validate(&state, &change),
            ValidationOutcome::Accepted
        ));
    }

    #[test]
    fn not_null_violation() {
        let (_state, cs) = setup();
        let validator = Validator::new(cs, 100);
        let state = CrdtState::new(1).unwrap();

        let change = ProposedChange {
            collection: "users".into(),
            row_id: "u1".into(),
            surrogate: nodedb_types::Surrogate::ZERO,
            fields: vec![(
                "email".into(),
                LoroValue::String("alice@example.com".into()),
            )],
        };

        match validator.validate(&state, &change) {
            ValidationOutcome::Rejected(v) => {
                assert_eq!(v.len(), 1);
                assert_eq!(v[0].constraint_name, "users_name_nn");
                assert!(matches!(
                    v[0].hint,
                    CompensationHint::ProvideRequiredField { .. }
                ));
            }
            _ => panic!("expected rejection"),
        }
    }

    #[test]
    fn foreign_key_violation() {
        let (state, cs) = setup();
        let validator = Validator::new(cs, 100);

        let change = ProposedChange {
            collection: "posts".into(),
            row_id: "p1".into(),
            surrogate: nodedb_types::Surrogate::ZERO,
            fields: vec![("author_id".into(), LoroValue::String("u1".into()))],
        };

        match validator.validate(&state, &change) {
            ValidationOutcome::Rejected(v) => {
                assert_eq!(v[0].constraint_name, "posts_author_fk");
                assert!(matches!(
                    v[0].hint,
                    CompensationHint::CreateReferencedRow { .. }
                ));
            }
            _ => panic!("expected FK violation"),
        }
    }

    #[test]
    fn foreign_key_passes_when_parent_exists() {
        let (state, cs) = setup();
        let validator = Validator::new(cs, 100);

        state
            .upsert(
                "users",
                "u1",
                &[
                    ("name", LoroValue::String("Alice".into())),
                    ("email", LoroValue::String("a@b.com".into())),
                ],
            )
            .unwrap();

        let change = ProposedChange {
            collection: "posts".into(),
            row_id: "p1".into(),
            surrogate: nodedb_types::Surrogate::ZERO,
            fields: vec![("author_id".into(), LoroValue::String("u1".into()))],
        };

        assert!(matches!(
            validator.validate(&state, &change),
            ValidationOutcome::Accepted
        ));
    }

    #[test]
    fn validate_or_reject_enqueues_to_dlq() {
        let (state, cs) = setup();
        let policies = crate::policy::PolicyRegistry::new();
        let mut validator = Validator::new_with_policies(cs, 100, policies, 100);

        let strict_policy = crate::policy::CollectionPolicy::strict();
        validator.policies_mut().set("users", strict_policy);

        let change = ProposedChange {
            collection: "users".into(),
            row_id: "u1".into(),
            surrogate: nodedb_types::Surrogate::ZERO,
            fields: vec![("email".into(), LoroValue::String("a@b.com".into()))],
        };

        let err = validator
            .validate_or_reject(
                &state,
                42,
                CrdtAuthContext::default(),
                &change,
                b"delta-bytes".to_vec(),
            )
            .unwrap_err();

        assert!(matches!(err, CrdtError::ConstraintViolation { .. }));
        assert_eq!(validator.dlq().len(), 1);

        let dl = validator.dlq().peek().unwrap();
        assert_eq!(dl.peer_id, 42);
        assert_eq!(dl.violated_constraint, "users_name_nn");
    }

    #[test]
    fn check_constraint_passes_when_predicate_true() {
        let state = CrdtState::new(1).unwrap();
        let mut cs = ConstraintSet::new();
        cs.add_check("people_age_check", "people", "age", "age > 0", "age > 0");
        let validator = Validator::new(cs, 100);

        let change = ProposedChange {
            collection: "people".into(),
            row_id: "p1".into(),
            surrogate: nodedb_types::Surrogate::ZERO,
            fields: vec![("age".into(), LoroValue::I64(5))],
        };

        assert!(matches!(
            validator.validate(&state, &change),
            ValidationOutcome::Accepted
        ));
    }

    #[test]
    fn check_constraint_rejects_when_predicate_false() {
        let state = CrdtState::new(1).unwrap();
        let mut cs = ConstraintSet::new();
        cs.add_check("people_age_check", "people", "age", "age > 0", "age > 0");
        let validator = Validator::new(cs, 100);

        let change = ProposedChange {
            collection: "people".into(),
            row_id: "p1".into(),
            surrogate: nodedb_types::Surrogate::ZERO,
            fields: vec![("age".into(), LoroValue::I64(-1))],
        };

        match validator.validate(&state, &change) {
            ValidationOutcome::Rejected(v) => {
                assert_eq!(v.len(), 1);
                assert_eq!(v[0].constraint_name, "people_age_check");
                assert!(matches!(
                    v[0].hint,
                    CompensationHint::ManualIntervention { .. }
                ));
            }
            _ => panic!("expected rejection"),
        }
    }

    #[test]
    fn check_constraint_rejects_malformed_expr() {
        let state = CrdtState::new(1).unwrap();
        let mut cs = ConstraintSet::new();
        cs.add_check("people_age_check", "people", "age", "age >", "age >");
        let validator = Validator::new(cs, 100);

        let change = ProposedChange {
            collection: "people".into(),
            row_id: "p1".into(),
            surrogate: nodedb_types::Surrogate::ZERO,
            fields: vec![("age".into(), LoroValue::I64(5))],
        };

        match validator.validate(&state, &change) {
            ValidationOutcome::Rejected(v) => {
                assert_eq!(v.len(), 1);
                assert_eq!(v[0].constraint_name, "people_age_check");
            }
            _ => panic!("expected rejection for malformed CHECK expression"),
        }
    }

    /// A CHECK predicate that divides by zero is an *evaluation* error, not an
    /// ordinary violation: `validate` returns `ValidationOutcome::EvalError` so the
    /// downstream conflict-policy machinery never "resolves" an unevaluable
    /// predicate. Distinct from both `Rejected` (a genuine violation) and
    /// `Accepted`.
    #[test]
    fn check_constraint_division_by_zero_surfaces_eval_error() {
        let state = CrdtState::new(1).unwrap();
        let mut cs = ConstraintSet::new();
        // `100 / age` is unevaluable when age = 0.
        cs.add_check(
            "people_ratio_check",
            "people",
            "age",
            "100 / age > 0",
            "100 / age > 0",
        );
        let validator = Validator::new(cs, 100);

        let change = ProposedChange {
            collection: "people".into(),
            row_id: "p1".into(),
            surrogate: nodedb_types::Surrogate::ZERO,
            fields: vec![("age".into(), LoroValue::I64(0))],
        };

        match validator.validate(&state, &change) {
            ValidationOutcome::EvalError {
                constraint_name,
                error,
            } => {
                assert_eq!(constraint_name, "people_ratio_check");
                assert_eq!(error, nodedb_query::EvalError::DivisionByZero);
            }
            other => panic!("expected ValidationOutcome::EvalError, got {other:?}"),
        }
    }
}

#[cfg(test)]
mod signing_tests {
    use loro::LoroValue;

    use super::*;
    use crate::constraint::ConstraintSet;
    use crate::state::CrdtState;

    fn change() -> ProposedChange {
        ProposedChange {
            collection: "secure_docs".into(),
            row_id: "row-1".into(),
            surrogate: nodedb_types::Surrogate::ZERO,
            fields: vec![("value".into(), LoroValue::I64(1))],
        }
    }

    #[test]
    fn required_signing_rejects_unsigned_delta() {
        let state = CrdtState::new(1).unwrap();
        let mut validator = Validator::new(ConstraintSet::new(), 8);
        validator.require_delta_signing("secure_docs");

        let error = validator
            .validate_or_reject(
                &state,
                2,
                CrdtAuthContext::default(),
                &change(),
                b"delta".to_vec(),
            )
            .unwrap_err();
        assert!(matches!(error, CrdtError::InvalidSignature { .. }));
    }

    #[test]
    fn signed_delta_without_verifier_fails_closed() {
        let state = CrdtState::new(1).unwrap();
        let mut validator = Validator::new(ConstraintSet::new(), 8);
        let auth = CrdtAuthContext {
            delta_signature: [7; 32],
            ..CrdtAuthContext::default()
        };

        let error = validator
            .validate_or_reject(&state, 2, auth, &change(), b"delta".to_vec())
            .unwrap_err();
        assert!(matches!(error, CrdtError::InvalidSignature { .. }));
    }
}
