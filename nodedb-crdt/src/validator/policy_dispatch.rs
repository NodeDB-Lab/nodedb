// SPDX-License-Identifier: Apache-2.0

//! Policy resolution dispatch for validation violations.

use crate::CrdtAuthContext;
use crate::constraint::{Constraint, ConstraintKind};
use crate::dead_letter::CompensationHint;
use crate::error::Result;
use crate::policy::{ConflictPolicy, PolicyResolution, ResolvedAction};
use crate::row_lookup::RowLookup;

use super::core::Validator;
use super::types::{ProposedChange, ValidationOutcome, Violation};

impl Validator {
    /// Validate with declarative policy resolution.
    ///
    /// This is the new core validation method. It attempts to resolve violations
    /// via policy before falling back to the DLQ.
    ///
    /// # Arguments
    ///
    /// * `state` — current CRDT state
    /// * `peer_id` — source peer ID
    /// * `change` — proposed change
    /// * `delta_bytes` — raw delta bytes
    /// * `hlc_timestamp` — Hybrid Logical Clock timestamp of the incoming write
    ///
    /// Returns:
    /// - `Ok(PolicyResolution::AutoResolved(_))` if the policy auto-fixed the violation
    /// - `Ok(PolicyResolution::Deferred { .. })` if deferred for retry (entry already enqueued)
    /// - `Ok(PolicyResolution::WebhookRequired { .. })` if webhook call needed (caller's responsibility)
    /// - `Ok(PolicyResolution::Escalate)` if escalating to DLQ (entry already enqueued)
    /// - `Err(_)` if an internal error occurred
    pub fn validate_with_policy(
        &mut self,
        state: &impl RowLookup,
        peer_id: u64,
        auth: CrdtAuthContext,
        change: &ProposedChange,
        delta_bytes: Vec<u8>,
        hlc_timestamp: u64,
    ) -> Result<PolicyResolution> {
        match self.validate(state, change) {
            ValidationOutcome::Accepted => {
                // No violation; return synthetic "auto-resolved" to maintain API consistency
                Ok(PolicyResolution::AutoResolved(
                    ResolvedAction::OverwriteExisting,
                ))
            }
            ValidationOutcome::Rejected(violations) => {
                // Exactly one violation per constraint (current design).
                let Some(v) = violations.first() else {
                    // Safety net: Rejected is only constructed when violations is non-empty.
                    return Ok(PolicyResolution::AutoResolved(
                        ResolvedAction::OverwriteExisting,
                    ));
                };
                let constraint = self
                    .constraints
                    .all()
                    .iter()
                    .find(|c| c.name == v.constraint_name)
                    .cloned()
                    .unwrap_or_else(|| Constraint {
                        name: v.constraint_name.clone(),
                        collection: change.collection.clone(),
                        field: String::new(),
                        kind: ConstraintKind::NotNull,
                    });

                let policy = self.policies.get_owned(&change.collection);
                let policy_for_kind = policy.for_kind(&constraint.kind);

                // Attempt policy resolution
                match policy_for_kind {
                    ConflictPolicy::LastWriterWins => {
                        tracing::info!(
                            constraint = %v.constraint_name,
                            collection = %change.collection,
                            timestamp = hlc_timestamp,
                            reason = %v.reason,
                            "resolved via LAST_WRITER_WINS"
                        );
                        Ok(PolicyResolution::AutoResolved(
                            ResolvedAction::OverwriteExisting,
                        ))
                    }

                    ConflictPolicy::RenameSuffix => {
                        let counter_key = (change.collection.clone(), constraint.field.clone());
                        let suffix = self.suffix_counter.entry(counter_key).or_insert(0);
                        *suffix += 1;
                        let new_value = format!(
                            "{}_{}",
                            change
                                .fields
                                .iter()
                                .find(|(f, _)| f == &constraint.field)
                                .map(|(_, v)| format!("{:?}", v))
                                .unwrap_or_else(|| "unknown".to_string()),
                            suffix
                        );

                        tracing::info!(
                            constraint = %v.constraint_name,
                            field = %constraint.field,
                            new_value = %new_value,
                            "resolved via RENAME_APPEND_SUFFIX"
                        );

                        Ok(PolicyResolution::AutoResolved(
                            ResolvedAction::RenamedField {
                                field: constraint.field.clone(),
                                new_value,
                            },
                        ))
                    }

                    ConflictPolicy::CascadeDefer {
                        max_retries,
                        ttl_secs,
                    } => {
                        let now_ms = nodedb_types::clock::since_epoch()
                            .unwrap_or_default()
                            .as_millis() as u64;

                        let base_ms = 500u64;
                        let first_retry_after_ms = base_ms;

                        let id = self.deferred.enqueue(crate::deferred::EnqueueDeferredArgs {
                            peer_id,
                            user_id: auth.user_id,
                            tenant_id: auth.tenant_id,
                            delta: delta_bytes,
                            collection: change.collection.clone(),
                            constraint_name: constraint.name.clone(),
                            attempt: 0,
                            max_retries: *max_retries,
                            now_ms,
                            first_retry_after_ms,
                            ttl_secs: *ttl_secs,
                        });

                        tracing::info!(
                            constraint = %v.constraint_name,
                            deferred_id = id,
                            reason = %v.reason,
                            "resolved via CASCADE_DEFER (queued for retry)"
                        );

                        Ok(PolicyResolution::Deferred {
                            retry_after_ms: first_retry_after_ms,
                            attempt: 0,
                            violations,
                        })
                    }

                    ConflictPolicy::Custom {
                        webhook_url,
                        timeout_secs,
                    } => {
                        tracing::info!(
                            constraint = %v.constraint_name,
                            webhook_url = %webhook_url,
                            "escalated to webhook"
                        );

                        Ok(PolicyResolution::WebhookRequired {
                            webhook_url: webhook_url.clone(),
                            timeout_secs: *timeout_secs,
                            violations,
                        })
                    }

                    ConflictPolicy::EscalateToDlq => {
                        self.dlq
                            .enqueue(crate::dead_letter::EnqueueDeadLetterArgs {
                                peer_id,
                                user_id: auth.user_id,
                                tenant_id: auth.tenant_id,
                                delta: delta_bytes,
                                constraint: &constraint,
                                reason: v.reason.clone(),
                                hint: v.hint.clone(),
                            })?;

                        tracing::info!(
                            constraint = %v.constraint_name,
                            collection = %change.collection,
                            "escalated to DLQ"
                        );

                        Ok(PolicyResolution::Escalate { violations })
                    }
                }
            }
            ValidationOutcome::EvalError {
                constraint_name,
                error,
            } => {
                // An unevaluable predicate (division/modulo by zero)
                // is NOT a resolvable conflict — it must never be
                // handed to a declarative policy, which would "resolve" a delta
                // the server cannot actually evaluate. Escalate straight to the
                // DLQ (fails closed) regardless of the collection's configured
                // policy, mirroring `EscalateToDlq` but for an eval error.
                let constraint = self
                    .constraints
                    .all()
                    .iter()
                    .find(|c| c.name == constraint_name)
                    .cloned()
                    .unwrap_or_else(|| Constraint {
                        name: constraint_name.clone(),
                        collection: change.collection.clone(),
                        field: String::new(),
                        kind: ConstraintKind::NotNull,
                    });
                let reason = format!("CHECK `{constraint_name}` failed to evaluate: {error}");
                let hint = CompensationHint::ManualIntervention {
                    reason: reason.clone(),
                };
                self.dlq
                    .enqueue(crate::dead_letter::EnqueueDeadLetterArgs {
                        peer_id,
                        user_id: auth.user_id,
                        tenant_id: auth.tenant_id,
                        delta: delta_bytes,
                        constraint: &constraint,
                        reason: reason.clone(),
                        hint: hint.clone(),
                    })?;
                tracing::warn!(
                    constraint = %constraint_name,
                    collection = %change.collection,
                    %error,
                    "CRDT CHECK predicate raised an evaluation error; escalated to DLQ"
                );
                Ok(PolicyResolution::Escalate {
                    violations: vec![Violation {
                        constraint_name,
                        reason,
                        hint,
                    }],
                })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use loro::LoroValue;

    use super::*;
    use crate::constraint::ConstraintSet;
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
    fn validate_with_policy_last_writer_wins() {
        let (state, cs) = setup();
        let mut policies = crate::policy::PolicyRegistry::new();
        policies.set("users", crate::policy::CollectionPolicy::ephemeral());

        let mut policy = crate::policy::CollectionPolicy::ephemeral();
        policy.not_null = crate::policy::ConflictPolicy::LastWriterWins;
        policies.set("users", policy);

        let mut validator = Validator::new_with_policies(cs, 100, policies, 100);

        let change = ProposedChange {
            collection: "users".into(),
            row_id: "u1".into(),
            surrogate: nodedb_types::Surrogate::ZERO,
            fields: vec![("email".into(), LoroValue::String("a@b.com".into()))],
        };

        let now = nodedb_types::clock::since_epoch().unwrap().as_millis() as u64;

        let resolution = validator
            .validate_with_policy(
                &state,
                42,
                CrdtAuthContext::default(),
                &change,
                b"delta".to_vec(),
                now,
            )
            .unwrap();

        assert!(matches!(
            resolution,
            PolicyResolution::AutoResolved(ResolvedAction::OverwriteExisting)
        ));
    }

    #[test]
    fn validate_with_policy_rename_suffix() {
        let (state, cs) = setup();
        let mut policies = crate::policy::PolicyRegistry::new();
        let mut policy = crate::policy::CollectionPolicy::ephemeral();
        policy.unique = crate::policy::ConflictPolicy::RenameSuffix;
        policies.set("users", policy);

        let mut validator = Validator::new_with_policies(cs, 100, policies, 100);

        state
            .upsert(
                "users",
                "u1",
                &[
                    ("name", LoroValue::String("Alice".into())),
                    ("email", LoroValue::String("alice@example.com".into())),
                ],
            )
            .unwrap();

        let change = ProposedChange {
            collection: "users".into(),
            row_id: "u2".into(),
            surrogate: nodedb_types::Surrogate::ZERO,
            fields: vec![
                ("name".into(), LoroValue::String("Bob".into())),
                (
                    "email".into(),
                    LoroValue::String("alice@example.com".into()),
                ),
            ],
        };

        let now = nodedb_types::clock::since_epoch().unwrap().as_millis() as u64;

        let resolution = validator
            .validate_with_policy(
                &state,
                42,
                CrdtAuthContext::default(),
                &change,
                b"delta".to_vec(),
                now,
            )
            .unwrap();

        match resolution {
            PolicyResolution::AutoResolved(ResolvedAction::RenamedField { field, new_value }) => {
                assert_eq!(field, "email");
                assert!(new_value.contains("_1"));
            }
            _ => panic!("expected RenamedField resolution"),
        }
    }

    #[test]
    fn validate_with_policy_cascade_defer() {
        let (state, cs) = setup();
        let mut policies = crate::policy::PolicyRegistry::new();
        let mut policy = crate::policy::CollectionPolicy::ephemeral();
        policy.foreign_key = crate::policy::ConflictPolicy::CascadeDefer {
            max_retries: 3,
            ttl_secs: 60,
        };
        policies.set("posts", policy);

        let mut validator = Validator::new_with_policies(cs, 100, policies, 100);

        let change = ProposedChange {
            collection: "posts".into(),
            row_id: "p1".into(),
            surrogate: nodedb_types::Surrogate::ZERO,
            fields: vec![("author_id".into(), LoroValue::String("u1".into()))],
        };

        let now = nodedb_types::clock::since_epoch().unwrap().as_millis() as u64;

        let resolution = validator
            .validate_with_policy(
                &state,
                42,
                CrdtAuthContext::default(),
                &change,
                b"delta".to_vec(),
                now,
            )
            .unwrap();

        match resolution {
            PolicyResolution::Deferred {
                retry_after_ms,
                attempt,
                ..
            } => {
                assert_eq!(attempt, 0);
                assert_eq!(retry_after_ms, 500);
            }
            _ => panic!("expected Deferred resolution"),
        }

        assert_eq!(validator.deferred().len(), 1);
    }

    #[test]
    fn validate_with_policy_escalate_to_dlq() {
        let (state, cs) = setup();
        let mut policies = crate::policy::PolicyRegistry::new();
        let mut policy = crate::policy::CollectionPolicy::ephemeral();
        policy.not_null = crate::policy::ConflictPolicy::EscalateToDlq;
        policies.set("users", policy);

        let mut validator = Validator::new_with_policies(cs, 100, policies, 100);

        let change = ProposedChange {
            collection: "users".into(),
            row_id: "u1".into(),
            surrogate: nodedb_types::Surrogate::ZERO,
            fields: vec![("email".into(), LoroValue::String("a@b.com".into()))],
        };

        let now = nodedb_types::clock::since_epoch().unwrap().as_millis() as u64;

        let resolution = validator
            .validate_with_policy(
                &state,
                42,
                CrdtAuthContext::default(),
                &change,
                b"delta".to_vec(),
                now,
            )
            .unwrap();

        assert!(matches!(resolution, PolicyResolution::Escalate { .. }));
        assert_eq!(validator.dlq().len(), 1);
    }
}
