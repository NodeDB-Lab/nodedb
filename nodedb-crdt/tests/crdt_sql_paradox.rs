// SPDX-License-Identifier: Apache-2.0

//! Integration test: The CRDT / SQL Paradox.
//!
//! Simulates the exact scenario Gemini identified as the 20% risk:
//!
//! 1. Agent A goes offline, creates a user with email "alice@example.com".
//! 2. Agent B (online) creates a user with the same email.
//! 3. Agent B's write commits successfully (UNIQUE constraint satisfied).
//! 4. Agent A comes back online and syncs its delta.
//! 5. The validator rejects Agent A's delta (UNIQUE violation).
//! 6. The dead-letter queue captures the rejection with a compensation hint.
//! 7. The application can inspect the DLQ and retry with a different email.

use loro::LoroValue;

use nodedb_crdt::CrdtError;
use nodedb_crdt::constraint::ConstraintSet;
use nodedb_crdt::dead_letter::CompensationHint;
use nodedb_crdt::policy::{CollectionPolicy, PolicyRegistry};
use nodedb_crdt::pre_validate::{self, PreValidationResult};
use nodedb_crdt::state::CrdtState;
use nodedb_crdt::validator::{ProposedChange, ValidationOutcome, Validator};

fn user_schema() -> ConstraintSet {
    let mut cs = ConstraintSet::new();
    cs.add_unique("users_email_unique", "users", "email");
    cs.add_not_null("users_name_nn", "users", "name");
    cs.add_foreign_key("posts_author_fk", "posts", "author_id", "users", "id");
    cs
}

/// The full split-brain scenario: two agents claim the same UNIQUE email.
#[test]
fn split_brain_unique_violation_with_compensation() {
    let leader_state = CrdtState::new(0).unwrap(); // Leader's committed state.

    // Create a policy registry with strict mode to route violations to DLQ
    let mut policies = PolicyRegistry::new();
    let strict_policy = CollectionPolicy::strict();
    policies.set("users", strict_policy);

    let mut validator = Validator::new_with_policies(user_schema(), 100, policies, 100);

    // Agent B (online) creates a user first — succeeds.
    let agent_b_change = ProposedChange {
        collection: "users".into(),
        row_id: "user-b".into(),
        surrogate: nodedb_types::Surrogate::ZERO,
        fields: vec![
            ("name".into(), LoroValue::String("Bob".into())),
            (
                "email".into(),
                LoroValue::String("alice@example.com".into()),
            ),
        ],
    };

    assert!(matches!(
        validator.validate(
            &leader_state,
            &agent_b_change,
            &nodedb_crs::CrsContext::builtin_only()
        ),
        ValidationOutcome::Accepted
    ));

    // Commit Agent B's write to the leader state.
    leader_state
        .upsert(
            "users",
            "user-b",
            &[
                ("name", LoroValue::String("Bob".into())),
                ("email", LoroValue::String("alice@example.com".into())),
            ],
        )
        .unwrap();

    // Agent A (was offline) comes back and tries the same email.
    let agent_a_change = ProposedChange {
        collection: "users".into(),
        row_id: "user-a".into(),
        surrogate: nodedb_types::Surrogate::ZERO,
        fields: vec![
            ("name".into(), LoroValue::String("Alice".into())),
            (
                "email".into(),
                LoroValue::String("alice@example.com".into()),
            ),
        ],
    };

    // Validation rejects Agent A's delta.
    let result = validator.validate_or_reject(
        &leader_state,
        1, // Agent A's peer ID
        nodedb_crdt::CrdtAuthContext::default(),
        &agent_a_change,
        b"agent-a-delta".to_vec(),
        &nodedb_crs::CrsContext::builtin_only(),
    );

    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(matches!(err, CrdtError::ConstraintViolation { .. }));

    // The DLQ has exactly one entry.
    assert_eq!(validator.dlq().len(), 1);

    // Inspect the dead letter — it has a compensation hint.
    let dl = validator.dlq().peek().unwrap();
    assert_eq!(dl.peer_id, 1);
    assert_eq!(dl.violated_constraint, "users_email_unique");
    assert!(matches!(
        dl.hint,
        CompensationHint::RetryWithDifferentValue { .. }
    ));

    // Application resolves: dequeue, retry with a different email.
    let _dl = validator.dlq_mut().dequeue().unwrap();
    let retry_change = ProposedChange {
        collection: "users".into(),
        row_id: "user-a".into(),
        surrogate: nodedb_types::Surrogate::ZERO,
        fields: vec![
            ("name".into(), LoroValue::String("Alice".into())),
            (
                "email".into(),
                LoroValue::String("alice+retry@example.com".into()),
            ),
        ],
    };

    // The retry succeeds.
    assert!(matches!(
        validator.validate(
            &leader_state,
            &retry_change,
            &nodedb_crs::CrsContext::builtin_only()
        ),
        ValidationOutcome::Accepted
    ));
    assert!(validator.dlq().is_empty());
}

/// Foreign key violation: agent creates a post referencing a non-existent user.
#[test]
fn offline_agent_references_deleted_user() {
    let leader_state = CrdtState::new(0).unwrap();

    // Create a policy registry with strict mode
    let mut policies = PolicyRegistry::new();
    let strict_policy = CollectionPolicy::strict();
    policies.set("posts", strict_policy);

    let mut validator = Validator::new_with_policies(user_schema(), 100, policies, 100);

    // Agent creates a post referencing user "u1" — but "u1" was deleted while offline.
    let change = ProposedChange {
        collection: "posts".into(),
        row_id: "post-1".into(),
        surrogate: nodedb_types::Surrogate::ZERO,
        fields: vec![("author_id".into(), LoroValue::String("u1".into()))],
    };

    let result = validator.validate_or_reject(
        &leader_state,
        42,
        nodedb_crdt::CrdtAuthContext::default(),
        &change,
        b"post-delta".to_vec(),
        &nodedb_crs::CrsContext::builtin_only(),
    );

    assert!(result.is_err());

    let dl = validator.dlq().peek().unwrap();
    assert_eq!(dl.violated_constraint, "posts_author_fk");

    // The hint tells the application to create the referenced user first.
    match &dl.hint {
        CompensationHint::CreateReferencedRow {
            ref_collection,
            missing_value,
            ..
        } => {
            assert_eq!(ref_collection, "users");
            assert_eq!(missing_value, "u1");
        }
        other => panic!("expected CreateReferencedRow hint, got: {other:?}"),
    }
}

/// Pre-validation fast-rejects before Raft round-trip.
#[test]
fn pre_validation_saves_raft_roundtrip() {
    let leader_state = CrdtState::new(0).unwrap();
    let validator = Validator::new(user_schema(), 100);

    // Leader already has a user with this email.
    leader_state
        .upsert(
            "users",
            "u1",
            &[
                ("name", LoroValue::String("Existing".into())),
                ("email", LoroValue::String("taken@example.com".into())),
            ],
        )
        .unwrap();

    // Pre-validate a conflicting change.
    let change = ProposedChange {
        collection: "users".into(),
        row_id: "u2".into(),
        surrogate: nodedb_types::Surrogate::ZERO,
        fields: vec![
            ("name".into(), LoroValue::String("New".into())),
            (
                "email".into(),
                LoroValue::String("taken@example.com".into()),
            ),
        ],
    };

    match pre_validate::pre_validate(
        &validator,
        &leader_state,
        &change,
        &nodedb_crs::CrsContext::builtin_only(),
    ) {
        PreValidationResult::FastReject { constraint, .. } => {
            assert_eq!(constraint, "users_email_unique");
        }
        _ => panic!("expected fast reject"),
    }

    // No Raft round-trip needed. No DLQ entry (pre-validation doesn't touch DLQ).
    assert!(validator.dlq().is_empty());
}

/// CRDT merge works correctly — two peers converge on the same state.
#[test]
fn crdt_merge_convergence() {
    let peer_a = CrdtState::new(1).unwrap();
    let peer_b = CrdtState::new(2).unwrap();

    // Peer A creates user "alice".
    peer_a
        .upsert(
            "users",
            "alice",
            &[("name", LoroValue::String("Alice".into()))],
        )
        .unwrap();

    // Peer B creates user "bob".
    peer_b
        .upsert("users", "bob", &[("name", LoroValue::String("Bob".into()))])
        .unwrap();

    // Sync: A → B and B → A.
    let snapshot_a = peer_a.export_snapshot().unwrap();
    let snapshot_b = peer_b.export_snapshot().unwrap();

    peer_b.import(&snapshot_a).unwrap();
    peer_a.import(&snapshot_b).unwrap();

    // Both peers converge: both see alice and bob.
    assert!(peer_a.row_exists("users", "alice"));
    assert!(peer_a.row_exists("users", "bob"));
    assert!(peer_b.row_exists("users", "alice"));
    assert!(peer_b.row_exists("users", "bob"));
}

/// NOT NULL violation with clear compensation hint.
#[test]
fn not_null_violation_hints_provide_field() {
    let state = CrdtState::new(0).unwrap();
    let validator = Validator::new(user_schema(), 100);

    let change = ProposedChange {
        collection: "users".into(),
        row_id: "u1".into(),
        surrogate: nodedb_types::Surrogate::ZERO,
        fields: vec![
            // Missing "name" field — violates NOT NULL.
            ("email".into(), LoroValue::String("a@b.com".into())),
        ],
    };

    match validator.validate(&state, &change, &nodedb_crs::CrsContext::builtin_only()) {
        ValidationOutcome::Rejected(violations) => {
            assert_eq!(violations.len(), 1);
            match &violations[0].hint {
                CompensationHint::ProvideRequiredField { field } => {
                    assert_eq!(field, "name");
                }
                other => panic!("expected ProvideRequiredField, got: {other:?}"),
            }
        }
        _ => panic!("expected rejection"),
    }
}
