use loro::LoroValue;

use nodedb_crdt::constraint::ConstraintSet;
use nodedb_crdt::policy::CollectionPolicy;
use nodedb_crdt::pre_validate::PreValidationResult;
use nodedb_crdt::validator::ProposedChange;

use crate::types::TenantId;

use super::core::TenantCrdtEngine;

fn test_constraints() -> ConstraintSet {
    let mut cs = ConstraintSet::new();
    cs.add_unique("users_email_unique", "users", "email");
    cs.add_not_null("users_name_nn", "users", "name");
    cs
}

#[test]
fn valid_write_applies() {
    let mut engine = TenantCrdtEngine::new(TenantId::new(1), 0, test_constraints()).unwrap();

    let change = ProposedChange {
        collection: "users".into(),
        row_id: "u1".into(),
        fields: vec![
            ("name".into(), LoroValue::String("Alice".into())),
            (
                "email".into(),
                LoroValue::String("alice@example.com".into()),
            ),
        ],
    };

    engine
        .validate_and_apply(
            1,
            nodedb_crdt::CrdtAuthContext::default(),
            &change,
            b"delta".to_vec(),
        )
        .unwrap();

    assert!(engine.row_exists("users", "u1"));
    assert_eq!(engine.dlq_len(), 0);
}

#[test]
fn constraint_violation_routes_to_dlq() {
    let mut engine = TenantCrdtEngine::new(TenantId::new(1), 0, test_constraints()).unwrap();
    // Use strict policy so violations escalate to DLQ instead of auto-resolving.
    engine
        .validator
        .policies_mut()
        .set("users", CollectionPolicy::strict());

    // Missing "name" field violates NOT NULL.
    let change = ProposedChange {
        collection: "users".into(),
        row_id: "u1".into(),
        fields: vec![("email".into(), LoroValue::String("a@b.com".into()))],
    };

    let err = engine
        .validate_and_apply(
            42,
            nodedb_crdt::CrdtAuthContext::default(),
            &change,
            b"delta".to_vec(),
        )
        .unwrap_err();

    assert!(matches!(err, crate::Error::Crdt(_)));
    assert_eq!(engine.dlq_len(), 1);
}

#[test]
fn pre_validate_fast_rejects() {
    let engine = TenantCrdtEngine::new(TenantId::new(1), 0, test_constraints()).unwrap();

    let change = ProposedChange {
        collection: "users".into(),
        row_id: "u1".into(),
        fields: vec![("email".into(), LoroValue::String("a@b.com".into()))],
    };

    match engine.pre_validate(&change) {
        PreValidationResult::FastReject { constraint, .. } => {
            assert_eq!(constraint, "users_name_nn");
        }
        _ => panic!("expected fast reject"),
    }
}

#[test]
fn unique_violation_after_first_write() {
    let mut engine = TenantCrdtEngine::new(TenantId::new(1), 0, test_constraints()).unwrap();
    // Strict mode: UNIQUE violations escalate to DLQ.
    engine
        .validator
        .policies_mut()
        .set("users", CollectionPolicy::strict());

    let first = ProposedChange {
        collection: "users".into(),
        row_id: "u1".into(),
        fields: vec![
            ("name".into(), LoroValue::String("Alice".into())),
            (
                "email".into(),
                LoroValue::String("alice@example.com".into()),
            ),
        ],
    };
    engine
        .validate_and_apply(
            1,
            nodedb_crdt::CrdtAuthContext::default(),
            &first,
            b"d1".to_vec(),
        )
        .unwrap();

    // Second write with same email should fail.
    let second = ProposedChange {
        collection: "users".into(),
        row_id: "u2".into(),
        fields: vec![
            ("name".into(), LoroValue::String("Bob".into())),
            (
                "email".into(),
                LoroValue::String("alice@example.com".into()),
            ),
        ],
    };
    assert!(
        engine
            .validate_and_apply(
                2,
                nodedb_crdt::CrdtAuthContext::default(),
                &second,
                b"d2".to_vec()
            )
            .is_err()
    );
    assert_eq!(engine.dlq_len(), 1);
}
