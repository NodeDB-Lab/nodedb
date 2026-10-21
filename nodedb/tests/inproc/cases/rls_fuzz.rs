// SPDX-License-Identifier: BUSL-1.1

//! Formal verification via proptest: fuzz RLS predicate evaluation.
//!
//! Properties:
//! - If a predicate evaluates to deny, no row is returned (substitution returns deny filter).
//! - If a predicate evaluates to allow, the correct row matches.
//! - Substitution never panics regardless of input combination.

use proptest::prelude::*;

use nodedb::control::security::auth_context::{AuthContext, AuthStatus};
use nodedb::control::security::identity::{AuthMethod, AuthenticatedIdentity, Role};
use nodedb::control::security::predicate::{CompareOp, PredicateValue, RlsPredicate};
use nodedb::control::security::predicate_eval::substitute_to_scan_filters;
use nodedb::types::TenantId;

/// Generate a random AuthContext.
fn arb_auth_context() -> impl Strategy<Value = AuthContext> {
    (
        "[a-z]{3,8}",                                                         // id
        "[a-z]{3,8}",                                                         // username
        proptest::option::of("[a-z]+@[a-z]+\\.[a-z]+"),                       // email
        1u64..100,                                                            // tenant_id
        proptest::collection::vec("[a-z]{3,8}".prop_map(String::from), 0..5), // roles
        proptest::collection::vec("[a-z]{3,8}".prop_map(String::from), 0..3), // groups
    )
        .prop_map(|(id, username, email, tid, roles, groups)| {
            let identity = AuthenticatedIdentity::new_regular(
                1,
                username.clone(),
                TenantId::new(tid),
                AuthMethod::Trust,
                vec![Role::ReadWrite],
                None,
                AuthenticatedIdentity::default_database_set(false),
            );
            let mut ctx = AuthContext::from_identity(&identity, "fuzz".into());
            ctx.id = id;
            ctx.username = username;
            ctx.email = email;
            ctx.roles = roles;
            ctx.groups = groups;
            ctx.status = AuthStatus::Active;
            ctx
        })
}

/// Generate a random RLS predicate.
fn arb_predicate() -> impl Strategy<Value = RlsPredicate> {
    prop_oneof![
        Just(RlsPredicate::AlwaysTrue),
        Just(RlsPredicate::AlwaysFalse),
        // Simple comparison: field = $auth.id
        "[a-z_]{3,10}".prop_map(|field| {
            RlsPredicate::Compare {
                field,
                op: CompareOp::Eq,
                value: PredicateValue::AuthRef("id".into()),
            }
        }),
        // Static comparison: field = 'literal'
        ("[a-z_]{3,10}", "[a-z]{3,8}").prop_map(|(field, val)| {
            RlsPredicate::Compare {
                field,
                op: CompareOp::Eq,
                value: PredicateValue::Literal(serde_json::json!(val)),
            }
        }),
        // Contains: $auth.roles CONTAINS 'x'
        "[a-z]{3,8}".prop_map(|role| {
            RlsPredicate::Contains {
                set: PredicateValue::AuthRef("roles".into()),
                element: PredicateValue::Literal(serde_json::json!(role)),
            }
        }),
    ]
}

proptest! {
    /// Property: substitute_to_scan_filters never panics.
    #[test]
    fn substitution_never_panics(
        auth in arb_auth_context(),
        pred in arb_predicate(),
    ) {
        // This should never panic, regardless of input.
        let _ = substitute_to_scan_filters(&pred, &auth);
    }

    /// Property: AlwaysTrue always produces a match_all filter.
    #[test]
    fn always_true_produces_match_all(auth in arb_auth_context()) {
        let filters = substitute_to_scan_filters(&RlsPredicate::AlwaysTrue, &auth).unwrap();
        prop_assert_eq!(filters.len(), 1);
        prop_assert_eq!(filters[0].op.as_str(), "match_all");
    }

    /// Property: AlwaysFalse always produces a deny filter.
    #[test]
    fn always_false_produces_deny(auth in arb_auth_context()) {
        let filters = substitute_to_scan_filters(&RlsPredicate::AlwaysFalse, &auth).unwrap();
        prop_assert_eq!(filters.len(), 1);
        prop_assert_eq!(filters[0].field.as_str(), "__rls_deny__");
    }

    /// Property: if predicate says deny, the resulting ScanFilter rejects any document.
    #[test]
    fn deny_filter_rejects_all_docs(
        auth in arb_auth_context(),
        doc_field in "[a-z_]{3,10}",
        doc_val in "[a-z]{3,8}",
    ) {
        let deny_filters = substitute_to_scan_filters(&RlsPredicate::AlwaysFalse, &auth).unwrap();
        let doc = serde_json::json!({ doc_field: doc_val });
        let msgpack = nodedb_types::json_msgpack::json_to_msgpack(&doc).unwrap();
        // Deny filter should NOT match any document.
        let passes = deny_filters.iter().all(|f| f.matches_binary(&msgpack, &nodedb_crs::CrsContext::builtin_only()).unwrap());
        prop_assert!(!passes, "deny filter should reject all documents");
    }

    /// Property: if $auth.roles CONTAINS a role that IS in the AuthContext,
    /// the result is a match_all (allow).
    #[test]
    fn contains_matching_role_allows(
        mut auth in arb_auth_context(),
    ) {
        // Ensure at least one role exists.
        if auth.roles.is_empty() {
            auth.roles.push("testrole".into());
        }
        let role = auth.roles[0].clone();
        let pred = RlsPredicate::Contains {
            set: PredicateValue::AuthRef("roles".into()),
            element: PredicateValue::Literal(serde_json::json!(role)),
        };
        let filters = substitute_to_scan_filters(&pred, &auth).unwrap();
        prop_assert_eq!(filters.len(), 1);
        prop_assert_eq!(filters[0].op.as_str(), "match_all");
    }
}
