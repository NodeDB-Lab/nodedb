// SPDX-License-Identifier: BUSL-1.1

//! Plan-time verdicts for comparisons that name no document field.
//!
//! `$auth.username = 'alice'` compares a session value with a literal. Its
//! answer is the same for every row the statement touches, so substitution
//! decides it once and lowers it to a match-all or deny filter. A row filter
//! here would read some document field in place of the session value, and a
//! writer controls the fields of the row it writes.

use std::collections::HashMap;

use nodedb_types::Value;

use super::filters::compare_filter;
use crate::control::security::auth_context::AuthContext;
use crate::control::security::predicate::{CompareOp, PredicateValue};

/// The key the left operand sits under while the comparison evaluates. It is
/// never a document field: the operands are the only values in scope.
const OPERAND: &str = "operand";

/// Whether `left <op> right` holds for this session.
///
/// Uses the comparison rules a row filter applies, so `'5' = 5` and `LIKE`
/// decide as they do on a stored row. `None` when either side is a document
/// field or a session reference the session cannot resolve: the caller
/// denies, as it does for every unresolved reference. An evaluation error
/// decides `false`.
pub fn compare_values(
    left: &PredicateValue,
    op: CompareOp,
    right: &PredicateValue,
    auth: &AuthContext,
) -> Option<bool> {
    if matches!(left, PredicateValue::Field(_)) || matches!(right, PredicateValue::Field(_)) {
        return None;
    }
    let left = left.resolve_scan_value(auth)?;
    let right = right.resolve_scan_value(auth)?;
    let filter = compare_filter(OPERAND, op.as_filter_op(), right);
    let operands = Value::Object(HashMap::from([(OPERAND.to_string(), left)]));
    Some(
        filter
            .matches_value(&operands, &nodedb_crs::CrsContext::builtin_only())
            .unwrap_or(false),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bridge::scan_filter::{FilterOp, ScanFilter};
    use crate::control::security::identity::{
        AuthMethod, AuthenticatedIdentity, DatabaseSet, Role,
    };
    use crate::control::security::predicate::RlsPredicate;
    use crate::control::security::predicate_eval::substitute_to_scan_filters;
    use crate::control::security::predicate_parser::parse_predicate;
    use crate::types::TenantId;
    use nodedb_types::id::DatabaseId;

    /// User 42 named `alice`, with an active `pro:all` scope.
    fn auth() -> AuthContext {
        let identity = AuthenticatedIdentity::new_regular(
            42,
            "alice",
            TenantId::new(1),
            AuthMethod::ScramSha256,
            vec![Role::ReadWrite],
            None,
            DatabaseSet::Some(smallvec::smallvec![DatabaseId::DEFAULT]),
        );
        let mut auth = AuthContext::from_identity(&identity, "s_test".into());
        auth.metadata
            .insert("scope_status.pro:all".into(), "active".into());
        auth
    }

    fn lowered(text: &str) -> Vec<ScanFilter> {
        let predicate = parse_predicate(text).expect("parse");
        substitute_to_scan_filters(&predicate, &auth()).expect("resolves")
    }

    /// The single filter a verdict lowers to: `true` for match-all, `false`
    /// for the deny filter. No filter reads a field named after nothing.
    fn verdict(text: &str) -> bool {
        let filters = lowered(text);
        assert_eq!(filters.len(), 1, "{text} lowers to one filter: {filters:?}");
        assert!(
            filters.iter().all(|filter| !filter.field.is_empty()
                || matches!(filter.op, FilterOp::MatchAll | FilterOp::Or)),
            "no comparison reads an empty field: {filters:?}"
        );
        filters[0].op == FilterOp::MatchAll
    }

    #[test]
    fn auth_against_a_literal_parses_to_a_value_comparison() {
        let predicate = parse_predicate("$auth.username = 'alice'").expect("parse");
        assert!(matches!(
            predicate,
            RlsPredicate::CompareValues {
                left: PredicateValue::AuthRef(ref field),
                op: CompareOp::Eq,
                right: PredicateValue::Literal(_),
            } if field == "username"
        ));
        let flipped = parse_predicate("'alice' = $auth.username").expect("parse");
        assert!(matches!(flipped, RlsPredicate::CompareValues { .. }));
        let bare = parse_predicate("$auth.username").expect("parse");
        assert!(matches!(
            bare,
            RlsPredicate::CompareValues {
                op: CompareOp::IsNotNull,
                ..
            }
        ));
    }

    #[test]
    fn a_holding_comparison_lowers_to_match_all() {
        assert!(verdict("$auth.username = 'alice'"));
        assert!(verdict("'alice' = $auth.username"));
        assert!(verdict("$auth.id = '42'"));
        assert!(verdict("$auth.username LIKE 'ali%'"));
        assert!(verdict("$auth.username"));
    }

    #[test]
    fn a_failing_comparison_lowers_to_deny() {
        assert!(!verdict("$auth.username = 'mallory'"));
        assert!(!verdict("$auth.username != 'alice'"));
        assert!(!verdict("$auth.id = '7'"));
    }

    #[test]
    fn negation_and_nesting_decide_the_verdict_whole() {
        assert!(!verdict("NOT $auth.username = 'alice'"));
        assert!(verdict("NOT $auth.username LIKE 'mal%'"));
        assert!(verdict(
            "$auth.username = 'mallory' OR $auth.username = 'alice'"
        ));
        assert!(!verdict("($auth.username = 'mallory' OR $auth.id = '7')"));
        let both = lowered("owner = $auth.id AND $auth.username = 'alice'");
        assert_eq!(both.len(), 2);
        assert_eq!(both[0].field, "owner");
        assert_eq!(both[1].op, FilterOp::MatchAll);
        let denied = lowered("owner = $auth.id AND $auth.username = 'mallory'");
        assert_eq!(denied[1].field, "__rls_deny__");
    }

    /// A session function resolves at plan time and decides the same way.
    #[test]
    fn a_session_function_decides_at_plan_time() {
        let scope = PredicateValue::AuthFunc {
            func: "scope_status".into(),
            args: vec!["pro:all".into()],
        };
        let active = PredicateValue::Literal(serde_json::json!("active"));
        let expired = PredicateValue::Literal(serde_json::json!("expired"));
        assert_eq!(
            compare_values(&scope, CompareOp::Eq, &active, &auth()),
            Some(true)
        );
        assert_eq!(
            compare_values(&scope, CompareOp::Eq, &expired, &auth()),
            Some(false)
        );
        let negated = RlsPredicate::Not(Box::new(RlsPredicate::CompareValues {
            left: scope,
            op: CompareOp::Eq,
            right: expired,
        }));
        let filters = substitute_to_scan_filters(&negated, &auth()).expect("resolves");
        assert_eq!(filters[0].op, FilterOp::MatchAll);
    }

    /// An unresolvable session reference has no verdict: the caller denies.
    #[test]
    fn an_unresolvable_reference_has_no_verdict() {
        let predicate = parse_predicate("$auth.org_id = 'acme'").expect("parse");
        assert!(substitute_to_scan_filters(&predicate, &auth()).is_none());
        let negated = parse_predicate("NOT $auth.org_id = 'acme'").expect("parse");
        assert!(substitute_to_scan_filters(&negated, &auth()).is_none());
    }
}
