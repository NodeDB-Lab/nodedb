// SPDX-License-Identifier: BUSL-1.1

//! `$auth.*` substitution over a predicate tree and policy combination.

use super::filters::{compare_filter, deny_filter, match_all_filter};
use super::sets::{substitute_contains, substitute_intersects};
use crate::bridge::scan_filter::{FilterOp, ScanFilter};
use crate::control::security::auth_context::AuthContext;
use crate::control::security::predicate::{CompareOp, PolicyMode, PredicateValue, RlsPredicate};

/// Substitute `$auth.*` references in a predicate tree and produce
/// concrete `ScanFilter` values for the Data Plane.
///
/// This is the core plan-time substitution. After this, the resulting
/// `ScanFilter` contains only literal values and field references — no
/// session variables. The Data Plane evaluates these without any auth
/// awareness.
///
/// Returns `None` if any required `$auth` reference cannot be resolved
/// (e.g., `$auth.org_id` when no org context). This causes the predicate
/// to evaluate as **deny** (fail-closed).
pub fn substitute_to_scan_filters(
    predicate: &RlsPredicate,
    auth: &AuthContext,
) -> Option<Vec<ScanFilter>> {
    match predicate {
        RlsPredicate::AlwaysTrue => Some(vec![match_all_filter()]),
        RlsPredicate::AlwaysFalse => Some(vec![deny_filter()]),

        RlsPredicate::Compare { field, op, value } => {
            // Field-to-field comparison is not expressible as a `ScanFilter`.
            if matches!(value, PredicateValue::Field(_)) {
                return None;
            }
            let resolved = value.resolve_scan_value(auth)?;
            Some(vec![compare_filter(field, op.as_filter_op(), resolved)])
        }

        RlsPredicate::Contains { set, element } => substitute_contains(set, element, auth),

        RlsPredicate::Intersects { left, right } => substitute_intersects(left, right, auth),

        RlsPredicate::And(children) => {
            let mut combined = Vec::new();
            for child in children {
                combined.extend(substitute_to_scan_filters(child, auth)?);
            }
            Some(combined)
        }

        RlsPredicate::Or(children) => substitute_or(children, auth),

        RlsPredicate::Not(inner) => substitute_not(inner, auth),
    }
}

/// Lower an `OR` node. A child that cannot be resolved contributes nothing;
/// a child that lowers to `match_all` short-circuits the whole disjunction.
fn substitute_or(children: &[RlsPredicate], auth: &AuthContext) -> Option<Vec<ScanFilter>> {
    let mut clause_groups: Vec<Vec<ScanFilter>> = Vec::new();
    for child in children {
        if let Some(filters) = substitute_to_scan_filters(child, auth) {
            if filters.len() == 1 && filters[0].op == FilterOp::MatchAll {
                return Some(filters);
            }
            clause_groups.push(filters);
        }
    }

    if clause_groups.len() == 1
        && let Some(single) = clause_groups.pop()
    {
        return Some(single);
    }
    if clause_groups.is_empty() {
        return Some(vec![deny_filter()]);
    }

    Some(vec![ScanFilter {
        field: String::new(),
        op: FilterOp::Or,
        value: nodedb_types::Value::Null,
        clauses: clause_groups,
        expr: None,
    }])
}

/// Combine multiple policies according to their modes.
///
/// Final result: `(any permissive passes) AND (all restrictive pass)`.
///
/// Returns the combined `ScanFilter` list to inject into the query.
/// Empty return = no RLS policies (allow all).
pub fn combine_policies(
    policies: &[(RlsPredicate, PolicyMode)],
    auth: &AuthContext,
) -> Option<Vec<ScanFilter>> {
    if policies.is_empty() {
        return Some(Vec::new()); // No policies → allow all
    }

    let mut permissive: Vec<&RlsPredicate> = Vec::new();
    let mut restrictive: Vec<&RlsPredicate> = Vec::new();

    for (pred, mode) in policies {
        match mode {
            PolicyMode::Permissive => permissive.push(pred),
            PolicyMode::Restrictive => restrictive.push(pred),
        }
    }

    let mut combined = Vec::new();

    // Permissive: OR-combine. If no permissive policies exist, default allow.
    if permissive.len() == 1 {
        combined.extend(substitute_to_scan_filters(permissive[0], auth)?);
    } else if permissive.len() > 1 {
        let or_children: Vec<RlsPredicate> = permissive.iter().map(|p| (*p).clone()).collect();
        let or_pred = RlsPredicate::Or(or_children);
        combined.extend(substitute_to_scan_filters(&or_pred, auth)?);
    }

    // Restrictive: AND-combine (each becomes additional filters).
    for pred in &restrictive {
        combined.extend(substitute_to_scan_filters(pred, auth)?);
    }

    Some(combined)
}

/// Lower a `NOT` node by negating the operator of the one comparison it
/// wraps. `LIKE` / `ILIKE` and composite children have no single negated
/// filter, so they deny.
fn substitute_not(inner: &RlsPredicate, auth: &AuthContext) -> Option<Vec<ScanFilter>> {
    match inner {
        RlsPredicate::AlwaysTrue => substitute_to_scan_filters(&RlsPredicate::AlwaysFalse, auth),
        RlsPredicate::AlwaysFalse => substitute_to_scan_filters(&RlsPredicate::AlwaysTrue, auth),
        RlsPredicate::Compare { field, op, value } => {
            let negated_op = match op {
                CompareOp::Eq => CompareOp::Ne,
                CompareOp::Ne => CompareOp::Eq,
                CompareOp::Gt => CompareOp::Lte,
                CompareOp::Gte => CompareOp::Lt,
                CompareOp::Lt => CompareOp::Gte,
                CompareOp::Lte => CompareOp::Gt,
                CompareOp::In => CompareOp::NotIn,
                CompareOp::NotIn => CompareOp::In,
                CompareOp::IsNull => CompareOp::IsNotNull,
                CompareOp::IsNotNull => CompareOp::IsNull,
                CompareOp::Like | CompareOp::ILike => return None,
            };
            substitute_to_scan_filters(
                &RlsPredicate::Compare {
                    field: field.clone(),
                    op: negated_op,
                    value: value.clone(),
                },
                auth,
            )
        }
        RlsPredicate::Contains { .. }
        | RlsPredicate::Intersects { .. }
        | RlsPredicate::And(_)
        | RlsPredicate::Or(_)
        | RlsPredicate::Not(_) => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::security::identity::{
        AuthMethod, AuthenticatedIdentity, DatabaseSet, Role,
    };
    use crate::types::TenantId;
    use nodedb_types::datetime::NdbDateTime;
    use nodedb_types::id::DatabaseId;
    use nodedb_types::json_msgpack::InstantKind;

    fn test_identity() -> AuthenticatedIdentity {
        AuthenticatedIdentity::new_regular(
            42,
            "alice",
            TenantId::new(1),
            AuthMethod::ScramSha256,
            vec![Role::ReadWrite],
            None,
            DatabaseSet::Some(smallvec::smallvec![DatabaseId::DEFAULT]),
        )
    }

    fn auth_with_database(db_id: DatabaseId) -> AuthContext {
        let mut ctx = AuthContext::from_identity(&test_identity(), "s_test".into());
        ctx.database_id = Some(db_id);
        ctx
    }

    fn auth_without_database() -> AuthContext {
        AuthContext::from_identity(&test_identity(), "s_test".into())
    }

    /// `$auth.database_id` resolves to the session's database id and the
    /// predicate produces the correct ScanFilter value.
    #[test]
    fn database_id_auth_ref_substitutes_correctly() {
        let db_id = DatabaseId::new(99);
        let auth = auth_with_database(db_id);

        let predicate = RlsPredicate::Compare {
            field: "owning_db".into(),
            op: CompareOp::Eq,
            value: PredicateValue::AuthRef("database_id".into()),
        };

        let filters = substitute_to_scan_filters(&predicate, &auth)
            .expect("should resolve when database_id is set");
        assert_eq!(filters.len(), 1);
        assert_eq!(filters[0].field, "owning_db");
        match &filters[0].value {
            nodedb_types::Value::Integer(n) => assert_eq!(*n as u64, db_id.as_u64()),
            other => panic!("expected numeric value, got {:?}", other),
        }
    }

    /// When `database_id` is `None` the predicate fails closed (returns None).
    #[test]
    fn database_id_auth_ref_fails_closed_when_none() {
        let auth = auth_without_database();

        let predicate = RlsPredicate::Compare {
            field: "owning_db".into(),
            op: CompareOp::Eq,
            value: PredicateValue::AuthRef("database_id".into()),
        };

        let result = substitute_to_scan_filters(&predicate, &auth);
        assert!(
            result.is_none(),
            "predicate must fail closed when database_id is None"
        );
    }

    /// `combine_policies` with a single permissive database_id policy produces
    /// the correct ScanFilter when the session has a bound database.
    #[test]
    fn combine_database_id_policy_passes_when_set() {
        let db_id = DatabaseId::new(77);
        let auth = auth_with_database(db_id);

        let predicate = RlsPredicate::Compare {
            field: "db".into(),
            op: CompareOp::Eq,
            value: PredicateValue::AuthRef("database_id".into()),
        };

        let policies = [(predicate, PolicyMode::Permissive)];
        let filters = combine_policies(&policies, &auth);
        assert!(
            filters.as_ref().is_some_and(|v| !v.is_empty()),
            "should produce scan filters when database_id is bound"
        );
    }

    /// A typed instant literal lowers to the typed `ScanFilter.value` of its
    /// declared kind, so the Data Plane compares an instant with an instant.
    #[test]
    fn instant_literal_lowers_to_a_typed_scan_value() {
        let auth = auth_without_database();
        let at = NdbDateTime::from_micros(1_583_402_400_000_000);

        let naive = RlsPredicate::Compare {
            field: "captured_at".into(),
            op: CompareOp::Gte,
            value: PredicateValue::Instant {
                at,
                kind: InstantKind::Naive,
            },
        };
        let filters = substitute_to_scan_filters(&naive, &auth).expect("an instant resolves");
        assert_eq!(filters.len(), 1);
        assert_eq!(filters[0].field, "captured_at");
        assert_eq!(filters[0].op, FilterOp::Gte);
        assert_eq!(filters[0].value, nodedb_types::Value::NaiveDateTime(at));

        let zoned = RlsPredicate::Compare {
            field: "captured_at".into(),
            op: CompareOp::Lt,
            value: PredicateValue::Instant {
                at,
                kind: InstantKind::Utc,
            },
        };
        let filters = substitute_to_scan_filters(&zoned, &auth).expect("an instant resolves");
        assert_eq!(filters[0].value, nodedb_types::Value::DateTime(at));
    }

    /// `NOT` over an instant comparison negates the operator and keeps the
    /// typed value.
    #[test]
    fn negated_instant_comparison_keeps_the_typed_value() {
        let auth = auth_without_database();
        let at = NdbDateTime::from_micros(1_583_402_400_000_000);
        let predicate = RlsPredicate::Not(Box::new(RlsPredicate::Compare {
            field: "captured_at".into(),
            op: CompareOp::Gte,
            value: PredicateValue::Instant {
                at,
                kind: InstantKind::Naive,
            },
        }));
        let filters = substitute_to_scan_filters(&predicate, &auth).expect("resolves");
        assert_eq!(filters[0].op, FilterOp::Lt);
        assert_eq!(filters[0].value, nodedb_types::Value::NaiveDateTime(at));
    }
}
