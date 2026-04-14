//! Plan-time predicate evaluation: substitute `$auth.*` references and
//! combine policies into concrete `ScanFilter` values.
//!
//! This module converts compiled [`RlsPredicate`] trees into static
//! `ScanFilter` lists that the Data Plane can evaluate without session
//! awareness.

use super::auth_context::AuthContext;
use super::predicate::{CompareOp, PolicyMode, PredicateValue, RlsPredicate};
use crate::bridge::scan_filter::{FilterOp, ScanFilter};

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
        RlsPredicate::AlwaysTrue => Some(vec![ScanFilter {
            field: String::new(),
            op: "match_all".into(),
            value: nodedb_types::Value::Null,
            clauses: Vec::new(),
            expr: None,
        }]),

        RlsPredicate::AlwaysFalse => {
            // Emit a filter that never matches: non-existent field must be non-null.
            Some(vec![ScanFilter {
                field: "__rls_deny__".into(),
                op: "is_not_null".into(),
                value: nodedb_types::Value::Null,
                clauses: Vec::new(),
                expr: None,
            }])
        }

        RlsPredicate::Compare { field, op, value } => {
            let resolved = match value {
                PredicateValue::Literal(v) => v.clone(),
                PredicateValue::AuthRef(auth_field) => auth.resolve_variable(auth_field)?,
                PredicateValue::AuthFunc { .. } => value.resolve(auth)?,
                PredicateValue::Field(_) => {
                    // Field-to-field comparison not supported in ScanFilter.
                    return None;
                }
            };

            Some(vec![ScanFilter {
                field: field.clone(),
                op: op.as_filter_op().into(),
                value: nodedb_types::Value::from(resolved),
                clauses: Vec::new(),
                expr: None,
            }])
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

        RlsPredicate::Or(children) => {
            let mut clause_groups: Vec<Vec<ScanFilter>> = Vec::new();
            for child in children {
                if let Some(filters) = substitute_to_scan_filters(child, auth) {
                    // Check for match_all (always-true) — short-circuit.
                    if filters.len() == 1 && filters[0].op == FilterOp::MatchAll {
                        return Some(filters);
                    }
                    clause_groups.push(filters);
                }
            }

            if clause_groups.is_empty() {
                return Some(vec![ScanFilter {
                    field: "__rls_deny__".into(),
                    op: "is_not_null".into(),
                    value: nodedb_types::Value::Null,
                    clauses: Vec::new(),
                    expr: None,
                }]);
            }

            if clause_groups.len() == 1 {
                return Some(clause_groups.into_iter().next().unwrap_or_default());
            }

            Some(vec![ScanFilter {
                field: String::new(),
                op: "or".into(),
                value: nodedb_types::Value::Null,
                clauses: clause_groups,
                expr: None,
            }])
        }

        RlsPredicate::Not(inner) => substitute_not(inner, auth),
    }
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

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

fn substitute_contains(
    set: &PredicateValue,
    element: &PredicateValue,
    auth: &AuthContext,
) -> Option<Vec<ScanFilter>> {
    match (set, element) {
        // $auth.roles CONTAINS 'admin' → resolved at plan time.
        (PredicateValue::AuthRef(auth_field), PredicateValue::Literal(lit)) => {
            let auth_val = auth.resolve_variable(auth_field)?;
            if let Some(arr) = auth_val.as_array() {
                if arr.contains(lit) {
                    Some(vec![ScanFilter {
                        field: String::new(),
                        op: "match_all".into(),
                        value: nodedb_types::Value::Null,
                        clauses: Vec::new(),
                        expr: None,
                    }])
                } else {
                    Some(vec![ScanFilter {
                        field: "__rls_deny__".into(),
                        op: "is_not_null".into(),
                        value: nodedb_types::Value::Null,
                        clauses: Vec::new(),
                        expr: None,
                    }])
                }
            } else {
                None // Expected array, got scalar → deny
            }
        }

        // $auth.scope_status('pro:all') CONTAINS 'active' → resolved at plan time.
        (PredicateValue::AuthFunc { .. }, PredicateValue::Literal(lit)) => {
            let auth_val = set.resolve(auth)?;
            if let Some(arr) = auth_val.as_array() {
                if arr.contains(lit) {
                    Some(vec![ScanFilter {
                        field: String::new(),
                        op: "match_all".into(),
                        value: nodedb_types::Value::Null,
                        clauses: Vec::new(),
                        expr: None,
                    }])
                } else {
                    Some(vec![ScanFilter {
                        field: "__rls_deny__".into(),
                        op: "is_not_null".into(),
                        value: nodedb_types::Value::Null,
                        clauses: Vec::new(),
                        expr: None,
                    }])
                }
            } else {
                None // Expected array, got scalar → deny
            }
        }

        // doc_field CONTAINS $auth.id → field "contains" resolved_value.
        (PredicateValue::Field(doc_field), PredicateValue::AuthRef(auth_field)) => {
            let auth_val = auth.resolve_variable(auth_field)?;
            Some(vec![ScanFilter {
                field: doc_field.clone(),
                op: "contains".into(),
                value: nodedb_types::Value::from(auth_val),
                clauses: Vec::new(),
                expr: None,
            }])
        }

        // doc_field CONTAINS $auth.scope_status('pro:all') → field "contains" resolved_value.
        (PredicateValue::Field(doc_field), PredicateValue::AuthFunc { .. }) => {
            let auth_val = element.resolve(auth)?;
            Some(vec![ScanFilter {
                field: doc_field.clone(),
                op: "contains".into(),
                value: nodedb_types::Value::from(auth_val),
                clauses: Vec::new(),
                expr: None,
            }])
        }

        // doc_field CONTAINS 'literal' → field "contains" literal.
        (PredicateValue::Field(doc_field), PredicateValue::Literal(lit)) => {
            Some(vec![ScanFilter {
                field: doc_field.clone(),
                op: "contains".into(),
                value: nodedb_types::Value::from(lit.clone()),
                clauses: Vec::new(),
                expr: None,
            }])
        }

        _ => None, // Unsupported combination → deny
    }
}

fn substitute_intersects(
    left: &PredicateValue,
    right: &PredicateValue,
    auth: &AuthContext,
) -> Option<Vec<ScanFilter>> {
    match (left, right) {
        // doc_field INTERSECTS $auth.groups → "any_in" operator.
        (PredicateValue::Field(doc_field), PredicateValue::AuthRef(auth_field))
        | (PredicateValue::AuthRef(auth_field), PredicateValue::Field(doc_field)) => {
            let auth_val = auth.resolve_variable(auth_field)?;
            Some(vec![ScanFilter {
                field: doc_field.clone(),
                op: "any_in".into(),
                value: nodedb_types::Value::from(auth_val),
                clauses: Vec::new(),
                expr: None,
            }])
        }

        // doc_field INTERSECTS $auth.scope_status('pro:all') → "any_in" operator.
        (PredicateValue::Field(doc_field), PredicateValue::AuthFunc { .. }) => {
            let auth_val = right.resolve(auth)?;
            Some(vec![ScanFilter {
                field: doc_field.clone(),
                op: "any_in".into(),
                value: nodedb_types::Value::from(auth_val),
                clauses: Vec::new(),
                expr: None,
            }])
        }
        (PredicateValue::AuthFunc { .. }, PredicateValue::Field(doc_field)) => {
            let auth_val = left.resolve(auth)?;
            Some(vec![ScanFilter {
                field: doc_field.clone(),
                op: "any_in".into(),
                value: nodedb_types::Value::from(auth_val),
                clauses: Vec::new(),
                expr: None,
            }])
        }

        // $auth.groups INTERSECTS $auth.allowed → plan-time evaluation.
        (PredicateValue::AuthRef(left_field), PredicateValue::AuthRef(right_field)) => {
            let left_val = auth.resolve_variable(left_field)?;
            let right_val = auth.resolve_variable(right_field)?;
            let intersects = if let (Some(l), Some(r)) = (left_val.as_array(), right_val.as_array())
            {
                l.iter().any(|v| r.contains(v))
            } else {
                false
            };

            if intersects {
                Some(vec![ScanFilter {
                    field: String::new(),
                    op: "match_all".into(),
                    value: nodedb_types::Value::Null,
                    clauses: Vec::new(),
                    expr: None,
                }])
            } else {
                Some(vec![ScanFilter {
                    field: "__rls_deny__".into(),
                    op: "is_not_null".into(),
                    value: nodedb_types::Value::Null,
                    clauses: Vec::new(),
                    expr: None,
                }])
            }
        }

        // AuthFunc on either or both sides → plan-time evaluation via resolve().
        (PredicateValue::AuthFunc { .. }, PredicateValue::AuthFunc { .. })
        | (PredicateValue::AuthRef(_), PredicateValue::AuthFunc { .. })
        | (PredicateValue::AuthFunc { .. }, PredicateValue::AuthRef(_)) => {
            let left_val = left.resolve(auth)?;
            let right_val = right.resolve(auth)?;
            let intersects = if let (Some(l), Some(r)) = (left_val.as_array(), right_val.as_array())
            {
                l.iter().any(|v| r.contains(v))
            } else {
                false
            };

            if intersects {
                Some(vec![ScanFilter {
                    field: String::new(),
                    op: "match_all".into(),
                    value: nodedb_types::Value::Null,
                    clauses: Vec::new(),
                    expr: None,
                }])
            } else {
                Some(vec![ScanFilter {
                    field: "__rls_deny__".into(),
                    op: "is_not_null".into(),
                    value: nodedb_types::Value::Null,
                    clauses: Vec::new(),
                    expr: None,
                }])
            }
        }

        _ => None,
    }
}

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
                _ => return None, // Can't negate LIKE/ILIKE simply
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
        _ => None, // Complex NOT not supported → deny
    }
}
