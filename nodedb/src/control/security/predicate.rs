// SPDX-License-Identifier: BUSL-1.1

//! Compiled RLS predicate AST with `$auth.*` session variable support.
//!
//! Predicates are parsed and validated at policy creation time, stored as
//! compiled AST nodes. At query time, the planner substitutes `$auth.*`
//! references with concrete values from `AuthContext`, producing static
//! `ScanFilter` values that the Data Plane can evaluate without any
//! awareness of sessions or JWT claims.
//!
//! # Predicate Expressions
//!
//! ```text
//! // Simple comparison with session variable:
//! user_id = $auth.id
//!
//! // Set membership:
//! $auth.roles CONTAINS 'admin'
//! allowed_groups INTERSECTS $auth.groups
//!
//! // Composite:
//! (user_id = $auth.id) OR ($auth.roles CONTAINS 'admin')
//!
//! // Nested metadata:
//! $auth.metadata.plan = 'enterprise'
//! ```
//!
//! # Policy Combination
//!
//! - **Permissive** (default): OR-combined. Row visible if ANY permissive policy passes.
//! - **Restrictive**: AND-combined. ALL restrictive policies must pass.
//! - Final: `(any permissive passes) AND (all restrictive pass)`

use nodedb_types::datetime::NdbDateTime;
use nodedb_types::json_msgpack::InstantKind;
use serde::{Deserialize, Serialize};

use super::auth_context::AuthContext;
use crate::bridge::scan_filter::FilterOp;

/// A compiled RLS predicate expression.
///
/// Parsed at policy creation time. Validated for correctness (unknown `$auth`
/// fields are rejected). Stored in `RlsPolicy::compiled_predicate`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RlsPredicate {
    /// Field comparison: `<doc_field> <op> <value_or_auth_ref>`.
    Compare {
        /// Document field path (e.g., "user_id", "tenant_id").
        field: String,
        /// Comparison operator.
        op: CompareOp,
        /// Right-hand side: literal or `$auth.*` reference.
        value: PredicateValue,
    },

    /// Set membership: `<set_source> CONTAINS <element>`.
    ///
    /// `set_source` is either a doc field (array) or `$auth.*` (array).
    /// `element` is either a literal or `$auth.*` (scalar).
    Contains {
        /// The set (must resolve to an array).
        set: PredicateValue,
        /// The element to check membership of.
        element: PredicateValue,
    },

    /// Set intersection: `<left_set> INTERSECTS <right_set>`.
    ///
    /// True if any element appears in both sets.
    Intersects {
        left: PredicateValue,
        right: PredicateValue,
    },

    /// Conjunction: all children must pass.
    And(Vec<RlsPredicate>),
    /// Disjunction: at least one child must pass.
    Or(Vec<RlsPredicate>),
    /// Negation.
    Not(Box<RlsPredicate>),

    /// Always-true sentinel (used for superuser bypass).
    AlwaysTrue,
    /// Always-false sentinel (used for deny-all).
    AlwaysFalse,
}

/// Comparison operators supported in RLS predicates.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CompareOp {
    Eq,
    Ne,
    Gt,
    Gte,
    Lt,
    Lte,
    In,
    NotIn,
    Like,
    ILike,
    IsNull,
    IsNotNull,
}

impl CompareOp {
    /// The `ScanFilter` operator this comparison lowers to.
    pub fn as_filter_op(&self) -> FilterOp {
        match self {
            Self::Eq => FilterOp::Eq,
            Self::Ne => FilterOp::Ne,
            Self::Gt => FilterOp::Gt,
            Self::Gte => FilterOp::Gte,
            Self::Lt => FilterOp::Lt,
            Self::Lte => FilterOp::Lte,
            Self::In => FilterOp::In,
            Self::NotIn => FilterOp::NotIn,
            Self::Like => FilterOp::Like,
            Self::ILike => FilterOp::Ilike,
            Self::IsNull => FilterOp::IsNull,
            Self::IsNotNull => FilterOp::IsNotNull,
        }
    }

    /// Parse from SQL-style operator string.
    pub fn from_str_sql(s: &str) -> Option<Self> {
        match s {
            "=" => Some(Self::Eq),
            "!=" | "<>" => Some(Self::Ne),
            ">" => Some(Self::Gt),
            ">=" => Some(Self::Gte),
            "<" => Some(Self::Lt),
            "<=" => Some(Self::Lte),
            _ => {
                let upper = s.to_uppercase();
                match upper.as_str() {
                    "IN" => Some(Self::In),
                    "NOT_IN" | "NOT IN" => Some(Self::NotIn),
                    "LIKE" => Some(Self::Like),
                    "ILIKE" => Some(Self::ILike),
                    "IS_NULL" | "IS NULL" => Some(Self::IsNull),
                    "IS_NOT_NULL" | "IS NOT NULL" => Some(Self::IsNotNull),
                    _ => None,
                }
            }
        }
    }
}

/// A value in a predicate: either a literal or an `$auth.*` session reference.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PredicateValue {
    /// A literal JSON value (string, number, bool, array, null).
    Literal(serde_json::Value),
    /// A literal typed against the declared `TIMESTAMP` / `TIMESTAMPTZ`
    /// column it is compared with. Policy compilation resolves a numeric
    /// (epoch milliseconds) or text (ISO-8601) literal to this at
    /// `CREATE RLS POLICY` and again at every load, by the same rule the
    /// planner applies to a query predicate, so the Data Plane compares an
    /// instant with an instant.
    Instant { at: NdbDateTime, kind: InstantKind },
    /// A document field reference (resolved at Data Plane scan time).
    Field(String),
    /// A session variable reference: `$auth.id`, `$auth.roles`, etc.
    /// Resolved at plan time via `AuthContext::resolve_variable()`.
    AuthRef(String),
    /// A session function call: `$auth.scope_status('pro:all')`, `$auth.scope_expires_at('pro:all')`.
    /// Resolved at plan time via the scope grant store.
    AuthFunc { func: String, args: Vec<String> },
}

impl PredicateValue {
    /// Check if this is an `$auth.*` reference or function.
    pub fn is_auth_ref(&self) -> bool {
        matches!(self, Self::AuthRef(_) | Self::AuthFunc { .. })
    }

    /// Resolve this value using the given `AuthContext`.
    ///
    /// - `Literal`: returned as-is.
    /// - `Instant`: its ISO-8601 rendering.
    /// - `Field`: `None` (resolved at scan time by the Data Plane).
    /// - `AuthRef`: resolved via `AuthContext::resolve_variable()`.
    /// - `AuthFunc`: resolved via `AuthContext` metadata (pre-computed).
    ///
    /// Returns `None` if an `AuthRef`/`AuthFunc` cannot be resolved.
    pub fn resolve(&self, auth: &AuthContext) -> Option<serde_json::Value> {
        match self {
            Self::Literal(v) => Some(v.clone()),
            Self::Instant { at, .. } => Some(serde_json::Value::String(at.to_iso8601())),
            Self::Field(_) => None,
            Self::AuthRef(field) => auth.resolve_variable(field),
            Self::AuthFunc { func, args } => {
                // Functions are resolved via pre-computed metadata keys.
                // e.g., scope_status('pro:all') → metadata["scope_status.pro:all"]
                let arg = args.first().map(|s| s.as_str()).unwrap_or("");
                let key = format!("{func}.{arg}");
                auth.resolve_variable(&format!("metadata.{key}"))
            }
        }
    }

    /// Resolve this value to the `nodedb_types::Value` a `ScanFilter` carries.
    ///
    /// An `Instant` keeps its typed kind (`Value::NaiveDateTime` for a
    /// `TIMESTAMP` column, `Value::DateTime` for `TIMESTAMPTZ`); every other
    /// variant goes through [`Self::resolve`]. Returns `None` when a `Field`
    /// is asked for a value, or an `$auth.*` reference cannot be resolved.
    pub fn resolve_scan_value(&self, auth: &AuthContext) -> Option<nodedb_types::Value> {
        match self {
            Self::Instant { at, kind } => Some(kind.value(*at)),
            Self::Literal(_) | Self::Field(_) | Self::AuthRef(_) | Self::AuthFunc { .. } => {
                self.resolve(auth).map(nodedb_types::Value::from)
            }
        }
    }
}

/// Whether a policy is permissive (OR-combined) or restrictive (AND-combined).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum PolicyMode {
    /// Default: OR-combined with other permissive policies.
    /// Row visible if ANY permissive policy passes.
    #[default]
    Permissive,
    /// AND-combined: ALL restrictive policies must pass.
    /// Applied after permissive evaluation.
    Restrictive,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::security::auth_context::AuthContext;
    use crate::control::security::identity::{AuthMethod, AuthenticatedIdentity, Role};
    use crate::control::security::predicate_eval::{combine_policies, substitute_to_scan_filters};
    use crate::control::security::predicate_parser::{parse_predicate, validate_auth_refs};
    use crate::types::TenantId;

    fn make_auth() -> AuthContext {
        let identity = AuthenticatedIdentity::new_regular(
            123,
            "alice",
            TenantId::new(1),
            AuthMethod::ApiKey,
            vec![Role::ReadWrite],
            None,
            crate::control::security::identity::DatabaseSet::Some(smallvec::smallvec![
                nodedb_types::id::DatabaseId::DEFAULT,
            ]),
        );
        AuthContext::from_identity(&identity, "test-session".into())
    }

    #[test]
    fn simple_equality_substitution() {
        let pred = RlsPredicate::Compare {
            field: "user_id".into(),
            op: CompareOp::Eq,
            value: PredicateValue::AuthRef("id".into()),
        };
        let auth = make_auth();
        let filters = substitute_to_scan_filters(&pred, &auth).unwrap();
        assert_eq!(filters.len(), 1);
        assert_eq!(filters[0].field, "user_id");
        assert_eq!(filters[0].op, crate::bridge::scan_filter::FilterOp::Eq);
        assert_eq!(filters[0].value, nodedb_types::Value::String("123".into()));
    }

    /// `$auth.quota_remaining('<scope>')` resolves through the same
    /// pre-computed `metadata["quota_remaining.<scope>"]` mechanism
    /// `scope_status` already uses — this test pins that seam directly
    /// rather than through a full RLS policy, since the metadata is
    /// populated by `enrich_auth_context_with_scopes`
    /// (`control::security::scope::enrichment`), not by this module.
    #[test]
    fn quota_remaining_auth_func_resolves_from_metadata() {
        let mut auth = make_auth();
        auth.metadata.insert(
            "quota_remaining.pro:all".into(),
            nodedb_types::Value::Integer(750),
        );
        let value = PredicateValue::AuthFunc {
            func: "quota_remaining".into(),
            args: vec!["pro:all".into()],
        };
        assert_eq!(value.resolve(&auth), Some(serde_json::json!(750)));
    }

    #[test]
    fn quota_pct_auth_func_resolves_from_metadata() {
        let mut auth = make_auth();
        auth.metadata
            .insert("quota_pct.pro:all".into(), nodedb_types::Value::Float(0.25));
        let value = PredicateValue::AuthFunc {
            func: "quota_pct".into(),
            args: vec!["pro:all".into()],
        };
        assert_eq!(value.resolve(&auth), Some(serde_json::json!(0.25)));
    }

    /// Before enrichment writes the metadata key (or for a scope with no
    /// quota defined), `quota_remaining`/`quota_pct` resolve to `None` —
    /// which fails an RLS predicate closed rather than panicking. This is
    /// the exact gap `enrich_auth_context_with_scopes` exists to close for
    /// every held scope with a quota defined.
    #[test]
    fn quota_remaining_auth_func_fails_closed_when_metadata_absent() {
        let auth = make_auth();
        let value = PredicateValue::AuthFunc {
            func: "quota_remaining".into(),
            args: vec!["pro:all".into()],
        };
        assert_eq!(value.resolve(&auth), None);
    }

    #[test]
    fn literal_comparison() {
        let pred = RlsPredicate::Compare {
            field: "status".into(),
            op: CompareOp::Eq,
            value: PredicateValue::Literal(serde_json::json!("active")),
        };
        let auth = make_auth();
        let filters = substitute_to_scan_filters(&pred, &auth).unwrap();
        assert_eq!(
            filters[0].value,
            nodedb_types::Value::String("active".into())
        );
    }

    #[test]
    fn auth_contains_check() {
        let pred = RlsPredicate::Contains {
            set: PredicateValue::AuthRef("roles".into()),
            element: PredicateValue::Literal(serde_json::json!("readwrite")),
        };
        let auth = make_auth();
        let filters = substitute_to_scan_filters(&pred, &auth).unwrap();
        assert_eq!(
            filters[0].op,
            crate::bridge::scan_filter::FilterOp::MatchAll
        );
    }

    #[test]
    fn field_contains_auth_ref() {
        let pred = RlsPredicate::Contains {
            set: PredicateValue::Field("allowed_users".into()),
            element: PredicateValue::AuthRef("id".into()),
        };
        let auth = make_auth();
        let filters = substitute_to_scan_filters(&pred, &auth).unwrap();
        assert_eq!(filters[0].field, "allowed_users");
        assert_eq!(
            filters[0].op,
            crate::bridge::scan_filter::FilterOp::ArrayContains
        );
        assert_eq!(filters[0].value, nodedb_types::Value::String("123".into()));
    }

    #[test]
    fn and_combination() {
        let pred = RlsPredicate::And(vec![
            RlsPredicate::Compare {
                field: "tenant_id".into(),
                op: CompareOp::Eq,
                value: PredicateValue::AuthRef("tenant_id".into()),
            },
            RlsPredicate::Compare {
                field: "status".into(),
                op: CompareOp::Eq,
                value: PredicateValue::Literal(serde_json::json!("published")),
            },
        ]);
        let auth = make_auth();
        let filters = substitute_to_scan_filters(&pred, &auth).unwrap();
        assert_eq!(filters.len(), 2);
    }

    #[test]
    fn or_combination() {
        let pred = RlsPredicate::Or(vec![
            RlsPredicate::Compare {
                field: "owner".into(),
                op: CompareOp::Eq,
                value: PredicateValue::AuthRef("id".into()),
            },
            RlsPredicate::Compare {
                field: "visibility".into(),
                op: CompareOp::Eq,
                value: PredicateValue::Literal(serde_json::json!("public")),
            },
        ]);
        let auth = make_auth();
        let filters = substitute_to_scan_filters(&pred, &auth).unwrap();
        assert_eq!(filters.len(), 1);
        assert_eq!(filters[0].op, crate::bridge::scan_filter::FilterOp::Or);
        assert_eq!(filters[0].clauses.len(), 2);
    }

    #[test]
    fn missing_auth_ref_denies() {
        let pred = RlsPredicate::Compare {
            field: "org_id".into(),
            op: CompareOp::Eq,
            value: PredicateValue::AuthRef("nonexistent_field".into()),
        };
        let auth = make_auth();
        assert!(substitute_to_scan_filters(&pred, &auth).is_none());
    }

    #[test]
    fn policy_combination_permissive_and_restrictive() {
        let policies = vec![
            (
                RlsPredicate::Compare {
                    field: "user_id".into(),
                    op: CompareOp::Eq,
                    value: PredicateValue::AuthRef("id".into()),
                },
                PolicyMode::Permissive,
            ),
            (
                RlsPredicate::Compare {
                    field: "status".into(),
                    op: CompareOp::Eq,
                    value: PredicateValue::Literal(serde_json::json!("active")),
                },
                PolicyMode::Restrictive,
            ),
        ];
        let auth = make_auth();
        let filters = combine_policies(&policies, &auth).unwrap();
        assert_eq!(filters.len(), 2);
    }

    #[test]
    fn empty_policies_allow_all() {
        let auth = make_auth();
        let filters = combine_policies(&[], &auth).unwrap();
        assert!(filters.is_empty());
    }

    #[test]
    fn parse_simple_equality() {
        let pred = parse_predicate("user_id = $auth.id").unwrap();
        match pred {
            RlsPredicate::Compare { field, op, value } => {
                assert_eq!(field, "user_id");
                assert_eq!(op, CompareOp::Eq);
                assert!(matches!(value, PredicateValue::AuthRef(ref f) if f == "id"));
            }
            _ => panic!("expected Compare"),
        }
    }

    #[test]
    fn parse_and_or() {
        let pred =
            parse_predicate("user_id = $auth.id AND status = 'active' OR visibility = 'public'")
                .unwrap();
        assert!(matches!(pred, RlsPredicate::Or(_)));
    }

    #[test]
    fn parse_parenthesized() {
        let pred =
            parse_predicate("(user_id = $auth.id OR shared = true) AND status = 'active'").unwrap();
        assert!(matches!(pred, RlsPredicate::And(_)));
    }

    #[test]
    fn parse_contains() {
        let pred = parse_predicate("$auth.roles CONTAINS 'admin'").unwrap();
        assert!(matches!(pred, RlsPredicate::Contains { .. }));
    }

    #[test]
    fn parse_intersects() {
        let pred = parse_predicate("allowed_groups INTERSECTS $auth.groups").unwrap();
        assert!(matches!(pred, RlsPredicate::Intersects { .. }));
    }

    #[test]
    fn parse_not() {
        let pred = parse_predicate("NOT status = 'archived'").unwrap();
        assert!(matches!(pred, RlsPredicate::Not(_)));
    }

    #[test]
    fn validate_auth_refs_passes() {
        let pred = parse_predicate("user_id = $auth.id AND $auth.roles CONTAINS 'admin'").unwrap();
        assert!(validate_auth_refs(&pred).is_ok());
    }

    #[test]
    fn validate_auth_refs_rejects_unknown() {
        let pred = parse_predicate("user_id = $auth.foobar").unwrap();
        assert!(validate_auth_refs(&pred).is_err());
    }

    #[test]
    fn validate_auth_refs_allows_metadata() {
        let pred = parse_predicate("plan = $auth.metadata.plan").unwrap();
        assert!(validate_auth_refs(&pred).is_ok());
    }

    #[test]
    fn not_negation_produces_correct_op() {
        let pred = RlsPredicate::Not(Box::new(RlsPredicate::Compare {
            field: "active".into(),
            op: CompareOp::Eq,
            value: PredicateValue::Literal(serde_json::json!(true)),
        }));
        let auth = make_auth();
        let filters = substitute_to_scan_filters(&pred, &auth).unwrap();
        assert_eq!(filters[0].op, crate::bridge::scan_filter::FilterOp::Ne);
    }
}
