// SPDX-License-Identifier: BUSL-1.1

//! `CONTAINS` / `INTERSECTS` lowering.
//!
//! A set that lives on the session (`$auth.roles`, `$auth.scope_status(..)`)
//! is decided at plan time and lowers to `match_all` or the deny filter. A
//! set that lives on the document lowers to an `array_contains` /
//! `array_overlap` filter the Data Plane evaluates per row. A combination with no set on either
//! side, or a document set on both sides, has no `ScanFilter` form and
//! denies.

use super::filters::{compare_filter, verdict_filter};
use crate::bridge::scan_filter::{FilterOp, ScanFilter};
use crate::control::security::auth_context::AuthContext;
use crate::control::security::predicate::PredicateValue;

/// Where one operand of a set predicate resolves.
enum Operand<'a> {
    /// A document field, evaluated per row by the Data Plane.
    Doc(&'a str),
    /// A session value, resolved at plan time.
    Session(&'a PredicateValue),
    /// A constant.
    Constant(&'a PredicateValue),
}

fn classify(value: &PredicateValue) -> Operand<'_> {
    match value {
        PredicateValue::Field(name) => Operand::Doc(name),
        PredicateValue::AuthRef(_) | PredicateValue::AuthFunc { .. } => Operand::Session(value),
        PredicateValue::Literal(_) | PredicateValue::Instant { .. } => Operand::Constant(value),
    }
}

/// `set CONTAINS element`.
pub(super) fn substitute_contains(
    set: &PredicateValue,
    element: &PredicateValue,
    auth: &AuthContext,
) -> Option<Vec<ScanFilter>> {
    match (classify(set), classify(element)) {
        // `$auth.roles CONTAINS 'admin'`: decided at plan time.
        (Operand::Session(set), Operand::Constant(element)) => {
            let members = set.resolve(auth)?;
            let needle = element.resolve(auth)?;
            let passes = members.as_array()?.contains(&needle);
            Some(vec![verdict_filter(passes)])
        }
        // `doc_field CONTAINS <session value | constant>`: per row. The
        // document side is an array, so this is `array_contains`
        // (membership), not `contains` (substring).
        (Operand::Doc(field), Operand::Session(element) | Operand::Constant(element)) => {
            let needle = element.resolve_scan_value(auth)?;
            Some(vec![compare_filter(field, FilterOp::ArrayContains, needle)])
        }
        (Operand::Session(_), Operand::Session(_) | Operand::Doc(_))
        | (Operand::Doc(_), Operand::Doc(_))
        | (Operand::Constant(_), _) => None,
    }
}

/// `left INTERSECTS right`.
pub(super) fn substitute_intersects(
    left: &PredicateValue,
    right: &PredicateValue,
    auth: &AuthContext,
) -> Option<Vec<ScanFilter>> {
    match (classify(left), classify(right)) {
        // `doc_field INTERSECTS $auth.groups`, either orientation: per row.
        (Operand::Doc(field), Operand::Session(other))
        | (Operand::Session(other), Operand::Doc(field)) => {
            let members = other.resolve_scan_value(auth)?;
            Some(vec![compare_filter(field, FilterOp::ArrayOverlap, members)])
        }
        // `$auth.groups INTERSECTS $auth.allowed`: decided at plan time.
        (Operand::Session(left), Operand::Session(right)) => {
            let left = left.resolve(auth)?;
            let right = right.resolve(auth)?;
            let passes = match (left.as_array(), right.as_array()) {
                (Some(l), Some(r)) => l.iter().any(|v| r.contains(v)),
                (None, _) | (_, None) => false,
            };
            Some(vec![verdict_filter(passes)])
        }
        (Operand::Doc(_), Operand::Doc(_) | Operand::Constant(_))
        | (Operand::Session(_), Operand::Constant(_))
        | (Operand::Constant(_), _) => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::security::identity::{
        AuthMethod, AuthenticatedIdentity, DatabaseSet, Role,
    };
    use crate::types::TenantId;
    use nodedb_types::id::DatabaseId;

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
        AuthContext::from_identity(&identity, "s_test".into())
    }

    /// A session set that holds the constant admits every row; one that
    /// does not denies every row.
    #[test]
    fn session_set_contains_constant_is_decided_at_plan_time() {
        let auth = auth();
        let held = substitute_contains(
            &PredicateValue::AuthRef("roles".into()),
            &PredicateValue::Literal(serde_json::json!("readwrite")),
            &auth,
        )
        .expect("resolves");
        assert_eq!(held[0].op, FilterOp::MatchAll);

        let missing = substitute_contains(
            &PredicateValue::AuthRef("roles".into()),
            &PredicateValue::Literal(serde_json::json!("admin")),
            &auth,
        )
        .expect("resolves");
        assert_eq!(missing[0].op, FilterOp::IsNotNull);
        assert_eq!(missing[0].field, "__rls_deny__");
    }

    /// A session scalar where a set is required denies.
    #[test]
    fn session_scalar_as_set_denies() {
        let auth = auth();
        assert!(
            substitute_contains(
                &PredicateValue::AuthRef("username".into()),
                &PredicateValue::Literal(serde_json::json!("alice")),
                &auth,
            )
            .is_none()
        );
    }

    /// A constant on the set side has no filter form and denies.
    #[test]
    fn constant_set_denies() {
        let auth = auth();
        assert!(
            substitute_contains(
                &PredicateValue::Literal(serde_json::json!(["a"])),
                &PredicateValue::Field("tags".into()),
                &auth,
            )
            .is_none()
        );
        assert!(
            substitute_intersects(
                &PredicateValue::Literal(serde_json::json!(["a"])),
                &PredicateValue::Field("tags".into()),
                &auth,
            )
            .is_none()
        );
    }

    /// `doc_field INTERSECTS $auth.groups` lowers to `array_overlap` in
    /// either orientation.
    #[test]
    fn doc_field_intersects_session_set_in_either_orientation() {
        let auth = auth();
        let field = PredicateValue::Field("allowed".into());
        let session = PredicateValue::AuthRef("roles".into());
        for (l, r) in [(&field, &session), (&session, &field)] {
            let filters = substitute_intersects(l, r, &auth).expect("resolves");
            assert_eq!(filters[0].field, "allowed");
            assert_eq!(filters[0].op, FilterOp::ArrayOverlap);
        }
    }
}
