// SPDX-License-Identifier: BUSL-1.1

//! A plan-time assignment to a document's identity column keeps its key.
//!
//! A document row stays stored under its key when a write rewrites it in
//! place, so its identity column must keep naming that key. A changed column
//! makes SQL reads and key reads name different documents. Changing a key is
//! a DELETE plus an INSERT under the new key.

use nodedb_sql::types::{SqlExpr, SqlValue};

use super::super::value::sql_value_to_string;

/// Refuse an assignment that moves the document stored under `doc_id` off its
/// key.
///
/// An assignment to `identity_column` keeps the key when its right-hand side
/// is the column itself or `EXCLUDED.<column>` (both hold the row's key), or a
/// literal that renders as `doc_id`. A NULL literal is a NOT NULL violation.
pub(super) fn check_assignments_keep_key(
    collection: &str,
    identity_column: &str,
    assignments: &[(String, SqlExpr)],
    doc_id: &str,
) -> crate::Result<()> {
    for (field, expr) in assignments {
        if !field.eq_ignore_ascii_case(identity_column) {
            continue;
        }
        let keeps_key = match expr {
            SqlExpr::Column { name, .. } => name.eq_ignore_ascii_case(identity_column),
            SqlExpr::Literal(SqlValue::Null) => {
                return Err(crate::Error::RejectedConstraint {
                    collection: collection.to_string(),
                    constraint: "not_null".to_string(),
                    detail: format!("primary key '{identity_column}' cannot be set to NULL"),
                });
            }
            SqlExpr::Literal(value) => sql_value_to_string(value) == doc_id,
            _ => false,
        };
        if !keeps_key {
            return Err(crate::Error::RejectedConstraint {
                collection: collection.to_string(),
                constraint: "primary_key_immutable".to_string(),
                detail: format!(
                    "cannot change primary key '{identity_column}' of document '{doc_id}' \
                     in '{collection}': the row stays stored under its current key; DELETE \
                     the row and INSERT it under the new key"
                ),
            });
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assign(rhs: SqlExpr) -> Vec<(String, SqlExpr)> {
        vec![("id".to_string(), rhs)]
    }

    fn constraint_of(result: crate::Result<()>) -> String {
        match result {
            Err(crate::Error::RejectedConstraint { constraint, .. }) => constraint,
            other => panic!("expected RejectedConstraint, got {other:?}"),
        }
    }

    #[test]
    fn an_assignment_naming_the_key_keeps_it() {
        for rhs in [
            SqlExpr::Column {
                table: Some("excluded".into()),
                name: "id".into(),
            },
            SqlExpr::Column {
                table: None,
                name: "id".into(),
            },
            SqlExpr::Literal(SqlValue::String("k1".into())),
        ] {
            check_assignments_keep_key("docs", "id", &assign(rhs), "k1")
                .expect("the assignment names the row's key");
        }
        let other_column = vec![(
            "v".to_string(),
            SqlExpr::Literal(SqlValue::String("x".into())),
        )];
        check_assignments_keep_key("docs", "id", &other_column, "k1")
            .expect("a non-key assignment is unchecked");
    }

    #[test]
    fn an_assignment_moving_the_key_is_refused() {
        let moved = assign(SqlExpr::Literal(SqlValue::String("k2".into())));
        assert_eq!(
            constraint_of(check_assignments_keep_key("docs", "id", &moved, "k1")),
            "primary_key_immutable"
        );
        let computed = assign(SqlExpr::Column {
            table: Some("excluded".into()),
            name: "v".into(),
        });
        assert_eq!(
            constraint_of(check_assignments_keep_key("docs", "id", &computed, "k1")),
            "primary_key_immutable"
        );
        let nulled = assign(SqlExpr::Literal(SqlValue::Null));
        assert_eq!(
            constraint_of(check_assignments_keep_key("docs", "id", &nulled, "k1")),
            "not_null"
        );
    }
}
