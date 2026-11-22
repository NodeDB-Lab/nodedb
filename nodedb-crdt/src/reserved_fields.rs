// SPDX-License-Identifier: Apache-2.0

//! Row fields the server owns on every CRDT collection.
//!
//! `_ts_system`, `_ts_valid_from` and `_ts_valid_until` name the audit
//! temporal columns. The server stamps them on its own history store. A
//! CRDT row that carried one would shadow the stamped value, so no row write
//! names one. Every local row write and every imported row checks here.

use nodedb_types::columnar::schema::TS_AUDIT_COLUMNS;

use crate::dead_letter::CompensationHint;
use crate::error::{CrdtError, Result};
use crate::validator::Violation;

/// Constraint name a reserved-field refusal carries.
pub const RESERVED_FIELD_CONSTRAINT: &str = "reserved_field";

/// The first reserved field among `names`, or `None`.
pub fn first_reserved_field<'a>(names: impl IntoIterator<Item = &'a str>) -> Option<&'a str> {
    names
        .into_iter()
        .find(|name| TS_AUDIT_COLUMNS.contains(name))
}

/// The refusal text for a write to `field` in `collection`.
fn refusal_detail(collection: &str, field: &str) -> String {
    format!(
        "field `{field}` in collection `{collection}` is reserved. The server owns \
         `_ts_system`, `_ts_valid_from` and `_ts_valid_until`. Remove the field and retry."
    )
}

/// Refuse a row write to `collection` whose field names include a reserved
/// field. The error is a constraint violation naming the field.
pub fn refuse_reserved_fields<'a>(
    collection: &str,
    names: impl IntoIterator<Item = &'a str>,
) -> Result<()> {
    match first_reserved_field(names) {
        Some(field) => Err(CrdtError::ConstraintViolation {
            constraint: RESERVED_FIELD_CONSTRAINT.to_string(),
            collection: collection.to_string(),
            detail: refusal_detail(collection, field),
        }),
        None => Ok(()),
    }
}

/// The violation a validated row raises when its field names include a
/// reserved field. Its hint is manual intervention, so no conflict policy
/// resolves it.
pub fn reserved_field_violation<'a>(
    collection: &str,
    names: impl IntoIterator<Item = &'a str>,
) -> Option<Violation> {
    let field = first_reserved_field(names)?;
    let reason = refusal_detail(collection, field);
    Some(Violation {
        constraint_name: RESERVED_FIELD_CONSTRAINT.to_string(),
        reason: reason.clone(),
        hint: CompensationHint::ManualIntervention { reason },
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_audit_column_is_refused() {
        for field in ["_ts_system", "_ts_valid_from", "_ts_valid_until"] {
            let err = refuse_reserved_fields("users", ["name", field]).unwrap_err();
            match err {
                CrdtError::ConstraintViolation {
                    constraint,
                    collection,
                    detail,
                } => {
                    assert_eq!(constraint, RESERVED_FIELD_CONSTRAINT);
                    assert_eq!(collection, "users");
                    assert!(detail.contains(field), "{detail}");
                }
                other => panic!("expected a constraint violation, got {other:?}"),
            }
        }
    }

    #[test]
    fn user_fields_pass() {
        refuse_reserved_fields("users", ["name", "email", "ts_system"]).unwrap();
        assert!(reserved_field_violation("users", ["name"]).is_none());
    }
}
