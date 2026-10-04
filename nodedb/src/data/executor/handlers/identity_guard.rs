// SPDX-License-Identifier: BUSL-1.1

//! A document UPDATE keeps the row's identity column.
//!
//! A document row is stored under the key its identity column held at insert.
//! An UPDATE rewrites the body in place under that key, so a changed identity
//! column makes SQL reads (which read the column) and key reads (which read
//! the key) name different documents. Changing a document's key is a DELETE
//! plus an INSERT under the new key.

use nodedb_physical::physical_plan::UpdateValue;
use nodedb_types::DEFAULT_IDENTITY_COLUMN;
use nodedb_types::columnar::{SchemaOps, StrictSchema};

/// Whether `field` is a column a document row's identity is read from: a
/// strict schema's primary-key column, else a schemaless collection's declared
/// primary key, else `id`.
fn is_identity_column(
    strict_schema: Option<&StrictSchema>,
    declared_primary_key: Option<&str>,
    field: &str,
) -> bool {
    match strict_schema {
        Some(schema) => schema
            .columns()
            .iter()
            .any(|column| column.primary_key && column.name == field),
        None => field == declared_primary_key.unwrap_or(DEFAULT_IDENTITY_COLUMN),
    }
}

/// Whether `updates` assigns any identity column of the collection.
pub(in crate::data::executor) fn assigns_identity(
    strict_schema: Option<&StrictSchema>,
    declared_primary_key: Option<&str>,
    updates: &[(String, UpdateValue)],
) -> bool {
    updates
        .iter()
        .any(|(field, _)| is_identity_column(strict_schema, declared_primary_key, field))
}

/// The pre-update values of the identity columns an UPDATE assigns. Empty,
/// and allocation-free, when the UPDATE assigns none.
pub(in crate::data::executor) struct IdentitySnapshot {
    columns: Vec<(String, Option<serde_json::Value>)>,
    /// The identity is a declared primary key, whose NOT NULL rule refuses a
    /// NULL or omitted post-image value with its own constraint.
    declared_key: bool,
}

impl IdentitySnapshot {
    /// Record the identity columns `updates` assigns, as `before` holds them.
    pub(in crate::data::executor) fn capture(
        strict_schema: Option<&StrictSchema>,
        declared_primary_key: Option<&str>,
        updates: &[(String, UpdateValue)],
        before: &serde_json::Value,
    ) -> Self {
        let columns = updates
            .iter()
            .filter(|(field, _)| is_identity_column(strict_schema, declared_primary_key, field))
            .map(|(field, _)| (field.clone(), before.get(field).cloned()))
            .collect();
        Self {
            columns,
            declared_key: strict_schema.is_some() || declared_primary_key.is_some(),
        }
    }

    /// Refuse `after` when an assigned identity column differs from its
    /// captured value. A NULL or omitted declared key is left to the NOT NULL
    /// rule, so that refusal keeps its own constraint.
    pub(in crate::data::executor) fn check_unchanged(
        &self,
        collection: &str,
        after: &serde_json::Value,
    ) -> crate::Result<()> {
        for (column, before) in &self.columns {
            let value = after.get(column);
            if self.declared_key && matches!(value, None | Some(serde_json::Value::Null)) {
                continue;
            }
            if before.as_ref() != value {
                return Err(identity_change_refused(collection, column));
            }
        }
        Ok(())
    }
}

fn identity_change_refused(collection: &str, column: &str) -> crate::Error {
    crate::Error::RejectedConstraint {
        collection: collection.to_string(),
        constraint: "primary_key_immutable".to_string(),
        detail: format!(
            "UPDATE cannot change primary key '{column}' of collection '{collection}': \
             the row stays stored under its current key; DELETE the row and INSERT it \
             under the new key"
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_types::columnar::{ColumnDef, ColumnType};
    use serde_json::json;

    fn literal(value: &nodedb_types::Value) -> UpdateValue {
        UpdateValue::Literal(nodedb_types::value_to_msgpack(value).expect("encode"))
    }

    fn set(field: &str) -> Vec<(String, UpdateValue)> {
        vec![(
            field.to_string(),
            literal(&nodedb_types::Value::String("x".into())),
        )]
    }

    fn strict() -> StrictSchema {
        StrictSchema::new(vec![
            ColumnDef::required("k", ColumnType::String).with_primary_key(),
            ColumnDef::nullable("v", ColumnType::String),
        ])
        .expect("schema")
    }

    #[test]
    fn schemaless_identity_is_id_unless_a_key_is_declared() {
        assert!(assigns_identity(None, None, &set("id")));
        assert!(!assigns_identity(None, None, &set("v")));
        assert!(assigns_identity(None, Some("sku"), &set("sku")));
        assert!(!assigns_identity(None, Some("sku"), &set("id")));
    }

    #[test]
    fn strict_identity_is_the_schema_primary_key() {
        let schema = strict();
        assert!(assigns_identity(Some(&schema), None, &set("k")));
        assert!(!assigns_identity(Some(&schema), None, &set("v")));
    }

    #[test]
    fn a_changed_identity_is_refused() {
        let updates = set("id");
        let snapshot = IdentitySnapshot::capture(None, None, &updates, &json!({"id": "a"}));
        match snapshot.check_unchanged("docs", &json!({"id": "b"})) {
            Err(crate::Error::RejectedConstraint {
                constraint, detail, ..
            }) => {
                assert_eq!(constraint, "primary_key_immutable");
                assert!(detail.contains("'id'"), "names the column: {detail}");
            }
            other => panic!("expected RejectedConstraint, got {other:?}"),
        }
    }

    #[test]
    fn an_identity_newly_written_to_an_id_less_body_is_refused() {
        let updates = set("id");
        let snapshot = IdentitySnapshot::capture(None, None, &updates, &json!({"v": 1}));
        assert!(
            snapshot
                .check_unchanged("docs", &json!({"id": "b", "v": 1}))
                .is_err()
        );
    }

    #[test]
    fn the_same_identity_is_accepted() {
        let updates = set("id");
        let snapshot = IdentitySnapshot::capture(None, None, &updates, &json!({"id": "a"}));
        snapshot
            .check_unchanged("docs", &json!({"id": "a", "v": 2}))
            .expect("assigning the current key keeps the identity");
    }

    #[test]
    fn a_null_declared_key_is_left_to_the_not_null_rule() {
        let schema = strict();
        let updates = set("k");
        let snapshot = IdentitySnapshot::capture(Some(&schema), None, &updates, &json!({"k": "a"}));
        snapshot
            .check_unchanged("docs", &json!({"k": null}))
            .expect("the NOT NULL rule owns a NULL declared key");
        let updates = set("id");
        let snapshot = IdentitySnapshot::capture(None, None, &updates, &json!({"id": "a"}));
        assert!(
            snapshot
                .check_unchanged("docs", &json!({"id": null}))
                .is_err(),
            "an undeclared `id` has no NOT NULL rule, so NULL is a changed identity"
        );
    }

    #[test]
    fn an_update_of_other_columns_is_never_checked() {
        let updates = set("v");
        let snapshot = IdentitySnapshot::capture(None, None, &updates, &json!({"id": "a"}));
        snapshot
            .check_unchanged("docs", &json!({"id": "changed-elsewhere"}))
            .expect("only assigned identity columns are checked");
    }
}
