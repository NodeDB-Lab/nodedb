// SPDX-License-Identifier: BUSL-1.1

//! Classify how a target collection's primary key drives surrogate
//! assignment for a row written on its behalf (mirrors the plain-`INSERT`
//! identity path).

use nodedb_types::columnar::DocumentMode;
use nodedb_types::{CollectionType, Value};

use crate::control::security::catalog::StoredCollection;

/// How the target collection's primary key drives surrogate assignment for a
/// row inserted on its behalf (mirrors the plain-`INSERT` identity path).
pub(crate) enum TargetPk {
    /// Auto-generated `_rowid` (no declared PK): every inserted row gets a
    /// fresh, distinct surrogate.
    AutoRowId,
    /// A declared / built-in primary-key field: the fresh surrogate is
    /// content-addressed on this field's value so a later point-get /
    /// cross-engine resolve lands on the same identity. `declared` is true
    /// only for a DDL-declared `PRIMARY KEY`, which implies `NOT NULL`; an
    /// `id`-by-convention field with no declaration stays unenforced.
    Field { name: String, declared: bool },
}

/// Resolve how the target collection's primary key maps a written row to a
/// surrogate, mirroring the plain-`INSERT` identity path. `op_label` (e.g.
/// `"MERGE"`, `"INSERT ... SELECT"`, `"UPDATE ... FROM"`) customizes the
/// unsupported-target error message per caller.
pub(crate) fn resolve_target_pk(
    target: &StoredCollection,
    op_label: &str,
) -> crate::Result<TargetPk> {
    match &target.collection_type {
        CollectionType::Document(DocumentMode::Strict(schema)) => {
            match schema.columns.iter().find(|c| c.primary_key) {
                // Reached only via an explicit `PRIMARY KEY` column; `_rowid`
                // already routes to `AutoRowId` above.
                Some(col) if col.name == "_rowid" => Ok(TargetPk::AutoRowId),
                Some(col) => Ok(TargetPk::Field {
                    name: col.name.clone(),
                    declared: true,
                }),
                None => Ok(TargetPk::AutoRowId),
            }
        }
        CollectionType::Document(DocumentMode::Schemaless) => Ok(TargetPk::Field {
            name: target
                .declared_primary_key
                .clone()
                .unwrap_or_else(|| "id".to_string()),
            declared: target.declared_primary_key.is_some(),
        }),
        CollectionType::KeyValue(_) | CollectionType::Columnar(_) => Err(crate::Error::PlanError {
            detail: format!(
                "{op_label} target '{}' must be a document collection",
                target.name
            ),
        }),
    }
}

/// Extract a stringified primary-key value from a MessagePack row body.
pub(super) fn extract_pk_value(body: &[u8], field: &str) -> Option<String> {
    let Value::Object(obj) = nodedb_types::value_from_msgpack(body).ok()? else {
        return None;
    };
    value_to_pk_string(obj.get(field)?)
}

/// Stringify a scalar value into its primary-key byte form (mirrors the
/// `sql_value_to_string` convention used by the plain-INSERT identity path).
fn value_to_pk_string(v: &Value) -> Option<String> {
    match v {
        Value::String(s) => Some(s.clone()),
        Value::Integer(n) => Some(n.to_string()),
        Value::Float(f) => Some(f.to_string()),
        Value::Bool(b) => Some(b.to_string()),
        Value::Decimal(d) => Some(d.to_string()),
        _ => None,
    }
}
