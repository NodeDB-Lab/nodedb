// SPDX-License-Identifier: Apache-2.0

//! The identity column of a collection, read from the server catalog.
//!
//! The SQL the remote client generates names the key column: the declared
//! primary key, else `id`. The client reads it from `DESCRIBE` on every
//! call that needs it, so a dropped and recreated collection never answers
//! with a stale key.

use nodedb_types::error::NodeDbResult;
use nodedb_types::value::Value;
use tokio_postgres::types::Type;

use crate::document_identity::identity_column_from_describe;
use crate::sql_escape::quote_identifier;

use super::core::NodeDbRemote;

impl NodeDbRemote {
    /// The identity column of `collection`.
    pub(super) async fn identity_column(&self, collection: &str) -> NodeDbResult<String> {
        let (columns, rows) = self.describe(collection).await?;
        identity_column_from_describe(collection, &columns, &rows, pg_text_bool)
    }

    /// The `DESCRIBE` result of `collection`: one row per field, with its
    /// declared type and key marker.
    pub(super) async fn describe(
        &self,
        collection: &str,
    ) -> NodeDbResult<(Vec<String>, Vec<Vec<Value>>)> {
        let sql = format!("DESCRIBE {}", quote_identifier(collection));
        self.simple_query_raw(&sql).await
    }
}

/// `DESCRIBE` output column naming each field.
const DESCRIBE_FIELD_COLUMN: &str = "field";
/// `DESCRIBE` output column holding each field's declared type.
const DESCRIBE_TYPE_COLUMN: &str = "type";

/// The declared fields whose values JSON and SQL text carry as strings, each
/// with the PG type that names its exact form.
pub(super) type DeclaredTypes = Vec<(String, Type)>;

/// The declared fields of a `DESCRIBE` result of type `DECIMAL` / `NUMERIC`,
/// `UUID`, `TIMESTAMP`, `TIMESTAMPTZ` or `BYTEA`. A result without the
/// `field` or `type` column declares none.
///
/// A read decodes such a field from its exact text: a JSON row read
/// carries each of these types as a string.
pub(super) fn declared_types(columns: &[String], rows: &[Vec<Value>]) -> DeclaredTypes {
    let position = |name: &str| columns.iter().position(|c| c == name);
    let (Some(field_idx), Some(type_idx)) = (
        position(DESCRIBE_FIELD_COLUMN),
        position(DESCRIBE_TYPE_COLUMN),
    ) else {
        return Vec::new();
    };
    rows.iter()
        .filter_map(|row| match (row.get(field_idx), row.get(type_idx)) {
            (Some(Value::String(field)), Some(Value::String(declared))) => {
                text_typed(declared).map(|ty| (field.clone(), ty))
            }
            _ => None,
        })
        .collect()
}

/// The PG type of a declared column type whose values travel as text.
/// `None` for every other declared type: its value carries its own kind.
fn text_typed(declared: &str) -> Option<Type> {
    let upper = declared.trim().to_ascii_uppercase();
    let name = upper
        .split(|c: char| c == '(' || c.is_whitespace())
        .next()
        .unwrap_or("");
    match name {
        "DECIMAL" | "NUMERIC" => Some(Type::NUMERIC),
        "UUID" => Some(Type::UUID),
        "TIMESTAMPTZ" => Some(Type::TIMESTAMPTZ),
        "TIMESTAMP" if upper.contains("WITH TIME ZONE") => Some(Type::TIMESTAMPTZ),
        "TIMESTAMP" => Some(Type::TIMESTAMP),
        "BYTEA" | "BYTES" => Some(Type::BYTEA),
        _ => None,
    }
}

/// The key column named by a `DESCRIBE` result read over the simple-query
/// protocol.
pub(super) fn identity_column_of(
    collection: &str,
    columns: &[String],
    rows: &[Vec<Value>],
) -> NodeDbResult<String> {
    identity_column_from_describe(collection, columns, rows, pg_text_bool)
}

/// A `bool` cell in the PostgreSQL text format the simple-query protocol
/// carries: `t` or `f`.
fn pg_text_bool(cell: &Value) -> Option<bool> {
    match cell {
        Value::String(s) if s == "t" => Some(true),
        Value::String(s) if s == "f" => Some(false),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_pg_text_bools_decode() {
        assert_eq!(pg_text_bool(&Value::String("t".into())), Some(true));
        assert_eq!(pg_text_bool(&Value::String("f".into())), Some(false));
        assert_eq!(pg_text_bool(&Value::String("true".into())), None);
        assert_eq!(pg_text_bool(&Value::Bool(true)), None);
    }
}
