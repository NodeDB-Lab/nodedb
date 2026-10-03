// SPDX-License-Identifier: Apache-2.0

//! The parameterized `INSERT` that writes one whole document over pgwire.
//!
//! Every top-level field is its own column, so SQL filters, projects and
//! text-searches the field by name. Every value is a bound parameter with a
//! declared type: the statement text carries quoted identifiers and
//! placeholders only. An array field binds as `ARRAY[$a, $b, ...]`.

use nodedb_types::DEFAULT_IDENTITY_COLUMN;
use nodedb_types::document::Document;
use nodedb_types::error::{NodeDbError, NodeDbResult};
use nodedb_types::value::Value;
use tokio_postgres::types::{ToSql, Type};

use crate::sql_escape::quote_identifier;

/// One bound parameter and the type the statement declares for it.
pub(super) type TypedParam = (Box<dyn ToSql + Sync + Send>, Type);

/// A whole-document `INSERT` and its typed parameters, in placeholder order.
pub(super) struct DocumentInsert {
    pub sql: String,
    pub params: Vec<TypedParam>,
}

impl DocumentInsert {
    /// The statement's declared parameter types, in placeholder order.
    pub fn types(&self) -> Vec<Type> {
        self.params.iter().map(|(_, ty)| ty.clone()).collect()
    }

    /// The parameters as the driver borrows them, in placeholder order.
    pub fn values(&self) -> Vec<&(dyn ToSql + Sync)> {
        self.params
            .iter()
            .map(|(value, _)| {
                let value: &(dyn ToSql + Sync) = value.as_ref();
                value
            })
            .collect()
    }
}

/// Build `INSERT INTO <collection> (id, <fields>) VALUES ($1, ...)`.
///
/// `$1` is the document id. A field named `id` is that same column: the
/// caller refuses one that differs from the document id first. Fields bind
/// in name order, so one field set always yields one statement text.
pub(super) fn document_insert(collection: &str, doc: &Document) -> NodeDbResult<DocumentInsert> {
    let mut fields: Vec<(&String, &Value)> = doc
        .fields
        .iter()
        .filter(|(name, _)| name.as_str() != DEFAULT_IDENTITY_COLUMN)
        .collect();
    fields.sort_by(|a, b| a.0.cmp(b.0));

    let id_param: TypedParam = (Box::new(doc.id.clone()), Type::TEXT);
    let mut params: Vec<TypedParam> = vec![id_param];
    let mut columns = vec![DEFAULT_IDENTITY_COLUMN.to_string()];
    let mut values = vec!["$1".to_string()];
    for (name, value) in fields {
        columns.push(quote_identifier(name));
        values.push(bind_value(collection, &doc.id, name, value, &mut params)?);
    }

    Ok(DocumentInsert {
        sql: format!(
            "INSERT INTO {} ({}) VALUES ({})",
            quote_identifier(collection),
            columns.join(", "),
            values.join(", ")
        ),
        params,
    })
}

/// Push `value`'s parameters and return the SQL expression that names them.
fn bind_value(
    collection: &str,
    doc_id: &str,
    field: &str,
    value: &Value,
    params: &mut Vec<TypedParam>,
) -> NodeDbResult<String> {
    let param: TypedParam = match value {
        Value::Null => (Box::new(None::<String>), Type::TEXT),
        Value::Bool(b) => (Box::new(*b), Type::BOOL),
        Value::Integer(i) => (Box::new(*i), Type::INT8),
        Value::Float(f) => (Box::new(*f), Type::FLOAT8),
        Value::String(s) => (Box::new(s.clone()), Type::TEXT),
        Value::Array(items) => {
            let elements = items
                .iter()
                .map(|item| bind_value(collection, doc_id, field, item, params))
                .collect::<NodeDbResult<Vec<_>>>()?;
            return Ok(format!("ARRAY[{}]", elements.join(", ")));
        }
        other => {
            return Err(NodeDbError::bad_request(format!(
                "document_put '{collection}'/'{doc_id}': field '{field}' holds {other:?}, \
                 which pgwire SQL cannot store as a typed document field; \
                 write this document with NativeClient"
            )));
        }
    };
    params.push(param);
    Ok(format!("${}", params.len()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn doc(fields: &[(&str, Value)]) -> Document {
        let mut doc = Document::new("d1");
        for (name, value) in fields {
            doc.set(*name, value.clone());
        }
        doc
    }

    #[test]
    fn every_field_is_its_own_bound_column() {
        let insert = document_insert(
            "docs",
            &doc(&[
                ("body", Value::String("x".into())),
                ("n", Value::Integer(3)),
            ]),
        )
        .expect("scalar fields bind");
        assert_eq!(
            insert.sql,
            "INSERT INTO \"docs\" (id, \"body\", \"n\") VALUES ($1, $2, $3)"
        );
        assert_eq!(insert.types(), vec![Type::TEXT, Type::TEXT, Type::INT8]);
        assert_eq!(insert.values().len(), 3);
    }

    #[test]
    fn an_array_field_binds_each_element() {
        let insert = document_insert(
            "docs",
            &doc(&[(
                "tags",
                Value::Array(vec![Value::String("a".into()), Value::String("b".into())]),
            )]),
        )
        .expect("array of scalars binds");
        assert_eq!(
            insert.sql,
            "INSERT INTO \"docs\" (id, \"tags\") VALUES ($1, ARRAY[$2, $3])"
        );
    }

    #[test]
    fn an_id_field_is_the_identity_column_not_a_second_one() {
        let insert = document_insert("docs", &doc(&[("id", Value::String("d1".into()))]))
            .expect("matching id binds");
        assert_eq!(insert.sql, "INSERT INTO \"docs\" (id) VALUES ($1)");
    }

    #[test]
    fn a_nested_object_is_refused_naming_the_field() {
        let Err(err) = document_insert(
            "docs",
            &doc(&[("meta", Value::Object(std::collections::HashMap::new()))]),
        ) else {
            panic!("object fields have no SQL value form");
        };
        assert!(err.to_string().contains("meta"));
    }
}
