// SPDX-License-Identifier: Apache-2.0

//! The parameterized `INSERT` that writes one whole document over pgwire.
//!
//! Every top-level field is its own column, so SQL filters, projects and
//! text-searches the field by name. Every value is a bound parameter with a
//! declared type: the statement text carries quoted identifiers and
//! placeholders only. An array field binds as `ARRAY[$a, $b, ...]`.
//!
//! Each value binds in its PostgreSQL binary form under the OID of its exact
//! type, into any field, declared or not:
//! - A Decimal binds as `numeric`, its exact digits and scale.
//! - A UUID binds as `uuid`, an instant as `timestamptz` or `timestamp` by
//!   its kind, bytes as `bytea`.
//!
//! The server decodes each parameter by its OID to the typed value, so a
//! schemaless field stores it typed and a declared column coerces it
//! exactly.

use nodedb_types::document::Document;
use nodedb_types::error::{NodeDbError, NodeDbResult};
use nodedb_types::value::Value;
use tokio_postgres::types::{ToSql, Type};

use crate::remote_parse::format_vector_array;
use crate::sql_escape::quote_identifier;

use super::pg_param::{PgInstant, PgUuid};
use super::vector_bind::VectorColumn;

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

/// Refuse a document whose identity-column field names a different document.
///
/// `key` is the collection's identity column. A field under it that differs
/// from `Document::id` makes SQL and key reads disagree on which document the
/// row is.
pub(super) fn check_identity_field(
    collection: &str,
    doc: &Document,
    key: &str,
) -> NodeDbResult<()> {
    identity_field_check("document_put", collection, doc, key)
}

/// [`check_identity_field`] for the client operation `op`, named in the
/// refusal.
pub(super) fn identity_field_check(
    op: &str,
    collection: &str,
    doc: &Document,
    key: &str,
) -> NodeDbResult<()> {
    match doc.fields.get(key) {
        None => Ok(()),
        Some(Value::String(s)) if *s == doc.id => Ok(()),
        Some(other) => Err(NodeDbError::bad_request(format!(
            "{op} '{collection}'/'{}': field '{key}' holds {other:?}, which differs \
             from the document id; '{key}' is the collection's identity column; remove the \
             field or set it to the document id",
            doc.id
        ))),
    }
}

/// Build `INSERT INTO <collection> (<key>, <fields>) VALUES ($1, ...)`.
///
/// `key` is the collection's identity column and `$1` is the document id. A
/// field named `key` is that same column: the caller refuses one that
/// differs from the document id first. Fields bind in name order, so one
/// field set always yields one statement text.
pub(super) fn document_insert(
    collection: &str,
    key: &str,
    doc: &Document,
) -> NodeDbResult<DocumentInsert> {
    row_statement(
        &Statement {
            verb: "INSERT",
            op: "document_put",
            collection,
            key,
        },
        doc,
        None,
    )
}

/// The statement a row write builds.
pub(super) struct Statement<'a> {
    /// `INSERT` or `UPSERT`.
    pub verb: &'a str,
    /// The client operation, named in a refusal.
    pub op: &'a str,
    pub collection: &'a str,
    /// The collection's identity column.
    pub key: &'a str,
}

/// `<verb> INTO <collection> (<key>[, <vector>], <fields>) VALUES ($1[,
/// ARRAY[...]], ...)`, every field bound in name order.
pub(super) fn row_statement(
    statement: &Statement<'_>,
    doc: &Document,
    vector: Option<&VectorColumn<'_>>,
) -> NodeDbResult<DocumentInsert> {
    let Statement {
        verb,
        op,
        collection,
        key,
    } = *statement;
    let mut fields: Vec<(&String, &Value)> = doc
        .fields
        .iter()
        .filter(|(name, _)| name.as_str() != key)
        .collect();
    fields.sort_by(|a, b| a.0.cmp(b.0));

    let id_param: TypedParam = (Box::new(doc.id.clone()), Type::TEXT);
    let mut params: Vec<TypedParam> = vec![id_param];
    let mut columns = vec![quote_identifier(key)];
    let mut values = vec!["$1".to_string()];
    if let Some(vector) = vector {
        columns.push(quote_identifier(vector.name));
        values.push(format_vector_array(vector.embedding));
    }
    for (name, value) in fields {
        columns.push(quote_identifier(name));
        let field = Field {
            op,
            collection,
            doc_id: &doc.id,
            name,
        };
        values.push(bind_value(&field, value, &mut params)?);
    }

    Ok(DocumentInsert {
        sql: format!(
            "{verb} INTO {} ({}) VALUES ({})",
            quote_identifier(collection),
            columns.join(", "),
            values.join(", ")
        ),
        params,
    })
}

/// The field one value binds into.
struct Field<'a> {
    /// The client operation, named in a refusal.
    op: &'a str,
    collection: &'a str,
    doc_id: &'a str,
    name: &'a str,
}

impl Field<'_> {
    fn refuse(&self, value: &Value, why: &str) -> NodeDbError {
        NodeDbError::bad_request(format!(
            "{} '{}'/'{}': field '{}' holds {value:?}, {why}; \
             write this row with NativeClient",
            self.op, self.collection, self.doc_id, self.name
        ))
    }
}

/// The typed parameter of a scalar `value`, under the OID of its exact
/// type. `Ok(None)` for an array, an object, or any other non-scalar. `Err`
/// names why a scalar has no parameter form.
pub(super) fn scalar_param(value: &Value) -> Result<Option<TypedParam>, &'static str> {
    Ok(Some(match value {
        Value::Null => (Box::new(None::<String>), Type::TEXT),
        Value::Bool(b) => (Box::new(*b), Type::BOOL),
        Value::Integer(i) => (Box::new(*i), Type::INT8),
        Value::Float(f) => (Box::new(*f), Type::FLOAT8),
        Value::String(s) => (Box::new(s.clone()), Type::TEXT),
        // Binary `numeric`: the exact digits and scale.
        Value::Decimal(d) => (Box::new(*d), Type::NUMERIC),
        // Binary `uuid`: its 16 bytes.
        Value::Uuid(text) => {
            let uuid = PgUuid::parse(text).ok_or("which is not a valid UUID")?;
            (Box::new(uuid), Type::UUID)
        }
        // Binary `timestamptz` / `timestamp`: microseconds from 2000-01-01.
        Value::DateTime(at) => (Box::new(instant(at.micros)?), Type::TIMESTAMPTZ),
        Value::NaiveDateTime(at) => (Box::new(instant(at.micros)?), Type::TIMESTAMP),
        // Binary `bytea`: the raw bytes.
        Value::Bytes(bytes) => (Box::new(bytes.clone()), Type::BYTEA),
        _ => return Ok(None),
    }))
}

fn instant(micros: i64) -> Result<PgInstant, &'static str> {
    PgInstant::from_unix_micros(micros).ok_or("which is outside the PostgreSQL timestamp range")
}

/// Push `value`'s parameters and return the SQL expression that names them.
fn bind_value(
    field: &Field<'_>,
    value: &Value,
    params: &mut Vec<TypedParam>,
) -> NodeDbResult<String> {
    if let Some(param) = scalar_param(value).map_err(|why| field.refuse(value, why))? {
        params.push(param);
        return Ok(format!("${}", params.len()));
    }
    match value {
        Value::Array(items) => {
            let elements = items
                .iter()
                .map(|item| bind_value(field, item, params))
                .collect::<NodeDbResult<Vec<_>>>()?;
            Ok(format!("ARRAY[{}]", elements.join(", ")))
        }
        other => Err(field.refuse(
            other,
            "which pgwire SQL cannot store as a typed document field",
        )),
    }
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
            "id",
            &doc(&[
                ("body", Value::String("x".into())),
                ("n", Value::Integer(3)),
            ]),
        )
        .expect("scalar fields bind");
        assert_eq!(
            insert.sql,
            "INSERT INTO \"docs\" (\"id\", \"body\", \"n\") VALUES ($1, $2, $3)"
        );
        assert_eq!(insert.types(), vec![Type::TEXT, Type::TEXT, Type::INT8]);
        assert_eq!(insert.values().len(), 3);
    }

    #[test]
    fn an_array_field_binds_each_element() {
        let insert = document_insert(
            "docs",
            "id",
            &doc(&[(
                "tags",
                Value::Array(vec![Value::String("a".into()), Value::String("b".into())]),
            )]),
        )
        .expect("array of scalars binds");
        assert_eq!(
            insert.sql,
            "INSERT INTO \"docs\" (\"id\", \"tags\") VALUES ($1, ARRAY[$2, $3])"
        );
    }

    #[test]
    fn an_id_field_is_the_identity_column_not_a_second_one() {
        let insert = document_insert("docs", "id", &doc(&[("id", Value::String("d1".into()))]))
            .expect("matching id binds");
        assert_eq!(insert.sql, "INSERT INTO \"docs\" (\"id\") VALUES ($1)");
    }

    #[test]
    fn an_identity_field_naming_another_document_is_refused() {
        assert!(check_identity_field("c", &doc(&[]), "id").is_ok());
        assert!(
            check_identity_field("c", &doc(&[("id", Value::String("d1".into()))]), "id").is_ok()
        );
        let err = check_identity_field("c", &doc(&[("id", Value::String("d2".into()))]), "id")
            .expect_err("conflicting id");
        assert!(err.to_string().contains("d1"), "{err}");
    }

    #[test]
    fn a_declared_key_is_checked_and_id_is_an_ordinary_field() {
        let other_id = doc(&[("id", Value::String("other".into()))]);
        assert!(check_identity_field("c", &other_id, "sku").is_ok());
        let err = check_identity_field("c", &doc(&[("sku", Value::String("d2".into()))]), "sku")
            .expect_err("conflicting key");
        assert!(err.to_string().contains("'sku'"), "{err}");
    }

    #[test]
    fn a_declared_key_is_the_identity_column_and_id_is_a_field() {
        let insert = document_insert(
            "items",
            "sku",
            &doc(&[
                ("sku", Value::String("d1".into())),
                ("id", Value::String("other".into())),
            ]),
        )
        .expect("matching key binds");
        assert_eq!(
            insert.sql,
            "INSERT INTO \"items\" (\"sku\", \"id\") VALUES ($1, $2)"
        );
    }

    #[test]
    fn a_nested_object_is_refused_naming_the_field() {
        let Err(err) = document_insert(
            "docs",
            "id",
            &doc(&[("meta", Value::Object(std::collections::HashMap::new()))]),
        ) else {
            panic!("object fields have no SQL value form");
        };
        assert!(err.to_string().contains("meta"));
    }

    /// Every typed value binds under the OID of its exact type, into any
    /// field: a Decimal as `numeric`, a UUID as `uuid`, an instant by its
    /// kind, bytes as `bytea`.
    #[test]
    fn typed_values_bind_under_their_exact_type() {
        let at = nodedb_types::NdbDateTime::parse("2024-01-02T03:04:05Z").unwrap();
        let insert = document_insert(
            "docs",
            "id",
            &doc(&[
                ("price", Value::Decimal("12.50".parse().unwrap())),
                (
                    "uid",
                    Value::Uuid("550e8400-e29b-41d4-a716-446655440000".into()),
                ),
                ("seen", Value::DateTime(at)),
                ("naive", Value::NaiveDateTime(at)),
                ("blob", Value::Bytes(vec![0x01, 0x02, 0xff])),
            ]),
        )
        .expect("typed values bind");
        // Fields bind in name order: blob, naive, price, seen, uid.
        assert_eq!(
            insert.types(),
            vec![
                Type::TEXT,
                Type::BYTEA,
                Type::TIMESTAMP,
                Type::NUMERIC,
                Type::TIMESTAMPTZ,
                Type::UUID
            ]
        );
        assert_eq!(insert.values().len(), 6);
    }

    /// A UUID value whose text is no UUID, and an instant outside the
    /// PostgreSQL range, are refused naming the field.
    #[test]
    fn an_unencodable_typed_value_is_refused_naming_the_field() {
        for value in [
            Value::Uuid("not-a-uuid".into()),
            Value::DateTime(nodedb_types::NdbDateTime::from_micros(i64::MIN)),
        ] {
            let Err(err) = document_insert("docs", "id", &doc(&[("v", value)])) else {
                panic!("the value has no binary parameter form");
            };
            assert!(err.to_string().contains("'v'"), "{err}");
        }
    }
}
