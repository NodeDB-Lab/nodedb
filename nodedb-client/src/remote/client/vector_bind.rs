// SPDX-License-Identifier: Apache-2.0

//! The parameterized `UPSERT` that writes one vector row over pgwire: the
//! row's key, its vector, and its metadata fields as columns.
//!
//! Each metadata field binds by the rules of `document_bind`: every typed
//! value binds under the OID of its exact type, so a Decimal keeps its
//! digits and scale, and a UUID, an instant or bytes stays typed, in any
//! field.

use nodedb_types::document::Document;
use nodedb_types::error::{NodeDbError, NodeDbResult};

use super::document_bind::{DocumentInsert, Statement, identity_field_check, row_statement};

/// The vector column a vector row writes, and its value.
pub(super) struct VectorColumn<'a> {
    pub name: &'a str,
    pub embedding: &'a [f32],
}

/// Build `UPSERT INTO <collection> (<key>, <vector>, <fields>) VALUES ($1,
/// ARRAY[...], ...)`: the row `doc.id` with `vector` and `doc`'s fields as
/// its metadata columns.
///
/// Each metadata field binds by the rules of `document_insert`. A field on
/// the identity column must name `doc.id`. A field on the vector column is
/// refused: it would overwrite the vector.
pub(super) fn vector_upsert(
    collection: &str,
    key: &str,
    vector: &VectorColumn<'_>,
    doc: &Document,
) -> NodeDbResult<DocumentInsert> {
    identity_field_check("vector_insert", collection, doc, key)?;
    if doc.fields.contains_key(vector.name) {
        return Err(NodeDbError::bad_request(format!(
            "vector_insert '{collection}'/'{}': metadata field '{}' is the vector column; \
             send the vector as the embedding, not as a field",
            doc.id, vector.name
        )));
    }
    row_statement(
        &Statement {
            verb: "UPSERT",
            op: "vector_insert",
            collection,
            key,
        },
        doc,
        Some(vector),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_types::value::Value;
    use tokio_postgres::types::Type;

    fn doc(fields: &[(&str, Value)]) -> Document {
        let mut doc = Document::new("d1");
        for (name, value) in fields {
            doc.set(*name, value.clone());
        }
        doc
    }

    /// A vector row binds its metadata fields typed after the key and the
    /// vector literal, as an `UPSERT`.
    #[test]
    fn a_vector_row_binds_typed_metadata_after_the_vector() {
        let upsert = vector_upsert(
            "vecs",
            "id",
            &VectorColumn {
                name: "embedding",
                embedding: &[1.0, 0.5],
            },
            &doc(&[
                ("price", Value::Decimal("12.50".parse().unwrap())),
                (
                    "uid",
                    Value::Uuid("550e8400-e29b-41d4-a716-446655440000".into()),
                ),
            ]),
        )
        .expect("typed metadata binds");
        assert_eq!(
            upsert.sql,
            "UPSERT INTO \"vecs\" (\"id\", \"embedding\", \"price\", \"uid\") \
             VALUES ($1, ARRAY[1,0.5], $2, $3)"
        );
        assert_eq!(upsert.types(), vec![Type::TEXT, Type::NUMERIC, Type::UUID]);
    }

    #[test]
    fn a_vector_row_refuses_a_field_on_the_vector_column_or_key() {
        let vector = VectorColumn {
            name: "embedding",
            embedding: &[1.0],
        };
        let err = vector_upsert(
            "vecs",
            "id",
            &vector,
            &doc(&[("embedding", Value::Integer(1))]),
        )
        .err()
        .expect("a field on the vector column");
        assert!(err.to_string().contains("'embedding'"), "{err}");
        let err = vector_upsert(
            "vecs",
            "id",
            &vector,
            &doc(&[("id", Value::String("other".into()))]),
        )
        .err()
        .expect("a key field naming another row");
        assert!(err.to_string().contains("vector_insert"), "{err}");
    }
}
