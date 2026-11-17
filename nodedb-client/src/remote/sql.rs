// SPDX-License-Identifier: Apache-2.0

//! SQL and parameter translation seams for the remote client.
//!
//! `build_vector_search_sql` renders `MetadataFilter` into a `WHERE`
//! clause and emits the SELECT form the SEARCH preprocessor would
//! produce, so the filter is applied before `vector_distance` ordering.

use std::collections::HashSet;

use nodedb_types::error::{NodeDbError, NodeDbResult};
use nodedb_types::filter::MetadataFilter;
use nodedb_types::value::Value;

use crate::remote_parse::format_vector_array;
use crate::row_decode::search_hit::{DISTANCE_COLUMN, ID_COLUMN};
use crate::search_sql::key_filter::key_in_list;
use crate::sql_escape::{quote_identifier, quote_string_literal};

/// Build the SQL for a vector search.
///
/// Always emits the canonical
/// `SELECT id, vector_distance(ARRAY[...]) AS distance FROM <coll>[ WHERE <pred>]
/// ORDER BY vector_distance(ARRAY[...]) LIMIT <k>` shape. The optional
/// `WHERE` clause precedes `ORDER BY`. The projection names the two columns
/// a hit decodes from.
///
/// `allowed_ids` pairs the allowed ids with the collection's identity
/// column. It adds a top-level `<key> IN (...)` conjunct. The server lowers
/// that key conjunct to the candidate bitmap the index search honors, so the
/// top-k is drawn from the allowed ids only.
pub(super) fn build_vector_search_sql(
    collection: &str,
    query: &[f32],
    k: usize,
    filter: Option<&MetadataFilter>,
    allowed_ids: Option<(&str, &HashSet<String>)>,
) -> NodeDbResult<String> {
    let collection = quote_identifier(collection);
    let mut conjuncts: Vec<String> = Vec::new();
    if let Some(f) = filter {
        conjuncts.push(render_metadata_filter(f)?);
    }
    if let Some((key, ids)) = allowed_ids {
        conjuncts.push(key_in_list(key, ids));
    }
    let where_clause = if conjuncts.is_empty() {
        String::new()
    } else {
        format!(" WHERE {}", conjuncts.join(" AND "))
    };
    let query = format_vector_array(query);
    Ok(format!(
        "SELECT {ID_COLUMN}, vector_distance({query}) AS {DISTANCE_COLUMN} \
         FROM {collection}{where_clause} ORDER BY vector_distance({query}) LIMIT {k}",
    ))
}

/// Crate-internal accessor so peer modules (e.g. the field-aware
/// `vector_search_field` override in `client.rs`) can reuse the same
/// `MetadataFilter` → SQL renderer without duplicating the variant
/// match. Kept private to the `remote` module — outside callers should
/// drive vector_search through the trait, not poke at the renderer.
pub(super) fn render_metadata_filter_public(filter: &MetadataFilter) -> NodeDbResult<String> {
    render_metadata_filter(filter)
}

/// Render a `MetadataFilter` tree into a SQL boolean expression.
///
/// Field names go through `quote_identifier` so reserved words and
/// non-ASCII identifiers survive the round-trip. Values are rendered
/// as SQL literals — strings get single-quote escaping; numeric and
/// boolean values are formatted directly. Variants the SQL boundary
/// cannot represent as a literal (binary, datetime, vector, nested
/// objects) return `Err` rather than producing malformed SQL.
fn render_metadata_filter(filter: &MetadataFilter) -> NodeDbResult<String> {
    match filter {
        MetadataFilter::Eq { field, value } => Ok(format!(
            "{} = {}",
            quote_identifier(field),
            render_sql_literal(value)?
        )),
        MetadataFilter::Ne { field, value } => Ok(format!(
            "{} <> {}",
            quote_identifier(field),
            render_sql_literal(value)?
        )),
        MetadataFilter::Gt { field, value } => Ok(format!(
            "{} > {}",
            quote_identifier(field),
            render_sql_literal(value)?
        )),
        MetadataFilter::Gte { field, value } => Ok(format!(
            "{} >= {}",
            quote_identifier(field),
            render_sql_literal(value)?
        )),
        MetadataFilter::Lt { field, value } => Ok(format!(
            "{} < {}",
            quote_identifier(field),
            render_sql_literal(value)?
        )),
        MetadataFilter::Lte { field, value } => Ok(format!(
            "{} <= {}",
            quote_identifier(field),
            render_sql_literal(value)?
        )),
        MetadataFilter::In { field, values } => {
            let rendered: NodeDbResult<Vec<_>> = values.iter().map(render_sql_literal).collect();
            Ok(format!(
                "{} IN ({})",
                quote_identifier(field),
                rendered?.join(", ")
            ))
        }
        MetadataFilter::NotIn { field, values } => {
            let rendered: NodeDbResult<Vec<_>> = values.iter().map(render_sql_literal).collect();
            Ok(format!(
                "{} NOT IN ({})",
                quote_identifier(field),
                rendered?.join(", ")
            ))
        }
        MetadataFilter::And(parts) => {
            if parts.is_empty() {
                return Ok("TRUE".into());
            }
            let rendered: NodeDbResult<Vec<_>> = parts.iter().map(render_metadata_filter).collect();
            Ok(format!("({})", rendered?.join(" AND ")))
        }
        MetadataFilter::Or(parts) => {
            if parts.is_empty() {
                return Ok("FALSE".into());
            }
            let rendered: NodeDbResult<Vec<_>> = parts.iter().map(render_metadata_filter).collect();
            Ok(format!("({})", rendered?.join(" OR ")))
        }
        MetadataFilter::Not(inner) => Ok(format!("NOT ({})", render_metadata_filter(inner)?)),
        // `MetadataFilter` is `#[non_exhaustive]`. Surface unknown
        // variants as a typed error so a future addition surfaces here
        // as a coverage gap rather than silently rendering nothing.
        _ => Err(NodeDbError::storage(
            "metadata filter variant not yet supported by SQL renderer",
        )),
    }
}

/// Render a single `Value` as a SQL literal, typed as the value is.
///
/// Strings are single-quote escaped. A decimal keeps its exact digits and
/// scale. A UUID, an instant and bytes render as their typed literal
/// (`UUID '...'`, `TIMESTAMPTZ '...'`, `TIMESTAMP '...'`, `BYTEA '\x...'`),
/// an array as `ARRAY[...]` of its elements, a duration as
/// `'<n>us'::DURATION`, a ULID as its text. Returns
/// `Err` for a non-finite float and for variants no SQL literal represents.
fn render_sql_literal(v: &Value) -> NodeDbResult<String> {
    let typed = |type_name: &str, text: &str| format!("{type_name} {}", quote_string_literal(text));
    match v {
        Value::Null => Ok("NULL".into()),
        Value::Bool(b) => Ok(if *b { "TRUE".into() } else { "FALSE".into() }),
        Value::Integer(i) => Ok(i.to_string()),
        Value::Float(f) if f.is_finite() => Ok(f.to_string()),
        Value::String(s) | Value::Ulid(s) => Ok(quote_string_literal(s)),
        Value::Decimal(d) => Ok(d.to_string()),
        Value::Uuid(u) => Ok(typed("UUID", u)),
        Value::DateTime(at) => Ok(typed("TIMESTAMPTZ", &at.to_iso8601())),
        Value::NaiveDateTime(at) => Ok(typed("TIMESTAMP", &at.to_iso8601())),
        Value::Bytes(b) => Ok(typed(
            "BYTEA",
            &nodedb_types::bytea_text::format_bytea_hex(b),
        )),
        Value::Array(items) => {
            let rendered: NodeDbResult<Vec<_>> = items.iter().map(render_sql_literal).collect();
            Ok(format!("ARRAY[{}]", rendered?.join(", ")))
        }
        Value::Duration(d) => Ok(format!("'{}us'::DURATION", d.micros)),
        other => Err(NodeDbError::storage(format!(
            "metadata filter value of type {} cannot be rendered as a SQL literal",
            other.type_name()
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn vector_search_sql_without_filter_renders_basic_form() {
        let sql = build_vector_search_sql("docs", &[0.1, 0.2, 0.3], 5, None, None)
            .expect("no-filter is fine");
        assert_eq!(
            sql,
            "SELECT id, vector_distance(ARRAY[0.1,0.2,0.3]) AS distance FROM \"docs\" \
             ORDER BY vector_distance(ARRAY[0.1,0.2,0.3]) LIMIT 5"
        );
    }

    #[test]
    fn vector_search_sql_renders_eq_metadata_filter() {
        // Spec: a non-None Eq filter renders into a server-side predicate
        // referencing both the field and the value.
        let filter = MetadataFilter::eq("category", Value::String("ai".into()));
        let sql = build_vector_search_sql("docs", &[0.1, 0.2], 5, Some(&filter), None)
            .expect("non-None metadata filter must be accepted client-side, not rejected");
        assert!(
            sql.contains("category"),
            "rendered SQL must reference the filtered field name; got: {sql}"
        );
        assert!(
            sql.contains("'ai'"),
            "rendered SQL must reference the filtered value; got: {sql}"
        );
        // Regression guard: helper must not return a rejection message.
        assert!(
            !sql.contains("not yet supported"),
            "rendered SQL must not be a rejection message; got: {sql}"
        );
        // `WHERE` precedes `ORDER BY` for valid SQL.
        let where_idx = sql.find(" WHERE ").expect("WHERE clause emitted");
        let order_idx = sql.find(" ORDER BY ").expect("ORDER BY clause emitted");
        assert!(
            where_idx < order_idx,
            "WHERE must precede ORDER BY; got: {sql}"
        );
    }

    #[test]
    fn vector_search_sql_renders_compound_metadata_filter() {
        // Spec: AND-of-filters renders both predicates.
        let filter = MetadataFilter::and(vec![
            MetadataFilter::eq("category", Value::String("ai".into())),
            MetadataFilter::Gt {
                field: "score".into(),
                value: Value::Float(0.5),
            },
        ]);
        let sql = build_vector_search_sql("docs", &[0.1], 3, Some(&filter), None)
            .expect("compound metadata filter must be rendered, not rejected");
        assert!(
            sql.contains("category"),
            "missing AND-leaf field; got: {sql}"
        );
        assert!(sql.contains("score"), "missing AND-leaf field; got: {sql}");
        assert!(
            sql.contains(" AND "),
            "compound must render with AND; got: {sql}"
        );
    }

    #[test]
    fn vector_search_sql_renders_in_filter() {
        let filter = MetadataFilter::In {
            field: "tag".into(),
            values: vec![
                Value::String("rust".into()),
                Value::String("databases".into()),
            ],
        };
        let sql = build_vector_search_sql("docs", &[0.0], 1, Some(&filter), None).unwrap();
        assert!(sql.contains(" IN ("));
        assert!(sql.contains("'rust'"));
        assert!(sql.contains("'databases'"));
    }

    #[test]
    fn vector_search_sql_restricts_to_allowed_ids() {
        let ids: HashSet<String> = ["b", "a'"].iter().map(|s| s.to_string()).collect();
        let sql = build_vector_search_sql("docs", &[0.0], 2, None, Some(("id", &ids))).unwrap();
        assert!(
            sql.contains(" WHERE \"id\" IN ('a''', 'b') ORDER BY "),
            "allowed ids render as a key conjunct; got: {sql}"
        );
        let filter = MetadataFilter::eq("category", Value::String("ai".into()));
        let sql =
            build_vector_search_sql("docs", &[0.0], 2, Some(&filter), Some(("sku", &ids))).unwrap();
        assert!(
            sql.contains(" WHERE \"category\" = 'ai' AND \"sku\" IN ('a''', 'b') ORDER BY "),
            "the key conjunct names the declared key and joins the filter at the top level; \
             got: {sql}"
        );
    }

    #[test]
    fn render_sql_literal_escapes_single_quotes() {
        let sql = render_sql_literal(&Value::String("o'reilly".into())).unwrap();
        assert_eq!(sql, "'o''reilly'");
    }

    #[test]
    fn render_sql_literal_keeps_typed_values_typed() {
        let price = Value::Decimal("12.50".parse().expect("decimal"));
        assert_eq!(render_sql_literal(&price).unwrap(), "12.50");
        let uid = Value::Uuid("550e8400-e29b-41d4-a716-446655440000".into());
        assert_eq!(
            render_sql_literal(&uid).unwrap(),
            "UUID '550e8400-e29b-41d4-a716-446655440000'"
        );
        let array = Value::Array(vec![Value::Integer(1), Value::Integer(2)]);
        assert_eq!(render_sql_literal(&array).unwrap(), "ARRAY[1, 2]");
    }

    #[test]
    fn render_sql_literal_rejects_unsupported_variants() {
        assert!(render_sql_literal(&Value::Object(Default::default())).is_err());
        assert!(render_sql_literal(&Value::Float(f64::NAN)).is_err());
    }
}
