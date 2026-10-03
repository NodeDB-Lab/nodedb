// SPDX-License-Identifier: Apache-2.0

//! SQL and parameter translation seams for the remote client.
//!
//! `build_vector_search_sql` renders `MetadataFilter` into a `WHERE`
//! clause and emits the SELECT form the SEARCH preprocessor would
//! produce, so the filter is applied before `vector_distance` ordering.
//! `translate_params` boxes each `&[Value]` parameter into a
//! `tokio_postgres::types::ToSql`-compatible owned value for the
//! pgwire driver; the trait method `execute_sql` borrows them as
//! `&[&(dyn ToSql + Sync)]` for the actual `Client::query` call.

use std::collections::HashSet;

use nodedb_types::error::{NodeDbError, NodeDbResult};
use nodedb_types::filter::MetadataFilter;
use nodedb_types::value::Value;

use crate::remote_parse::format_vector_array;
use crate::sql_escape::{quote_identifier, quote_string_literal};

/// Build the SQL for a vector search.
///
/// Always emits the canonical
/// `SELECT * FROM <coll>[ WHERE <pred>] ORDER BY vector_distance(ARRAY[...]) LIMIT <k>`
/// shape so the optional `WHERE` clause precedes `ORDER BY` (the SEARCH
/// preprocessor's "trailing append" form would have placed `WHERE` after
/// `ORDER BY`, which is invalid SQL).
///
/// `allowed_ids` adds a top-level `id IN (...)` conjunct. The server lowers
/// that key conjunct to the candidate bitmap the index search honors, so the
/// top-k is drawn from the allowed ids only.
pub(super) fn build_vector_search_sql(
    collection: &str,
    query: &[f32],
    k: usize,
    filter: Option<&MetadataFilter>,
    allowed_ids: Option<&HashSet<String>>,
) -> NodeDbResult<String> {
    let collection = quote_identifier(collection);
    let mut conjuncts: Vec<String> = Vec::new();
    if let Some(f) = filter {
        conjuncts.push(render_metadata_filter(f)?);
    }
    if let Some(ids) = allowed_ids {
        let mut ids: Vec<&String> = ids.iter().collect();
        ids.sort();
        let list: Vec<String> = ids
            .iter()
            .map(|id| quote_string_literal(id.as_str()))
            .collect();
        conjuncts.push(format!("id IN ({})", list.join(", ")));
    }
    let where_clause = if conjuncts.is_empty() {
        String::new()
    } else {
        format!(" WHERE {}", conjuncts.join(" AND "))
    };
    Ok(format!(
        "SELECT * FROM {collection}{where_clause} ORDER BY vector_distance({}) LIMIT {k}",
        format_vector_array(query),
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

/// Render a single `Value` as a SQL literal. Strings are single-quote
/// escaped; numeric / boolean / null variants are formatted directly.
/// Returns `Err` for variants the SQL literal layer cannot represent.
fn render_sql_literal(v: &Value) -> NodeDbResult<String> {
    match v {
        Value::Null => Ok("NULL".into()),
        Value::Bool(b) => Ok(if *b { "TRUE".into() } else { "FALSE".into() }),
        Value::Integer(i) => Ok(i.to_string()),
        Value::Float(f) => Ok(f.to_string()),
        Value::String(s) => Ok(quote_string_literal(s)),
        other => Err(NodeDbError::storage(format!(
            "metadata filter value cannot be rendered as a SQL literal: {other:?}"
        ))),
    }
}

/// Translate trait-level `&[Value]` parameters into owned, `ToSql`-
/// compatible boxes for `tokio_postgres`. Returns one boxed parameter
/// per input `Value`; the caller borrows them as `&[&(dyn ToSql + Sync)]`
/// for the driver call.
///
/// Unsupported value variants (objects, arrays, datetimes, etc.) return
/// `Err` rather than silently coercing — surfacing the gap to the
/// caller so the trait contract stays honest.
pub(super) fn translate_params(
    params: &[Value],
) -> NodeDbResult<Vec<Box<dyn tokio_postgres::types::ToSql + Sync + Send>>> {
    params.iter().map(translate_value).collect()
}

fn translate_value(v: &Value) -> NodeDbResult<Box<dyn tokio_postgres::types::ToSql + Sync + Send>> {
    match v {
        Value::Null => Ok(Box::new(None::<i64>)),
        Value::Bool(b) => Ok(Box::new(*b)),
        Value::Integer(i) => Ok(Box::new(*i)),
        Value::Float(f) => Ok(Box::new(*f)),
        Value::String(s) => Ok(Box::new(s.clone())),
        Value::Bytes(b) => Ok(Box::new(b.clone())),
        other => Err(NodeDbError::storage(format!(
            "execute_sql parameter cannot be translated to a pgwire bind: {other:?}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn vector_search_sql_without_filter_renders_basic_form() {
        // No-filter path: SELECT * FROM <coll> ORDER BY vector_distance(ARRAY[..]) LIMIT k.
        let sql =
            build_vector_search_sql("docs", &[0.1, 0.2, 0.3], 5, None, None)
                .expect("no-filter is fine");
        assert!(sql.contains("SELECT"));
        assert!(sql.contains("docs"));
        assert!(sql.contains("vector_distance"));
        assert!(sql.contains("ARRAY[0.1,0.2,0.3]"));
        assert!(sql.contains("LIMIT 5"));
        assert!(
            !sql.contains(" WHERE "),
            "no-filter SQL must not have WHERE; got: {sql}"
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
        let sql = build_vector_search_sql("docs", &[0.0], 2, None, Some(&ids)).unwrap();
        assert!(
            sql.contains(" WHERE id IN ('a''', 'b') ORDER BY "),
            "allowed ids render as a key conjunct; got: {sql}"
        );
        let filter = MetadataFilter::eq("category", Value::String("ai".into()));
        let sql = build_vector_search_sql("docs", &[0.0], 2, Some(&filter), Some(&ids)).unwrap();
        assert!(
            sql.contains(" WHERE \"category\" = 'ai' AND id IN ('a''', 'b') ORDER BY "),
            "the key conjunct joins the filter at the top level; got: {sql}"
        );
    }

    #[test]
    fn render_sql_literal_escapes_single_quotes() {
        let sql = render_sql_literal(&Value::String("o'reilly".into())).unwrap();
        assert_eq!(sql, "'o''reilly'");
    }

    #[test]
    fn render_sql_literal_rejects_unsupported_variants() {
        let array = Value::Array(vec![Value::Integer(1), Value::Integer(2)]);
        assert!(render_sql_literal(&array).is_err());
    }

    #[test]
    fn translate_params_passes_through_empty() {
        // Empty params is a no-op; translate produces an empty vec.
        let translated = translate_params(&[]).expect("empty translate is fine");
        assert!(translated.is_empty());
    }

    #[test]
    fn translate_params_accepts_bound_parameters() {
        // Spec: non-empty params translate cleanly without rejection.
        let params = vec![Value::String("alice".into()), Value::Integer(42)];
        let translated = translate_params(&params)
            .expect("non-empty params must translate for the pgwire driver");
        assert_eq!(translated.len(), 2);
    }

    #[test]
    fn translate_params_supports_common_value_variants() {
        let params = vec![
            Value::Null,
            Value::Bool(true),
            Value::Integer(7),
            Value::Float(2.5),
            Value::String("hi".into()),
            Value::Bytes(vec![1, 2, 3]),
        ];
        let translated = translate_params(&params).expect("common variants translate cleanly");
        assert_eq!(translated.len(), 6);
    }

    #[test]
    fn translate_params_rejects_unsupported_variants() {
        // Object isn't a pgwire scalar; the translator must surface
        // the gap rather than silently coercing.
        let params = vec![Value::Object(Default::default())];
        assert!(translate_params(&params).is_err());
    }
}
