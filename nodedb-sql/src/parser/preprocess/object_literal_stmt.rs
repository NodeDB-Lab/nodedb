// SPDX-License-Identifier: Apache-2.0

//! Rewrite `INSERT/UPSERT INTO coll { ... }` (and `[{ ... }, ...]`) into
//! standard `INSERT INTO coll (cols) VALUES (row), ...`.

use super::lex::first_sql_word;
use super::literal::value_to_sql_literal;
use crate::error::SqlError;
use crate::parser::object_literal::{
    Literal, ObjectTemplate, object_literal_end, parse_object_template_array_complete,
    parse_object_template_complete,
};

/// Try to rewrite `INSERT INTO coll { ... }` or `INSERT INTO coll [{ ... }, { ... }]`
/// into standard `INSERT INTO coll (cols) VALUES (row1), (row2)`.
///
/// `Ok(None)` means the statement does not use object-literal syntax and the
/// caller should carry on with the original text. `Err` means it does and is
/// malformed — including the case where the author wrote a clause after the
/// literal that the rewrite cannot carry.
///
/// [`fields_to_values_sql`] reconstructs the statement from the parsed fields
/// alone, so text after the literal has nowhere to go. A trailing
/// `RETURNING` list is the one clause carried: it follows the literal, holds
/// no part of the row, and the rebuilt `(cols) VALUES (…)` statement takes it
/// verbatim, so it is split off here and appended to the rebuilt text. Any
/// other trailing clause, such as `ON CONFLICT`, is refused and points at the
/// `(cols) VALUES (…)` form, which carries it.
pub(super) fn try_rewrite_object_literal(sql: &str) -> Result<Option<String>, SqlError> {
    let Some(after_into) = text_after_into(sql) else {
        return Ok(None);
    };
    let Some(coll_end) = after_into.find(|c: char| c.is_whitespace()) else {
        return Ok(None);
    };
    let coll_name = &after_into[..coll_end];
    let rest = after_into[coll_end..].trim_start();

    // Strip trailing semicolon before parsing.
    let obj_str = rest.trim_end_matches(';').trim_end();
    if !obj_str.starts_with('[') && !obj_str.starts_with('{') {
        return Ok(None);
    }
    let (literal, returning) = split_trailing_returning(obj_str);
    let rewritten = if literal.starts_with('[') {
        rewrite_array_form(coll_name, literal)?
    } else {
        rewrite_object_form(coll_name, literal)?
    };
    Ok(rewritten.map(|values_sql| match returning {
        Some(clause) => format!("{values_sql} {clause}"),
        None => values_sql,
    }))
}

/// Split a `RETURNING …` clause that directly follows the literal.
///
/// Returns the literal text and the clause text, keyword included. Text after
/// the literal that is not a `RETURNING` clause stays on the literal, so the
/// strict parser refuses it by name.
fn split_trailing_returning(obj_str: &str) -> (&str, Option<&str>) {
    const KEYWORD: &str = "RETURNING";
    let Some(end) = object_literal_end(obj_str) else {
        return (obj_str, None);
    };
    let rest = obj_str[end..].trim_start();
    let is_clause = nodedb_types::starts_with_ascii_case_insensitive(rest, KEYWORD)
        && rest[KEYWORD.len()..]
            .chars()
            .next()
            .is_some_and(|next| !(next.is_ascii_alphanumeric() || next == '_'));
    if is_clause {
        (&obj_str[..end], Some(rest))
    } else {
        (obj_str, None)
    }
}

/// Rewrite `{ ... }` → single-row VALUES.
fn rewrite_object_form(coll_name: &str, obj_str: &str) -> Result<Option<String>, SqlError> {
    let Some(parsed) = parse_object_template_complete(obj_str) else {
        return Ok(None);
    };
    let fields = parsed?;
    if fields.is_empty() {
        return Ok(None);
    }
    fields_to_values_sql(coll_name, &[fields]).map(Some)
}

/// The text after `INSERT INTO` or `UPSERT INTO`, or `None` when `sql` does not hold that shape.
///
/// Comments can lead the statement, any run of whitespace can separate the keywords,
/// and the text can end at any byte.
pub(super) fn text_after_into(sql: &str) -> Option<&str> {
    let verb = first_sql_word(sql)?;
    // `verb` borrows from `sql`, so the pointer distance is its byte offset.
    let verb_end = verb.as_ptr() as usize - sql.as_ptr() as usize + verb.len();
    let rest = sql.get(verb_end..)?;
    let rest = rest.strip_prefix(char::is_whitespace)?.trim_start();
    if !nodedb_types::starts_with_ascii_case_insensitive(rest, "INTO") {
        return None;
    }
    let rest = rest.get("INTO".len()..)?;
    rest.strip_prefix(char::is_whitespace).map(str::trim_start)
}

/// Rewrite `[{ ... }, { ... }]` → multi-row VALUES.
fn rewrite_array_form(coll_name: &str, obj_str: &str) -> Result<Option<String>, SqlError> {
    let Some(parsed) = parse_object_template_array_complete(obj_str) else {
        return Ok(None);
    };
    let objects = parsed?;
    if objects.is_empty() {
        return Ok(None);
    }
    fields_to_values_sql(coll_name, &objects).map(Some)
}

/// Build `INSERT INTO coll (col_union) VALUES (row1), (row2), ...`
///
/// Collects the union of all keys across all rows. Missing keys get NULL.
/// A `$N` parameter reference stays `$N` in the row, so the statement's
/// parameter binder binds it typed, as in a `VALUES` clause.
fn fields_to_values_sql(coll_name: &str, rows: &[ObjectTemplate]) -> Result<String, SqlError> {
    let mut all_keys: Vec<String> = rows
        .iter()
        .flat_map(|r| r.keys().cloned())
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .collect();
    all_keys.sort();

    let col_list = all_keys.join(", ");

    let row_strs: Vec<String> = rows
        .iter()
        .map(|row| {
            let vals: Vec<String> = all_keys
                .iter()
                .map(|k| match row.get(k) {
                    Some(v) => literal_sql(v),
                    None => "NULL".to_string(),
                })
                .collect();
            format!("({})", vals.join(", "))
        })
        .collect();

    Ok(format!(
        "INSERT INTO {} ({}) VALUES {}",
        coll_name,
        col_list,
        row_strs.join(", ")
    ))
}

/// The `VALUES` text of a literal. A parameter is `$N`, an array
/// `ARRAY[...]` of its elements, a nested object the typed object
/// `jsonb_build_object('k', v, ...)` of its fields in key order. Every value
/// at any depth, a parameter included, then resolves through the one
/// `VALUES` cell rule and keeps its exact type.
fn literal_sql(literal: &Literal) -> String {
    match literal {
        Literal::Value(nodedb_types::Value::Object(fields)) => {
            let mut fields: Vec<_> = fields.iter().collect();
            fields.sort_by(|a, b| a.0.cmp(b.0));
            object_sql(
                fields
                    .into_iter()
                    .map(|(key, value)| (key, value_sql(value))),
            )
        }
        Literal::Value(value) => value_sql(value),
        Literal::Param(n) => format!("${n}"),
        Literal::Array(items) => {
            let items: Vec<String> = items.iter().map(literal_sql).collect();
            format!("ARRAY[{}]", items.join(", "))
        }
        Literal::Object(fields) => {
            let mut fields: Vec<_> = fields.iter().collect();
            fields.sort_by(|a, b| a.0.cmp(b.0));
            object_sql(
                fields
                    .into_iter()
                    .map(|(key, value)| (key, literal_sql(value))),
            )
        }
    }
}

/// The `VALUES` text of a plain value: a nested object is the typed object
/// call, every other value its canonical literal.
fn value_sql(value: &nodedb_types::Value) -> String {
    match value {
        nodedb_types::Value::Object(_) => literal_sql(&Literal::Value(value.clone())),
        nodedb_types::Value::Array(items) => {
            let items: Vec<String> = items.iter().map(value_sql).collect();
            format!("ARRAY[{}]", items.join(", "))
        }
        other => value_to_sql_literal(other),
    }
}

/// `jsonb_build_object('k1', v1, ...)` of rendered fields.
fn object_sql<'a>(fields: impl Iterator<Item = (&'a String, String)>) -> String {
    let args: Vec<String> = fields
        .map(|(key, value)| format!("{}, {value}", nodedb_types::sql_quote::quote_literal(key)))
        .collect();
    format!(
        "{}({})",
        crate::planner::dml_helpers::OBJECT_FUNCTION,
        args.join(", ")
    )
}

#[cfg(test)]
mod template_tests {
    use super::*;

    #[test]
    fn parameters_stay_parameters_in_both_brace_forms() {
        assert_eq!(
            try_rewrite_object_literal("INSERT INTO c { id: 'a', u: $1, tags: [$2, 'x'] }"),
            Ok(Some(
                "INSERT INTO c (id, tags, u) VALUES ('a', ARRAY[$2, 'x'], $1)".to_string()
            ))
        );
        assert_eq!(
            try_rewrite_object_literal("INSERT INTO c [{ id: $1 }, { id: $2, n: 3 }]"),
            Ok(Some(
                "INSERT INTO c (id, n) VALUES ($1, NULL), ($2, 3)".to_string()
            ))
        );
        assert_eq!(
            try_rewrite_object_literal("INSERT INTO c { id: 'a', m: { k: $1, j: { x: 1 } } }"),
            Ok(Some(
                "INSERT INTO c (id, m) VALUES ('a', jsonb_build_object('j', \
                 jsonb_build_object('x', 1), 'k', $1))"
                    .to_string()
            ))
        );
    }
}
