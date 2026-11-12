// SPDX-License-Identifier: Apache-2.0

//! The binding entry points: `bind_dsl` for a DSL statement's pgwire
//! parameters, `bind_values` for a native statement's values.

use super::literal::{param_literal, value_literal};
use super::splice::{Form, bind_tokens, first_word_is, tokenize};
use crate::error::Result;
use crate::params::ParamValue;
use crate::params_int_width::{int_param_cast_text, width_placeholder_positions};

/// SQL text whose prepared-statement `$N` placeholders have been
/// substituted with concrete literals.
///
/// The DSL dispatcher's prepared-statement entry points take
/// `BoundDslSql`, not `&str`. This makes it impossible for a caller
/// to forget parameter binding on a DSL path — the types enforce it.
#[derive(Debug, Clone)]
pub struct BoundDslSql(String);

impl BoundDslSql {
    /// Construct from SQL known not to need binding (simple-query path).
    ///
    /// The simple-query protocol does not carry prepared parameters, so
    /// passing the raw text is correct at that layer. This constructor
    /// is the only way to bypass `bind_dsl` and is named loudly so that
    /// any future use is obvious in review.
    pub fn from_simple_query(sql: String) -> Self {
        Self(sql)
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn into_string(self) -> String {
        self.0
    }
}

/// Substitute `$N` placeholders in DSL SQL text with concrete literals.
///
/// Uses sqlparser's own tokenizer so that string literals, quoted
/// identifiers, comments, and dollar-quoted strings are never
/// accidentally rewritten — the tokenizer has already classified them.
pub fn bind_dsl(sql: &str, params: &[ParamValue]) -> Result<BoundDslSql> {
    if params.is_empty() {
        return Ok(BoundDslSql(sql.to_owned()));
    }
    let tokens = tokenize(sql)?;
    let typed = first_word_is(&tokens, "UPSERT");
    let width_at = if typed {
        width_placeholder_positions(sql)
    } else {
        Vec::new()
    };
    let out = bind_tokens(
        sql,
        &tokens,
        typed,
        params.len(),
        &width_at,
        |index, form, at_width| {
            params.get(index).map(|param| match (form, at_width) {
                (Form::Sql, true) => {
                    int_param_cast_text(param).unwrap_or_else(|| param_literal(param, form))
                }
                _ => param_literal(param, form),
            })
        },
    )?;
    Ok(BoundDslSql(out))
}

/// Substitute `$N` placeholders in any statement with the literals of
/// `values`, the native protocol's bound parameters.
///
/// A UUID, an instant, bytes and a decimal take the typed literal a pgwire
/// parameter of that type resolves through, by the same rules as
/// [`bind_dsl`]: inside an object literal the object-literal typed form,
/// elsewhere the SQL typed literal, except in a DSL whose own grammar reads
/// a quoted string there (`GRAPH`, `MATCH`, `SEARCH`, `CRDT`, `CALL`),
/// which takes plain text. Every other value takes its canonical literal.
///
/// Text the tokenizer refuses, and a placeholder with no value, are
/// [`SqlError::Parse`](crate::error::SqlError::Parse) (SQLSTATE `42601`), never a fallback rewrite.
pub fn bind_values(sql: &str, values: &[nodedb_types::Value]) -> Result<String> {
    if values.is_empty() {
        return Ok(sql.to_owned());
    }
    let tokens = tokenize(sql)?;
    let typed = !["GRAPH", "MATCH", "OPTIONAL", "SEARCH", "CRDT", "CALL"]
        .iter()
        .any(|word| first_word_is(&tokens, word));
    // A native integer carries no declared width, so at a width position
    // it is `bigint`, as an undeclared pgwire integer is.
    let width_at = if typed {
        width_placeholder_positions(sql)
    } else {
        Vec::new()
    };
    bind_tokens(
        sql,
        &tokens,
        typed,
        values.len(),
        &width_at,
        |index, form, at_width| {
            values
                .get(index)
                .map(|value| match (form, at_width, value) {
                    (Form::Sql, true, nodedb_types::Value::Integer(i)) => {
                        int_param_cast_text(&ParamValue::Int64(*i))
                            .unwrap_or_else(|| value_literal(value, form))
                    }
                    _ => value_literal(value, form),
                })
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn upsert_int_and_text_params() {
        let bound = bind_dsl(
            "UPSERT INTO t (id, n) VALUES ($1, $2)",
            &[ParamValue::Text("alice".into()), ParamValue::Int64(42)],
        )
        .unwrap();
        assert!(
            bound.as_str().contains("'alice'"),
            "text param not substituted: {}",
            bound.as_str()
        );
        assert!(
            bound.as_str().contains("42"),
            "int param not substituted: {}",
            bound.as_str()
        );
        assert!(
            !bound.as_str().contains('$'),
            "placeholder survived: {}",
            bound.as_str()
        );
    }

    #[test]
    fn search_top_k_param() {
        let bound = bind_dsl(
            "SEARCH v USING VECTOR(ARRAY[1.0, 0.0, 0.0], $1)",
            &[ParamValue::Int64(5)],
        )
        .unwrap();
        assert!(bound.as_str().contains(", 5)"), "got: {}", bound.as_str());
    }

    #[test]
    fn null_param() {
        let bound = bind_dsl(
            "UPSERT INTO t (id, n) VALUES ($1, $2)",
            &[ParamValue::Text("x".into()), ParamValue::Null],
        )
        .unwrap();
        let s = bound.as_str();
        assert!(s.to_uppercase().contains("NULL"), "got: {s}");
    }

    #[test]
    fn empty_params_is_noop() {
        let sql = "UPSERT INTO t (id) VALUES ('a')";
        let bound = bind_dsl(sql, &[]).unwrap();
        assert_eq!(bound.as_str(), sql);
    }
}
