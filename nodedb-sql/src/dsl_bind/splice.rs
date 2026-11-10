// SPDX-License-Identifier: Apache-2.0

//! Token-located splicing: find each `$N` placeholder with sqlparser's
//! tokenizer and copy every other byte of the statement unchanged.

use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::tokenizer::{Location, Token, TokenWithSpan, Tokenizer};

use crate::error::{Result, SqlError};

pub(super) fn tokenize(sql: &str) -> Result<Vec<TokenWithSpan>> {
    let dialect = PostgreSqlDialect {};
    Tokenizer::new(&dialect, sql)
        .tokenize_with_location()
        .map_err(|e| SqlError::Parse {
            detail: format!("tokenize statement for parameter binding: {e}"),
        })
}

/// `sql` with every `$N` placeholder replaced by `literal(N - 1, form)`.
///
/// Every byte outside a placeholder is copied from `sql` unchanged: the
/// tokenizer only locates the placeholders, so a string literal, a quoted
/// identifier, a comment or a dollar-quoted body is never re-spelled, and a
/// `$N` inside one is never a placeholder. Each literal quotes its own text.
///
/// `typed` selects the SQL typed form outside object literals. A
/// placeholder inside a `{ ... }` object literal takes the object-literal
/// typed form. A `MATCH` pattern's braces are a Cypher property map, parsed
/// by its own grammar, so they take plain text.
pub(super) fn bind_tokens(
    sql: &str,
    tokens: &[TokenWithSpan],
    typed: bool,
    count: usize,
    literal: impl Fn(usize, Form) -> Option<String>,
) -> Result<String> {
    let object_braces = !first_word_is(tokens, "MATCH") && !first_word_is(tokens, "OPTIONAL");
    let line_starts: Vec<usize> = std::iter::once(0)
        .chain(sql.match_indices('\n').map(|(at, _)| at + 1))
        .collect();
    let mut depth = 0usize;
    let mut out = String::with_capacity(sql.len());
    let mut copied = 0usize;
    for tok in tokens {
        match &tok.token {
            Token::LBrace => depth += 1,
            Token::RBrace => depth = depth.saturating_sub(1),
            _ => {}
        }
        let Token::Placeholder(p) = &tok.token else {
            continue;
        };
        if !p.starts_with('$') {
            continue;
        }
        let form = if object_braces && depth > 0 {
            Form::ObjectLiteral
        } else if typed {
            Form::Sql
        } else {
            Form::Text
        };
        // Every `$N` must resolve to a provided parameter. A silent
        // pass-through here would let an out-of-range placeholder reach the
        // engine as a raw `$N` literal — exactly the bug class this module
        // exists to close.
        let replacement = p
            .strip_prefix('$')
            .and_then(|digits| digits.parse::<usize>().ok())
            .and_then(|n| n.checked_sub(1))
            .and_then(|index| literal(index, form))
            .ok_or_else(|| SqlError::Parse {
                detail: format!(
                    "DSL parameter bind: placeholder {p} has no corresponding \
                     parameter ({count} provided)"
                ),
            })?;
        let start = byte_offset(sql, &line_starts, tok.span.start)
            .filter(|&start| {
                start >= copied
                    && sql
                        .get(start..)
                        .is_some_and(|rest| rest.starts_with(p.as_str()))
            })
            .ok_or_else(|| SqlError::Parse {
                detail: format!("parameter bind: placeholder {p} has no source position"),
            })?;
        out.push_str(&sql[copied..start]);
        push_literal(&mut out, &replacement, form);
        copied = start + p.len();
    }
    out.push_str(&sql[copied..]);
    Ok(out)
}

/// Append a spliced `literal` to `out`.
///
/// A negative literal never joins a `-` before it into `--`, which starts a
/// comment. In SQL it is wrapped in parentheses, so it also stays one value
/// next to an operator that binds tighter than unary minus (`$1::text`,
/// `$1 ^ 2`). An object literal and a DSL grammar read a bare number, so
/// there it is only separated from a `-` before it by a space.
fn push_literal(out: &mut String, literal: &str, form: Form) {
    if !literal.starts_with('-') {
        out.push_str(literal);
        return;
    }
    match form {
        Form::Sql => {
            out.push('(');
            out.push_str(literal);
            out.push(')');
        }
        Form::ObjectLiteral | Form::Text => {
            if out.ends_with('-') {
                out.push(' ');
            }
            out.push_str(literal);
        }
    }
}

/// The byte offset of a 1-based tokenizer `location` in `sql`.
fn byte_offset(sql: &str, line_starts: &[usize], location: Location) -> Option<usize> {
    let line = usize::try_from(location.line).ok()?.checked_sub(1)?;
    let column = usize::try_from(location.column).ok()?.checked_sub(1)?;
    let start = *line_starts.get(line)?;
    let rest = sql.get(start..)?;
    rest.char_indices()
        .nth(column)
        .map(|(at, _)| start + at)
        .or_else(|| (rest.chars().count() == column).then_some(sql.len()))
}

/// Whether the statement's first word is `word`. For `UPSERT`: the DSL
/// statement is SQL once preprocessed, so a typed parameter takes the SQL
/// typed-literal spelling (`UUID '...'`) and its value stays typed.
/// `UPSERT INTO` is rewritten to `INSERT`. Every other DSL parses its own
/// grammar, which reads a quoted string, so a typed parameter takes its
/// PostgreSQL text there.
pub(super) fn first_word_is(tokens: &[TokenWithSpan], word: &str) -> bool {
    tokens
        .iter()
        .find(|tok| !matches!(tok.token, Token::Whitespace(_)))
        .is_some_and(
            |tok| matches!(&tok.token, Token::Word(w) if w.value.eq_ignore_ascii_case(word)),
        )
}

/// How a placeholder's typed parameter is spelled.
#[derive(Clone, Copy)]
pub(super) enum Form {
    /// The SQL typed literal: `UUID '...'`, exact decimal digits.
    Sql,
    /// Inside an object literal: the SQL typed literal, with a decimal as
    /// `DECIMAL '...'`, since an object literal reads a bare fraction as a
    /// float.
    ObjectLiteral,
    /// Plain PostgreSQL text, for a DSL grammar that reads a quoted string.
    Text,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dsl_bind::{bind_dsl, bind_values};
    use crate::params::ParamValue;

    /// String literals that happen to contain `$1` must not be touched
    /// — the tokenizer has already classified the `$1` inside quotes
    /// as part of `Token::SingleQuotedString`, not as a placeholder.
    #[test]
    fn placeholder_inside_string_literal_untouched() {
        let bound = bind_dsl(
            "UPSERT INTO t (id, note) VALUES ($1, 'your $1 change')",
            &[ParamValue::Text("abc".into())],
        )
        .unwrap();
        assert!(
            bound.as_str().contains("'your $1 change'"),
            "string literal was rewritten: {}",
            bound.as_str()
        );
        assert!(
            bound.as_str().contains("'abc'"),
            "real placeholder not bound: {}",
            bound.as_str()
        );
    }

    #[test]
    fn out_of_range_placeholder_errors() {
        let err = bind_dsl(
            "UPSERT INTO t (id, n) VALUES ($1, $2)",
            &[ParamValue::Text("only-one".into())],
        )
        .unwrap_err();
        let msg = format!("{err:?}");
        assert!(
            msg.contains("$2") && msg.to_lowercase().contains("placeholder"),
            "error must name the unresolved placeholder: {msg}"
        );
    }

    #[test]
    fn zero_placeholder_errors() {
        // `$0` is not a valid pgwire parameter reference — must error,
        // not silently pass through to the engine.
        let err = bind_dsl(
            "UPSERT INTO t (id) VALUES ($0)",
            &[ParamValue::Text("x".into())],
        )
        .unwrap_err();
        assert!(format!("{err:?}").contains("$0"));
    }

    /// The single string token a bound literal reads back as.
    fn bound_string(sql: &str) -> String {
        let tokens = tokenize(sql).unwrap();
        let strings: Vec<String> = tokens
            .into_iter()
            .filter_map(|tok| match tok.token {
                Token::SingleQuotedString(s) => Some(s),
                _ => None,
            })
            .collect();
        assert_eq!(strings.len(), 1, "one string literal in {sql:?}");
        strings.into_iter().next().unwrap_or_default()
    }

    /// A text value with a quote, a backslash, a NUL, a `$1` and braces
    /// binds as one string literal holding exactly that text, in a DSL and
    /// in a native statement, inside and outside an object literal.
    #[test]
    fn bound_text_is_one_literal_of_the_exact_text() {
        let hostile = "x'); DROP COLLECTION t; --\\ \u{0} $1 {a: 1} '' }";
        for sql in ["SEARCH t USING FTS($1)", "UPSERT INTO t { note: $1 }"] {
            let bound = bind_dsl(sql, &[ParamValue::Text(hostile.into())]).unwrap();
            assert_eq!(bound_string(bound.as_str()), hostile, "{sql}");
        }
        for sql in ["SELECT $1", "INSERT INTO t { note: $1 }"] {
            let bound = bind_values(sql, &[nodedb_types::Value::String(hostile.into())]).unwrap();
            assert_eq!(bound_string(&bound), hostile, "{sql}");
        }
    }

    /// An existing literal with an escaped quote, a `$1` inside a string,
    /// and a comment keep their exact source text.
    #[test]
    fn text_outside_placeholders_is_copied_unchanged() {
        let sql = "UPSERT INTO t { a: 'O''Brien $1', b: $1 } -- $1 note";
        let bound = bind_dsl(sql, &[ParamValue::Int64(7)]).unwrap();
        assert_eq!(
            bound.as_str(),
            "UPSERT INTO t { a: 'O''Brien $1', b: 7 } -- $1 note"
        );
        let multi = "SELECT $1,\n  $2";
        let bound = bind_values(
            multi,
            &[
                nodedb_types::Value::Integer(1),
                nodedb_types::Value::Integer(2),
            ],
        )
        .unwrap();
        assert_eq!(bound, "SELECT 1,\n  2");
    }

    /// A statement the tokenizer refuses is a parse error, never rewritten.
    #[test]
    fn untokenizable_text_is_a_parse_error() {
        assert!(matches!(
            bind_values(
                "SELECT 'unterminated $1",
                &[nodedb_types::Value::Integer(1)]
            ),
            Err(SqlError::Parse { .. })
        ));
        assert!(matches!(
            bind_values("SELECT $2", &[nodedb_types::Value::Integer(1)]),
            Err(SqlError::Parse { .. })
        ));
    }

    /// A negative literal after a `-` never forms `--`, which would turn the
    /// rest of the line into a comment.
    #[test]
    fn a_negative_literal_never_forms_a_comment() {
        let negatives = [
            ParamValue::Int64(-5),
            ParamValue::Float64(-1.5),
            ParamValue::Decimal("-2.50".parse().unwrap()),
        ];
        for param in &negatives {
            let bound = bind_dsl(
                "UPSERT INTO t (id, n) VALUES ('a', 1 -$1)",
                std::slice::from_ref(param),
            )
            .unwrap();
            assert!(!bound.as_str().contains("--"), "{}", bound.as_str());
            assert!(bound.as_str().contains("1 -(-"), "{}", bound.as_str());
        }
        assert_eq!(
            bind_values(
                "SELECT 1 -$1, $2::text",
                &[
                    nodedb_types::Value::Integer(-5),
                    nodedb_types::Value::Integer(-7)
                ]
            )
            .unwrap(),
            "SELECT 1 -(-5), (-7)::text"
        );
        let bound = bind_dsl(
            "SEARCH v USING VECTOR(ARRAY[1.0], 1 -$1)",
            &[ParamValue::Int64(-5)],
        )
        .unwrap();
        assert_eq!(bound.as_str(), "SEARCH v USING VECTOR(ARRAY[1.0], 1 - -5)");
        let bound = bind_dsl("UPSERT INTO t { n: $1 }", &[ParamValue::Int64(-5)]).unwrap();
        assert_eq!(bound.as_str(), "UPSERT INTO t { n: -5 }");
    }
}
