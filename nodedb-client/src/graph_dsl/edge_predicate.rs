// SPDX-License-Identifier: Apache-2.0

//! Render `EdgeFilter::property_filters` as the `EDGE WHERE` clause of
//! `GRAPH TRAVERSE` / `GRAPH PATH`.
//!
//! Every property name renders quoted, so its case and any reserved word
//! survive. Every term renders parenthesised, so operator precedence never
//! depends on the server's grammar.

use nodedb_types::error::{NodeDbError, NodeDbResult};
use nodedb_types::filter::MetadataFilter;
use nodedb_types::value::Value;

use crate::sql_escape::{quote_identifier, quote_string_literal};

/// ` EDGE WHERE <expr>` for `filters`, AND-ed. Empty filters render nothing.
pub(crate) fn edge_where_clause(filters: &[MetadataFilter]) -> NodeDbResult<String> {
    if filters.is_empty() {
        return Ok(String::new());
    }
    let mut out = String::from(" EDGE WHERE ");
    for (i, filter) in filters.iter().enumerate() {
        if i > 0 {
            out.push_str(" AND ");
        }
        out.push('(');
        render(filter, &mut out)?;
        out.push(')');
    }
    Ok(out)
}

fn render(filter: &MetadataFilter, out: &mut String) -> NodeDbResult<()> {
    match filter {
        MetadataFilter::Eq { field, value } if value.is_null() => {
            null_test(field, "IS NULL", out);
            Ok(())
        }
        MetadataFilter::Ne { field, value } if value.is_null() => {
            null_test(field, "IS NOT NULL", out);
            Ok(())
        }
        MetadataFilter::Eq { field, value } => compare(field, "=", value, out),
        MetadataFilter::Ne { field, value } => compare(field, "<>", value, out),
        MetadataFilter::Gt { field, value } => compare(field, ">", value, out),
        MetadataFilter::Gte { field, value } => compare(field, ">=", value, out),
        MetadataFilter::Lt { field, value } => compare(field, "<", value, out),
        MetadataFilter::Lte { field, value } => compare(field, "<=", value, out),
        // An empty IN admits nothing. An empty NOT IN admits everything.
        MetadataFilter::In { values, .. } if values.is_empty() => {
            out.push_str("FALSE");
            Ok(())
        }
        MetadataFilter::NotIn { values, .. } if values.is_empty() => {
            out.push_str("TRUE");
            Ok(())
        }
        MetadataFilter::In { field, values } => list(field, "IN", values, out),
        MetadataFilter::NotIn { field, values } => list(field, "NOT IN", values, out),
        MetadataFilter::And(children) => group(children, " AND ", "TRUE", out),
        MetadataFilter::Or(children) => group(children, " OR ", "FALSE", out),
        MetadataFilter::Not(inner) => {
            out.push_str("NOT (");
            render(inner, out)?;
            out.push(')');
            Ok(())
        }
        // `MetadataFilter` is `#[non_exhaustive]`: a variant added later has
        // no rendering until one is written here.
        other => Err(NodeDbError::bad_request(format!(
            "edge filter {other:?} has no GRAPH SQL form: use Eq, Ne, Gt, Gte, Lt, Lte, In, \
             NotIn, And, Or or Not"
        ))),
    }
}

fn null_test(field: &str, test: &str, out: &mut String) {
    out.push_str(&quote_identifier(field));
    out.push(' ');
    out.push_str(test);
}

fn compare(field: &str, op: &str, value: &Value, out: &mut String) -> NodeDbResult<()> {
    out.push_str(&quote_identifier(field));
    out.push(' ');
    out.push_str(op);
    out.push(' ');
    literal(value, out)
}

fn list(field: &str, op: &str, values: &[Value], out: &mut String) -> NodeDbResult<()> {
    out.push_str(&quote_identifier(field));
    out.push(' ');
    out.push_str(op);
    out.push_str(" (");
    for (i, value) in values.iter().enumerate() {
        if i > 0 {
            out.push_str(", ");
        }
        literal(value, out)?;
    }
    out.push(')');
    Ok(())
}

fn group(
    children: &[MetadataFilter],
    joiner: &str,
    empty: &str,
    out: &mut String,
) -> NodeDbResult<()> {
    if children.is_empty() {
        out.push_str(empty);
        return Ok(());
    }
    for (i, child) in children.iter().enumerate() {
        if i > 0 {
            out.push_str(joiner);
        }
        out.push('(');
        render(child, out)?;
        out.push(')');
    }
    Ok(())
}

fn literal(value: &Value, out: &mut String) -> NodeDbResult<()> {
    match value {
        Value::Null => out.push_str("NULL"),
        Value::Bool(b) => out.push_str(if *b { "TRUE" } else { "FALSE" }),
        Value::Integer(i) => out.push_str(&i.to_string()),
        // `{:?}` keeps a `.` or an exponent, so the server reads a float back.
        Value::Float(f) if f.is_finite() => out.push_str(&format!("{f:?}")),
        Value::String(s) | Value::Uuid(s) | Value::Ulid(s) | Value::Regex(s) => {
            out.push_str(&quote_string_literal(s))
        }
        Value::DateTime(dt) | Value::NaiveDateTime(dt) => {
            out.push_str(&quote_string_literal(&dt.to_iso8601()))
        }
        Value::Decimal(d) => out.push_str(&d.to_string()),
        other => {
            return Err(NodeDbError::bad_request(format!(
                "edge filter value {other:?} has no GRAPH SQL literal: use null, bool, a finite \
                 number, string or timestamp"
            )));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn clause(filters: Vec<MetadataFilter>) -> String {
        edge_where_clause(&filters).expect("renders")
    }

    fn cmp(field: &str, value: Value, ctor: fn(String, Value) -> MetadataFilter) -> MetadataFilter {
        ctor(field.to_string(), value)
    }

    #[test]
    fn no_filters_render_nothing() {
        assert_eq!(clause(Vec::new()), "");
    }

    #[test]
    fn every_comparison_renders_with_a_quoted_name() {
        let cases: [(MetadataFilter, &str); 6] = [
            (
                cmp("score", Value::Integer(5), |field, value| {
                    MetadataFilter::Eq { field, value }
                }),
                r#" EDGE WHERE ("score" = 5)"#,
            ),
            (
                cmp("score", Value::Integer(5), |field, value| {
                    MetadataFilter::Ne { field, value }
                }),
                r#" EDGE WHERE ("score" <> 5)"#,
            ),
            (
                cmp("score", Value::Float(2.5), |field, value| {
                    MetadataFilter::Gt { field, value }
                }),
                r#" EDGE WHERE ("score" > 2.5)"#,
            ),
            (
                cmp("score", Value::Float(5.0), |field, value| {
                    MetadataFilter::Gte { field, value }
                }),
                r#" EDGE WHERE ("score" >= 5.0)"#,
            ),
            (
                cmp("score", Value::Integer(-3), |field, value| {
                    MetadataFilter::Lt { field, value }
                }),
                r#" EDGE WHERE ("score" < -3)"#,
            ),
            (
                cmp("Score", Value::Float(1e-7), |field, value| {
                    MetadataFilter::Lte { field, value }
                }),
                r#" EDGE WHERE ("Score" <= 1e-7)"#,
            ),
        ];
        for (filter, expected) in cases {
            assert_eq!(clause(vec![filter]), expected);
        }
    }

    #[test]
    fn null_comparisons_render_null_tests() {
        assert_eq!(
            clause(vec![
                MetadataFilter::eq("gone", Value::Null),
                MetadataFilter::Ne {
                    field: "here".into(),
                    value: Value::Null,
                },
            ]),
            r#" EDGE WHERE ("gone" IS NULL) AND ("here" IS NOT NULL)"#
        );
    }

    #[test]
    fn lists_and_empty_lists() {
        assert_eq!(
            clause(vec![MetadataFilter::In {
                field: "kind".into(),
                values: vec![Value::from("road"), Value::from("rail")],
            }]),
            r#" EDGE WHERE ("kind" IN ('road', 'rail'))"#
        );
        assert_eq!(
            clause(vec![MetadataFilter::NotIn {
                field: "tag".into(),
                values: vec![Value::Integer(1), Value::Bool(true)],
            }]),
            r#" EDGE WHERE ("tag" NOT IN (1, TRUE))"#
        );
        assert_eq!(
            clause(vec![MetadataFilter::In {
                field: "kind".into(),
                values: Vec::new(),
            }]),
            " EDGE WHERE (FALSE)"
        );
        assert_eq!(
            clause(vec![MetadataFilter::NotIn {
                field: "kind".into(),
                values: Vec::new(),
            }]),
            " EDGE WHERE (TRUE)"
        );
    }

    #[test]
    fn groups_nest_and_empties_render_constants() {
        assert_eq!(
            clause(vec![MetadataFilter::Not(Box::new(MetadataFilter::Or(
                vec![
                    MetadataFilter::eq("a", 1i64),
                    MetadataFilter::And(vec![
                        MetadataFilter::eq("b", 2i64),
                        MetadataFilter::eq("c", 3i64),
                    ]),
                ]
            )))]),
            r#" EDGE WHERE (NOT (("a" = 1) OR (("b" = 2) AND ("c" = 3))))"#
        );
        assert_eq!(
            clause(vec![MetadataFilter::And(Vec::new())]),
            " EDGE WHERE (TRUE)"
        );
        assert_eq!(
            clause(vec![MetadataFilter::Or(Vec::new())]),
            " EDGE WHERE (FALSE)"
        );
    }

    #[test]
    fn names_and_strings_escape_their_quotes() {
        assert_eq!(
            clause(vec![MetadataFilter::eq("we\"ird", "O'Reilly")]),
            r#" EDGE WHERE ("we""ird" = 'O''Reilly')"#
        );
    }

    #[test]
    fn bytes_and_non_finite_floats_are_refused() {
        assert!(edge_where_clause(&[MetadataFilter::eq("b", Value::Bytes(vec![1]))]).is_err());
        assert!(edge_where_clause(&[MetadataFilter::eq("f", Value::Float(f64::NAN))]).is_err());
        assert!(
            edge_where_clause(&[MetadataFilter::eq("f", Value::Float(f64::INFINITY))]).is_err()
        );
    }
}
