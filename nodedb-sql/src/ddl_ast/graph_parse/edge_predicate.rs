// SPDX-License-Identifier: Apache-2.0

//! `EDGE WHERE <predicate>`: the edge-property filter of GRAPH TRAVERSE and
//! GRAPH PATH.
//!
//! The clause is last in the statement. The text before it is tokenized
//! alone, so a property named like a DSL keyword (`depth`, `label`) never
//! reads as a clause. The predicate parses as a PostgreSQL expression and
//! maps onto `MetadataFilter`:
//!
//! - `=` `<>` `!=` `>` `>=` `<` `<=` between a property name and a literal,
//!   on either side.
//! - `IN (…)`, `NOT IN (…)`, `IS NULL`, `IS NOT NULL`.
//! - `AND`, `OR`, `NOT`, parentheses. Bare `TRUE` admits, bare `FALSE`
//!   rejects.
//!
//! Literals: `NULL`, `TRUE`/`FALSE`, single-quoted strings, integers that
//! fit `i64`, finite floats. A property name is one identifier. Anything
//! else is a parse error.

use nodedb_types::Value;
use nodedb_types::filter::MetadataFilter;
use sqlparser::ast::{BinaryOperator, Expr, UnaryOperator, Value as SqlValue};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;
use sqlparser::tokenizer::Token;

use crate::error::SqlError;
use crate::parser::normalize::normalize_ident;

/// Split `sql` at its top-level `EDGE WHERE`. Quoted literals, quoted
/// identifiers and `{…}` object literals never split.
pub(super) fn split_edge_where(sql: &str) -> Result<(&str, Option<&str>), SqlError> {
    let bytes = sql.as_bytes();
    let mut i = 0;
    let mut depth = 0usize;
    let mut prev: Option<(usize, usize)> = None;
    while i < bytes.len() {
        match bytes[i] {
            b'\'' | b'"' => {
                i = skip_quoted(bytes, i);
                prev = None;
            }
            b'{' => {
                depth += 1;
                i += 1;
                prev = None;
            }
            b'}' => {
                depth = depth.saturating_sub(1);
                i += 1;
                prev = None;
            }
            c if c.is_ascii_alphanumeric() || c == b'_' => {
                let start = i;
                while i < bytes.len() && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_') {
                    i += 1;
                }
                if depth == 0 {
                    let after_edge =
                        prev.is_some_and(|(s, e)| sql[s..e].eq_ignore_ascii_case("EDGE"));
                    if after_edge && sql[start..i].eq_ignore_ascii_case("WHERE") {
                        let head = &sql[..prev.map_or(start, |(s, _)| s)];
                        let predicate = sql[i..].trim().trim_end_matches(';').trim_end();
                        if predicate.is_empty() {
                            return Err(parse_err("EDGE WHERE requires a predicate".to_owned()));
                        }
                        return Ok((head, Some(predicate)));
                    }
                    prev = Some((start, i));
                }
            }
            c if c.is_ascii_whitespace() => i += 1,
            _ => {
                i += 1;
                prev = None;
            }
        }
    }
    Ok((sql, None))
}

/// The index just past the quoted run opening at `start`. A doubled quote
/// is an escaped quote inside the run.
fn skip_quoted(bytes: &[u8], start: usize) -> usize {
    let quote = bytes[start];
    let mut j = start + 1;
    while j < bytes.len() {
        if bytes[j] == quote {
            if bytes.get(j + 1) == Some(&quote) {
                j += 2;
                continue;
            }
            return j + 1;
        }
        j += 1;
    }
    j
}

/// Parse the predicate text after `EDGE WHERE` into its AND-ed filters.
pub(super) fn parse_edge_predicate(text: &str) -> Result<Vec<MetadataFilter>, SqlError> {
    let dialect = PostgreSqlDialect {};
    let mut parser = Parser::new(&dialect)
        .try_with_sql(text)
        .map_err(|e| parse_err(format!("EDGE WHERE: {e}")))?;
    let expr = parser
        .parse_expr()
        .map_err(|e| parse_err(format!("EDGE WHERE: {e}")))?;
    parser
        .expect_token(&Token::EOF)
        .map_err(|e| parse_err(format!("EDGE WHERE: {e}")))?;
    Ok(match convert(&expr)? {
        MetadataFilter::And(children) => children,
        single => vec![single],
    })
}

fn convert(expr: &Expr) -> Result<MetadataFilter, SqlError> {
    match expr {
        Expr::Nested(inner) => convert(inner),
        Expr::BinaryOp {
            left,
            op: BinaryOperator::And,
            right,
        } => {
            let mut children = Vec::new();
            for side in [convert(left)?, convert(right)?] {
                match side {
                    MetadataFilter::And(inner) => children.extend(inner),
                    other => children.push(other),
                }
            }
            Ok(MetadataFilter::And(children))
        }
        Expr::BinaryOp {
            left,
            op: BinaryOperator::Or,
            right,
        } => Ok(MetadataFilter::Or(vec![convert(left)?, convert(right)?])),
        Expr::UnaryOp {
            op: UnaryOperator::Not,
            expr,
        } => Ok(MetadataFilter::Not(Box::new(convert(expr)?))),
        Expr::BinaryOp { left, op, right } => comparison(left, op, right),
        Expr::InList {
            expr,
            list,
            negated,
        } => {
            let field = field(expr)?;
            let values = list.iter().map(literal).collect::<Result<Vec<_>, _>>()?;
            Ok(if *negated {
                MetadataFilter::NotIn { field, values }
            } else {
                MetadataFilter::In { field, values }
            })
        }
        Expr::IsNull(inner) => Ok(MetadataFilter::Eq {
            field: field(inner)?,
            value: Value::Null,
        }),
        Expr::IsNotNull(inner) => Ok(MetadataFilter::Ne {
            field: field(inner)?,
            value: Value::Null,
        }),
        Expr::Value(v) => match &v.value {
            SqlValue::Boolean(true) => Ok(MetadataFilter::And(Vec::new())),
            SqlValue::Boolean(false) => Ok(MetadataFilter::Or(Vec::new())),
            _ => Err(parse_err(format!(
                "EDGE WHERE term '{expr}' is not a predicate"
            ))),
        },
        other => Err(parse_err(format!("EDGE WHERE does not support '{other}'"))),
    }
}

fn comparison(left: &Expr, op: &BinaryOperator, right: &Expr) -> Result<MetadataFilter, SqlError> {
    let (field, value, op) = match (field(left), field(right)) {
        (Ok(_), Ok(_)) => {
            return Err(parse_err(format!(
                "EDGE WHERE compares a property with a literal, found '{left} {op} {right}'"
            )));
        }
        (Ok(f), Err(_)) => (f, literal(right)?, op.clone()),
        (Err(_), Ok(f)) => (f, literal(left)?, mirror(op)),
        (Err(e), Err(_)) => return Err(e),
    };
    Ok(match op {
        BinaryOperator::Eq => MetadataFilter::Eq { field, value },
        BinaryOperator::NotEq => MetadataFilter::Ne { field, value },
        BinaryOperator::Gt => MetadataFilter::Gt { field, value },
        BinaryOperator::GtEq => MetadataFilter::Gte { field, value },
        BinaryOperator::Lt => MetadataFilter::Lt { field, value },
        BinaryOperator::LtEq => MetadataFilter::Lte { field, value },
        other => {
            return Err(parse_err(format!(
                "EDGE WHERE does not support operator '{other}'"
            )));
        }
    })
}

/// The operator that keeps `literal OP field` true as `field OP' literal`.
fn mirror(op: &BinaryOperator) -> BinaryOperator {
    match op {
        BinaryOperator::Gt => BinaryOperator::Lt,
        BinaryOperator::GtEq => BinaryOperator::LtEq,
        BinaryOperator::Lt => BinaryOperator::Gt,
        BinaryOperator::LtEq => BinaryOperator::GtEq,
        other => other.clone(),
    }
}

fn field(expr: &Expr) -> Result<String, SqlError> {
    match expr {
        Expr::Identifier(ident) => Ok(normalize_ident(ident)),
        Expr::Nested(inner) => field(inner),
        other => Err(parse_err(format!(
            "EDGE WHERE expects a property name, found '{other}': quote names with \"…\""
        ))),
    }
}

fn literal(expr: &Expr) -> Result<Value, SqlError> {
    match expr {
        Expr::Nested(inner) => literal(inner),
        Expr::Value(v) => match &v.value {
            SqlValue::Null => Ok(Value::Null),
            SqlValue::Boolean(b) => Ok(Value::Bool(*b)),
            SqlValue::SingleQuotedString(s) => Ok(Value::String(s.clone())),
            SqlValue::Number(n, _) => number(n),
            other => Err(parse_err(format!(
                "EDGE WHERE does not support literal '{other}'"
            ))),
        },
        Expr::UnaryOp {
            op: UnaryOperator::Minus,
            expr: inner,
        } => match inner.as_ref() {
            Expr::Value(v) => match &v.value {
                SqlValue::Number(n, _) => number(&format!("-{n}")),
                other => Err(parse_err(format!("EDGE WHERE cannot negate '{other}'"))),
            },
            other => Err(parse_err(format!("EDGE WHERE cannot negate '{other}'"))),
        },
        other => Err(parse_err(format!(
            "EDGE WHERE expects a literal, found '{other}'"
        ))),
    }
}

/// An integer when `text` has no `.` or exponent and fits `i64`, else a
/// finite float.
fn number(text: &str) -> Result<Value, SqlError> {
    if !text.contains(['.', 'e', 'E'])
        && let Ok(i) = text.parse::<i64>()
    {
        return Ok(Value::Integer(i));
    }
    text.parse::<f64>()
        .ok()
        .filter(|f| f.is_finite())
        .map(Value::Float)
        .ok_or_else(|| parse_err(format!("EDGE WHERE number '{text}' is not a finite number")))
}

fn parse_err(detail: String) -> SqlError {
    SqlError::Parse { detail }
}

#[cfg(test)]
mod tests {
    use super::super::entry::try_parse;
    use super::*;
    use crate::ddl_ast::statement::{GraphStmt, NodedbStatement};

    fn predicate_of(sql: &str) -> Vec<MetadataFilter> {
        match try_parse(sql)
            .expect("graph DSL")
            .expect("well-formed statement")
        {
            NodedbStatement::Graph(GraphStmt::GraphTraverse { edge_predicate, .. })
            | NodedbStatement::Graph(GraphStmt::GraphPath { edge_predicate, .. }) => edge_predicate,
            other => panic!("expected TRAVERSE or PATH, got {other:?}"),
        }
    }

    fn traverse(predicate: &str) -> Vec<MetadataFilter> {
        predicate_of(&format!(
            "GRAPH TRAVERSE IN 'g' FROM 'a' DEPTH 2 EDGE WHERE {predicate}"
        ))
    }

    fn parse_error(sql: &str) -> String {
        match try_parse(sql).expect("graph DSL") {
            Err(SqlError::Parse { detail }) => detail,
            other => panic!("expected a parse error for {sql}, got {other:?}"),
        }
    }

    fn gt(field: &str, value: Value) -> MetadataFilter {
        MetadataFilter::Gt {
            field: field.into(),
            value,
        }
    }

    #[test]
    fn two_filters_are_the_and_ed_list() {
        assert_eq!(
            traverse("score > 5 AND active = TRUE"),
            vec![
                gt("score", Value::Integer(5)),
                MetadataFilter::Eq {
                    field: "active".into(),
                    value: Value::Bool(true),
                },
            ]
        );
    }

    #[test]
    fn a_literal_on_the_left_mirrors_the_operator() {
        assert_eq!(traverse("5 < score"), vec![gt("score", Value::Integer(5))]);
        assert_eq!(
            traverse("5 = score"),
            vec![MetadataFilter::Eq {
                field: "score".into(),
                value: Value::Integer(5),
            }]
        );
    }

    #[test]
    fn in_not_in_and_null_tests() {
        assert_eq!(
            traverse("kind IN ('road', 'rail') AND tag NOT IN (1, 2)"),
            vec![
                MetadataFilter::In {
                    field: "kind".into(),
                    values: vec![Value::String("road".into()), Value::String("rail".into())],
                },
                MetadataFilter::NotIn {
                    field: "tag".into(),
                    values: vec![Value::Integer(1), Value::Integer(2)],
                },
            ]
        );
        assert_eq!(
            traverse("missing IS NULL AND present IS NOT NULL"),
            vec![
                MetadataFilter::Eq {
                    field: "missing".into(),
                    value: Value::Null,
                },
                MetadataFilter::Ne {
                    field: "present".into(),
                    value: Value::Null,
                },
            ]
        );
    }

    #[test]
    fn or_not_and_parentheses_nest() {
        assert_eq!(
            traverse("NOT (closed = TRUE) AND (a = 1 OR b = 2)"),
            vec![
                MetadataFilter::Not(Box::new(MetadataFilter::Eq {
                    field: "closed".into(),
                    value: Value::Bool(true),
                })),
                MetadataFilter::Or(vec![
                    MetadataFilter::Eq {
                        field: "a".into(),
                        value: Value::Integer(1),
                    },
                    MetadataFilter::Eq {
                        field: "b".into(),
                        value: Value::Integer(2),
                    },
                ]),
            ]
        );
        assert_eq!(traverse("TRUE"), Vec::<MetadataFilter>::new());
        assert_eq!(traverse("FALSE"), vec![MetadataFilter::Or(Vec::new())]);
    }

    #[test]
    fn quoted_names_and_escaped_literals() {
        assert_eq!(
            traverse(r#""we""ird" = 'O''Reilly'"#),
            vec![MetadataFilter::Eq {
                field: "we\"ird".into(),
                value: Value::String("O'Reilly".into()),
            }]
        );
        // An unquoted name folds to lower case, a quoted one keeps its case.
        assert_eq!(
            traverse(r#"Score > 1 AND "Score" > 2"#),
            vec![
                gt("score", Value::Integer(1)),
                gt("Score", Value::Integer(2))
            ]
        );
    }

    #[test]
    fn numbers_keep_their_kind() {
        assert_eq!(
            traverse("n = -9223372036854775808"),
            vec![MetadataFilter::Eq {
                field: "n".into(),
                value: Value::Integer(i64::MIN),
            }]
        );
        assert_eq!(traverse("n > 1e-7"), vec![gt("n", Value::Float(1e-7))]);
        assert_eq!(traverse("n > 2.5"), vec![gt("n", Value::Float(2.5))]);
    }

    #[test]
    fn keyword_shaped_properties_do_not_hijack_clauses() {
        match try_parse(
            "GRAPH TRAVERSE IN 'g' FROM 'a' DEPTH 3 LABEL 'L' EDGE WHERE depth > 7 AND label = 'x'",
        )
        .expect("graph DSL")
        .expect("well-formed")
        {
            NodedbStatement::Graph(GraphStmt::GraphTraverse {
                depth,
                edge_labels,
                edge_predicate,
                ..
            }) => {
                assert_eq!(depth, 3);
                assert_eq!(edge_labels, vec!["L".to_string()]);
                assert_eq!(
                    edge_predicate,
                    vec![
                        gt("depth", Value::Integer(7)),
                        MetadataFilter::Eq {
                            field: "label".into(),
                            value: Value::String("x".into()),
                        },
                    ]
                );
            }
            other => panic!("expected GraphTraverse, got {other:?}"),
        }
    }

    #[test]
    fn edge_where_inside_a_quoted_node_id_does_not_split() {
        match try_parse("GRAPH TRAVERSE IN 'g' FROM 'x EDGE WHERE y' DEPTH 1")
            .expect("graph DSL")
            .expect("well-formed")
        {
            NodedbStatement::Graph(GraphStmt::GraphTraverse {
                start,
                edge_predicate,
                ..
            }) => {
                assert_eq!(start, "x EDGE WHERE y");
                assert!(edge_predicate.is_empty());
            }
            other => panic!("expected GraphTraverse, got {other:?}"),
        }
    }

    #[test]
    fn graph_path_takes_a_predicate() {
        assert_eq!(
            predicate_of("GRAPH PATH IN 'g' FROM 'a' TO 'z' MAX_DEPTH 6 EDGE WHERE score > 5;"),
            vec![gt("score", Value::Integer(5))]
        );
    }

    #[test]
    fn unsupported_predicates_are_parse_errors() {
        for predicate in [
            "score > other",
            "name LIKE 'a%'",
            "lower(name) = 'a'",
            "score > 5 extra",
            "a.b = 1",
        ] {
            parse_error(&format!(
                "GRAPH TRAVERSE IN 'g' FROM 'a' EDGE WHERE {predicate}"
            ));
        }
        let detail = parse_error("GRAPH TRAVERSE IN 'g' FROM 'a' EDGE WHERE   ");
        assert!(detail.contains("requires a predicate"), "{detail}");
    }

    #[test]
    fn other_graph_statements_refuse_edge_where() {
        let detail = parse_error("GRAPH NEIGHBORS IN 'g' OF 'a' EDGE WHERE score > 1");
        assert!(
            detail.contains("GRAPH NEIGHBORS does not accept EDGE WHERE"),
            "{detail}"
        );
    }
}
