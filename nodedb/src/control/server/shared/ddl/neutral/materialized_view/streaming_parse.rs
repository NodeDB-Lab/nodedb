// SPDX-License-Identifier: BUSL-1.1

//! Parser for the streaming variant of `CREATE MATERIALIZED VIEW ... STREAMING`.
//!
//! Ported from the deleted pgwire `ddl::streaming_mv::create` parser. The
//! extraction logic (source stream from the `FROM` clause, `GROUP BY` columns,
//! `WHERE` filter, and the `COUNT/SUM/MIN/MAX/AVG` aggregate list) is preserved
//! verbatim; the only behavioural change is that it operates on the already-split
//! query body (`query_sql`, the text after ` AS `) plus the view `name` that the
//! DDL parser extracted, rather than re-parsing the full statement string. Parse
//! failures surface as protocol-neutral [`DdlError`] with SQLSTATE `42601`.

use crate::event::streaming_mv::types::{AggDef, AggFunction};
use nodedb_sql::parser::preprocess::lex::{
    find_ascii_case_insensitive, rfind_ascii_case_insensitive,
};

use super::super::super::result::DdlError;

fn parse_err(message: &str) -> DdlError {
    DdlError {
        sqlstate: "42601".to_string(),
        message: message.to_string(),
    }
}

/// Parsed pieces of a streaming materialized view definition.
pub struct ParsedStreamingMv {
    /// GROUP BY column names (lowercased).
    pub group_by_columns: Vec<String>,
    /// Aggregate functions to compute, in SELECT-list order.
    pub aggregates: Vec<AggDef>,
    /// Optional WHERE filter expression (raw SQL fragment).
    pub filter_expr: Option<String>,
    /// Source change stream name (lowercased), derived from the FROM clause.
    pub source_stream: String,
}

/// Parse the SELECT body of a streaming MV.
///
/// `query_sql` is the query text after ` AS ` (e.g.
/// `SELECT status, count(*) AS cnt FROM orders_stream GROUP BY status`). The
/// source stream is the token following `FROM` — a change stream, not the `ON`
/// lineage collection.
pub fn parse_streaming_mv(query_sql: &str) -> Result<ParsedStreamingMv, DdlError> {
    let query = query_sql.trim().trim_end_matches(';').trim();
    let query_upper = query.to_uppercase();

    // Extract FROM <stream>.
    let from_pos = find_ascii_case_insensitive(query, " FROM ")
        .ok_or_else(|| parse_err("expected FROM clause"))?;
    let after_from = query[from_pos + 6..].trim();
    let source_stream = after_from
        .split_whitespace()
        .next()
        .unwrap_or("")
        .to_lowercase();

    // Extract GROUP BY columns.
    let group_by_columns = if let Some(gb_pos) = find_ascii_case_insensitive(query, " GROUP BY ") {
        let gb_str = query[gb_pos + 10..].trim();
        gb_str
            .split(',')
            .map(|s| s.trim().to_lowercase())
            .filter(|s| !s.is_empty())
            .collect()
    } else {
        Vec::new()
    };

    // Extract WHERE filter (between FROM <stream> and GROUP BY).
    let filter_expr = if let Some(where_pos) = find_ascii_case_insensitive(query, " WHERE ") {
        let end = find_ascii_case_insensitive(query, " GROUP BY ").unwrap_or(query.len());
        if where_pos < end {
            Some(query[where_pos + 7..end].trim().to_string())
        } else {
            None
        }
    } else {
        None
    };

    // Extract SELECT list (between SELECT and FROM).
    if !query_upper.starts_with("SELECT ") {
        return Err(parse_err("expected SELECT"));
    }
    let select_list = query[7..from_pos].trim();

    // Parse aggregates from SELECT list.
    let aggregates = parse_select_aggregates(select_list);

    if aggregates.is_empty() {
        return Err(parse_err(
            "streaming MV requires at least one aggregate function (COUNT, SUM, MIN, MAX, AVG)",
        ));
    }

    Ok(ParsedStreamingMv {
        group_by_columns,
        aggregates,
        filter_expr,
        source_stream,
    })
}

/// Parse aggregate functions from a SELECT list.
///
/// Supports: `count(*) AS cnt`, `sum(field) AS total`, `min(field)`, etc.
/// Non-aggregate items (e.g. GROUP BY column references) are skipped.
fn parse_select_aggregates(select_list: &str) -> Vec<AggDef> {
    let mut aggregates = Vec::new();

    for item in select_list.split(',') {
        let item = item.trim();
        if item.is_empty() || item == "*" {
            continue;
        }

        // Split on AS to get alias.
        let (expr_part, alias) = if let Some(as_pos) = rfind_ascii_case_insensitive(item, " AS ") {
            (
                item[..as_pos].trim(),
                item[as_pos + 4..].trim().to_lowercase(),
            )
        } else {
            (item, item.to_lowercase().replace(['(', ')', '*', ' '], "_"))
        };

        // Parse aggregate function: func(args).
        let expr_upper = expr_part.to_uppercase();
        let func = if expr_upper.starts_with("COUNT(") {
            Some(AggFunction::Count)
        } else if expr_upper.starts_with("SUM(") {
            Some(AggFunction::Sum)
        } else if expr_upper.starts_with("MIN(") {
            Some(AggFunction::Min)
        } else if expr_upper.starts_with("MAX(") {
            Some(AggFunction::Max)
        } else if expr_upper.starts_with("AVG(") {
            Some(AggFunction::Avg)
        } else {
            // Not an aggregate — skip (could be a GROUP BY column reference).
            None
        };

        if let Some(function) = func {
            // Extract the input expression between parentheses.
            let inner = expr_part
                .split_once('(')
                .and_then(|(_, rest)| rest.rsplit_once(')'))
                .map(|(inner, _)| inner.trim().to_string())
                .unwrap_or_default();

            let input_expr = if inner == "*" {
                // COUNT(*).
                String::new()
            } else {
                inner
            };

            aggregates.push(AggDef {
                output_name: alias,
                function,
                input_expr,
            });
        }
    }

    aggregates
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_basic_streaming_mv() {
        let query = "SELECT event_type, count(*) AS cnt \
                     FROM orders_stream \
                     GROUP BY event_type";
        let parsed = parse_streaming_mv(query).unwrap();
        assert_eq!(parsed.source_stream, "orders_stream");
        assert_eq!(parsed.group_by_columns, vec!["event_type"]);
        assert_eq!(parsed.aggregates.len(), 1);
        assert_eq!(parsed.aggregates[0].function, AggFunction::Count);
        assert_eq!(parsed.aggregates[0].output_name, "cnt");
        assert!(parsed.aggregates[0].input_expr.is_empty());
    }

    #[test]
    fn parse_multi_aggregate() {
        let query = "SELECT count(*) AS cnt, sum(total) AS revenue \
                     FROM orders_stream \
                     GROUP BY event_type";
        let parsed = parse_streaming_mv(query).unwrap();
        assert_eq!(parsed.aggregates.len(), 2);
        assert_eq!(parsed.aggregates[1].function, AggFunction::Sum);
        assert_eq!(parsed.aggregates[1].input_expr, "total");
    }

    #[test]
    fn aggregate_alias_after_unicode_expression_preserves_original_offsets() {
        let parsed =
            parse_streaming_mv("SELECT sum(ﬀﬀ) AS total FROM orders_stream GROUP BY event_type")
                .expect("streaming aggregate should parse");
        assert_eq!(parsed.aggregates.len(), 1);
        assert_eq!(parsed.aggregates[0].input_expr, "ﬀﬀ");
        assert_eq!(parsed.aggregates[0].output_name, "total");
    }

    #[test]
    fn parse_with_where() {
        let query = "SELECT count(*) AS cnt \
                     FROM orders_stream \
                     WHERE event_type = 'INSERT' \
                     GROUP BY collection";
        let parsed = parse_streaming_mv(query).unwrap();
        assert!(parsed.filter_expr.is_some());
        assert!(parsed.filter_expr.unwrap().contains("event_type"));
    }

    #[test]
    fn requires_at_least_one_aggregate() {
        let query = "SELECT status FROM orders_stream GROUP BY status";
        assert!(parse_streaming_mv(query).is_err());
    }

    #[test]
    fn requires_from_clause() {
        let query = "SELECT count(*) AS cnt GROUP BY status";
        assert!(parse_streaming_mv(query).is_err());
    }
}
