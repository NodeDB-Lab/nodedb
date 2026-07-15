// SPDX-License-Identifier: Apache-2.0

//! Parse CREATE/DROP MATERIALIZED VIEW + CREATE/DROP CONTINUOUS AGGREGATE.

use super::helpers::extract_name_after_if_exists;
use crate::ddl_ast::statement::{NodedbStatement, StreamViewStmt};
use crate::error::SqlError;
use crate::parser::preprocess::lex::{
    find_ascii_case_insensitive, find_ascii_case_insensitive_from, rfind_ascii_case_insensitive,
};

pub(super) fn try_parse(
    upper: &str,
    parts: &[&str],
    trimmed: &str,
) -> Option<Result<NodedbStatement, SqlError>> {
    (|| -> Result<Option<NodedbStatement>, SqlError> {
        if upper.starts_with("CREATE MATERIALIZED VIEW ") {
            return Ok(Some(parse_create_mv(upper, trimmed)));
        }
        if upper.starts_with("DROP MATERIALIZED VIEW ") {
            let if_exists = upper.contains("IF EXISTS");
            let name = match extract_name_after_if_exists(parts, "VIEW") {
                None => return Ok(None),
                Some(r) => r?,
            };
            return Ok(Some(NodedbStatement::StreamView(
                StreamViewStmt::DropMaterializedView { name, if_exists },
            )));
        }
        if upper.starts_with("SHOW MATERIALIZED VIEWS") {
            return Ok(Some(NodedbStatement::StreamView(
                StreamViewStmt::ShowMaterializedViews,
            )));
        }
        if upper.starts_with("CREATE CONTINUOUS AGGREGATE ") {
            return Ok(Some(parse_create_continuous_aggregate(upper, trimmed)));
        }
        if upper.starts_with("DROP CONTINUOUS AGGREGATE ") {
            let if_exists = upper.contains("IF EXISTS");
            let name = match extract_name_after_if_exists(parts, "AGGREGATE") {
                None => return Ok(None),
                Some(r) => r?,
            };
            return Ok(Some(NodedbStatement::StreamView(
                StreamViewStmt::DropContinuousAggregate { name, if_exists },
            )));
        }
        if upper.starts_with("SHOW CONTINUOUS AGGREGATES") {
            return Ok(Some(NodedbStatement::StreamView(
                StreamViewStmt::ShowContinuousAggregates,
            )));
        }
        Ok(None)
    })()
    .transpose()
}

/// Structural extraction for `CREATE MATERIALIZED VIEW`.
///
/// Extracts name, source, query_sql, and refresh_mode string.
/// The handler converts refresh_mode to the internal enum.
fn parse_create_mv(upper: &str, trimmed: &str) -> NodedbStatement {
    const KW_MV: &str = "MATERIALIZED VIEW ";
    const KW_ON: &str = " ON ";
    const KW_AS: &str = " AS ";

    let mv_pos = find_ascii_case_insensitive(trimmed, KW_MV).unwrap_or(0);
    let after_mv_start = mv_pos + KW_MV.len();
    let after_mv = trimmed[after_mv_start..].trim_start();

    let name = after_mv
        .split_whitespace()
        .next()
        .unwrap_or("")
        .to_lowercase();

    let source = find_ascii_case_insensitive_from(trimmed, KW_ON, after_mv_start)
        .map(|on_pos| {
            let after_on_start = on_pos + KW_ON.len();
            trimmed[after_on_start..]
                .split_whitespace()
                .next()
                .unwrap_or("")
                .to_lowercase()
        })
        .unwrap_or_default();

    let after_on_start = find_ascii_case_insensitive_from(trimmed, KW_ON, after_mv_start)
        .map(|position| position + KW_ON.len())
        .unwrap_or(after_mv_start);

    let query_sql = find_ascii_case_insensitive_from(trimmed, KW_AS, after_on_start)
        .map(|as_pos| {
            let query_start = as_pos + KW_AS.len();
            let query_end =
                find_trailing_with_options(trimmed, query_start).unwrap_or(trimmed.len());
            trimmed[query_start..query_end].trim().to_string()
        })
        .unwrap_or_default();

    let refresh_mode = extract_refresh_mode(upper, trimmed);

    NodedbStatement::StreamView(StreamViewStmt::CreateMaterializedView {
        name,
        source,
        query_sql,
        refresh_mode,
    })
}

/// Find trailing `WITH (` options clause (not a CTE WITH).
fn find_trailing_with_options(sql: &str, query_start: usize) -> Option<usize> {
    let region = &sql[query_start..];
    let mut search_end = region.len();
    loop {
        let pos = rfind_ascii_case_insensitive(&region[..search_end], "WITH")?;
        let after = region[pos + 4..].trim_start();
        if after.starts_with('(') {
            let before = &region[..pos];
            let ok = pos == 0 || {
                let last = before.chars().last().unwrap_or(' ');
                !last.is_alphanumeric() && last != '_'
            };
            if ok {
                return Some(query_start + pos);
            }
        }
        if pos == 0 {
            return None;
        }
        search_end = pos;
    }
}

fn extract_refresh_mode(upper: &str, trimmed: &str) -> String {
    // Look for WITH (...) at the end; find REFRESH key.
    if let Some(pos) = find_trailing_with_options(trimmed, 0) {
        let after = trimmed[pos + 4..].trim_start();
        if let Some(inner) = after
            .strip_prefix('(')
            .and_then(|s| s.split_once(')'))
            .map(|(i, _)| i)
        {
            for pair in inner.split(',') {
                if let Some((k, v)) = pair.split_once('=')
                    && k.trim().to_uppercase() == "REFRESH"
                {
                    return v.trim().trim_matches('\'').trim_matches('"').to_uppercase();
                }
            }
        }
    }
    // STREAMING keyword in main SQL → STREAMING mode.
    if upper.contains(" STREAMING ") || upper.ends_with(" STREAMING") {
        return "STREAMING".to_string();
    }
    "FULL".to_string()
}

/// Structural extraction for `CREATE CONTINUOUS AGGREGATE`.
///
/// Extracts name, source, bucket_raw, aggregate_exprs_raw, group_by, with_clause_raw.
fn parse_create_continuous_aggregate(_upper: &str, trimmed: &str) -> NodedbStatement {
    const PREFIX: &str = "CREATE CONTINUOUS AGGREGATE ";

    let rest = &trimmed[PREFIX.len()..];
    // name = token after "CONTINUOUS AGGREGATE "
    let name = rest.split_whitespace().next().unwrap_or("").to_lowercase();

    // source = token after " ON "
    let source = find_ascii_case_insensitive(rest, " ON ")
        .map(|pos| {
            rest[pos + 4..]
                .split_whitespace()
                .next()
                .unwrap_or("")
                .to_lowercase()
        })
        .unwrap_or_default();

    // bucket_raw = quoted token after BUCKET keyword
    let bucket_raw = find_ascii_case_insensitive(rest, "BUCKET")
        .and_then(|pos| {
            let after = rest[pos + 6..].trim_start();
            if let Some(inner) = after.strip_prefix('\'') {
                inner.find('\'').map(|end| inner[..end].to_string())
            } else {
                after.split_whitespace().next().map(|s| s.to_string())
            }
        })
        .unwrap_or_default();

    // aggregate_exprs_raw = text after AGGREGATE up to GROUP BY or WITH
    let aggregate_exprs_raw = find_ascii_case_insensitive(rest, "AGGREGATE ")
        .map(|pos| {
            let after_start = pos + 10;
            let end = ["GROUP BY ", "WITH "]
                .iter()
                .filter_map(|kw| find_ascii_case_insensitive_from(rest, kw, after_start))
                .min()
                .unwrap_or(rest.len());
            rest[after_start..end].trim().to_string()
        })
        .unwrap_or_default();

    // GROUP BY columns
    let group_by = find_ascii_case_insensitive(rest, "GROUP BY ")
        .map(|pos| {
            let after_start = pos + 9;
            let end =
                find_ascii_case_insensitive_from(rest, "WITH ", after_start).unwrap_or(rest.len());
            rest[after_start..end]
                .split(',')
                .map(|s| s.trim().to_lowercase())
                .filter(|s| !s.is_empty())
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();

    // WITH clause raw inner text
    let with_clause_raw = find_ascii_case_insensitive(rest, "WITH ")
        .and_then(|pos| {
            let after = rest[pos + 5..].trim_start();
            after
                .strip_prefix('(')
                .and_then(|s| s.split_once(')'))
                .map(|(inner, _)| inner.to_string())
        })
        .unwrap_or_default();

    NodedbStatement::StreamView(StreamViewStmt::CreateContinuousAggregate {
        name,
        source,
        bucket_raw,
        aggregate_exprs_raw,
        group_by,
        with_clause_raw,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_mv_unicode_name_preserves_original_offsets() {
        let sql = "CREATE MATERIALIZED VIEW summaryﬀﬀ ON orders AS SELECT count(*) FROM orders";
        let upper = sql.to_uppercase();
        if let NodedbStatement::StreamView(StreamViewStmt::CreateMaterializedView {
            name,
            source,
            query_sql,
            ..
        }) = parse_create_mv(&upper, sql)
        {
            assert_eq!(name, "summaryﬀﬀ");
            assert_eq!(source, "orders");
            assert_eq!(query_sql, "SELECT count(*) FROM orders");
        } else {
            panic!("expected CreateMaterializedView");
        }
    }

    #[test]
    fn parse_mv_basic() {
        let sql = "CREATE MATERIALIZED VIEW summary ON orders AS SELECT count(*) FROM orders";
        let upper = sql.to_uppercase();
        if let NodedbStatement::StreamView(StreamViewStmt::CreateMaterializedView {
            name,
            source,
            query_sql,
            refresh_mode,
        }) = parse_create_mv(&upper, sql)
        {
            assert_eq!(name, "summary");
            assert_eq!(source, "orders");
            assert!(query_sql.contains("count(*)"));
            assert_eq!(refresh_mode, "FULL");
        } else {
            panic!("expected CreateMaterializedView");
        }
    }

    #[test]
    fn parse_continuous_aggregate_unicode_name_preserves_original_offsets() {
        let sql = "CREATE CONTINUOUS AGGREGATE hourlyﬀﬀ ON metrics BUCKET '1h' AGGREGATE avg(cpu) AS mean_cpu";
        let upper = sql.to_uppercase();
        if let NodedbStatement::StreamView(StreamViewStmt::CreateContinuousAggregate {
            name,
            source,
            bucket_raw,
            aggregate_exprs_raw,
            ..
        }) = parse_create_continuous_aggregate(&upper, sql)
        {
            assert_eq!(name, "hourlyﬀﬀ");
            assert_eq!(source, "metrics");
            assert_eq!(bucket_raw, "1h");
            assert!(aggregate_exprs_raw.contains("avg(cpu)"));
        } else {
            panic!("expected CreateContinuousAggregate");
        }
    }

    #[test]
    fn parse_continuous_aggregate_basic() {
        let sql = "CREATE CONTINUOUS AGGREGATE hourly_avg ON metrics BUCKET '1h' AGGREGATE avg(cpu) AS mean_cpu";
        let upper = sql.to_uppercase();
        if let NodedbStatement::StreamView(StreamViewStmt::CreateContinuousAggregate {
            name,
            source,
            bucket_raw,
            aggregate_exprs_raw,
            ..
        }) = parse_create_continuous_aggregate(&upper, sql)
        {
            assert_eq!(name, "hourly_avg");
            assert_eq!(source, "metrics");
            assert_eq!(bucket_raw, "1h");
            assert!(aggregate_exprs_raw.to_uppercase().contains("AVG(CPU)"));
        } else {
            panic!("expected CreateContinuousAggregate");
        }
    }
}
