// SPDX-License-Identifier: BUSL-1.1

//! SQL parsing helpers for `CREATE CONTINUOUS AGGREGATE`.
//!
//! Ported from the pgwire `ddl::continuous_agg::parse` helpers. The parsing
//! logic, keyword tables, auto-alias derivation, and SQLSTATE codes are
//! preserved verbatim; only the error type changed from pgwire
//! `PgWireError` (via `sqlstate_error`) to the protocol-neutral [`DdlError`].

use nodedb_sql::parser::preprocess::lex::{
    find_ascii_case_insensitive, find_ascii_case_insensitive_from, rfind_ascii_case_insensitive,
};

use crate::engine::timeseries::continuous_agg::{
    AggFunction, AggregateExpr, ContinuousAggregateDef, RefreshPolicy,
};

use super::super::super::result::DdlError;

const KW_CONTINUOUS_AGGREGATE: &str = "CONTINUOUS AGGREGATE ";
const KW_ON: &str = " ON ";
const KW_BUCKET: &str = "BUCKET";
const KW_AGGREGATE: &str = "AGGREGATE ";
const KW_GROUP_BY: &str = "GROUP BY ";
const KW_AS: &str = " AS ";

fn err(sqlstate: &str, message: String) -> DdlError {
    DdlError {
        sqlstate: sqlstate.to_string(),
        message,
    }
}

/// Parse CREATE CONTINUOUS AGGREGATE SQL.
///
/// Syntax:
/// ```text
/// CREATE CONTINUOUS AGGREGATE <name> ON <source>
///   BUCKET '<interval>'
///   AGGREGATE <func>(col) [AS alias], ...
///   [GROUP BY col, ...]
///   [WITH (refresh_policy = '...', retention = '...')]
/// ```
pub(super) fn parse_create_sql(sql: &str) -> Result<ContinuousAggregateDef, DdlError> {
    let upper = sql.to_uppercase();

    // Extract name: word after "CONTINUOUS AGGREGATE"
    let ca_pos = find_ascii_case_insensitive(sql, KW_CONTINUOUS_AGGREGATE)
        .ok_or_else(|| err("42601", "expected CONTINUOUS AGGREGATE keyword".to_string()))?;
    let after_ca_start = ca_pos + KW_CONTINUOUS_AGGREGATE.len();
    let after_ca = sql[after_ca_start..].trim_start();
    let name = after_ca
        .split_whitespace()
        .next()
        .ok_or_else(|| err("42601", "missing aggregate name".to_string()))?
        .to_lowercase();

    // Extract source: word after "ON"
    let on_pos = find_ascii_case_insensitive_from(sql, KW_ON, after_ca_start)
        .ok_or_else(|| err("42601", "expected ON <source> clause".to_string()))?;
    let after_on_start = on_pos + KW_ON.len();
    let after_on = sql[after_on_start..].trim_start();
    let source = after_on
        .split_whitespace()
        .next()
        .ok_or_else(|| err("42601", "missing source collection name".to_string()))?
        .to_lowercase();

    // Extract bucket interval: between BUCKET ' and '
    let bucket_interval = extract_quoted_value(&upper, sql, KW_BUCKET)
        .ok_or_else(|| err("42601", "expected BUCKET '<interval>' clause".to_string()))?;

    let bucket_interval_ms = nodedb_types::kv_parsing::parse_interval_to_ms(&bucket_interval)
        .map_err(|e| err("42601", format!("invalid bucket interval: {e}")))?
        as i64;

    // Extract aggregates: between AGGREGATE and GROUP BY / WITH / end
    let aggregates = extract_aggregates(&upper, sql)?;
    if aggregates.is_empty() {
        return Err(err(
            "42601",
            "expected AGGREGATE <func>(col), ... clause".to_string(),
        ));
    }

    // Extract GROUP BY columns (optional).
    let group_by = extract_group_by(&upper, sql);

    // Extract WITH options (optional).
    let (refresh_policy, retention_period_ms) = extract_with_options(&upper, sql);

    Ok(ContinuousAggregateDef {
        // Placeholder: the caller (`create_continuous_aggregate`) rebuilds
        // the def with the session's real `database_id`. This parse-only
        // intermediate never reaches the catalog or Data Plane.
        database_id: nodedb_types::DatabaseId::DEFAULT.as_u64(),
        name,
        source,
        bucket_interval,
        bucket_interval_ms,
        group_by,
        aggregates,
        refresh_policy,
        retention_period_ms,
        stale: false,
    })
}

/// Extract a quoted value after a keyword: `KEYWORD 'value'`.
pub(super) fn extract_quoted_value(_upper: &str, sql: &str, keyword: &str) -> Option<String> {
    let pos = find_ascii_case_insensitive(sql, keyword)?;
    let after = sql[pos + keyword.len()..].trim_start();
    let start = after.find('\'')?;
    let end = after[start + 1..].find('\'')?;
    Some(after[start + 1..start + 1 + end].to_string())
}

/// Extract aggregate expressions from AGGREGATE clause.
///
/// Parses: `AGGREGATE sum(value) AS value_sum, count(*) AS row_count, avg(cpu)`
fn extract_aggregates(_upper: &str, sql: &str) -> Result<Vec<AggregateExpr>, DdlError> {
    // Find standalone AGGREGATE keyword. Skip past "CONTINUOUS AGGREGATE" by
    // searching after the BUCKET clause (which always precedes AGGREGATE).
    let search_start = find_ascii_case_insensitive(sql, KW_BUCKET).unwrap_or(0);
    let agg_pos = match find_ascii_case_insensitive_from(sql, KW_AGGREGATE, search_start) {
        Some(position) => position,
        None => return Ok(Vec::new()),
    };
    let after_agg_start = agg_pos + KW_AGGREGATE.len();

    // Find end: GROUP BY, WITH, or end of string.
    let end_pos = [KW_GROUP_BY, "WITH (", "WITH("]
        .iter()
        .filter_map(|kw| find_ascii_case_insensitive_from(sql, kw, after_agg_start))
        .min()
        .unwrap_or(sql.len());

    let agg_str = sql[after_agg_start..end_pos].trim().trim_end_matches(',');
    let mut exprs = Vec::new();

    for part in agg_str.split(',') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        let expr = parse_single_aggregate(part)?;
        exprs.push(expr);
    }

    Ok(exprs)
}

/// Parse a single aggregate expression: `func(col) [AS alias]`.
fn parse_single_aggregate(s: &str) -> Result<AggregateExpr, DdlError> {
    // Split on AS for alias.
    let (func_part, alias) = if let Some(as_pos) = find_ascii_case_insensitive(s, KW_AS) {
        (
            &s[..as_pos],
            Some(s[as_pos + KW_AS.len()..].trim().to_lowercase()),
        )
    } else {
        (s, None)
    };
    let func_part = func_part.trim();

    // Parse func(col).
    let open = func_part
        .find('(')
        .ok_or_else(|| err("42601", format!("expected function(column) syntax: {s}")))?;
    let close = func_part
        .rfind(')')
        .ok_or_else(|| err("42601", format!("missing closing parenthesis: {s}")))?;

    let func_name = func_part[..open].trim().to_lowercase();
    let col_name = func_part[open + 1..close].trim().to_lowercase();

    let function = match func_name.as_str() {
        "sum" => AggFunction::Sum,
        "count" => AggFunction::Count,
        "min" => AggFunction::Min,
        "max" => AggFunction::Max,
        "avg" => AggFunction::Avg,
        "first" => AggFunction::First,
        "last" => AggFunction::Last,
        "count_distinct" => AggFunction::CountDistinct,
        other => {
            return Err(err("42601", format!("unknown aggregate function: {other}")));
        }
    };

    let output_column = alias.unwrap_or_else(|| {
        if col_name == "*" {
            func_name.clone()
        } else {
            format!("{func_name}_{col_name}")
        }
    });

    Ok(AggregateExpr {
        function,
        source_column: col_name,
        output_column,
    })
}

/// Extract GROUP BY columns.
fn extract_group_by(_upper: &str, sql: &str) -> Vec<String> {
    let gb_pos = match find_ascii_case_insensitive(sql, KW_GROUP_BY) {
        Some(p) => p,
        None => return Vec::new(),
    };
    let after_gb_start = gb_pos + KW_GROUP_BY.len();

    // Find end: WITH or end of string.
    let end_pos = ["WITH (", "WITH("]
        .iter()
        .filter_map(|kw| find_ascii_case_insensitive_from(sql, kw, after_gb_start))
        .min()
        .unwrap_or(sql.len());

    sql[after_gb_start..end_pos]
        .split(',')
        .map(|s| s.trim().to_lowercase())
        .filter(|s| !s.is_empty())
        .collect()
}

/// Extract WITH options: refresh_policy and retention.
pub(super) fn extract_with_options(_upper: &str, sql: &str) -> (RefreshPolicy, u64) {
    let mut refresh = RefreshPolicy::OnFlush;
    let mut retention_ms = 0u64;

    let with_pos = match rfind_ascii_case_insensitive(sql, "WITH") {
        Some(p) => p,
        None => return (refresh, retention_ms),
    };
    let after_with = sql[with_pos + 4..].trim_start();
    let open = match after_with.find('(') {
        Some(p) => p,
        None => return (refresh, retention_ms),
    };
    let close = match after_with.rfind(')') {
        Some(p) => p,
        None => return (refresh, retention_ms),
    };
    if close <= open {
        return (refresh, retention_ms);
    }

    let inner = &after_with[open + 1..close];
    for pair in inner.split(',') {
        let pair = pair.trim();
        if let Some(eq) = pair.find('=') {
            let key = pair[..eq].trim().to_lowercase();
            let val = pair[eq + 1..].trim().trim_matches('\'').trim_matches('"');
            match key.as_str() {
                "refresh_policy" | "refresh" => {
                    refresh = match val.to_lowercase().as_str() {
                        "on_flush" | "onflush" => RefreshPolicy::OnFlush,
                        "on_seal" | "onseal" => RefreshPolicy::OnSeal,
                        "manual" => RefreshPolicy::Manual,
                        other => {
                            if let Ok(ms) = nodedb_types::kv_parsing::parse_interval_to_ms(other) {
                                RefreshPolicy::Periodic(ms)
                            } else {
                                RefreshPolicy::OnFlush
                            }
                        }
                    };
                }
                "retention" | "retention_period" => {
                    if let Ok(ms) = nodedb_types::kv_parsing::parse_interval_to_ms(val) {
                        retention_ms = ms;
                    }
                }
                _ => {}
            }
        }
    }

    (refresh, retention_ms)
}
