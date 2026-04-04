//! SQL parsing helpers for retention policy DDL.
//!
//! Extracted from create.rs to keep files under the 500-line limit.

use pgwire::error::PgWireResult;

use crate::engine::timeseries::continuous_agg::{AggFunction, AggregateExpr};
use crate::engine::timeseries::retention_policy::types::{
    ArchiveTarget, RetentionPolicyDef, TierDef,
};

use super::super::super::types::sqlstate_error;

pub(super) struct ParsedRetentionPolicy {
    pub name: String,
    pub collection: String,
    pub tiers: Vec<TierDef>,
    pub tier_count: usize,
    pub eval_interval_ms: u64,
}

/// Parse the CREATE RETENTION POLICY SQL.
pub(super) fn parse_create_retention_policy(sql: &str) -> PgWireResult<ParsedRetentionPolicy> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    let upper = trimmed.to_uppercase();

    // Extract name: "CREATE RETENTION POLICY <name> ON ..."
    let prefix = "CREATE RETENTION POLICY ";
    if !upper.starts_with(prefix) {
        return Err(sqlstate_error("42601", "expected CREATE RETENTION POLICY"));
    }
    let after_prefix = &trimmed[prefix.len()..];
    let name = after_prefix
        .split_whitespace()
        .next()
        .ok_or_else(|| sqlstate_error("42601", "missing policy name"))?
        .to_lowercase();

    // Extract collection: "... ON <collection> (...)"
    let upper_after = upper[prefix.len()..].to_string();
    let on_pos = upper_after
        .find(" ON ")
        .ok_or_else(|| sqlstate_error("42601", "expected ON <collection>"))?;
    let after_on = after_prefix[on_pos + 4..].trim_start();
    let collection = after_on
        .split(|c: char| c.is_whitespace() || c == '(')
        .next()
        .ok_or_else(|| sqlstate_error("42601", "missing collection name"))?
        .to_lowercase();

    // Find the tier body between balanced outer parentheses.
    let body_start = trimmed
        .find('(')
        .ok_or_else(|| sqlstate_error("42601", "expected '(' after collection name"))?;
    let body_end = find_matching_paren(trimmed, body_start)
        .ok_or_else(|| sqlstate_error("42601", "missing closing ')'"))?;
    if body_end <= body_start {
        return Err(sqlstate_error("42601", "empty tier definition body"));
    }
    let body = &trimmed[body_start + 1..body_end];

    // Parse tiers from body.
    let tiers = parse_tiers(body)?;
    if tiers.is_empty() {
        return Err(sqlstate_error(
            "42601",
            "at least one tier (RAW) is required",
        ));
    }
    if !tiers[0].is_raw() {
        return Err(sqlstate_error("42601", "first tier must be RAW"));
    }

    let tier_count = tiers.len();

    // Parse optional WITH clause after the closing ')'.
    let eval_interval_ms = parse_with_clause(trimmed, body_end);

    Ok(ParsedRetentionPolicy {
        name,
        collection,
        tiers,
        tier_count,
        eval_interval_ms,
    })
}

/// Parse the tier definitions from the body between outer parentheses.
///
/// Handles: RAW RETAIN, DOWNSAMPLE TO, ARCHIVE TO.
/// Splits on top-level commas (respecting nested parentheses).
fn parse_tiers(body: &str) -> PgWireResult<Vec<TierDef>> {
    let clauses = split_top_level_commas(body);
    let mut tiers = Vec::new();
    let mut tier_index = 0u32;

    for clause in &clauses {
        let clause = clause.trim();
        if clause.is_empty() {
            continue;
        }
        let upper = clause.to_uppercase();

        if upper.starts_with("RAW") {
            let retain_ms = extract_retain(clause)?;
            tiers.push(TierDef {
                tier_index,
                resolution_ms: 0,
                aggregates: Vec::new(),
                retain_ms,
                archive: None,
            });
            tier_index += 1;
        } else if upper.starts_with("DOWNSAMPLE") {
            let resolution_ms = extract_downsample_interval(clause)?;
            let aggregates = extract_tier_aggregates(clause)?;
            if aggregates.is_empty() {
                return Err(sqlstate_error(
                    "42601",
                    "DOWNSAMPLE tier requires at least one AGGREGATE",
                ));
            }
            let retain_ms = extract_retain(clause)?;
            tiers.push(TierDef {
                tier_index,
                resolution_ms,
                aggregates,
                retain_ms,
                archive: None,
            });
            tier_index += 1;
        } else if upper.starts_with("ARCHIVE") {
            let url = extract_archive_url(clause)?;
            if let Some(last) = tiers.last_mut() {
                last.archive = Some(ArchiveTarget::S3 { url });
            } else {
                return Err(sqlstate_error(
                    "42601",
                    "ARCHIVE TO must follow at least one tier",
                ));
            }
        } else {
            return Err(sqlstate_error(
                "42601",
                &format!(
                    "unexpected tier clause: {}",
                    &clause[..clause.len().min(40)]
                ),
            ));
        }
    }

    Ok(tiers)
}

/// Split on commas that are NOT inside parentheses.
fn split_top_level_commas(s: &str) -> Vec<&str> {
    let mut results = Vec::new();
    let mut depth = 0usize;
    let mut start = 0;

    for (i, ch) in s.char_indices() {
        match ch {
            '(' => depth += 1,
            ')' => depth = depth.saturating_sub(1),
            ',' if depth == 0 => {
                results.push(&s[start..i]);
                start = i + 1;
            }
            _ => {}
        }
    }
    if start < s.len() {
        results.push(&s[start..]);
    }
    results
}

/// Extract RETAIN '<duration>' → milliseconds.
fn extract_retain(clause: &str) -> PgWireResult<u64> {
    let upper = clause.to_uppercase();
    let pos = upper
        .find("RETAIN")
        .ok_or_else(|| sqlstate_error("42601", "missing RETAIN clause"))?;
    let after = clause[pos + 6..].trim_start();
    let val = extract_quoted_string(after)?;

    if val.eq_ignore_ascii_case("forever") {
        return Ok(0);
    }

    nodedb_types::kv_parsing::parse_interval_to_ms(&val)
        .map_err(|e| sqlstate_error("42601", &format!("invalid retain duration '{val}': {e}")))
}

/// Extract DOWNSAMPLE TO '<interval>' → milliseconds.
fn extract_downsample_interval(clause: &str) -> PgWireResult<u64> {
    let upper = clause.to_uppercase();
    let pos = upper
        .find("TO")
        .ok_or_else(|| sqlstate_error("42601", "expected DOWNSAMPLE TO '<interval>'"))?;
    let after = clause[pos + 2..].trim_start();
    let val = extract_quoted_string(after)?;

    nodedb_types::kv_parsing::parse_interval_to_ms(&val).map_err(|e| {
        sqlstate_error(
            "42601",
            &format!("invalid downsample interval '{val}': {e}"),
        )
    })
}

/// Extract ARCHIVE TO '<url>'.
fn extract_archive_url(clause: &str) -> PgWireResult<String> {
    let upper = clause.to_uppercase();
    let pos = upper
        .find("TO")
        .ok_or_else(|| sqlstate_error("42601", "expected ARCHIVE TO '<url>'"))?;
    let after = clause[pos + 2..].trim_start();
    extract_quoted_string(after)
}

/// Extract aggregate expressions from AGGREGATE (...) in a tier clause.
fn extract_tier_aggregates(clause: &str) -> PgWireResult<Vec<AggregateExpr>> {
    let upper = clause.to_uppercase();
    let agg_pos = match upper.find("AGGREGATE") {
        Some(p) => p,
        None => return Ok(Vec::new()),
    };
    let after_agg = &clause[agg_pos + 9..].trim_start();

    let open = after_agg
        .find('(')
        .ok_or_else(|| sqlstate_error("42601", "expected '(' after AGGREGATE"))?;

    let close = find_matching_paren(after_agg, open)
        .ok_or_else(|| sqlstate_error("42601", "missing ')' after AGGREGATE expressions"))?;
    if close <= open + 1 {
        return Err(sqlstate_error("42601", "empty AGGREGATE expression list"));
    }

    let inner = &after_agg[open + 1..close];
    let mut exprs = Vec::new();

    for part in inner.split(',') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        let expr = parse_agg_expr(part)?;
        exprs.push(expr);
    }

    Ok(exprs)
}

/// Find the index of the closing paren that matches the opening paren at `open`.
fn find_matching_paren(s: &str, open: usize) -> Option<usize> {
    let mut depth = 0usize;
    for (i, ch) in s[open..].char_indices() {
        match ch {
            '(' => depth += 1,
            ')' => {
                depth -= 1;
                if depth == 0 {
                    return Some(open + i);
                }
            }
            _ => {}
        }
    }
    None
}

/// Parse a single aggregate expression: `func(col)` or `func(col) AS alias`.
pub(super) fn parse_agg_expr(s: &str) -> PgWireResult<AggregateExpr> {
    let upper = s.to_uppercase();

    let (func_part, alias) = if let Some(as_pos) = upper.find(" AS ") {
        (&s[..as_pos], Some(s[as_pos + 4..].trim().to_lowercase()))
    } else {
        (s, None)
    };
    let func_part = func_part.trim();

    let open = func_part
        .find('(')
        .ok_or_else(|| sqlstate_error("42601", &format!("expected func(col): {s}")))?;
    let close = func_part
        .rfind(')')
        .ok_or_else(|| sqlstate_error("42601", &format!("missing ')': {s}")))?;

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
            return Err(sqlstate_error(
                "42601",
                &format!("unknown aggregate function: {other}"),
            ));
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

/// Extract a single-quoted string value.
fn extract_quoted_string(s: &str) -> PgWireResult<String> {
    let start = s
        .find('\'')
        .ok_or_else(|| sqlstate_error("42601", "expected quoted string"))?;
    let end = s[start + 1..]
        .find('\'')
        .ok_or_else(|| sqlstate_error("42601", "missing closing quote"))?;
    Ok(s[start + 1..start + 1 + end].to_string())
}

/// Parse optional WITH (EVAL_INTERVAL = '<duration>') after closing ')'.
fn parse_with_clause(sql: &str, body_end: usize) -> u64 {
    let after_body = &sql[body_end + 1..];
    let upper = after_body.to_uppercase();
    let with_pos = match upper.find("WITH") {
        Some(p) => p,
        None => return RetentionPolicyDef::DEFAULT_EVAL_INTERVAL_MS,
    };
    let after_with = after_body[with_pos + 4..].trim_start();
    let inner = match after_with
        .strip_prefix('(')
        .and_then(|s| s.split_once(')'))
        .map(|(inner, _)| inner)
    {
        Some(inner) => inner,
        None => return RetentionPolicyDef::DEFAULT_EVAL_INTERVAL_MS,
    };

    for pair in inner.split(',') {
        let pair = pair.trim();
        if let Some((key, val)) = pair.split_once('=') {
            let key = key.trim().to_uppercase();
            let val = val.trim().trim_matches('\'').trim_matches('"');
            if key == "EVAL_INTERVAL"
                && let Ok(ms) = nodedb_types::kv_parsing::parse_interval_to_ms(val)
            {
                return ms;
            }
        }
    }

    RetentionPolicyDef::DEFAULT_EVAL_INTERVAL_MS
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::timeseries::retention_policy::types::{ArchiveTarget, RetentionPolicyDef};

    #[test]
    fn parse_basic_policy() {
        let sql = "CREATE RETENTION POLICY sensor_policy ON sensor_data (\
                    RAW RETAIN '7 days', \
                    DOWNSAMPLE TO '1 minute' AGGREGATE (AVG(value), MIN(value), MAX(value), COUNT(*)) RETAIN '90 days', \
                    DOWNSAMPLE TO '1 hour' AGGREGATE (AVG(value), MIN(value), MAX(value), COUNT(*)) RETAIN '2 years', \
                    ARCHIVE TO 's3://bucket/sensor-data/'\
                    )";
        let parsed = parse_create_retention_policy(sql).unwrap();
        assert_eq!(parsed.name, "sensor_policy");
        assert_eq!(parsed.collection, "sensor_data");
        assert_eq!(parsed.tiers.len(), 3);

        assert!(parsed.tiers[0].is_raw());
        assert_eq!(parsed.tiers[0].retain_ms, 604_800_000);

        assert_eq!(parsed.tiers[1].resolution_ms, 60_000);
        assert_eq!(parsed.tiers[1].aggregates.len(), 4);
        assert_eq!(parsed.tiers[1].retain_ms, 7_776_000_000);

        assert_eq!(parsed.tiers[2].resolution_ms, 3_600_000);
        assert_eq!(parsed.tiers[2].aggregates.len(), 4);
        assert!(matches!(
            &parsed.tiers[2].archive,
            Some(ArchiveTarget::S3 { url }) if url == "s3://bucket/sensor-data/"
        ));

        assert_eq!(
            parsed.eval_interval_ms,
            RetentionPolicyDef::DEFAULT_EVAL_INTERVAL_MS
        );
    }

    #[test]
    fn parse_with_eval_interval() {
        let sql = "CREATE RETENTION POLICY p1 ON metrics (\
                    RAW RETAIN '30 days'\
                    ) WITH (EVAL_INTERVAL = '30m')";
        let parsed = parse_create_retention_policy(sql).unwrap();
        assert_eq!(parsed.eval_interval_ms, 1_800_000);
    }

    #[test]
    fn parse_forever_retain() {
        let sql = "CREATE RETENTION POLICY p1 ON metrics (\
                    RAW RETAIN 'forever'\
                    )";
        let parsed = parse_create_retention_policy(sql).unwrap();
        assert_eq!(parsed.tiers[0].retain_ms, 0);
    }

    #[test]
    fn parse_errors_no_raw() {
        let sql = "CREATE RETENTION POLICY p1 ON metrics (\
                    DOWNSAMPLE TO '1h' AGGREGATE (AVG(v)) RETAIN '30d'\
                    )";
        assert!(parse_create_retention_policy(sql).is_err());
    }

    #[test]
    fn parse_errors_empty_body() {
        let sql = "CREATE RETENTION POLICY p1 ON metrics ()";
        assert!(parse_create_retention_policy(sql).is_err());
    }

    #[test]
    fn parse_agg_with_alias() {
        let expr = parse_agg_expr("AVG(temperature) AS avg_temp").unwrap();
        assert!(matches!(expr.function, AggFunction::Avg));
        assert_eq!(expr.source_column, "temperature");
        assert_eq!(expr.output_column, "avg_temp");
    }

    #[test]
    fn parse_agg_auto_alias() {
        let expr = parse_agg_expr("COUNT(*)").unwrap();
        assert!(matches!(expr.function, AggFunction::Count));
        assert_eq!(expr.output_column, "count");
    }

    #[test]
    fn split_commas_respects_parens() {
        let input = "RAW RETAIN '7d', DOWNSAMPLE TO '1m' AGGREGATE (AVG(v), MAX(v)) RETAIN '90d'";
        let parts = split_top_level_commas(input);
        assert_eq!(parts.len(), 2);
        assert!(parts[0].trim().starts_with("RAW"));
        assert!(parts[1].trim().starts_with("DOWNSAMPLE"));
    }
}
