//! `CREATE MATERIALIZED VIEW` SQL parser + WITH-clause helpers.

use pgwire::error::PgWireResult;

use super::super::super::types::sqlstate_error;

const KW_MATERIALIZED_VIEW: &str = "MATERIALIZED VIEW ";
const KW_ON: &str = " ON ";
const KW_AS: &str = " AS ";

/// Parse CREATE MATERIALIZED VIEW SQL.
///
/// Syntax:
/// ```text
/// CREATE MATERIALIZED VIEW <name> ON <source> AS SELECT ...
///   [WITH (refresh = 'auto'|'manual')]
/// ```
///
/// Returns `(name, source, query_sql, refresh_mode)`.
pub fn parse_create_mv(sql: &str) -> PgWireResult<(String, String, String, String)> {
    let upper = sql.to_uppercase();

    // Extract name: word after "MATERIALIZED VIEW".
    let mv_pos = upper
        .find(KW_MATERIALIZED_VIEW)
        .ok_or_else(|| sqlstate_error("42601", "expected MATERIALIZED VIEW keyword"))?;
    let after_mv_start = mv_pos + KW_MATERIALIZED_VIEW.len();
    let after_mv = sql[after_mv_start..].trim_start();
    let name = after_mv
        .split_whitespace()
        .next()
        .ok_or_else(|| sqlstate_error("42601", "missing view name"))?
        .to_lowercase();

    // Extract source: word after "ON".
    let on_pos = upper[after_mv_start..]
        .find(KW_ON)
        .ok_or_else(|| sqlstate_error("42601", "expected ON <source> clause"))?;
    let after_on_start = after_mv_start + on_pos + KW_ON.len();
    let after_on = sql[after_on_start..].trim_start();
    let source = after_on
        .split_whitespace()
        .next()
        .ok_or_else(|| sqlstate_error("42601", "missing source collection name"))?
        .to_lowercase();

    // Extract query SQL: everything after "AS" up to "WITH" or end.
    let as_pos = upper[after_on_start..]
        .find(KW_AS)
        .ok_or_else(|| sqlstate_error("42601", "expected AS SELECT ... clause"))?;
    let query_start = after_on_start + as_pos + KW_AS.len();

    // Find end of query: a trailing `WITH (...)` options clause, or end of string.
    // We must not match a CTE `WITH cte AS (...)` inside the SELECT body.
    // The options clause is always `WITH (` — a CTE `WITH` is followed by
    // an identifier, never `(`.
    let query_end = find_trailing_with_options(&upper, query_start).unwrap_or(sql.len());
    let query_sql = sql[query_start..query_end].trim().to_string();

    if query_sql.is_empty() {
        return Err(sqlstate_error("42601", "empty query after AS"));
    }

    let refresh_mode = extract_refresh_mode(&upper, sql);

    Ok((name, source, query_sql, refresh_mode))
}

/// Find a trailing `WITH (...)` options clause that is NOT a CTE.
///
/// Scans backward from the end of the string for `WITH` followed (after
/// optional whitespace) by `(`. CTE syntax is `WITH <name> AS (...)` — the
/// word after `WITH` is an identifier, not `(`.
fn find_trailing_with_options(upper: &str, query_start: usize) -> Option<usize> {
    let region = &upper[query_start..];
    // Search backward for the last " WITH" or "WITH" that is followed by "(".
    let mut search_end = region.len();
    loop {
        let pos = region[..search_end].rfind("WITH")?;
        // Verify word boundary before WITH.
        if pos > 0 {
            let before = region.as_bytes()[pos - 1];
            if before.is_ascii_alphanumeric() || before == b'_' {
                search_end = pos;
                continue;
            }
        }
        // Check that WITH is followed by `(` (possibly with whitespace).
        let after_with = region[pos + 4..].trim_start();
        if after_with.starts_with('(') {
            return Some(query_start + pos);
        }
        // This WITH is a CTE or identifier — keep searching backward.
        search_end = pos;
    }
}

/// Extract refresh mode from WITH clause.
fn extract_refresh_mode(upper: &str, sql: &str) -> String {
    let with_pos = match upper.rfind("WITH") {
        Some(p) => p,
        None => return "auto".into(),
    };
    let after_with = sql[with_pos + 4..].trim_start();
    let open = match after_with.find('(') {
        Some(p) => p,
        None => return "auto".into(),
    };
    let close = match after_with.rfind(')') {
        Some(p) => p,
        None => return "auto".into(),
    };
    if close <= open {
        return "auto".into();
    }

    let inner = &after_with[open + 1..close];
    for pair in inner.split(',') {
        let pair = pair.trim();
        if let Some(eq) = pair.find('=') {
            let key = pair[..eq].trim().to_lowercase();
            let val = pair[eq + 1..]
                .trim()
                .trim_matches('\'')
                .trim_matches('"')
                .to_lowercase();
            if key == "refresh" || key == "refresh_mode" {
                return val;
            }
        }
    }
    "auto".into()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_create_basic() {
        let sql = "CREATE MATERIALIZED VIEW sales_daily ON orders \
                    AS SELECT date, SUM(amount) FROM orders GROUP BY date";
        let (name, source, query, refresh) = parse_create_mv(sql).unwrap();
        assert_eq!(name, "sales_daily");
        assert_eq!(source, "orders");
        assert!(query.contains("SUM(amount)"));
        assert_eq!(refresh, "auto");
    }

    #[test]
    fn parse_create_with_refresh() {
        let sql = "CREATE MATERIALIZED VIEW m1 ON src \
                    AS SELECT * FROM src \
                    WITH (refresh = 'manual')";
        let (name, source, query, refresh) = parse_create_mv(sql).unwrap();
        assert_eq!(name, "m1");
        assert_eq!(source, "src");
        assert_eq!(query, "SELECT * FROM src");
        assert_eq!(refresh, "manual");
    }

    #[test]
    fn parse_create_missing_as_errors() {
        let sql = "CREATE MATERIALIZED VIEW m1 ON src";
        assert!(parse_create_mv(sql).is_err());
    }

    #[test]
    fn parse_create_empty_query_errors() {
        let sql = "CREATE MATERIALIZED VIEW m1 ON src AS WITH (refresh = 'manual')";
        assert!(parse_create_mv(sql).is_err());
    }
}
