// SPDX-License-Identifier: Apache-2.0

//! Extract and strip NodeDB-specific bitemporal clauses before sqlparser
//! sees the statement.
//!
//! Supported forms (case-insensitive, whitespace-tolerant):
//!
//! ```sql
//! -- Table scan style (existing):
//! SELECT ... FROM t FOR SYSTEM_TIME AS OF 1700000000000 WHERE ...
//! SELECT ... FROM t FOR VALID_TIME CONTAINS 1700000000000 WHERE ...
//! SELECT ... FROM t FOR VALID_TIME FROM 1700000000000 TO 1700001000000 WHERE ...
//!
//! -- Array read style (CockroachDB-inspired, two orthogonal clauses):
//! SELECT ... FROM array_slice(...) AS OF SYSTEM TIME 1700000000000
//! SELECT ... FROM array_slice(...) AS OF VALID TIME 1700000000000
//! SELECT ... FROM array_slice(...) AS OF SYSTEM TIME 1700000000000 AS OF VALID TIME 1700000001000
//! ```
//!
//! A function-form escape hatch is also accepted, anywhere in the statement,
//! for pgwire drivers that reject non-standard clauses:
//!
//! ```sql
//! SELECT __system_as_of__(1700000000000) FROM t WHERE ...
//! ```
//!
//! All recognised clauses are extracted once per statement. Repeated clauses
//! of the same kind return `Err`; conflicting valid-time forms (CONTAINS +
//! FROM/TO) return `Err`.
//!
//! Timestamps are **milliseconds since Unix epoch**. Accepted expression forms:
//! - Integer literal (milliseconds since epoch)
//! - `NOW()` — resolved to the current wall-clock millisecond
//! - ISO-8601 / RFC-3339 timestamp string `'2024-01-15T00:00:00Z'`
//!
//! Any other expression form is rejected with a typed `TemporalParseError`
//! naming the unsupported form.

use super::lex::{
    find_ascii_case_insensitive, find_ascii_case_insensitive_from,
    keyword_position_outside_literals,
};
use crate::temporal::{TemporalScope, ValidTime};
use nodedb_types::SystemTimeScope;

/// Output of the temporal preprocess stage.
#[derive(Debug)]
pub struct Extracted {
    /// SQL with every temporal clause stripped, safe to hand to sqlparser.
    pub sql: String,
    /// Extracted temporal qualifier. Default when no clause was present.
    pub temporal: TemporalScope,
}

/// Error shape — surfaced as `SqlError::Parse` by the caller.
#[derive(Debug)]
pub struct TemporalParseError(pub String);

/// Strip temporal clauses from `sql` and return the rewritten text plus the
/// extracted `TemporalScope`. Returns `Ok(None)` when no temporal clause is
/// present so the caller can short-circuit to the existing pipeline.
pub fn extract(sql: &str) -> Result<Option<Extracted>, TemporalParseError> {
    let mut scope = TemporalScope::default();
    let mut working = sql.to_string();
    let mut any = false;

    // FOR SYSTEM_TIME AS OF (table-scan style)
    if let Some((rewritten, ms)) = strip_system_time_as_of(&working)? {
        working = rewritten;
        scope.system_time = SystemTimeScope::AsOf(ms);
        any = true;
    }
    // __system_as_of__(<int>) function escape hatch
    if let Some((rewritten, ms)) = strip_system_as_of_function(&working)? {
        if scope.system_time.is_temporal() {
            return Err(TemporalParseError(
                "multiple FOR SYSTEM_TIME / __system_as_of__ clauses".into(),
            ));
        }
        working = rewritten;
        scope.system_time = SystemTimeScope::AsOf(ms);
        any = true;
    }
    // AS OF SYSTEM TIME <expr> (array read style, CockroachDB-inspired)
    if let Some((rewritten, sys_scope)) = strip_as_of_system_time(&working)? {
        if scope.system_time.is_temporal() {
            return Err(TemporalParseError(
                "multiple system-time AS OF clauses in one statement".into(),
            ));
        }
        working = rewritten;
        scope.system_time = sys_scope;
        any = true;
        if strip_as_of_system_time(&working)?.is_some() {
            return Err(TemporalParseError(
                "multiple system-time AS OF clauses in one statement".into(),
            ));
        }
    }
    // FOR VALID_TIME CONTAINS/FROM…TO (table-scan style)
    if let Some((rewritten, vt)) = strip_valid_time(&working)? {
        working = rewritten;
        scope.valid_time = vt;
        any = true;
    }
    // AS OF VALID TIME <expr> (array read style)
    if let Some((rewritten, ms)) = strip_as_of_valid_time(&working)? {
        if !matches!(scope.valid_time, ValidTime::Any) {
            return Err(TemporalParseError(
                "multiple valid-time AS OF clauses in one statement".into(),
            ));
        }
        working = rewritten;
        scope.valid_time = ValidTime::At(ms);
        any = true;
    }

    if any {
        Ok(Some(Extracted {
            sql: working,
            temporal: scope,
        }))
    } else {
        Ok(None)
    }
}

/// Match `FOR SYSTEM_TIME AS OF <integer>` case-insensitively and strip it.
fn strip_system_time_as_of(sql: &str) -> Result<Option<(String, i64)>, TemporalParseError> {
    let Some(start) = keyword_position_outside_literals(sql, "FOR SYSTEM_TIME") else {
        return Ok(None);
    };
    // Locate `AS OF` after the keyword.
    let after_kw = start + "FOR SYSTEM_TIME".len();
    let tail_upper = sql[after_kw..].to_uppercase();
    let Some(_as_of_rel) = tail_upper.trim_start().strip_prefix("AS OF") else {
        return Err(TemporalParseError(
            "FOR SYSTEM_TIME must be followed by AS OF <ms>".into(),
        ));
    };
    let leading_ws = sql[after_kw..].len() - sql[after_kw..].trim_start().len();
    let as_of_abs = after_kw + leading_ws + "AS OF".len();
    let (ms, end_abs) = parse_trailing_i64(sql, as_of_abs)?;
    let mut out = String::with_capacity(sql.len());
    out.push_str(&sql[..start]);
    out.push(' ');
    out.push_str(&sql[end_abs..]);
    Ok(Some((out, ms)))
}

/// Match `__system_as_of__(<int>)` anywhere in the statement and strip the
/// call by replacing it with the literal `TRUE` so the surrounding
/// expression still parses (e.g. `SELECT __system_as_of__(100), x` →
/// `SELECT TRUE, x`). Returning `TRUE` keeps the projection column count
/// stable; the planner ignores the synthetic column because the temporal
/// scope is carried out-of-band.
fn strip_system_as_of_function(sql: &str) -> Result<Option<(String, i64)>, TemporalParseError> {
    let Some(start) = find_ascii_case_insensitive(sql, "__SYSTEM_AS_OF__(") else {
        return Ok(None);
    };
    let after_open = start + "__SYSTEM_AS_OF__(".len();
    let Some(close_rel) = sql[after_open..].find(')') else {
        return Err(TemporalParseError(
            "__system_as_of__(...) missing closing paren".into(),
        ));
    };
    let close_abs = after_open + close_rel;
    let arg = sql[after_open..close_abs].trim();
    let ms: i64 = arg
        .parse()
        .map_err(|_| TemporalParseError(format!("__system_as_of__ arg not i64: {arg}")))?;
    let mut out = String::with_capacity(sql.len());
    out.push_str(&sql[..start]);
    out.push_str("TRUE");
    out.push_str(&sql[close_abs + 1..]);
    Ok(Some((out, ms)))
}

/// Match `FOR VALID_TIME CONTAINS <int>` or
/// `FOR VALID_TIME FROM <int> TO <int>` and strip whichever is present.
fn strip_valid_time(sql: &str) -> Result<Option<(String, ValidTime)>, TemporalParseError> {
    let Some(start) = keyword_position_outside_literals(sql, "FOR VALID_TIME") else {
        return Ok(None);
    };
    let after_kw = start + "FOR VALID_TIME".len();
    let tail_raw = &sql[after_kw..];
    let tail_upper_owned = tail_raw.to_uppercase();
    let tail_upper = tail_upper_owned.trim_start();
    let leading_ws = tail_raw.len() - tail_raw.trim_start().len();

    if let Some(rest) = tail_upper.strip_prefix("CONTAINS") {
        let arg_abs = after_kw + leading_ws + "CONTAINS".len();
        let (ms, end_abs) = parse_trailing_i64(sql, arg_abs)?;
        let _ = rest;
        let mut out = String::with_capacity(sql.len());
        out.push_str(&sql[..start]);
        out.push(' ');
        out.push_str(&sql[end_abs..]);
        return Ok(Some((out, ValidTime::At(ms))));
    }
    if let Some(rest) = tail_upper.strip_prefix("FROM") {
        let arg_abs = after_kw + leading_ws + "FROM".len();
        let (lo, lo_end) = parse_trailing_i64(sql, arg_abs)?;
        let after_lo_raw = &sql[lo_end..];
        let after_lo_upper_owned = after_lo_raw.to_uppercase();
        let after_lo_upper = after_lo_upper_owned.trim_start();
        let leading_ws2 = after_lo_raw.len() - after_lo_raw.trim_start().len();
        let Some(_after_to) = after_lo_upper.strip_prefix("TO") else {
            return Err(TemporalParseError(
                "FOR VALID_TIME FROM <ms> must be followed by TO <ms>".into(),
            ));
        };
        let hi_arg = lo_end + leading_ws2 + "TO".len();
        let (hi, hi_end) = parse_trailing_i64(sql, hi_arg)?;
        let _ = rest;
        let mut out = String::with_capacity(sql.len());
        out.push_str(&sql[..start]);
        out.push(' ');
        out.push_str(&sql[hi_end..]);
        if hi <= lo {
            return Err(TemporalParseError(format!(
                "FOR VALID_TIME FROM {lo} TO {hi}: hi must be > lo"
            )));
        }
        return Ok(Some((out, ValidTime::Range(lo, hi))));
    }
    Err(TemporalParseError(
        "FOR VALID_TIME must be followed by CONTAINS or FROM".into(),
    ))
}

/// Resolve a temporal expression token to milliseconds since Unix epoch.
///
/// Supported forms:
/// - Integer literal — interpreted directly as milliseconds.
/// - `NOW()` — resolved to the current wall-clock millisecond via
///   `std::time::SystemTime`. This is evaluated once at parse time, not
///   deferred, so the resolved value is stable for the duration of the query.
/// - ISO-8601 / RFC-3339 string literal `'2024-01-15T00:00:00Z'` — parsed
///   with [`chrono`]'s `DateTime::parse_from_rfc3339`.
///
/// Any other expression form is rejected with a descriptive error rather than
/// silently defaulting, so callers always get exactly the version they asked for.
fn parse_temporal_expr(token: &str) -> Result<i64, TemporalParseError> {
    let t = token.trim();

    // Integer literal
    if let Ok(v) = t.parse::<i64>() {
        return Ok(v);
    }

    // NOW() — case-insensitive
    if t.to_uppercase() == "NOW()" {
        let ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);
        return Ok(ms);
    }

    // ISO-8601 string literal: 'YYYY-MM-DDTHH:MM:SSZ' (with surrounding quotes)
    if (t.starts_with('\'') && t.ends_with('\'')) || (t.starts_with('"') && t.ends_with('"')) {
        let inner = &t[1..t.len() - 1];
        // Try RFC-3339 first (most common).
        if let Ok(dt) = inner.parse::<chrono::DateTime<chrono::Utc>>() {
            return Ok(dt.timestamp_millis());
        }
        // Also try NaiveDateTime + assume UTC.
        if let Ok(ndt) = chrono::NaiveDateTime::parse_from_str(inner, "%Y-%m-%dT%H:%M:%S") {
            use chrono::TimeZone as _;
            return Ok(chrono::Utc.from_utc_datetime(&ndt).timestamp_millis());
        }
        return Err(TemporalParseError(format!(
            "AS OF temporal expression: cannot parse timestamp string '{inner}'; \
             expected RFC-3339 / ISO-8601 (e.g. '2024-01-15T00:00:00Z')"
        )));
    }

    Err(TemporalParseError(format!(
        "AS OF temporal expression '{t}' is not supported; \
         use an integer (ms since epoch), NOW(), or an ISO-8601 string literal \
         (e.g. '2024-01-15T00:00:00Z')"
    )))
}

/// Match `AS OF SYSTEM TIME <expr>` (case-insensitive) anywhere in `sql` and
/// strip it. The expression may be an integer literal, `NOW()`, or an ISO-8601
/// timestamp string. Returns `None` if the clause is absent.
///
/// This is the CockroachDB-inspired form used on array read queries. It is
/// parsed by NodeDB's pre-processor (option b — string-based extraction before
/// sqlparser-rs sees the statement) because sqlparser-rs does not natively
/// support the `AS OF VALID TIME` variant that the array engine also needs.
/// Using the same pre-processor approach for both keeps the two clauses
/// consistent and avoids mixing native sqlparser AS-OF support with custom
/// preprocessing in the same pipeline.
fn strip_as_of_system_time(
    sql: &str,
) -> Result<Option<(String, SystemTimeScope)>, TemporalParseError> {
    let keyword = "AS OF SYSTEM TIME";
    let Some(start) = keyword_position_outside_literals(sql, keyword) else {
        return Ok(None);
    };
    let after_kw = start + keyword.len();
    let (scope, end_abs) = parse_as_of_expr(sql, after_kw)?;
    let mut out = String::with_capacity(sql.len());
    out.push_str(sql[..start].trim_end());
    out.push(' ');
    out.push_str(sql[end_abs..].trim_start());
    Ok(Some((out.trim().to_string(), scope)))
}

/// Match `AS OF VALID TIME <expr>` (case-insensitive) anywhere in `sql` and
/// strip it. Same expression forms as `strip_as_of_system_time`. Returns `None`
/// if the clause is absent.
fn strip_as_of_valid_time(sql: &str) -> Result<Option<(String, i64)>, TemporalParseError> {
    let keyword = "AS OF VALID TIME";
    let Some(start) = keyword_position_outside_literals(sql, keyword) else {
        return Ok(None);
    };
    let after_kw = start + keyword.len();
    let (scope, end_abs) = parse_as_of_expr(sql, after_kw)?;
    let ms = match scope {
        SystemTimeScope::AsOf(ms) => ms,
        SystemTimeScope::AllVersions => {
            return Err(TemporalParseError(
                "AS OF VALID TIME NULL is not supported; NULL (all-versions) \
                 applies only to SYSTEM TIME"
                    .into(),
            ));
        }
        SystemTimeScope::Current => {
            return Err(TemporalParseError(
                "AS OF VALID TIME requires an expression".into(),
            ));
        }
    };
    let mut out = String::with_capacity(sql.len());
    out.push_str(sql[..start].trim_end());
    out.push(' ');
    out.push_str(sql[end_abs..].trim_start());
    Ok(Some((out.trim().to_string(), ms)))
}

/// Extract the temporal expression that follows an AS-OF keyword at `offset`
/// in `sql`. Returns `(resolved_ms, end_offset_exclusive)`.
///
/// The expression is delimited by end-of-string or the next keyword-boundary
/// token (a subsequent `AS OF`, or certain SQL keywords that cannot appear
/// inside a temporal expression). Quoted string literals are consumed whole.
fn parse_as_of_expr(
    sql: &str,
    offset: usize,
) -> Result<(SystemTimeScope, usize), TemporalParseError> {
    let rest = &sql[offset..];
    let trimmed = rest.trim_start();
    let leading = rest.len() - trimmed.len();
    let abs_start = offset + leading;

    // If the next non-whitespace character opens a quoted string, consume
    // through the closing quote.
    if trimmed.starts_with('\'') || trimmed.starts_with('"') {
        let quote = trimmed
            .chars()
            .next()
            .expect("invariant: trimmed.starts_with('\\'' | '\"') guarantees at least one char");
        let inner_start = 1;
        let close = trimmed[inner_start..].find(quote).ok_or_else(|| {
            TemporalParseError(format!(
                "AS OF temporal expression: unterminated string literal at offset {abs_start}"
            ))
        })?;
        let end_rel = inner_start + close + 1; // include closing quote
        let token = &trimmed[..end_rel];
        let ms = parse_temporal_expr(token)?;
        return Ok((SystemTimeScope::AsOf(ms), abs_start + end_rel));
    }

    // Otherwise scan to the next whitespace-delimited boundary. We stop at:
    // a) another `AS OF` sequence (another clause),
    // b) certain SQL delimiter tokens: WHERE, LIMIT, ORDER, GROUP, HAVING,
    //    UNION, EXCEPT, INTERSECT, FETCH, FOR, OFFSET, semicolon.
    let stop_tokens = [
        "AS OF",
        "WHERE",
        "LIMIT",
        "ORDER",
        "GROUP",
        "HAVING",
        "UNION",
        "EXCEPT",
        "INTERSECT",
        "FETCH",
        "OFFSET",
        ";",
    ];
    let mut end_rel = trimmed.len();
    for stop in &stop_tokens {
        // Only match at a word boundary (preceded by whitespace or start).
        let mut search_from = 0;
        while let Some(abs_pos) = find_ascii_case_insensitive_from(trimmed, stop, search_from) {
            let at_boundary =
                abs_pos == 0 || trimmed[..abs_pos].ends_with(|c: char| c.is_ascii_whitespace());
            if at_boundary {
                end_rel = end_rel.min(abs_pos);
                break;
            }
            search_from = abs_pos + stop.len();
        }
    }
    // Trim trailing whitespace from the extracted token.
    let token = trimmed[..end_rel].trim();
    if token.is_empty() {
        return Err(TemporalParseError(format!(
            "AS OF clause at offset {abs_start} has no expression"
        )));
    }
    // Bare NULL (case-insensitive) → all-versions (audit-log) mode.
    if token.eq_ignore_ascii_case("NULL") {
        let raw_end = abs_start + end_rel;
        return Ok((SystemTimeScope::AllVersions, raw_end));
    }
    let ms = parse_temporal_expr(token)?;
    // Advance past the expression (including trailing whitespace that was trimmed).
    let raw_end = abs_start + end_rel;
    Ok((SystemTimeScope::AsOf(ms), raw_end))
}

/// Parse an optionally-signed integer starting at `offset` in `sql`, skipping
/// leading whitespace. Returns `(value, end_offset_exclusive)`.
fn parse_trailing_i64(sql: &str, offset: usize) -> Result<(i64, usize), TemporalParseError> {
    let rest = &sql[offset..];
    let trimmed = rest.trim_start();
    let leading = rest.len() - trimmed.len();
    let end_rel = trimmed
        .char_indices()
        .take_while(|(i, c)| c.is_ascii_digit() || (*i == 0 && *c == '-'))
        .map(|(i, c)| i + c.len_utf8())
        .last()
        .ok_or_else(|| TemporalParseError(format!("expected integer at offset {offset}")))?;
    let num_str = &trimmed[..end_rel];
    if trimmed[end_rel..]
        .chars()
        .next()
        .is_some_and(|c| !c.is_ascii_whitespace() && !matches!(c, ';' | ',' | ')'))
    {
        return Err(TemporalParseError(format!(
            "invalid character after integer temporal expression at offset {}",
            offset + leading + end_rel
        )));
    }
    let v: i64 = num_str
        .parse()
        .map_err(|_| TemporalParseError(format!("not an i64: {num_str}")))?;
    Ok((v, offset + leading + end_rel))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn passthrough_no_temporal() {
        assert!(extract("SELECT * FROM t WHERE x = 1").unwrap().is_none());
    }

    #[test]
    fn unicode_in_invalid_as_of_expression_returns_error_without_panicking() {
        let result = extract("SELECT * FROM t FOR SYSTEM_TIME AS OF 100ﬀﬀ LIMIT 1");
        assert!(result.is_err());
    }

    #[test]
    fn system_time_as_of() {
        let ex = extract("SELECT * FROM t FOR SYSTEM_TIME AS OF 100 WHERE x = 1")
            .unwrap()
            .unwrap();
        assert_eq!(ex.temporal.system_time, SystemTimeScope::AsOf(100));
        assert_eq!(ex.temporal.valid_time, ValidTime::Any);
        assert!(!ex.sql.to_uppercase().contains("FOR SYSTEM_TIME"));
        assert!(ex.sql.contains("WHERE x = 1"));
    }

    #[test]
    fn valid_time_contains() {
        let ex = extract("SELECT * FROM t FOR VALID_TIME CONTAINS 250")
            .unwrap()
            .unwrap();
        assert_eq!(ex.temporal.valid_time, ValidTime::At(250));
        assert!(!ex.sql.to_uppercase().contains("FOR VALID_TIME"));
    }

    #[test]
    fn valid_time_range() {
        let ex = extract("SELECT * FROM t FOR VALID_TIME FROM 100 TO 300 LIMIT 10")
            .unwrap()
            .unwrap();
        assert_eq!(ex.temporal.valid_time, ValidTime::Range(100, 300));
        assert!(ex.sql.contains("LIMIT 10"));
    }

    #[test]
    fn combined_system_and_valid() {
        let ex = extract(
            "SELECT * FROM t FOR SYSTEM_TIME AS OF 500 FOR VALID_TIME CONTAINS 250 LIMIT 5",
        )
        .unwrap()
        .unwrap();
        assert_eq!(ex.temporal.system_time, SystemTimeScope::AsOf(500));
        assert_eq!(ex.temporal.valid_time, ValidTime::At(250));
    }

    #[test]
    fn function_form() {
        let ex = extract("SELECT __system_as_of__(777), x FROM t")
            .unwrap()
            .unwrap();
        assert_eq!(ex.temporal.system_time, SystemTimeScope::AsOf(777));
        assert!(ex.sql.contains("TRUE"));
        assert!(!ex.sql.contains("__system_as_of__"));
    }

    #[test]
    fn invalid_range_rejects() {
        assert!(extract("SELECT * FROM t FOR VALID_TIME FROM 500 TO 100").is_err());
    }

    #[test]
    fn valid_time_missing_verb() {
        assert!(extract("SELECT * FROM t FOR VALID_TIME 100").is_err());
    }

    #[test]
    fn as_of_system_time_integer() {
        let ex = extract("SELECT * FROM array_slice('g', '{}') AS OF SYSTEM TIME 1700000000000")
            .unwrap()
            .unwrap();
        assert_eq!(
            ex.temporal.system_time,
            SystemTimeScope::AsOf(1_700_000_000_000)
        );
        assert!(!ex.sql.to_uppercase().contains("AS OF SYSTEM TIME"));
    }

    #[test]
    fn as_of_valid_time_integer() {
        let ex = extract("SELECT * FROM array_slice('g', '{}') AS OF VALID TIME 1700000000001")
            .unwrap()
            .unwrap();
        assert_eq!(ex.temporal.valid_time, ValidTime::At(1_700_000_000_001));
        assert!(!ex.sql.to_uppercase().contains("AS OF VALID TIME"));
    }

    #[test]
    fn as_of_system_time_iso8601() {
        let ex = extract(
            "SELECT * FROM array_slice('g', '{}') AS OF SYSTEM TIME '2024-01-15T00:00:00Z'",
        )
        .unwrap()
        .unwrap();
        // 2024-01-15T00:00:00Z in ms
        assert_eq!(
            ex.temporal.system_time,
            SystemTimeScope::AsOf(1_705_276_800_000)
        );
        assert!(!ex.sql.to_uppercase().contains("AS OF SYSTEM TIME"));
    }

    #[test]
    fn as_of_both_clauses() {
        let ex = extract(
            "SELECT * FROM array_slice('g', '{}') AS OF SYSTEM TIME 500 AS OF VALID TIME 250",
        )
        .unwrap()
        .unwrap();
        assert_eq!(ex.temporal.system_time, SystemTimeScope::AsOf(500));
        assert_eq!(ex.temporal.valid_time, ValidTime::At(250));
    }

    #[test]
    fn as_of_system_time_now() {
        let before = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        let ex = extract("SELECT * FROM array_slice('g', '{}') AS OF SYSTEM TIME NOW()")
            .unwrap()
            .unwrap();
        let after = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        let ts = ex
            .temporal
            .system_time
            .as_of_ms()
            .expect("NOW() resolves to AsOf");
        assert!(
            ts >= before && ts <= after,
            "NOW() ts {ts} not in [{before}, {after}]"
        );
    }

    #[test]
    fn as_of_system_time_unsupported_expr_rejected() {
        let err = extract(
            "SELECT * FROM array_slice('g', '{}') AS OF SYSTEM TIME NOW() - INTERVAL '1 day'",
        );
        assert!(err.is_err(), "INTERVAL expression should be rejected");
    }

    #[test]
    fn as_of_duplicate_system_time_rejected() {
        assert!(
            extract("SELECT * FROM t AS OF SYSTEM TIME 100 AS OF SYSTEM TIME 200").is_err(),
            "duplicate AS OF SYSTEM TIME should be rejected"
        );
    }

    #[test]
    fn system_time_in_string_literal_not_triggered() {
        // `FOR SYSTEM_TIME` inside a string literal must NOT be detected as a
        // temporal clause.
        let result = extract("SELECT * FROM t WHERE name = 'FOR SYSTEM_TIME'").unwrap();
        assert!(
            result.is_none(),
            "FOR SYSTEM_TIME inside string literal must not trigger, got: {result:?}"
        );
    }

    #[test]
    fn system_time_as_of_outside_literal_triggered() {
        let ex = extract("SELECT * FROM t FOR SYSTEM_TIME AS OF 100")
            .unwrap()
            .unwrap();
        assert_eq!(ex.temporal.system_time, SystemTimeScope::AsOf(100));
    }

    #[test]
    fn as_of_system_time_null_is_all_versions() {
        let ex = extract("SELECT * FROM t AS OF SYSTEM TIME NULL WHERE x = 1")
            .unwrap()
            .unwrap();
        assert_eq!(ex.temporal.system_time, SystemTimeScope::AllVersions);
        assert!(!ex.sql.to_uppercase().contains("AS OF SYSTEM TIME"));
        assert!(ex.sql.contains("WHERE x = 1"));
    }

    #[test]
    fn as_of_system_time_null_case_insensitive() {
        let ex = extract("SELECT * FROM t AS OF SYSTEM TIME null")
            .unwrap()
            .unwrap();
        assert_eq!(ex.temporal.system_time, SystemTimeScope::AllVersions);
    }

    #[test]
    fn as_of_valid_time_null_rejected() {
        assert!(
            extract("SELECT * FROM t AS OF VALID TIME NULL").is_err(),
            "AS OF VALID TIME NULL must be rejected"
        );
    }
}
