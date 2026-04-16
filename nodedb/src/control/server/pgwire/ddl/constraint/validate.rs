//! DDL-time validation for CHECK constraint expressions.

use pgwire::error::PgWireResult;

use super::err;

/// Validate that a subquery CHECK expression uses a supported pattern.
///
/// Supported patterns:
/// - `expr IN (SELECT col FROM tbl [WHERE ...])`
/// - `expr NOT IN (SELECT col FROM tbl [WHERE ...])`
pub(super) fn validate_subquery_pattern(check_sql: &str) -> PgWireResult<()> {
    let upper = check_sql.to_uppercase();

    if upper.contains(" IN (SELECT ") || upper.contains(" IN(SELECT ") {
        return Ok(());
    }

    Err(err(
        "0A000",
        &format!(
            "unsupported subquery CHECK pattern. \
             Supported: `expr IN (SELECT col FROM tbl)`, \
             `expr NOT IN (SELECT col FROM tbl)`. \
             Got: {check_sql}"
        ),
    ))
}

/// Strip `NEW.` prefix for validation parsing.
pub(super) fn strip_new_prefix_for_validation(sql: &str) -> String {
    let chars: Vec<char> = sql.chars().collect();
    let mut result = String::with_capacity(sql.len());
    let mut i = 0;
    while i < chars.len() {
        if i + 4 <= chars.len() {
            let window: String = chars[i..i + 4].iter().collect();
            if window.eq_ignore_ascii_case("NEW.") {
                if i > 0 && (chars[i - 1].is_ascii_alphanumeric() || chars[i - 1] == '_') {
                    result.push(chars[i]);
                    i += 1;
                    continue;
                }
                i += 4;
                continue;
            }
        }
        result.push(chars[i]);
        i += 1;
    }
    result
}
