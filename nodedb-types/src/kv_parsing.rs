//! Shared KV DDL parsing helpers used by both Origin (pgwire) and Lite.
//!
//! Extracts interval parsing, storage-mode detection, and WITH-clause
//! helpers into one place so both runtimes share identical logic.

/// Errors produced by interval parsing.
#[derive(Debug, Clone, thiserror::Error)]
pub enum IntervalParseError {
    /// The interval expression was empty after stripping the INTERVAL keyword.
    #[error("empty interval expression")]
    Empty,
    /// The numeric amount could not be parsed.
    #[error("invalid interval amount: '{0}'")]
    InvalidAmount(String),
    /// The unit suffix is not recognized.
    #[error("unknown interval unit: '{0}'")]
    UnknownUnit(String),
    /// General parse failure.
    #[error("cannot parse interval: '{0}'")]
    Unparseable(String),
}

/// Detect KV storage mode from uppercased SQL.
///
/// Matches `WITH storage = 'kv'` or `WITH STORAGE KV` keyword patterns.
/// Must not false-positive on field names containing "kv".
pub fn is_kv_storage_mode(upper: &str) -> bool {
    if !upper.contains("STORAGE") {
        return false;
    }
    if let Some(pos) = upper.find("STORAGE") {
        let after = &upper[pos + 7..];
        let trimmed =
            after.trim_start_matches(|c: char| c.is_whitespace() || c == '=' || c == '\'');
        return trimmed.starts_with("KV")
            && (trimmed.len() == 2
                || trimmed[2..].starts_with(|c: char| {
                    c.is_whitespace() || c == '\'' || c == ',' || c == ';'
                }));
    }
    false
}

/// Parse an INTERVAL literal to milliseconds.
///
/// Supports: `INTERVAL '15 minutes'`, `INTERVAL '1h'`, `INTERVAL '30s'`,
/// `INTERVAL '1 hour'`, `INTERVAL '2 days'`, `'15 minutes'` (without INTERVAL keyword).
pub fn parse_interval_to_ms(s: &str) -> Result<u64, IntervalParseError> {
    let trimmed = s.trim();
    let inner = if trimmed.to_uppercase().starts_with("INTERVAL") {
        trimmed[8..].trim()
    } else {
        trimmed
    };
    let unquoted = inner.trim_matches('\'').trim();

    if unquoted.is_empty() {
        return Err(IntervalParseError::Empty);
    }

    // Short-form: "15m", "1h", "30s", "2d"
    if let Some(ms) = try_parse_short_interval(unquoted) {
        return Ok(ms);
    }

    // Long-form: "15 minutes", "1 hour", "2 days", "30 seconds"
    let parts: Vec<&str> = unquoted.split_whitespace().collect();
    if parts.len() == 2 {
        let amount: u64 = parts[0]
            .parse()
            .map_err(|_| IntervalParseError::InvalidAmount(parts[0].to_string()))?;
        let unit = parts[1].to_lowercase();
        let ms = match unit.as_str() {
            "ms" | "millisecond" | "milliseconds" => amount,
            "s" | "sec" | "second" | "seconds" => amount * 1_000,
            "m" | "min" | "minute" | "minutes" => amount * 60_000,
            "h" | "hr" | "hour" | "hours" => amount * 3_600_000,
            "d" | "day" | "days" => amount * 86_400_000,
            other => return Err(IntervalParseError::UnknownUnit(other.to_string())),
        };
        return Ok(ms);
    }

    // Bare number: treat as milliseconds.
    if parts.len() == 1
        && let Ok(ms) = unquoted.parse::<u64>()
    {
        return Ok(ms);
    }

    Err(IntervalParseError::Unparseable(unquoted.to_string()))
}

/// Try to parse a short-form interval like "15m", "1h", "30s", "2d".
pub fn try_parse_short_interval(s: &str) -> Option<u64> {
    let s = s.trim();
    if s.is_empty() {
        return None;
    }
    let num_end = s.find(|c: char| !c.is_ascii_digit()).unwrap_or(s.len());
    if num_end == 0 || num_end == s.len() {
        return None;
    }
    let amount: u64 = s[..num_end].parse().ok()?;
    let unit = &s[num_end..].to_lowercase();
    let ms = match unit.as_str() {
        "ms" => amount,
        "s" => amount * 1_000,
        "m" => amount * 60_000,
        "h" => amount * 3_600_000,
        "d" => amount * 86_400_000,
        _ => return None,
    };
    Some(ms)
}

/// Find the byte position of a named option in the WITH clause.
///
/// Only searches after the WITH keyword to avoid matching column names.
pub fn find_with_option(upper: &str, option: &str) -> Option<usize> {
    let with_pos = upper.find("WITH")?;
    let after_with = &upper[with_pos..];
    after_with.find(option).map(|p| with_pos + p)
}

/// Find the end of a WITH option value expression.
///
/// Ends at the next unquoted comma, semicolon, or end of string.
pub fn find_with_option_end(s: &str) -> usize {
    let mut in_quote = false;
    for (i, c) in s.char_indices() {
        match c {
            '\'' => in_quote = !in_quote,
            ',' | ';' if !in_quote => return i,
            _ => {}
        }
    }
    s.len()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detect_kv_storage_mode() {
        assert!(is_kv_storage_mode("WITH STORAGE = 'KV'"));
        assert!(is_kv_storage_mode("WITH STORAGE KV"));
        assert!(is_kv_storage_mode("WITH STORAGE='KV'"));
        assert!(is_kv_storage_mode(
            "WITH STORAGE = 'KV', TTL = INTERVAL '1H'"
        ));
        assert!(!is_kv_storage_mode("WITH STORAGE = 'STRICT'"));
        assert!(!is_kv_storage_mode("WITH STORAGE = 'COLUMNAR'"));
        assert!(!is_kv_storage_mode("CREATE COLLECTION KV_STUFF"));
    }

    #[test]
    fn interval_parsing_short_form() {
        assert_eq!(parse_interval_to_ms("INTERVAL '15m'").unwrap(), 900_000);
        assert_eq!(parse_interval_to_ms("INTERVAL '1h'").unwrap(), 3_600_000);
        assert_eq!(parse_interval_to_ms("INTERVAL '30s'").unwrap(), 30_000);
        assert_eq!(parse_interval_to_ms("INTERVAL '2d'").unwrap(), 172_800_000);
        assert_eq!(parse_interval_to_ms("'500ms'").unwrap(), 500);
    }

    #[test]
    fn interval_parsing_long_form() {
        assert_eq!(
            parse_interval_to_ms("INTERVAL '15 minutes'").unwrap(),
            900_000
        );
        assert_eq!(
            parse_interval_to_ms("INTERVAL '1 hour'").unwrap(),
            3_600_000
        );
        assert_eq!(
            parse_interval_to_ms("INTERVAL '30 seconds'").unwrap(),
            30_000
        );
        assert_eq!(
            parse_interval_to_ms("INTERVAL '2 days'").unwrap(),
            172_800_000
        );
    }

    #[test]
    fn interval_parsing_bare_number() {
        assert_eq!(parse_interval_to_ms("5000").unwrap(), 5000);
    }

    #[test]
    fn interval_parsing_errors() {
        assert!(parse_interval_to_ms("INTERVAL ''").is_err());
        assert!(parse_interval_to_ms("INTERVAL 'abc'").is_err());
        assert!(parse_interval_to_ms("INTERVAL '15 foobar'").is_err());
    }

    #[test]
    fn with_option_end_respects_quotes() {
        assert_eq!(find_with_option_end("'hello, world', next"), 14);
        assert_eq!(find_with_option_end("simple, next"), 6);
        assert_eq!(find_with_option_end("no_comma"), 8);
    }
}
