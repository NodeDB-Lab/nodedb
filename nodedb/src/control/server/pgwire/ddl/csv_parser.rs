//! Fast CSV parsing using memchr (SIMD byte scanning) + lexical-core (fast f64).
//!
//! Replaces `split(',')` with `memchr::memchr_iter` for ~2-3x delimiter detection
//! throughput on wide CSVs. Uses `lexical_core::parse` for ~10-15x f64 parsing.

/// Split a CSV line by commas using SIMD-accelerated byte scanning.
///
/// This is a simple unquoted CSV split — does not handle quoted fields with
/// embedded commas. For proper RFC 4180 parsing, a full CSV parser is needed.
pub fn split_csv_line(line: &str) -> Vec<&str> {
    let bytes = line.as_bytes();
    let mut result = Vec::with_capacity(16); // pre-allocate for typical column count
    let mut start = 0;
    for pos in memchr::memchr_iter(b',', bytes) {
        result.push(&line[start..pos]);
        start = pos + 1;
    }
    result.push(&line[start..]);
    result
}

/// Parse a string as f64 using lexical-core (SIMD-optimized).
///
/// Returns `None` if the string is not a valid float.
pub fn fast_parse_f64(s: &str) -> Option<f64> {
    lexical_core::parse::<f64>(s.as_bytes()).ok()
}

/// Parse a string as i64 using lexical-core.
pub fn fast_parse_i64(s: &str) -> Option<i64> {
    lexical_core::parse::<i64>(s.as_bytes()).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn split_basic() {
        let parts = split_csv_line("a,b,c");
        assert_eq!(parts, vec!["a", "b", "c"]);
    }

    #[test]
    fn split_single() {
        let parts = split_csv_line("hello");
        assert_eq!(parts, vec!["hello"]);
    }

    #[test]
    fn split_empty_fields() {
        let parts = split_csv_line(",a,,b,");
        assert_eq!(parts, vec!["", "a", "", "b", ""]);
    }

    #[test]
    fn parse_f64_valid() {
        assert_eq!(fast_parse_f64("3.25"), Some(3.25));
        assert_eq!(fast_parse_f64("-1.5e10"), Some(-1.5e10));
    }

    #[test]
    fn parse_f64_invalid() {
        assert!(fast_parse_f64("abc").is_none());
    }

    #[test]
    fn parse_i64_valid() {
        assert_eq!(fast_parse_i64("42"), Some(42));
        assert_eq!(fast_parse_i64("-100"), Some(-100));
    }

    #[test]
    fn parse_i64_invalid() {
        assert!(fast_parse_i64("3.14").is_none());
    }
}
