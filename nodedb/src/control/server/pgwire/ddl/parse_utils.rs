//! Shared parsing utilities for DDL handlers.

/// Find the matching closing paren for the open paren at `start`.
///
/// Returns the index of the closing `)`, or `None` if unmatched.
pub(super) fn find_matching_paren(s: &str, start: usize) -> Option<usize> {
    let bytes = s.as_bytes();
    let mut depth = 0i32;
    for (i, &b) in bytes.iter().enumerate().skip(start) {
        match b {
            b'(' => depth += 1,
            b')' => {
                depth -= 1;
                if depth == 0 {
                    return Some(i);
                }
            }
            _ => {}
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn matching_parens() {
        assert_eq!(find_matching_paren("(a, b)", 0), Some(5));
        assert_eq!(find_matching_paren("((a))", 0), Some(4));
        assert_eq!(find_matching_paren("(", 0), None);
    }
}
