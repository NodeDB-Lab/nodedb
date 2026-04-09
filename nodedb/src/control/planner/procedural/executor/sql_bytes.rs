//! Low-level byte-oriented helpers for SQL string manipulation.

/// Advance past ASCII whitespace characters starting at position `i`.
pub(super) fn skip_ascii_whitespace(bytes: &[u8], mut i: usize) -> usize {
    while let Some(byte) = bytes.get(i) {
        if !byte.is_ascii_whitespace() {
            break;
        }
        i += 1;
    }
    i
}

/// Return `true` if `byte` is an ASCII alphanumeric or underscore character.
pub(super) fn is_identifier_char(byte: Option<u8>) -> bool {
    byte.is_some_and(|b| b.is_ascii_alphanumeric() || b == b'_')
}
