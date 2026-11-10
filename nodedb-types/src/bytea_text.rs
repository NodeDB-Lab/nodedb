// SPDX-License-Identifier: Apache-2.0

//! The PostgreSQL text forms of `bytea`.
//!
//! PostgreSQL reads two input forms. The hex form is `\x` then pairs of hex
//! digits, with whitespace allowed between pairs. The escape form is any
//! other text: `\\` is one backslash, `\ooo` is the byte with that octal
//! value, and every other character is its own UTF-8 bytes. PostgreSQL
//! writes the hex form.

/// The bytes a `bytea` text value denotes, or `None` when the text is
/// neither a valid hex form nor a valid escape form.
pub fn parse_bytea_text(text: &str) -> Option<Vec<u8>> {
    match text
        .strip_prefix("\\x")
        .or_else(|| text.strip_prefix("\\X"))
    {
        Some(hex) => parse_hex(hex),
        None => parse_escape(text.as_bytes()),
    }
}

/// The hex text form PostgreSQL writes: `\x` then two lowercase hex digits
/// per byte.
pub fn format_bytea_hex(bytes: &[u8]) -> String {
    format!("\\x{}", hex::encode(bytes))
}

fn parse_hex(hex: &str) -> Option<Vec<u8>> {
    let mut out = Vec::with_capacity(hex.len() / 2);
    let mut digits = hex.bytes().filter(|b| !b.is_ascii_whitespace());
    while let Some(high) = digits.next() {
        let low = digits.next()?;
        out.push((hex_value(high)? << 4) | hex_value(low)?);
    }
    Some(out)
}

fn hex_value(digit: u8) -> Option<u8> {
    match digit {
        b'0'..=b'9' => Some(digit - b'0'),
        b'a'..=b'f' => Some(digit - b'a' + 10),
        b'A'..=b'F' => Some(digit - b'A' + 10),
        _ => None,
    }
}

fn parse_escape(text: &[u8]) -> Option<Vec<u8>> {
    let mut out = Vec::with_capacity(text.len());
    let mut rest = text;
    while let Some((&byte, tail)) = rest.split_first() {
        if byte != b'\\' {
            out.push(byte);
            rest = tail;
            continue;
        }
        match tail {
            [b'\\', after @ ..] => {
                out.push(b'\\');
                rest = after;
            }
            [
                a @ b'0'..=b'3',
                b @ b'0'..=b'7',
                c @ b'0'..=b'7',
                after @ ..,
            ] => {
                out.push(((a - b'0') << 6) | ((b - b'0') << 3) | (c - b'0'));
                rest = after;
            }
            _ => return None,
        }
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hex_form_reads_pairs_with_whitespace_between() {
        assert_eq!(parse_bytea_text("\\x0102ff"), Some(vec![1, 2, 0xff]));
        assert_eq!(parse_bytea_text("\\XDE AD"), Some(vec![0xde, 0xad]));
        assert_eq!(parse_bytea_text("\\x"), Some(Vec::new()));
        assert_eq!(parse_bytea_text("\\x0"), None);
        assert_eq!(parse_bytea_text("\\xzz"), None);
    }

    #[test]
    fn escape_form_reads_octal_and_backslash() {
        assert_eq!(parse_bytea_text("ab"), Some(b"ab".to_vec()));
        assert_eq!(parse_bytea_text("a\\\\b"), Some(b"a\\b".to_vec()));
        assert_eq!(parse_bytea_text("\\001\\377"), Some(vec![1, 0xff]));
        assert_eq!(parse_bytea_text("\\9"), None);
        assert_eq!(parse_bytea_text("trailing\\"), None);
    }

    #[test]
    fn hex_output_reads_back() {
        let bytes = vec![0, 0xab, 7];
        assert_eq!(format_bytea_hex(&bytes), "\\x00ab07");
        assert_eq!(parse_bytea_text(&format_bytea_hex(&bytes)), Some(bytes));
    }
}
