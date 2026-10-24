// SPDX-License-Identifier: Apache-2.0

//! Shape check of the `VALUES` tail of a batched edge statement.
//!
//! The tokenizer drops `(`, `)` and `,`, so the token list cannot tell
//! `('a','b','L'), ('c','d','L')` from `('a','b','L') ('c','d','L')` or from
//! three bare words. This check reads the statement's own text instead.

/// Whether the text after the first unquoted `VALUES` is a comma-separated
/// list of parenthesized triples, with an optional trailing `;`.
pub(super) fn values_tail_is_triples(sql: &str) -> bool {
    let Some(tail) = after_values(sql) else {
        return false;
    };
    let mut chars = tail.chars().peekable();
    loop {
        skip_space(&mut chars);
        if chars.next() != Some('(') {
            return false;
        }
        for field in 0..3 {
            skip_space(&mut chars);
            if !read_field(&mut chars) {
                return false;
            }
            skip_space(&mut chars);
            let expected = if field == 2 { ')' } else { ',' };
            if chars.next() != Some(expected) {
                return false;
            }
        }
        skip_space(&mut chars);
        match chars.next() {
            Some(',') => continue,
            Some(';') => {
                skip_space(&mut chars);
                return chars.next().is_none();
            }
            None => return true,
            Some(_) => return false,
        }
    }
}

type Chars<'a> = std::iter::Peekable<std::str::Chars<'a>>;

fn skip_space(chars: &mut Chars<'_>) {
    while chars.peek().is_some_and(|c| c.is_whitespace()) {
        chars.next();
    }
}

/// Read one quoted string (`''` escapes a quote) or one bare word.
fn read_field(chars: &mut Chars<'_>) -> bool {
    match chars.peek().copied() {
        Some(quote @ ('\'' | '"')) => {
            chars.next();
            loop {
                match chars.next() {
                    None => return false,
                    Some(c) if c == quote => {
                        if chars.peek() == Some(&quote) {
                            chars.next();
                        } else {
                            return true;
                        }
                    }
                    Some(_) => {}
                }
            }
        }
        Some(c) if !c.is_whitespace() && !matches!(c, ',' | '(' | ')' | ';') => {
            while chars
                .peek()
                .is_some_and(|c| !c.is_whitespace() && !matches!(c, ',' | '(' | ')' | ';'))
            {
                chars.next();
            }
            true
        }
        _ => false,
    }
}

/// The text after the first `VALUES` word that sits outside a quoted string.
fn after_values(sql: &str) -> Option<&str> {
    let bytes = sql.as_bytes();
    let mut quote: Option<u8> = None;
    let mut i = 0;
    while i < bytes.len() {
        let b = bytes[i];
        match quote {
            Some(q) => {
                if b == q {
                    quote = None;
                }
            }
            None => {
                if b == b'\'' || b == b'"' {
                    quote = Some(b);
                } else if sql.is_char_boundary(i)
                    && sql[i..].len() >= 6
                    && sql.is_char_boundary(i + 6)
                    && sql[i..i + 6].eq_ignore_ascii_case("VALUES")
                    && (i == 0 || !bytes[i - 1].is_ascii_alphanumeric())
                    && bytes.get(i + 6).is_none_or(|n| !n.is_ascii_alphanumeric())
                {
                    return Some(&sql[i + 6..]);
                }
            }
        }
        i += 1;
    }
    None
}

#[cfg(test)]
mod tests {
    use super::values_tail_is_triples as ok;

    #[test]
    fn accepts_comma_separated_triples() {
        assert!(ok("IN 'e' VALUES ('a','b','L')"));
        assert!(ok("IN 'e' VALUES ('a','b','L'), ('c','d','L');"));
        assert!(ok("IN 'e' VALUES (a, b, L)"));
        assert!(ok("IN 'e' VALUES ('a,(x)','it''s','L')"));
    }

    #[test]
    fn refuses_a_missing_comma_between_triples() {
        assert!(!ok("IN 'e' VALUES ('a','b','L') ('c','d','L')"));
    }

    #[test]
    fn refuses_bare_words_without_parentheses() {
        assert!(!ok("IN 'e' VALUES a b L"));
    }

    #[test]
    fn refuses_a_trailing_comma_and_a_short_triple() {
        assert!(!ok("IN 'e' VALUES ('a','b','L'),"));
        assert!(!ok("IN 'e' VALUES ('a','b')"));
        assert!(!ok("IN 'e' VALUES ('a','b','L','c')"));
    }

    #[test]
    fn a_values_word_inside_the_collection_name_is_not_the_clause() {
        assert!(ok("IN 'values' VALUES ('a','b','L')"));
    }
}
