// SPDX-License-Identifier: BUSL-1.1

//! Tokenizer for the index-DDL surfaces.
//!
//! Whitespace splitting cannot tell `coll(field)` from `coll (field)`, cannot
//! see the `,` inside a field list, and turns `metric = 'cosine'` into three
//! tokens of which the middle one reads as a value. Every one of those
//! confusions produced a statement that parsed into something other than what
//! was written. This lexer emits structural punctuation as its own tokens so
//! the grammar above it can require an exact shape.

/// One lexical unit of an index-DDL statement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Tok {
    /// A bare identifier, keyword, or numeric literal.
    Word(String),
    /// A single-quoted string literal, quotes stripped.
    Quoted(String),
    /// A double-quoted identifier, quotes stripped. Its case is kept.
    QuotedIdent(String),
    LParen,
    RParen,
    Comma,
    Eq,
}

impl Tok {
    /// The word text if this is a [`Tok::Word`], else `None`.
    pub fn word(&self) -> Option<&str> {
        match self {
            Tok::Word(w) => Some(w.as_str()),
            _ => None,
        }
    }

    /// Whether this is a [`Tok::Word`] equal to `kw`, ignoring ASCII case.
    pub fn is_keyword(&self, kw: &str) -> bool {
        self.word().is_some_and(|w| w.eq_ignore_ascii_case(kw))
    }

    /// Rendering used in error messages.
    pub fn describe(&self) -> String {
        match self {
            Tok::Word(w) => w.clone(),
            Tok::Quoted(s) => format!("'{s}'"),
            Tok::QuotedIdent(s) => format!("\"{s}\""),
            Tok::LParen => "(".to_string(),
            Tok::RParen => ")".to_string(),
            Tok::Comma => ",".to_string(),
            Tok::Eq => "=".to_string(),
        }
    }
}

/// Split `sql` into [`Tok`]s.
///
/// A trailing `;` and any statement terminator whitespace are dropped. An
/// unterminated quoted literal yields `None` — callers turn that into a
/// syntax error rather than guessing where the literal ended.
pub fn tokenize(sql: &str) -> Option<Vec<Tok>> {
    let mut out = Vec::new();
    let mut chars = sql.char_indices().peekable();
    let bytes = sql.as_bytes();

    while let Some((idx, ch)) = chars.next() {
        match ch {
            c if c.is_whitespace() => {}
            ';' => {
                // Only a trailing terminator is tolerated; anything after it
                // is a second statement this grammar does not accept.
                if sql[idx + 1..].trim().is_empty() {
                    break;
                }
                return None;
            }
            '(' => out.push(Tok::LParen),
            ')' => out.push(Tok::RParen),
            ',' => out.push(Tok::Comma),
            '=' => out.push(Tok::Eq),
            '\'' | '"' => {
                let quote = ch;
                let start = idx + quote.len_utf8();
                let mut end = None;
                for (i, c) in chars.by_ref() {
                    if c == quote {
                        end = Some(i);
                        break;
                    }
                }
                let text = sql[start..end?].to_string();
                out.push(if quote == '"' {
                    Tok::QuotedIdent(text)
                } else {
                    Tok::Quoted(text)
                });
            }
            _ => {
                let start = idx;
                let mut end = sql.len();
                while let Some(&(i, c)) = chars.peek() {
                    if c.is_whitespace() || matches!(c, '(' | ')' | ',' | '=' | ';' | '\'' | '"') {
                        end = i;
                        break;
                    }
                    chars.next();
                }
                if end == sql.len() {
                    // Ran to the end of input without hitting a delimiter.
                    end = bytes.len();
                }
                out.push(Tok::Word(sql[start..end].to_string()));
            }
        }
    }

    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn words(sql: &str) -> Vec<String> {
        tokenize(sql).unwrap().iter().map(Tok::describe).collect()
    }

    #[test]
    fn splits_attached_parens() {
        assert_eq!(words("ON coll(field)"), ["ON", "coll", "(", "field", ")"]);
    }

    #[test]
    fn spaced_parens_lex_identically() {
        assert_eq!(words("ON coll ( field )"), words("ON coll(field)"));
    }

    #[test]
    fn field_list_keeps_commas() {
        assert_eq!(
            words("(title, content)"),
            ["(", "title", ",", "content", ")"]
        );
    }

    #[test]
    fn equals_and_quotes_are_structural() {
        assert_eq!(
            words("WITH (metric = 'cosine')"),
            ["WITH", "(", "metric", "=", "'cosine'", ")"]
        );
    }

    #[test]
    fn trailing_semicolon_is_dropped() {
        assert_eq!(words("DIM 4;"), ["DIM", "4"]);
    }

    #[test]
    fn embedded_semicolon_is_rejected() {
        assert!(tokenize("DIM 4; DROP COLLECTION x").is_none());
    }

    #[test]
    fn unterminated_literal_is_rejected() {
        assert!(tokenize("ANALYZER 'english").is_none());
    }

    #[test]
    fn double_quoted_identifier_unwraps() {
        assert_eq!(words("(\"Odd Name\")"), ["(", "\"Odd Name\"", ")"]);
        assert_eq!(
            tokenize("\"Body\"").unwrap(),
            [Tok::QuotedIdent("Body".to_string())]
        );
        assert_eq!(
            tokenize("'Body'").unwrap(),
            [Tok::Quoted("Body".to_string())]
        );
    }

    #[test]
    fn unicode_quoted_value_tokenizes() {
        // Regression: a multi-byte quoted value (e.g. an ANALYZER name) must
        // round-trip intact and never panic on a byte-index slice — the failure
        // mode of the pre-grammar `[1..]` / `[..end]` analyzer-name parser.
        assert_eq!(words("ANALYZER '日本語'"), ["ANALYZER", "'日本語'"]);
    }
}
