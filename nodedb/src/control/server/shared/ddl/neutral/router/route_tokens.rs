// SPDX-License-Identifier: BUSL-1.1

//! Statement tokens for routing a string-recognized DDL statement.
//!
//! A router decides on keywords at known positions: the leading words, and
//! the words that follow a named object. String literals, quoted
//! identifiers, and comments are opaque, so a keyword inside a literal never
//! matches. A name is skipped as a unit, so a keyword spelled as a name never
//! matches either. A word inside parentheses is never a top-level clause.

use nodedb_sql::parser::preprocess::lex::{SqlSegment, segments};

#[derive(Debug, Clone, PartialEq, Eq)]
enum Kind {
    /// A bare word, upper-cased.
    Word,
    /// One punctuation character.
    Punct,
    /// A string literal or quoted identifier. Matches no keyword.
    Quoted,
}

#[derive(Debug, Clone)]
struct Token {
    kind: Kind,
    text: String,
    /// Parenthesis depth the token sits at. Top level is 0.
    depth: u32,
}

/// Whether `sql` holds `word` in any case. A route keyed on a word cannot
/// match a statement without it, and checking this first skips the tokenizer,
/// whose cost grows with the statement's length.
pub(super) fn mentions(sql: &str, word: &str) -> bool {
    nodedb_types::ascii::find_ascii_case_insensitive_from(sql, word, 0).is_some()
}

/// The routing tokens of one statement.
pub(super) struct RouteTokens {
    tokens: Vec<Token>,
}

impl RouteTokens {
    pub(super) fn new(sql: &str) -> Self {
        let mut tokens = Vec::new();
        let mut depth: u32 = 0;
        for segment in segments(sql) {
            match segment {
                SqlSegment::Text(text) => lex_text(text, &mut depth, &mut tokens),
                SqlSegment::SingleQuotedString(_) | SqlSegment::QuotedIdent(_) => {
                    tokens.push(Token {
                        kind: Kind::Quoted,
                        text: String::new(),
                        depth,
                    })
                }
                SqlSegment::LineComment(_) | SqlSegment::BlockComment(_) => {}
            }
        }
        Self { tokens }
    }

    /// Whether the statement opens with `seq`. Each element is a keyword or
    /// a single punctuation character.
    pub(super) fn leads_with(&self, seq: &[&str]) -> bool {
        self.phrase_at(0, seq)
    }

    /// Whether `seq` sits at token `index`.
    pub(super) fn phrase_at(&self, index: usize, seq: &[&str]) -> bool {
        seq.iter().enumerate().all(|(offset, expected)| {
            self.tokens
                .get(index + offset)
                .is_some_and(|token| token.matches(expected))
        })
    }

    /// The index just past the name that starts at `index`: a bare word or a
    /// quoted identifier, joined by `.` to further parts. `index` itself when
    /// no name starts there.
    pub(super) fn skip_name(&self, index: usize) -> usize {
        if !self.is_name_part(index) {
            return index;
        }
        let mut end = index + 1;
        while self
            .tokens
            .get(end)
            .is_some_and(|token| token.kind == Kind::Punct && token.text == ".")
            && self.is_name_part(end + 1)
        {
            end += 2;
        }
        end
    }

    /// Whether the keyword `word` appears at the top level at or after
    /// token `from`.
    pub(super) fn has_word_from(&self, from: usize, word: &str) -> bool {
        self.top_level_word_from(from, word).is_some()
    }

    /// The index of the first top-level keyword `word` at or after `from`.
    pub(super) fn top_level_word_from(&self, from: usize, word: &str) -> Option<usize> {
        self.tokens
            .iter()
            .enumerate()
            .skip(from)
            .find(|(_, token)| token.depth == 0 && token.kind == Kind::Word && token.text == word)
            .map(|(index, _)| index)
    }

    /// Whether the top-level phrase `seq` appears before the first top-level
    /// keyword `stop`, or anywhere when `stop` is absent.
    pub(super) fn has_phrase_before(&self, seq: &[&str], stop: &str) -> bool {
        let end = self
            .top_level_word_from(0, stop)
            .unwrap_or(self.tokens.len());
        (0..end).any(|index| self.tokens[index].depth == 0 && self.phrase_at(index, seq))
    }

    fn is_name_part(&self, index: usize) -> bool {
        self.tokens
            .get(index)
            .is_some_and(|token| matches!(token.kind, Kind::Word | Kind::Quoted))
    }
}

impl Token {
    fn matches(&self, expected: &str) -> bool {
        match self.kind {
            Kind::Word => self.text == expected,
            Kind::Punct => self.text == expected,
            Kind::Quoted => false,
        }
    }
}

fn is_word_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'$' || !byte.is_ascii()
}

/// Split one unquoted text segment into words and punctuation.
fn lex_text(text: &str, depth: &mut u32, out: &mut Vec<Token>) {
    let bytes = text.as_bytes();
    let mut index = 0;
    while index < bytes.len() {
        let byte = bytes[index];
        if byte.is_ascii_whitespace() {
            index += 1;
            continue;
        }
        if is_word_byte(byte) {
            let start = index;
            while index < bytes.len() && is_word_byte(bytes[index]) {
                index += 1;
            }
            out.push(Token {
                kind: Kind::Word,
                text: text[start..index].to_ascii_uppercase(),
                depth: *depth,
            });
            continue;
        }
        if byte == b')' {
            *depth = depth.saturating_sub(1);
        }
        out.push(Token {
            kind: Kind::Punct,
            text: char::from(byte).to_string(),
            depth: *depth,
        });
        if byte == b'(' {
            *depth += 1;
        }
        index += 1;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mentions_finds_a_word_in_any_case_and_nothing_else() {
        assert!(mentions("select * from d at Version 'c'", "VERSION"));
        assert!(mentions("RESTORE d", "RESTORE"));
        assert!(!mentions("SELECT id FROM turns WHERE a = 1", "VERSION"));
        assert!(!mentions("", "VERSION"));
    }

    #[test]
    fn leading_words_ignore_case_spacing_and_comments() {
        let tokens = RouteTokens::new("  /* c */ select  topk ( 'i', 3)");
        assert!(tokens.leads_with(&["SELECT", "TOPK", "("]));
    }

    #[test]
    fn a_keyword_inside_a_literal_never_matches() {
        let tokens = RouteTokens::new("SELECT * FROM t WHERE note = 'AT VERSION'");
        assert!(!tokens.has_word_from(0, "AT"));
        assert!(!tokens.has_word_from(0, "VERSION"));
    }

    #[test]
    fn a_name_is_skipped_as_a_unit() {
        let tokens = RouteTokens::new("ALTER COLLECTION db.\"set\" SET PERMISSION_TREE = '{}'");
        let after = tokens.skip_name(2);
        assert_eq!(after, 5);
        assert!(tokens.phrase_at(after, &["SET", "PERMISSION_TREE"]));
    }

    #[test]
    fn a_word_inside_parentheses_is_not_top_level() {
        let tokens = RouteTokens::new("ALTER COLLECTION c ADD CONSTRAINT k CHECK (status <> 'x')");
        assert!(tokens.has_word_from(0, "CHECK"));
        assert!(!tokens.has_word_from(0, "STATUS"));
    }

    #[test]
    fn a_phrase_before_a_stop_word() {
        let wasm =
            RouteTokens::new("CREATE FUNCTION f(x INT) RETURNS INT LANGUAGE WASM AS 'AGFzbQ=='");
        assert!(wasm.has_phrase_before(&["LANGUAGE", "WASM"], "AS"));
        let sql_body =
            RouteTokens::new("CREATE FUNCTION f(x INT) RETURNS INT AS SELECT language wasm FROM t");
        assert!(!sql_body.has_phrase_before(&["LANGUAGE", "WASM"], "AS"));
    }
}
