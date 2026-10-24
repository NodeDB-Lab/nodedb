// SPDX-License-Identifier: Apache-2.0

//! Left-to-right clause reader for the graph DSL.
//!
//! A statement parser reads its clauses in source order. A clause keyword is
//! matched only at a clause position, and its value is read from the tokens
//! right after it. A token at a clause position that names no clause of the
//! statement is a parse error, so a misspelled keyword never falls back to
//! the clause default. A clause given twice is a parse error.

use super::super::graph_types::{GraphDirection, GraphProperties};
use super::tokenizer::Tok;
use crate::error::SqlError;

pub(super) struct Cursor<'t, 'a> {
    toks: &'t [Tok<'a>],
    pos: usize,
    statement: &'static str,
}

impl<'t, 'a> Cursor<'t, 'a> {
    /// A cursor over `toks` after its `command_words` (2 for `GRAPH TRAVERSE`).
    pub(super) fn new(toks: &'t [Tok<'a>], statement: &'static str, command_words: usize) -> Self {
        Self {
            toks,
            pos: command_words.min(toks.len()),
            statement,
        }
    }

    /// The statement name error messages use, such as `GRAPH TRAVERSE`.
    pub(super) fn statement(&self) -> &'static str {
        self.statement
    }

    /// The next token that is not a comma. Every reader goes through here,
    /// so a comma outside a list is as if it were absent.
    fn peek(&mut self) -> Option<&'t Tok<'a>> {
        let toks: &'t [Tok<'a>] = self.toks;
        while matches!(toks.get(self.pos), Some(Tok::Comma)) {
            self.pos += 1;
        }
        toks.get(self.pos)
    }

    /// The next clause keyword, upper-cased. `None` at the end of the
    /// statement. A string, identifier, object or list at a clause position
    /// is refused.
    pub(super) fn next_clause(&mut self) -> Result<Option<String>, SqlError> {
        match self.peek() {
            None => Ok(None),
            Some(Tok::Word(word)) => {
                self.pos += 1;
                Ok(Some(word.to_ascii_uppercase()))
            }
            Some(other) => Err(self.unexpected(other)),
        }
    }

    /// The error for the keyword `next_clause` returned last, when the
    /// statement has no such clause.
    pub(super) fn unknown_clause(&self) -> SqlError {
        match self
            .pos
            .checked_sub(1)
            .and_then(|index| self.toks.get(index))
        {
            Some(tok) => self.unexpected(tok),
            None => parse_error(format!("unexpected end of {}", self.statement)),
        }
    }

    /// The tokens not yet consumed.
    pub(super) fn rest(&self) -> &'t [Tok<'a>] {
        let toks: &'t [Tok<'a>] = self.toks;
        toks.get(self.pos..).unwrap_or_default()
    }

    /// Consume the next token when it is the bare word `keyword`.
    pub(super) fn eat_keyword(&mut self, keyword: &str) -> bool {
        let hit =
            matches!(self.peek(), Some(Tok::Word(word)) if word.eq_ignore_ascii_case(keyword));
        if hit {
            self.pos += 1;
        }
        hit
    }

    /// A node, collection or label: a quoted string, a quoted identifier, or
    /// a bare word.
    pub(super) fn value(&mut self, clause: &str) -> Result<String, SqlError> {
        let value = match self.peek() {
            Some(Tok::Quoted(text) | Tok::Ident(text)) => text.clone().into_owned(),
            Some(Tok::Word(word)) => (*word).to_string(),
            other => {
                return Err(parse_error(format!(
                    "{clause} needs a value — found {}",
                    describe_opt(other)
                )));
            }
        };
        self.pos += 1;
        Ok(value)
    }

    /// A bare word.
    pub(super) fn word(&mut self, clause: &str) -> Result<&'a str, SqlError> {
        match self.peek() {
            Some(Tok::Word(word)) => {
                self.pos += 1;
                Ok(*word)
            }
            other => Err(parse_error(format!(
                "{clause} needs a word — found {}",
                describe_opt(other)
            ))),
        }
    }

    pub(super) fn usize(&mut self, clause: &str) -> Result<usize, SqlError> {
        let text = self.number_text(clause, "a non-negative integer")?;
        text.parse().map_err(|_| {
            parse_error(format!(
                "{clause} must be a non-negative integer — found '{text}'"
            ))
        })
    }

    pub(super) fn f64(&mut self, clause: &str) -> Result<f64, SqlError> {
        let text = self.number_text(clause, "a number")?;
        text.parse()
            .map_err(|_| parse_error(format!("{clause} must be a number — found '{text}'")))
    }

    /// Consume the next word when it parses as a number.
    pub(super) fn number_word(&mut self) -> Option<f64> {
        let Some(Tok::Word(word)) = self.peek() else {
            return None;
        };
        let number = word.parse().ok()?;
        self.pos += 1;
        Some(number)
    }

    fn number_text(&mut self, clause: &str, expected: &str) -> Result<&'a str, SqlError> {
        match self.peek() {
            Some(Tok::Word(word)) => {
                self.pos += 1;
                Ok(*word)
            }
            other => Err(parse_error(format!(
                "{clause} must be {expected} — found {}",
                describe_opt(other)
            ))),
        }
    }

    /// One or more quoted strings: `'a', 'b'` or `('a', 'b')`. Exactly one
    /// comma separates two items. A comma after the last item is skipped.
    pub(super) fn quoted_list(&mut self, clause: &str) -> Result<Vec<String>, SqlError> {
        let mut out = Vec::new();
        if let Some(Tok::Quoted(text)) = self.peek() {
            out.push(text.clone().into_owned());
            self.pos += 1;
        }
        if out.is_empty() {
            return Err(parse_error(format!(
                "{} {clause} needs one or more quoted labels, as {clause} 'a', 'b'",
                self.statement
            )));
        }
        loop {
            let toks: &'t [Tok<'a>] = self.toks;
            let commas = toks
                .get(self.pos..)
                .unwrap_or_default()
                .iter()
                .take_while(|tok| matches!(tok, Tok::Comma))
                .count();
            let Some(Tok::Quoted(text)) = toks.get(self.pos + commas) else {
                return Ok(out);
            };
            if commas != 1 {
                return Err(parse_error(format!(
                    "{} {clause} needs a comma between labels, as {clause} 'a', 'b'",
                    self.statement
                )));
            }
            out.push(text.clone().into_owned());
            self.pos += 2;
        }
    }

    /// A `{...}` object literal, braces included.
    pub(super) fn object(&mut self, clause: &str) -> Result<String, SqlError> {
        match self.peek() {
            Some(Tok::Object(text)) => {
                self.pos += 1;
                Ok((*text).to_string())
            }
            other => Err(parse_error(format!(
                "{clause} needs an object literal {{...}} — found {}",
                describe_opt(other)
            ))),
        }
    }

    /// The content of a `[...]` list.
    pub(super) fn list(&mut self, clause: &str) -> Result<&'a str, SqlError> {
        match self.peek() {
            Some(Tok::List(text)) => {
                self.pos += 1;
                Ok(*text)
            }
            other => Err(parse_error(format!(
                "{clause} needs ARRAY[...] — found {}",
                describe_opt(other)
            ))),
        }
    }

    /// A `PROPERTIES` value: an object literal or a quoted JSON document.
    pub(super) fn properties(&mut self) -> Result<GraphProperties, SqlError> {
        let properties = match self.peek() {
            Some(Tok::Object(text)) => GraphProperties::Object((*text).to_string()),
            Some(Tok::Quoted(text)) => GraphProperties::Quoted(text.clone().into_owned()),
            other => {
                return Err(parse_error(format!(
                    "PROPERTIES needs an object literal {{...}} or a quoted JSON document — found {}",
                    describe_opt(other)
                )));
            }
        };
        self.pos += 1;
        Ok(properties)
    }

    /// A `DIRECTION` value. A word outside `in`, `out`, `both` is refused by
    /// name.
    pub(super) fn direction(&mut self) -> Result<GraphDirection, SqlError> {
        let word = match self.peek() {
            Some(Tok::Word(word)) => {
                self.pos += 1;
                *word
            }
            other => {
                return Err(parse_error(format!(
                    "DIRECTION must be one of in, out, both — found {}",
                    describe_opt(other)
                )));
            }
        };
        match word.to_ascii_uppercase().as_str() {
            "IN" => Ok(GraphDirection::In),
            "OUT" => Ok(GraphDirection::Out),
            "BOTH" => Ok(GraphDirection::Both),
            _ => Err(parse_error(format!(
                "DIRECTION must be one of in, out, both — found '{word}'"
            ))),
        }
    }

    fn unexpected(&self, tok: &Tok<'_>) -> SqlError {
        parse_error(format!(
            "unexpected {} in {}",
            describe(tok),
            self.statement
        ))
    }
}

/// Store a clause value. A clause the statement already has is refused.
pub(super) fn set_once<T>(
    slot: &mut Option<T>,
    value: T,
    clause: &str,
    statement: &str,
) -> Result<(), SqlError> {
    if slot.is_some() {
        return Err(parse_error(format!(
            "{clause} given more than once in {statement}"
        )));
    }
    *slot = Some(value);
    Ok(())
}

/// A required clause was absent.
pub(super) fn missing_clause(statement: &str, clause: &str) -> SqlError {
    parse_error(format!("{statement} requires {clause}"))
}

fn describe(tok: &Tok<'_>) -> String {
    match tok {
        Tok::Word(word) => format!("token '{word}'"),
        Tok::Quoted(text) => format!("string '{text}'"),
        Tok::Ident(text) => format!("identifier \"{text}\""),
        Tok::Object(_) => "an object literal".to_owned(),
        Tok::List(_) => "a [...] list".to_owned(),
        Tok::Comma => "','".to_owned(),
    }
}

fn describe_opt(tok: Option<&Tok<'_>>) -> String {
    tok.map_or_else(|| "end of statement".to_owned(), describe)
}

fn parse_error(detail: String) -> SqlError {
    SqlError::Parse { detail }
}
