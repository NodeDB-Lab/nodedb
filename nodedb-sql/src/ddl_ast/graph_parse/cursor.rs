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

    /// The index of the next token that is not a `,`. A comma outside a
    /// list carries no meaning, so every read but a list item skips it.
    fn head(&self) -> usize {
        let mut index = self.pos;
        while matches!(self.toks.get(index), Some(Tok::Comma)) {
            index += 1;
        }
        index
    }

    fn peek(&self) -> Option<&'t Tok<'a>> {
        let toks: &'t [Tok<'a>] = self.toks;
        toks.get(self.head())
    }

    /// Step past the token `peek` returned.
    fn advance(&mut self) {
        self.pos = self.head() + 1;
    }

    /// The next clause keyword, upper-cased. `None` at the end of the
    /// statement. A string, identifier, object or list at a clause position
    /// is refused.
    pub(super) fn next_clause(&mut self) -> Result<Option<String>, SqlError> {
        match self.peek() {
            None => Ok(None),
            Some(Tok::Word(word)) => {
                self.advance();
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

    /// Consume the next token when it is the bare word `keyword`.
    pub(super) fn eat_keyword(&mut self, keyword: &str) -> bool {
        let hit =
            matches!(self.peek(), Some(Tok::Word(word)) if word.eq_ignore_ascii_case(keyword));
        if hit {
            self.advance();
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
        self.advance();
        Ok(value)
    }

    /// A bare word.
    pub(super) fn word(&mut self, clause: &str) -> Result<&'a str, SqlError> {
        match self.peek() {
            Some(Tok::Word(word)) => {
                self.advance();
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

    /// A timestamp in ms since epoch, in the forms `AS OF VALID TIME`
    /// accepts: an integer, `NOW()`, or an ISO-8601 string.
    pub(super) fn timestamp(&mut self, clause: &str) -> Result<i64, SqlError> {
        let expression = match self.peek() {
            Some(Tok::Quoted(text)) => format!("'{text}'"),
            // The tokenizer drops `(` and `)`, so `NOW()` arrives as `NOW`.
            Some(Tok::Word(word)) if word.eq_ignore_ascii_case("NOW") => "NOW()".to_owned(),
            Some(Tok::Word(word)) => (*word).to_owned(),
            other => {
                return Err(parse_error(format!(
                    "{clause} needs a timestamp — found {}",
                    describe_opt(other)
                )));
            }
        };
        self.advance();
        crate::parser::preprocess::temporal::parse_temporal_expr(&expression)
            .map_err(|error| parse_error(format!("{clause}: {}", error.0)))
    }

    /// One or more numbers separated by commas: `60.0, 35.0` or
    /// `(60.0, 35.0)`. Empty when the next token is no number.
    pub(super) fn number_list(&mut self, clause: &str) -> Result<Vec<f64>, SqlError> {
        self.comma_list(clause, |tok| match tok {
            Tok::Word(word) => word.parse().ok().map(|number| (number, (*word).to_owned())),
            _ => None,
        })
    }

    fn number_text(&mut self, clause: &str, expected: &str) -> Result<&'a str, SqlError> {
        match self.peek() {
            Some(Tok::Word(word)) => {
                self.advance();
                Ok(*word)
            }
            other => Err(parse_error(format!(
                "{clause} must be {expected} — found {}",
                describe_opt(other)
            ))),
        }
    }

    /// One or more quoted strings separated by commas: `'a', 'b'` or
    /// `('a', 'b')`.
    pub(super) fn quoted_list(&mut self, clause: &str) -> Result<Vec<String>, SqlError> {
        let out = self.comma_list(clause, |tok| match tok {
            Tok::Quoted(text) => Some((text.clone().into_owned(), format!("'{text}'"))),
            _ => None,
        })?;
        if out.is_empty() {
            return Err(parse_error(format!(
                "{} {clause} needs one or more quoted labels, as {clause} 'a', 'b'",
                self.statement
            )));
        }
        Ok(out)
    }

    /// The items `item` reads, separated by commas. `item` returns the value
    /// and its source text for an error message. Empty when the next token
    /// is no item. Two items with no comma between them are refused, and so
    /// is a comma with no item after it.
    fn comma_list<T>(
        &mut self,
        clause: &str,
        item: impl Fn(&Tok<'a>) -> Option<(T, String)>,
    ) -> Result<Vec<T>, SqlError> {
        let toks: &'t [Tok<'a>] = self.toks;
        let mut out = Vec::new();
        let Some((first, mut shown)) = self.peek().and_then(&item) else {
            return Ok(out);
        };
        self.advance();
        out.push(first);
        loop {
            match toks.get(self.pos) {
                Some(Tok::Comma) => {
                    self.pos += 1;
                    let next = toks.get(self.pos);
                    let Some((value, text)) = next.and_then(&item) else {
                        return Err(parse_error(format!(
                            "{} {clause} list ends with a comma — found {}",
                            self.statement,
                            describe_opt(next)
                        )));
                    };
                    self.pos += 1;
                    out.push(value);
                    shown = text;
                }
                Some(tok) => {
                    if let Some((_, text)) = item(tok) {
                        return Err(parse_error(format!(
                            "{} {clause} list items must be separated by commas: {shown} {text} \
                             — write {shown}, {text}",
                            self.statement
                        )));
                    }
                    return Ok(out);
                }
                None => return Ok(out),
            }
        }
    }

    /// A `{...}` object literal, braces included.
    pub(super) fn object(&mut self, clause: &str) -> Result<String, SqlError> {
        match self.peek() {
            Some(Tok::Object(text)) => {
                self.advance();
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
                self.advance();
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
        self.advance();
        Ok(properties)
    }

    /// A `DIRECTION` value. A word outside `in`, `out`, `both` is refused by
    /// name.
    pub(super) fn direction(&mut self) -> Result<GraphDirection, SqlError> {
        let word = match self.peek() {
            Some(Tok::Word(word)) => {
                self.advance();
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

#[cfg(test)]
mod tests {
    use super::super::try_parse;

    fn refused(sql: &str) -> String {
        try_parse(sql)
            .expect("input is graph DSL")
            .expect_err("statement must be refused")
            .to_string()
    }

    /// Two labels with no comma between them are refused on every statement
    /// that takes a label list. The error names the input and the fix.
    #[test]
    fn label_list_without_a_comma_is_refused() {
        for (sql, statement, clause) in [
            (
                "GRAPH TRAVERSE IN 'c' FROM 'x' DEPTH 2 LABEL 'a' 'b'",
                "GRAPH TRAVERSE",
                "LABEL",
            ),
            (
                "GRAPH NEIGHBORS IN 'c' OF 'x' LABEL 'a' 'b' DIRECTION out",
                "GRAPH NEIGHBORS",
                "LABEL",
            ),
            (
                "GRAPH PATH IN 'c' FROM 'x' TO 'y' MAX_DEPTH 3 LABEL 'a' 'b'",
                "GRAPH PATH",
                "LABEL",
            ),
            (
                "GRAPH NEIGHBORS IN 'c' OF 'x' LABEL ('a' 'b')",
                "GRAPH NEIGHBORS",
                "LABEL",
            ),
            ("GRAPH LABEL 'alice' AS 'a' 'b'", "GRAPH LABEL", "AS"),
            ("GRAPH UNLABEL 'alice' AS 'a' 'b'", "GRAPH UNLABEL", "AS"),
        ] {
            let error = refused(sql);
            assert!(
                error.contains(&format!(
                    "{statement} {clause} list items must be separated by commas: 'a' 'b'"
                )),
                "{sql}: {error}"
            );
            assert!(error.contains("write 'a', 'b'"), "{sql}: {error}");
        }
    }

    /// A missing comma later in the list is refused too, and names the pair.
    #[test]
    fn a_missing_comma_after_the_first_item_is_refused() {
        let error = refused("GRAPH NEIGHBORS IN 'c' OF 'x' LABEL 'a', 'b' 'c'");
        assert!(
            error.contains("LABEL list items must be separated by commas: 'b' 'c'"),
            "{error}"
        );
    }

    /// A comma with no label after it is refused.
    #[test]
    fn label_list_ending_with_a_comma_is_refused() {
        for sql in [
            "GRAPH NEIGHBORS IN 'c' OF 'x' LABEL 'a', DIRECTION out",
            "GRAPH TRAVERSE IN 'c' FROM 'x' LABEL 'a',",
            "GRAPH LABEL 'alice' AS 'a', , 'b'",
        ] {
            let error = refused(sql);
            assert!(error.contains("list ends with a comma"), "{sql}: {error}");
        }
    }

    /// A comma outside a list carries no meaning, as before.
    #[test]
    fn a_comma_between_clauses_is_skipped() {
        for sql in [
            "GRAPH NEIGHBORS IN 'c', OF 'x', LABEL 'a', 'b' DIRECTION out",
            "GRAPH TRAVERSE IN 'c' FROM 'x', DEPTH 2, LABEL ('a', 'b')",
        ] {
            assert!(
                try_parse(sql).expect("graph DSL").is_ok(),
                "{sql} must parse"
            );
        }
    }
}
