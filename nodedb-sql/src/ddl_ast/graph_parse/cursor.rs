// SPDX-License-Identifier: Apache-2.0

//! Consume-tracking cursor over a graph DSL token list.
//!
//! The module's parse is seek-based: a clause reader finds its keyword
//! wherever it appears and ignores every token between. That is fine while
//! every token belongs to some clause. It is wrong the moment a token belongs
//! to none: `GRAPH TRAVERSE FROM 1 DEPTS 3 IN g` finds no `DEPTH`, defaults
//! the depth, and leaves `DEPTS 3` unread — the statement answers a different
//! question than it asked, with no error.
//!
//! The cursor records which tokens each clause consumed. After the statement
//! is built, [`Cursor::finish`] refuses the first token no clause claimed.
//! A typo is then a parse error that names the token, and a new clause cannot
//! be added without deciding what it consumes.

use super::tokenizer::Tok;
use crate::error::SqlError;

/// A token list plus the set of tokens a clause has claimed.
pub(super) struct Cursor<'a> {
    toks: Vec<Tok<'a>>,
    used: Vec<bool>,
}

impl<'a> Cursor<'a> {
    /// Build a cursor over `toks`. The first `prefix_len` tokens are the
    /// command words (`GRAPH TRAVERSE`), which the dispatcher matched and no
    /// clause will claim.
    pub(super) fn new(toks: Vec<Tok<'a>>, prefix_len: usize) -> Self {
        let mut used = vec![false; toks.len()];
        for slot in used.iter_mut().take(prefix_len.min(toks.len())) {
            *slot = true;
        }
        Self { toks, used }
    }

    fn is_keyword(tok: &Tok<'_>, keyword: &str) -> bool {
        matches!(tok, Tok::Word(w) if w.eq_ignore_ascii_case(keyword))
    }

    /// Position of `keyword`, claiming it. A keyword already claimed by an
    /// earlier clause still matches: `IN` in one statement is one clause, but
    /// the seek-based readers may be called in any order.
    fn find(&mut self, keyword: &str) -> Option<usize> {
        let pos = self
            .toks
            .iter()
            .position(|tok| Self::is_keyword(tok, keyword))?;
        self.used[pos] = true;
        Some(pos)
    }

    /// Claim the value token at `pos` when it is a word or a quoted literal.
    /// An object literal is claimed by the callers that accept one.
    fn claim_text(&mut self, pos: usize) -> Option<String> {
        let value = match self.toks.get(pos)? {
            Tok::Quoted(s) => s.clone().into_owned(),
            Tok::Word(w) => (*w).to_string(),
            Tok::Object(_) => return None,
        };
        self.used[pos] = true;
        Some(value)
    }

    /// The word or quoted literal after `keyword`.
    pub(super) fn quoted_after(&mut self, keyword: &str) -> Option<String> {
        let pos = self.find(keyword)?;
        self.claim_text(pos + 1)
    }

    /// Every consecutive word or quoted literal after `keyword`, up to the
    /// first token that is neither. Used by `AS <label> [, <label>…]`.
    pub(super) fn quoted_list_after(&mut self, keyword: &str) -> Vec<String> {
        let Some(pos) = self.find(keyword) else {
            return Vec::new();
        };
        let mut out = Vec::new();
        let mut at = pos + 1;
        while let Some(value) = self.claim_text(at) {
            out.push(value);
            at += 1;
        }
        out
    }

    /// The brace-balanced object literal after `keyword`.
    pub(super) fn object_after(&mut self, keyword: &str) -> Option<String> {
        let pos = self.find(keyword)?;
        match self.toks.get(pos + 1)? {
            Tok::Object(s) => {
                self.used[pos + 1] = true;
                Some((*s).to_string())
            }
            _ => None,
        }
    }

    /// The bare word after `keyword`.
    pub(super) fn word_after(&mut self, keyword: &str) -> Option<String> {
        let pos = self.find(keyword)?;
        match self.toks.get(pos + 1)? {
            Tok::Word(w) => {
                self.used[pos + 1] = true;
                Some((*w).to_string())
            }
            _ => None,
        }
    }

    /// The unsigned integer after `keyword`.
    pub(super) fn usize_after(&mut self, keyword: &str) -> Option<usize> {
        self.word_after(keyword)?.parse().ok()
    }

    /// The float after `keyword`.
    pub(super) fn float_after(&mut self, keyword: &str) -> Option<f64> {
        self.word_after(keyword)?.parse().ok()
    }

    /// The next `count` float words after `keyword`.
    pub(super) fn floats_after<const N: usize>(&mut self, keyword: &str) -> Option<[f64; N]> {
        let pos = self.find(keyword)?;
        let mut out = [0.0f64; N];
        for (offset, slot) in out.iter_mut().enumerate() {
            let value = match self.toks.get(pos + 1 + offset)? {
                Tok::Word(w) => w.parse::<f64>().ok()?,
                _ => return None,
            };
            self.used[pos + 1 + offset] = true;
            *slot = value;
        }
        Some(out)
    }

    /// Read a `DIRECTION` clause.
    ///
    /// An omitted clause defaults to `out`. A value outside the vocabulary is
    /// refused by name — defaulting it would answer a question the caller did
    /// not ask, and `DIRECTION INBOUND` is indistinguishable from `DIRECTION
    /// BANANA` once both have become `out`.
    pub(super) fn direction_after(
        &mut self,
        keyword: &str,
    ) -> Result<super::super::statement::GraphDirection, SqlError> {
        use super::super::statement::GraphDirection;
        let Some(word) = self.word_after(keyword) else {
            return Ok(GraphDirection::Out);
        };
        match word.to_ascii_uppercase().as_str() {
            "IN" => Ok(GraphDirection::In),
            "OUT" => Ok(GraphDirection::Out),
            "BOTH" => Ok(GraphDirection::Both),
            _ => Err(SqlError::Parse {
                detail: format!("{keyword} must be one of in, out, both — found '{word}'"),
            }),
        }
    }

    /// An optional numeric clause, refusing a value that is present but not a
    /// number. `Ok(None)` means the clause was omitted, so the caller applies
    /// its own default; it never means "the value was unreadable".
    pub(super) fn usize_after_checked(&mut self, keyword: &str) -> Result<Option<usize>, SqlError> {
        let Some(word) = self.word_after(keyword) else {
            return Ok(None);
        };
        word.parse().map(Some).map_err(|_| SqlError::Parse {
            detail: format!("{keyword} must be a non-negative integer — found '{word}'"),
        })
    }

    /// The `PROPERTIES {…}` / `PROPERTIES 'literal'` clause.
    pub(super) fn extract_properties(&mut self) -> super::super::statement::GraphProperties {
        use super::super::statement::GraphProperties;
        let Some(pos) = self.find("PROPERTIES") else {
            return GraphProperties::None;
        };
        match self.toks.get(pos + 1) {
            Some(Tok::Object(obj_str)) => {
                self.used[pos + 1] = true;
                GraphProperties::Object((*obj_str).to_string())
            }
            Some(Tok::Quoted(s)) => {
                self.used[pos + 1] = true;
                GraphProperties::Quoted(s.clone().into_owned())
            }
            _ => GraphProperties::None,
        }
    }

    /// The token slice, for readers that validate their own span (fusion
    /// parameters read the bracket payload from the raw statement text).
    pub(super) fn tokens(&self) -> &[Tok<'a>] {
        &self.toks
    }

    /// Claim every remaining token.
    ///
    /// For a statement whose parameters a dedicated extractor validated
    /// (`GRAPH RAG FUSION`), where this cursor's clause readers do not see the
    /// bracket payload the extractor reads from the raw text.
    pub(super) fn consume_rest(&mut self) {
        for slot in self.used.iter_mut() {
            *slot = true;
        }
    }

    /// Refuse the first token no clause claimed.
    pub(super) fn finish(self, statement: &str) -> Result<(), SqlError> {
        for (tok, used) in self.toks.iter().zip(self.used.iter()) {
            if *used {
                continue;
            }
            let text = match tok {
                Tok::Word(w) => (*w).to_string(),
                Tok::Quoted(s) => s.to_string(),
                Tok::Object(_) => "{…}".to_string(),
            };
            return Err(SqlError::Parse {
                detail: format!("{statement}: unexpected token '{text}'"),
            });
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ddl_ast::graph_parse::tokenizer::tokenize;

    #[test]
    fn a_mistyped_keyword_is_left_unclaimed() {
        let toks = tokenize("GRAPH TRAVERSE FROM 1 DEPTS 3 IN g");
        let mut cursor = Cursor::new(toks, 2);
        assert_eq!(cursor.quoted_after("FROM").as_deref(), Some("1"));
        assert_eq!(cursor.quoted_after("IN").as_deref(), Some("g"));
        assert!(cursor.usize_after_checked("DEPTH").unwrap().is_none());
        let err = cursor.finish("GRAPH TRAVERSE").unwrap_err();
        assert!(
            err.to_string().contains("DEPTS"),
            "the error must name the unclaimed token: {err}"
        );
    }

    #[test]
    fn a_consumed_clause_leaves_nothing_unclaimed() {
        let toks = tokenize("GRAPH TRAVERSE FROM 1 DEPTH 3 IN g DIRECTION both");
        let mut cursor = Cursor::new(toks, 2);
        assert_eq!(cursor.quoted_after("FROM").as_deref(), Some("1"));
        assert_eq!(cursor.usize_after_checked("DEPTH").unwrap(), Some(3));
        assert_eq!(cursor.quoted_after("IN").as_deref(), Some("g"));
        let _ = cursor.direction_after("DIRECTION").unwrap();
        cursor
            .finish("GRAPH TRAVERSE")
            .expect("every token claimed");
    }

    #[test]
    fn a_stray_quoted_literal_is_refused() {
        let toks = tokenize("GRAPH PATH FROM 'a' TO 'b' 'stray'");
        let mut cursor = Cursor::new(toks, 2);
        assert_eq!(cursor.quoted_after("FROM").as_deref(), Some("a"));
        assert_eq!(cursor.quoted_after("TO").as_deref(), Some("b"));
        let err = cursor.finish("GRAPH PATH").unwrap_err();
        assert!(err.to_string().contains("stray"), "{err}");
    }

    #[test]
    fn a_float_pair_is_claimed_together() {
        let toks = tokenize("GRAPH RAG FUSION ON g RRF_K (60.0, 35.0)");
        let mut cursor = Cursor::new(toks, 3);
        assert_eq!(cursor.word_after("ON").as_deref(), Some("g"));
        assert_eq!(cursor.floats_after::<2>("RRF_K"), Some([60.0, 35.0]));
        cursor
            .finish("GRAPH RAG FUSION")
            .expect("every token claimed");
    }
}
