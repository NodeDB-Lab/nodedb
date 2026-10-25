// SPDX-License-Identifier: Apache-2.0

//! WKT text as a tree of `KEYWORD[value, ...]` nodes.
//!
//! The parser reads the shared WKT grammar: square or round brackets,
//! double-quoted text with `""` for a quote, numbers, and bare words such
//! as axis directions. It nests at most [`MAX_DEPTH`] levels, so hostile
//! input cannot exhaust the stack.

use crate::error::CrsParseError;

/// Format name reported by WKT2 errors.
pub const WKT2_FORMAT: &str = "WKT2";

/// Deepest node nesting the parser accepts.
const MAX_DEPTH: usize = 32;

/// One argument of a node.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum WktValue {
    Node(WktNode),
    Text(String),
    Number(f64),
    Word(String),
}

/// A `KEYWORD[...]` node. The keyword is upper-cased.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct WktNode {
    pub keyword: String,
    pub args: Vec<WktValue>,
}

impl WktNode {
    /// Argument `i` as text.
    pub(crate) fn text(&self, i: usize) -> Option<&str> {
        match self.args.get(i)? {
            WktValue::Text(t) => Some(t),
            WktValue::Node(_) | WktValue::Number(_) | WktValue::Word(_) => None,
        }
    }

    /// Argument `i` as a number.
    pub(crate) fn number(&self, i: usize) -> Option<f64> {
        match self.args.get(i)? {
            WktValue::Number(n) => Some(*n),
            WktValue::Node(_) | WktValue::Text(_) | WktValue::Word(_) => None,
        }
    }

    /// Argument `i` as a bare word.
    pub(crate) fn word(&self, i: usize) -> Option<&str> {
        match self.args.get(i)? {
            WktValue::Word(w) => Some(w),
            WktValue::Node(_) | WktValue::Text(_) | WktValue::Number(_) => None,
        }
    }

    /// Every child node whose keyword is in `keywords`.
    pub(crate) fn children<'a>(
        &'a self,
        keywords: &'a [&'a str],
    ) -> impl Iterator<Item = &'a WktNode> + 'a {
        self.args.iter().filter_map(move |arg| match arg {
            WktValue::Node(n) if keywords.contains(&n.keyword.as_str()) => Some(n),
            WktValue::Node(_) | WktValue::Text(_) | WktValue::Number(_) | WktValue::Word(_) => None,
        })
    }

    /// The first child node whose keyword is in `keywords`.
    pub(crate) fn child<'a>(&'a self, keywords: &'a [&'a str]) -> Option<&'a WktNode> {
        self.children(keywords).next()
    }

    /// The first child node of any keyword.
    pub(crate) fn first_node(&self) -> Option<&WktNode> {
        self.args.iter().find_map(|arg| match arg {
            WktValue::Node(n) => Some(n),
            WktValue::Text(_) | WktValue::Number(_) | WktValue::Word(_) => None,
        })
    }
}

/// Parse `text` into its root node. Only whitespace may follow it.
pub(crate) fn parse_tree(text: &str) -> Result<WktNode, CrsParseError> {
    let mut parser = Parser {
        bytes: text.as_bytes(),
        text,
        pos: 0,
    };
    parser.skip_space();
    let root = parser.node(0)?;
    parser.skip_space();
    if parser.pos != parser.bytes.len() {
        return Err(parser.malformed());
    }
    Ok(root)
}

struct Parser<'a> {
    bytes: &'a [u8],
    text: &'a str,
    pos: usize,
}

impl Parser<'_> {
    fn malformed(&self) -> CrsParseError {
        CrsParseError::Malformed {
            format: WKT2_FORMAT,
            offset: self.pos,
        }
    }

    fn peek(&self) -> Option<u8> {
        self.bytes.get(self.pos).copied()
    }

    fn skip_space(&mut self) {
        while self.peek().is_some_and(|b| b.is_ascii_whitespace()) {
            self.pos += 1;
        }
    }

    /// A run of word characters: letters, digits and `_`.
    fn word(&mut self) -> &str {
        let start = self.pos;
        while self
            .peek()
            .is_some_and(|b| b.is_ascii_alphanumeric() || b == b'_')
        {
            self.pos += 1;
        }
        &self.text[start..self.pos]
    }

    fn node(&mut self, depth: usize) -> Result<WktNode, CrsParseError> {
        if depth >= MAX_DEPTH {
            return Err(self.malformed());
        }
        let keyword = self.word().to_ascii_uppercase();
        if keyword.is_empty() || !keyword.as_bytes()[0].is_ascii_alphabetic() {
            return Err(self.malformed());
        }
        self.skip_space();
        let close = match self.peek() {
            Some(b'[') => b']',
            Some(b'(') => b')',
            _ => return Err(self.malformed()),
        };
        self.pos += 1;
        let mut args = Vec::new();
        self.skip_space();
        if self.peek() == Some(close) {
            self.pos += 1;
            return Ok(WktNode { keyword, args });
        }
        loop {
            self.skip_space();
            args.push(self.value(depth)?);
            self.skip_space();
            match self.peek() {
                Some(b',') => self.pos += 1,
                Some(b) if b == close => {
                    self.pos += 1;
                    return Ok(WktNode { keyword, args });
                }
                _ => return Err(self.malformed()),
            }
        }
    }

    fn value(&mut self, depth: usize) -> Result<WktValue, CrsParseError> {
        match self.peek() {
            Some(b'"') => self.quoted().map(WktValue::Text),
            Some(b) if b == b'-' || b == b'+' || b == b'.' || b.is_ascii_digit() => {
                self.number().map(WktValue::Number)
            }
            Some(b) if b.is_ascii_alphabetic() => {
                let start = self.pos;
                let word = self.word().to_string();
                self.skip_space();
                if matches!(self.peek(), Some(b'[' | b'(')) {
                    self.pos = start;
                    self.node(depth + 1).map(WktValue::Node)
                } else {
                    Ok(WktValue::Word(word))
                }
            }
            _ => Err(self.malformed()),
        }
    }

    fn quoted(&mut self) -> Result<String, CrsParseError> {
        let start = self.pos;
        self.pos += 1;
        let mut out = String::new();
        loop {
            let rest = &self.text[self.pos..];
            let Some(quote) = rest.find('"') else {
                self.pos = start;
                return Err(self.malformed());
            };
            out.push_str(&rest[..quote]);
            self.pos += quote + 1;
            if self.peek() == Some(b'"') {
                out.push('"');
                self.pos += 1;
            } else {
                return Ok(out);
            }
        }
    }

    fn number(&mut self) -> Result<f64, CrsParseError> {
        let start = self.pos;
        while self
            .peek()
            .is_some_and(|b| b.is_ascii_digit() || matches!(b, b'-' | b'+' | b'.' | b'e' | b'E'))
        {
            self.pos += 1;
        }
        let value: f64 =
            self.text[start..self.pos]
                .parse()
                .map_err(|_| CrsParseError::Malformed {
                    format: WKT2_FORMAT,
                    offset: start,
                })?;
        if value.is_finite() {
            Ok(value)
        } else {
            Err(CrsParseError::Malformed {
                format: WKT2_FORMAT,
                offset: start,
            })
        }
    }
}

/// Append `text` as a WKT quoted string.
pub(crate) fn push_quoted(out: &mut String, text: &str) {
    out.push('"');
    out.push_str(&text.replace('"', "\"\""));
    out.push('"');
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nodes_text_numbers_and_words() {
        let root = parse_tree(
            r#"PROJCRS["a ""b""", CS[Cartesian, 2], AXIS["x", east], LENGTHUNIT["metre", 1.5E0]]"#,
        )
        .unwrap();
        assert_eq!(root.keyword, "PROJCRS");
        assert_eq!(root.text(0), Some(r#"a "b""#));
        let cs = root.child(&["CS"]).unwrap();
        assert_eq!(cs.word(0), Some("Cartesian"));
        assert_eq!(cs.number(1), Some(2.0));
        assert_eq!(root.child(&["LENGTHUNIT"]).unwrap().number(1), Some(1.5));
        let round = parse_tree("unit(\"m\",1)").unwrap();
        assert_eq!(round.keyword, "UNIT");
    }

    #[test]
    fn malformed_input_names_the_offset() {
        for bad in ["", "A[", "A[1,]", "A[\"x]", "A[1] B", "[1]", "A[1e999]"] {
            assert!(
                matches!(parse_tree(bad), Err(CrsParseError::Malformed { .. })),
                "{bad}"
            );
        }
        let deep = "A[".repeat(40) + &"]".repeat(40);
        assert!(parse_tree(&deep).is_err());
    }

    #[test]
    fn quoting_round_trips() {
        let mut out = String::new();
        push_quoted(&mut out, r#"x"y"#);
        assert_eq!(out, r#""x""y""#);
    }
}
