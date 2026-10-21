// SPDX-License-Identifier: Apache-2.0

//! A small JSON reader and string writer for PROJJSON.
//!
//! The crate takes no JSON dependency, so it builds for every target with
//! no allocator or SIMD assumptions. The reader nests at most
//! [`MAX_DEPTH`] levels, so hostile input cannot exhaust the stack.

use crate::error::CrsParseError;

/// Format name reported by PROJJSON errors.
pub const PROJJSON_FORMAT: &str = "PROJJSON";

/// Deepest nesting the reader accepts.
const MAX_DEPTH: usize = 64;

/// One JSON value. Object members keep their order.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Json {
    Null,
    Bool(bool),
    Number(f64),
    String(String),
    Array(Vec<Json>),
    Object(Vec<(String, Json)>),
}

impl Json {
    /// Member `key` of an object.
    pub(crate) fn get(&self, key: &str) -> Option<&Json> {
        match self {
            Json::Object(members) => members.iter().find(|(k, _)| k == key).map(|(_, v)| v),
            Json::Null | Json::Bool(_) | Json::Number(_) | Json::String(_) | Json::Array(_) => None,
        }
    }

    pub(crate) fn as_str(&self) -> Option<&str> {
        match self {
            Json::String(s) => Some(s),
            Json::Null | Json::Bool(_) | Json::Number(_) | Json::Array(_) | Json::Object(_) => None,
        }
    }

    pub(crate) fn as_f64(&self) -> Option<f64> {
        match self {
            Json::Number(n) => Some(*n),
            Json::Null | Json::Bool(_) | Json::String(_) | Json::Array(_) | Json::Object(_) => None,
        }
    }

    pub(crate) fn as_array(&self) -> Option<&[Json]> {
        match self {
            Json::Array(items) => Some(items),
            Json::Null | Json::Bool(_) | Json::Number(_) | Json::String(_) | Json::Object(_) => {
                None
            }
        }
    }
}

/// Parse `text` as one JSON value. Only whitespace may follow it.
pub(crate) fn parse_json(text: &str) -> Result<Json, CrsParseError> {
    let mut reader = Reader {
        text,
        bytes: text.as_bytes(),
        pos: 0,
    };
    reader.skip_space();
    let value = reader.value(0)?;
    reader.skip_space();
    if reader.pos != reader.bytes.len() {
        return Err(reader.malformed());
    }
    Ok(value)
}

struct Reader<'a> {
    text: &'a str,
    bytes: &'a [u8],
    pos: usize,
}

impl Reader<'_> {
    fn malformed(&self) -> CrsParseError {
        CrsParseError::Malformed {
            format: PROJJSON_FORMAT,
            offset: self.pos,
        }
    }

    fn peek(&self) -> Option<u8> {
        self.bytes.get(self.pos).copied()
    }

    fn skip_space(&mut self) {
        while matches!(self.peek(), Some(b' ' | b'\t' | b'\n' | b'\r')) {
            self.pos += 1;
        }
    }

    fn expect(&mut self, byte: u8) -> Result<(), CrsParseError> {
        if self.peek() == Some(byte) {
            self.pos += 1;
            Ok(())
        } else {
            Err(self.malformed())
        }
    }

    fn literal(&mut self, word: &str, value: Json) -> Result<Json, CrsParseError> {
        if self.text[self.pos..].starts_with(word) {
            self.pos += word.len();
            Ok(value)
        } else {
            Err(self.malformed())
        }
    }

    fn value(&mut self, depth: usize) -> Result<Json, CrsParseError> {
        if depth >= MAX_DEPTH {
            return Err(self.malformed());
        }
        match self.peek() {
            Some(b'{') => self.object(depth),
            Some(b'[') => self.array(depth),
            Some(b'"') => self.string().map(Json::String),
            Some(b't') => self.literal("true", Json::Bool(true)),
            Some(b'f') => self.literal("false", Json::Bool(false)),
            Some(b'n') => self.literal("null", Json::Null),
            Some(b) if b == b'-' || b.is_ascii_digit() => self.number(),
            _ => Err(self.malformed()),
        }
    }

    fn object(&mut self, depth: usize) -> Result<Json, CrsParseError> {
        self.pos += 1;
        let mut members = Vec::new();
        self.skip_space();
        if self.peek() == Some(b'}') {
            self.pos += 1;
            return Ok(Json::Object(members));
        }
        loop {
            self.skip_space();
            let key = self.string()?;
            self.skip_space();
            self.expect(b':')?;
            self.skip_space();
            members.push((key, self.value(depth + 1)?));
            self.skip_space();
            match self.peek() {
                Some(b',') => self.pos += 1,
                Some(b'}') => {
                    self.pos += 1;
                    return Ok(Json::Object(members));
                }
                _ => return Err(self.malformed()),
            }
        }
    }

    fn array(&mut self, depth: usize) -> Result<Json, CrsParseError> {
        self.pos += 1;
        let mut items = Vec::new();
        self.skip_space();
        if self.peek() == Some(b']') {
            self.pos += 1;
            return Ok(Json::Array(items));
        }
        loop {
            self.skip_space();
            items.push(self.value(depth + 1)?);
            self.skip_space();
            match self.peek() {
                Some(b',') => self.pos += 1,
                Some(b']') => {
                    self.pos += 1;
                    return Ok(Json::Array(items));
                }
                _ => return Err(self.malformed()),
            }
        }
    }

    fn string(&mut self) -> Result<String, CrsParseError> {
        self.expect(b'"')?;
        let mut out = String::new();
        loop {
            let rest = &self.text[self.pos..];
            let Some(stop) = rest.find(['"', '\\']) else {
                return Err(self.malformed());
            };
            let run = &rest[..stop];
            if run.chars().any(|c| c < ' ') {
                return Err(self.malformed());
            }
            out.push_str(run);
            self.pos += stop;
            match self.peek() {
                Some(b'"') => {
                    self.pos += 1;
                    return Ok(out);
                }
                _ => {
                    self.pos += 1;
                    self.escape(&mut out)?;
                }
            }
        }
    }

    fn escape(&mut self, out: &mut String) -> Result<(), CrsParseError> {
        let c = match self.peek() {
            Some(b'"') => '"',
            Some(b'\\') => '\\',
            Some(b'/') => '/',
            Some(b'b') => '\u{8}',
            Some(b'f') => '\u{c}',
            Some(b'n') => '\n',
            Some(b'r') => '\r',
            Some(b't') => '\t',
            Some(b'u') => {
                self.pos += 1;
                let first = self.hex4()?;
                let code = if (0xD800..0xDC00).contains(&first) {
                    self.expect(b'\\')?;
                    self.expect(b'u')?;
                    let second = self.hex4()?;
                    if !(0xDC00..0xE000).contains(&second) {
                        return Err(self.malformed());
                    }
                    0x10000 + ((first - 0xD800) << 10) + (second - 0xDC00)
                } else {
                    first
                };
                let c = char::from_u32(code).ok_or_else(|| self.malformed())?;
                out.push(c);
                return Ok(());
            }
            _ => return Err(self.malformed()),
        };
        self.pos += 1;
        out.push(c);
        Ok(())
    }

    fn hex4(&mut self) -> Result<u32, CrsParseError> {
        let digits = self
            .text
            .get(self.pos..self.pos + 4)
            .ok_or_else(|| self.malformed())?;
        let value = u32::from_str_radix(digits, 16).map_err(|_| self.malformed())?;
        self.pos += 4;
        Ok(value)
    }

    fn number(&mut self) -> Result<Json, CrsParseError> {
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
                    format: PROJJSON_FORMAT,
                    offset: start,
                })?;
        if value.is_finite() {
            Ok(Json::Number(value))
        } else {
            Err(CrsParseError::Malformed {
                format: PROJJSON_FORMAT,
                offset: start,
            })
        }
    }
}

/// Append `text` as a JSON string.
pub(crate) fn push_json_string(out: &mut String, text: &str) {
    out.push('"');
    for c in text.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if c < ' ' => {
                let _ = core::fmt::Write::write_fmt(out, format_args!("\\u{:04x}", u32::from(c)));
            }
            c => out.push(c),
        }
    }
    out.push('"');
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn values_and_escapes() {
        let v = parse_json(r#" {"a": [1, -2.5e1, true, null], "b": "x\"é😀\n"} "#).unwrap();
        assert_eq!(
            v.get("a").and_then(Json::as_array).map(<[Json]>::len),
            Some(4)
        );
        assert_eq!(
            v.get("a").and_then(|a| a.as_array()?.get(1)?.as_f64()),
            Some(-25.0)
        );
        assert_eq!(v.get("b").and_then(Json::as_str), Some("x\"é😀\n"));
        let mut out = String::new();
        push_json_string(&mut out, "x\"é\n\u{1}");
        assert_eq!(
            parse_json(&out).unwrap(),
            Json::String("x\"é\n\u{1}".into())
        );
    }

    #[test]
    fn malformed_input_names_the_offset() {
        for bad in [
            "",
            "{",
            "{\"a\" 1}",
            "[1,]",
            "\"x",
            "tru",
            "1e999",
            "{} x",
            "\"\u{1}\"",
        ] {
            assert!(
                matches!(parse_json(bad), Err(CrsParseError::Malformed { .. })),
                "{bad}"
            );
        }
        let deep = "[".repeat(70) + &"]".repeat(70);
        assert!(parse_json(&deep).is_err());
    }
}
