//! Tiny hand-written tokenizer for array DDL/DML.
//!
//! Scope is limited to the four `ARRAY` statements — identifiers,
//! integer/float literals, single-quoted strings, parentheses, brackets,
//! commas, dots, double-dots, and a handful of keywords. Keywords are
//! left as `Ident` tokens; the parser handles them case-insensitively.

use crate::error::SqlError;

#[derive(Debug, Clone, PartialEq)]
pub enum Tok {
    Ident(String),
    Int(i64),
    Float(f64),
    Str(String),
    LParen,
    RParen,
    LBracket,
    RBracket,
    Comma,
    DotDot,
    /// `=` — used by `WITH (prefix_bits = N)`.
    Eq,
    /// `NULL` literal — kept distinct from `Ident` so insert parsing is
    /// unambiguous about a bare `NULL` value vs an identifier value.
    Null,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Token {
    pub tok: Tok,
    /// Byte offset into the source string — used for error messages.
    pub pos: usize,
}

/// Tokenize a SQL slice. Returns `Vec<Token>` on success; any unexpected
/// character produces `SqlError::Parse`.
pub fn tokenize(src: &str) -> Result<Vec<Token>, SqlError> {
    let bytes = src.as_bytes();
    let mut out = Vec::with_capacity(src.len() / 4);
    let mut i = 0;
    while i < bytes.len() {
        let b = bytes[i];
        // Whitespace.
        if b.is_ascii_whitespace() {
            i += 1;
            continue;
        }
        // Single-line comment `-- ...`.
        if b == b'-' && i + 1 < bytes.len() && bytes[i + 1] == b'-' {
            while i < bytes.len() && bytes[i] != b'\n' {
                i += 1;
            }
            continue;
        }
        // Punctuation.
        match b {
            b'(' => {
                out.push(Token {
                    tok: Tok::LParen,
                    pos: i,
                });
                i += 1;
                continue;
            }
            b')' => {
                out.push(Token {
                    tok: Tok::RParen,
                    pos: i,
                });
                i += 1;
                continue;
            }
            b'[' => {
                out.push(Token {
                    tok: Tok::LBracket,
                    pos: i,
                });
                i += 1;
                continue;
            }
            b']' => {
                out.push(Token {
                    tok: Tok::RBracket,
                    pos: i,
                });
                i += 1;
                continue;
            }
            b',' => {
                out.push(Token {
                    tok: Tok::Comma,
                    pos: i,
                });
                i += 1;
                continue;
            }
            b'.' if i + 1 < bytes.len() && bytes[i + 1] == b'.' => {
                out.push(Token {
                    tok: Tok::DotDot,
                    pos: i,
                });
                i += 2;
                continue;
            }
            b'=' => {
                out.push(Token {
                    tok: Tok::Eq,
                    pos: i,
                });
                i += 1;
                continue;
            }
            _ => {}
        }
        // Single-quoted string.
        if b == b'\'' {
            let start = i + 1;
            let mut j = start;
            let mut s = String::new();
            while j < bytes.len() {
                if bytes[j] == b'\'' {
                    // Doubled `''` → literal single quote.
                    if j + 1 < bytes.len() && bytes[j + 1] == b'\'' {
                        s.push('\'');
                        j += 2;
                        continue;
                    }
                    break;
                }
                s.push(bytes[j] as char);
                j += 1;
            }
            if j >= bytes.len() {
                return Err(SqlError::Parse {
                    detail: format!("unterminated string literal at offset {i}"),
                });
            }
            out.push(Token {
                tok: Tok::Str(s),
                pos: i,
            });
            i = j + 1;
            continue;
        }
        // Number (int or float, with optional leading `-`).
        if b.is_ascii_digit() || (b == b'-' && i + 1 < bytes.len() && bytes[i + 1].is_ascii_digit())
        {
            let start = i;
            let mut j = i;
            if bytes[j] == b'-' {
                j += 1;
            }
            while j < bytes.len() && bytes[j].is_ascii_digit() {
                j += 1;
            }
            // Float? Require a digit after the dot — `..` is a range token.
            let is_float = j + 1 < bytes.len()
                && bytes[j] == b'.'
                && bytes[j + 1] != b'.'
                && bytes[j + 1].is_ascii_digit();
            if is_float {
                j += 1;
                while j < bytes.len() && bytes[j].is_ascii_digit() {
                    j += 1;
                }
                let txt = &src[start..j];
                let f: f64 = txt.parse().map_err(|_| SqlError::Parse {
                    detail: format!("invalid float literal '{txt}'"),
                })?;
                out.push(Token {
                    tok: Tok::Float(f),
                    pos: start,
                });
            } else {
                let txt = &src[start..j];
                let n: i64 = txt.parse().map_err(|_| SqlError::Parse {
                    detail: format!("invalid integer literal '{txt}'"),
                })?;
                out.push(Token {
                    tok: Tok::Int(n),
                    pos: start,
                });
            }
            i = j;
            continue;
        }
        // Identifier — letters / digits / underscore. Identifiers may
        // start with `_` or letter.
        if b == b'_' || b.is_ascii_alphabetic() {
            let start = i;
            let mut j = i;
            while j < bytes.len() && (bytes[j] == b'_' || bytes[j].is_ascii_alphanumeric()) {
                j += 1;
            }
            let txt = &src[start..j];
            if txt.eq_ignore_ascii_case("NULL") {
                out.push(Token {
                    tok: Tok::Null,
                    pos: start,
                });
            } else {
                out.push(Token {
                    tok: Tok::Ident(txt.to_string()),
                    pos: start,
                });
            }
            i = j;
            continue;
        }
        return Err(SqlError::Parse {
            detail: format!("unexpected character '{}' at offset {i}", b as char),
        });
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tokenize_simple() {
        let toks = tokenize("CREATE ARRAY a (1, 2.5, 'x')").unwrap();
        assert!(matches!(toks[0].tok, Tok::Ident(ref s) if s == "CREATE"));
        assert!(matches!(toks[1].tok, Tok::Ident(ref s) if s == "ARRAY"));
        assert!(matches!(toks[3].tok, Tok::LParen));
        assert!(matches!(toks[4].tok, Tok::Int(1)));
        assert!(matches!(toks[6].tok, Tok::Float(f) if (f - 2.5).abs() < 1e-9));
        assert!(matches!(toks[8].tok, Tok::Str(ref s) if s == "x"));
    }

    #[test]
    fn tokenize_dotdot_range() {
        let toks = tokenize("[0..23]").unwrap();
        assert!(matches!(toks[1].tok, Tok::Int(0)));
        assert!(matches!(toks[2].tok, Tok::DotDot));
        assert!(matches!(toks[3].tok, Tok::Int(23)));
    }

    #[test]
    fn tokenize_negative() {
        let toks = tokenize("-7").unwrap();
        assert!(matches!(toks[0].tok, Tok::Int(-7)));
    }
}
