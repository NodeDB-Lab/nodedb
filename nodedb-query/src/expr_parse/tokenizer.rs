//! UTF-8-safe tokenizer for SQL expression text.
//!
//! Iterates by `char` (not byte) to avoid panicking on multi-byte codepoints.
//! Supports: identifiers, numbers, string literals (with `''` escape), single
//! and two-char operators, parentheses, commas.

/// A single token produced by the tokenizer.
#[derive(Debug, Clone)]
pub struct Token {
    pub text: String,
    pub kind: TokenKind,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum TokenKind {
    Ident,
    Number,
    StringLit,
    LParen,
    RParen,
    Comma,
    Op,
}

/// Tokenize a SQL expression string into a list of [`Token`]s.
///
/// This implementation iterates over `char`s (via `char_indices`) so that
/// multi-byte UTF-8 codepoints are handled correctly. Slicing always uses
/// byte offsets returned by `char_indices`, which are guaranteed to be on
/// char boundaries.
pub fn tokenize(input: &str) -> Result<Vec<Token>, String> {
    let chars: Vec<(usize, char)> = input.char_indices().collect();
    let mut tokens = Vec::new();
    let mut i = 0;

    while i < chars.len() {
        let (_, ch) = chars[i];

        // Skip whitespace.
        if ch.is_ascii_whitespace() {
            i += 1;
            continue;
        }

        // Single-char structural tokens.
        if ch == '(' {
            tokens.push(Token {
                text: "(".into(),
                kind: TokenKind::LParen,
            });
            i += 1;
            continue;
        }
        if ch == ')' {
            tokens.push(Token {
                text: ")".into(),
                kind: TokenKind::RParen,
            });
            i += 1;
            continue;
        }
        if ch == ',' {
            tokens.push(Token {
                text: ",".into(),
                kind: TokenKind::Comma,
            });
            i += 1;
            continue;
        }

        // Two-char operators: <=, >=, !=, <>, ||
        if i + 1 < chars.len() {
            let (_, next_ch) = chars[i + 1];
            let two: String = [ch, next_ch].iter().collect();
            if matches!(two.as_str(), "<=" | ">=" | "!=" | "<>" | "||") {
                tokens.push(Token {
                    text: two,
                    kind: TokenKind::Op,
                });
                i += 2;
                continue;
            }
        }

        // Single-char operators.
        if matches!(ch, '+' | '-' | '*' | '/' | '%' | '=' | '<' | '>') {
            tokens.push(Token {
                text: ch.to_string(),
                kind: TokenKind::Op,
            });
            i += 1;
            continue;
        }

        // String literal (single-quoted).
        if ch == '\'' {
            let mut s = String::new();
            i += 1;
            while i < chars.len() {
                let (_, c) = chars[i];
                if c == '\'' {
                    // Check for '' escape.
                    if i + 1 < chars.len() && chars[i + 1].1 == '\'' {
                        s.push('\'');
                        i += 2;
                        continue;
                    }
                    i += 1;
                    break;
                }
                s.push(c);
                i += 1;
            }
            tokens.push(Token {
                text: s,
                kind: TokenKind::StringLit,
            });
            continue;
        }

        // Number.
        if ch.is_ascii_digit()
            || (ch == '.' && i + 1 < chars.len() && chars[i + 1].1.is_ascii_digit())
        {
            let start_byte = chars[i].0;
            let start_i = i;
            while i < chars.len() && (chars[i].1.is_ascii_digit() || chars[i].1 == '.') {
                i += 1;
            }
            let end_byte = if i < chars.len() {
                chars[i].0
            } else {
                input.len()
            };
            tokens.push(Token {
                text: input[start_byte..end_byte].to_string(),
                kind: TokenKind::Number,
            });
            let _ = start_i; // suppress unused warning
            continue;
        }

        // Identifier or keyword (ASCII letters, digits, underscore).
        if ch.is_ascii_alphabetic() || ch == '_' {
            let start_byte = chars[i].0;
            while i < chars.len() && (chars[i].1.is_ascii_alphanumeric() || chars[i].1 == '_') {
                i += 1;
            }
            let end_byte = if i < chars.len() {
                chars[i].0
            } else {
                input.len()
            };
            tokens.push(Token {
                text: input[start_byte..end_byte].to_string(),
                kind: TokenKind::Ident,
            });
            continue;
        }

        // Non-ASCII characters in an unquoted context: skip gracefully.
        // This can happen with stray Unicode in expressions; previously this
        // caused a panic. We emit an error with the character for diagnostics.
        return Err(format!(
            "unexpected character: '{ch}' (U+{:04X})",
            ch as u32
        ));
    }

    Ok(tokens)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ascii_expression() {
        let tokens = tokenize("price * (1 + tax_rate)").unwrap();
        assert_eq!(tokens.len(), 7);
    }

    #[test]
    fn cjk_string_literal() {
        let tokens = tokenize("'你好' || name").unwrap();
        assert_eq!(tokens.len(), 3);
        assert_eq!(tokens[0].kind, TokenKind::StringLit);
        assert_eq!(tokens[0].text, "你好");
    }

    #[test]
    fn emoji_string_literal() {
        let tokens = tokenize("'🎉' || tag").unwrap();
        assert_eq!(tokens.len(), 3);
        assert_eq!(tokens[0].text, "🎉");
    }

    #[test]
    fn two_char_op_after_multibyte_string() {
        // Previously panicked: &input[i..i+2] crossed a char boundary.
        let tokens = tokenize("'你' || x").unwrap();
        assert_eq!(tokens.len(), 3);
        assert_eq!(tokens[1].text, "||");
    }

    #[test]
    fn escaped_quote_in_string() {
        let tokens = tokenize("'it''s'").unwrap();
        assert_eq!(tokens.len(), 1);
        assert_eq!(tokens[0].text, "it's");
    }

    #[test]
    fn latin_diacritics_in_string() {
        let tokens = tokenize("'café'").unwrap();
        assert_eq!(tokens[0].text, "café");
    }

    #[test]
    fn comparison_after_cjk() {
        let tokens = tokenize("name != '禁止'").unwrap();
        assert_eq!(tokens.len(), 3);
        assert_eq!(tokens[1].text, "!=");
        assert_eq!(tokens[2].text, "禁止");
    }
}
