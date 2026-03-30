//! Procedural SQL tokenizer.
//!
//! Splits procedural SQL text into a stream of tokens. SQL expressions
//! between procedural keywords are captured as opaque `SqlFragment` tokens
//! — DataFusion parses them later during compilation.

/// A token in the procedural SQL stream.
#[derive(Debug, Clone, PartialEq)]
pub enum Token {
    // ── Block delimiters ──
    Begin,
    End,

    // ── Control flow ──
    If,
    Then,
    Elsif,
    Else,
    EndIf, // "END IF"
    Loop,
    EndLoop, // "END LOOP"
    While,
    For,
    In,
    Reverse,
    DotDot, // ".."
    Break,
    Continue,

    // ── Declarations ──
    Declare,
    /// `:=` assignment operator.
    Assign,

    // ── Return ──
    Return,
    ReturnQuery, // "RETURN QUERY"

    // ── Error handling ──
    Raise,
    Notice,
    Warning,
    Exception,

    // ── DML (detected for rejection in function bodies) ──
    Insert,
    Update,
    Delete,
    Commit,
    Rollback,

    // ── Structure ──
    Semicolon,

    /// An identifier (variable name, type name, etc.).
    Ident(String),

    /// A SQL expression fragment — everything between procedural keywords.
    /// Contains raw SQL text that DataFusion will parse.
    SqlFragment(String),

    /// A string literal ('...').
    StringLit(String),

    /// A numeric literal.
    NumberLit(String),
}

/// Tokenize procedural SQL text into a token stream.
///
/// The tokenizer is keyword-aware: it recognizes procedural keywords and
/// captures everything else as `SqlFragment` tokens. String literals are
/// preserved (not split on keywords inside strings).
pub fn tokenize(input: &str) -> Result<Vec<Token>, String> {
    let mut tokens = Vec::new();
    let bytes = input.as_bytes();
    let len = bytes.len();
    let mut i = 0;

    while i < len {
        // Skip whitespace.
        if bytes[i].is_ascii_whitespace() {
            i += 1;
            continue;
        }

        // Skip SQL comments.
        if i + 1 < len && bytes[i] == b'-' && bytes[i + 1] == b'-' {
            while i < len && bytes[i] != b'\n' {
                i += 1;
            }
            continue;
        }

        // Semicolon.
        if bytes[i] == b';' {
            tokens.push(Token::Semicolon);
            i += 1;
            continue;
        }

        // `:=` assignment.
        if i + 1 < len && bytes[i] == b':' && bytes[i + 1] == b'=' {
            tokens.push(Token::Assign);
            i += 2;
            continue;
        }

        // `..` range operator.
        if i + 1 < len && bytes[i] == b'.' && bytes[i + 1] == b'.' {
            tokens.push(Token::DotDot);
            i += 2;
            continue;
        }

        // String literal.
        if bytes[i] == b'\'' {
            let (lit, end) = read_string_literal(input, i)?;
            tokens.push(Token::StringLit(lit));
            i = end;
            continue;
        }

        // Number literal.
        if bytes[i].is_ascii_digit() {
            let start = i;
            while i < len {
                if bytes[i].is_ascii_digit() {
                    i += 1;
                } else if bytes[i] == b'.' {
                    // Check for `..` (range operator) — don't consume the dot.
                    if i + 1 < len && bytes[i + 1] == b'.' {
                        break;
                    }
                    i += 1;
                } else {
                    break;
                }
            }
            tokens.push(Token::NumberLit(input[start..i].to_string()));
            continue;
        }

        // Identifier or keyword.
        if bytes[i].is_ascii_alphabetic() || bytes[i] == b'_' {
            let start = i;
            while i < len && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_') {
                i += 1;
            }
            let word = &input[start..i];
            let upper = word.to_uppercase();

            // Check for two-word keywords by peeking ahead.
            if let Some(two_word) = peek_two_word_keyword(&upper, input, i) {
                match two_word.0 {
                    "END IF" => {
                        tokens.push(Token::EndIf);
                        i = two_word.1;
                        continue;
                    }
                    "END LOOP" => {
                        tokens.push(Token::EndLoop);
                        i = two_word.1;
                        continue;
                    }
                    "RETURN QUERY" => {
                        tokens.push(Token::ReturnQuery);
                        i = two_word.1;
                        continue;
                    }
                    _ => {}
                }
            }

            match upper.as_str() {
                "BEGIN" => tokens.push(Token::Begin),
                "END" => tokens.push(Token::End),
                "IF" => tokens.push(Token::If),
                "THEN" => tokens.push(Token::Then),
                "ELSIF" | "ELSEIF" => tokens.push(Token::Elsif),
                "ELSE" => tokens.push(Token::Else),
                "LOOP" => tokens.push(Token::Loop),
                "WHILE" => tokens.push(Token::While),
                "FOR" => tokens.push(Token::For),
                "IN" => tokens.push(Token::In),
                "REVERSE" => tokens.push(Token::Reverse),
                "BREAK" | "EXIT" => tokens.push(Token::Break),
                "CONTINUE" => tokens.push(Token::Continue),
                "DECLARE" => tokens.push(Token::Declare),
                "RETURN" => tokens.push(Token::Return),
                "RAISE" => tokens.push(Token::Raise),
                "NOTICE" => tokens.push(Token::Notice),
                "WARNING" => tokens.push(Token::Warning),
                "EXCEPTION" => tokens.push(Token::Exception),
                "INSERT" => tokens.push(Token::Insert),
                "UPDATE" => tokens.push(Token::Update),
                "DELETE" => tokens.push(Token::Delete),
                "COMMIT" => tokens.push(Token::Commit),
                "ROLLBACK" => tokens.push(Token::Rollback),
                _ => tokens.push(Token::Ident(word.to_string())),
            }
            continue;
        }

        // Any other character — collect as part of a SQL fragment.
        // This handles operators, parens, etc.
        tokens.push(Token::Ident(input[i..i + 1].to_string()));
        i += 1;
    }

    Ok(tokens)
}

/// Read a single-quoted string literal, handling escaped quotes ('').
fn read_string_literal(input: &str, start: usize) -> Result<(String, usize), String> {
    let bytes = input.as_bytes();
    let mut i = start + 1; // skip opening quote
    let mut result = String::new();

    while i < bytes.len() {
        if bytes[i] == b'\'' {
            // Check for escaped quote ('').
            if i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                result.push('\'');
                i += 2;
            } else {
                // End of string literal.
                return Ok((result, i + 1));
            }
        } else {
            result.push(bytes[i] as char);
            i += 1;
        }
    }
    Err("unterminated string literal".to_string())
}

/// Check if the current word + next word form a two-word keyword.
/// Returns (keyword, position_after_second_word) if matched.
fn peek_two_word_keyword(
    first_upper: &str,
    input: &str,
    after_first: usize,
) -> Option<(&'static str, usize)> {
    let bytes = input.as_bytes();
    let mut j = after_first;

    // Skip whitespace between words.
    while j < bytes.len() && bytes[j].is_ascii_whitespace() {
        j += 1;
    }

    // Read next word.
    if j >= bytes.len() || !(bytes[j].is_ascii_alphabetic() || bytes[j] == b'_') {
        return None;
    }
    let start = j;
    while j < bytes.len() && (bytes[j].is_ascii_alphanumeric() || bytes[j] == b'_') {
        j += 1;
    }
    let second_upper = input[start..j].to_uppercase();

    match (first_upper, second_upper.as_str()) {
        ("END", "IF") => Some(("END IF", j)),
        ("END", "LOOP") => Some(("END LOOP", j)),
        ("RETURN", "QUERY") => Some(("RETURN QUERY", j)),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tokenize_simple_if() {
        let tokens = tokenize("IF x > 0 THEN RETURN 1; END IF;").unwrap();
        assert_eq!(tokens[0], Token::If);
        assert_eq!(tokens[1], Token::Ident("x".into()));
        // '>' is captured as Ident(">")
        assert!(tokens.contains(&Token::Then));
        assert!(tokens.contains(&Token::Return));
        assert!(tokens.contains(&Token::EndIf));
    }

    #[test]
    fn tokenize_begin_end() {
        let tokens = tokenize("BEGIN RETURN 42; END").unwrap();
        assert_eq!(tokens[0], Token::Begin);
        assert_eq!(tokens[1], Token::Return);
        assert_eq!(tokens[2], Token::NumberLit("42".into()));
        assert_eq!(tokens[3], Token::Semicolon);
        assert_eq!(tokens[4], Token::End);
    }

    #[test]
    fn tokenize_declare() {
        let tokens = tokenize("DECLARE x INT := 0;").unwrap();
        assert_eq!(tokens[0], Token::Declare);
        assert_eq!(tokens[1], Token::Ident("x".into()));
        assert_eq!(tokens[2], Token::Ident("INT".into()));
        assert_eq!(tokens[3], Token::Assign);
        assert_eq!(tokens[4], Token::NumberLit("0".into()));
        assert_eq!(tokens[5], Token::Semicolon);
    }

    #[test]
    fn tokenize_string_literal() {
        let tokens = tokenize("RETURN 'hello world';").unwrap();
        assert_eq!(tokens[0], Token::Return);
        assert_eq!(tokens[1], Token::StringLit("hello world".into()));
        assert_eq!(tokens[2], Token::Semicolon);
    }

    #[test]
    fn tokenize_escaped_string() {
        let tokens = tokenize("RETURN 'it''s';").unwrap();
        assert_eq!(tokens[1], Token::StringLit("it's".into()));
    }

    #[test]
    fn tokenize_while_loop() {
        let tokens = tokenize("WHILE i < 10 LOOP i := i + 1; END LOOP;").unwrap();
        assert_eq!(tokens[0], Token::While);
        assert!(tokens.contains(&Token::Loop));
        assert!(tokens.contains(&Token::Assign));
        assert!(tokens.contains(&Token::EndLoop));
    }

    #[test]
    fn tokenize_for_loop() {
        let tokens = tokenize("FOR i IN 1..10 LOOP BREAK; END LOOP;").unwrap();
        assert_eq!(tokens[0], Token::For);
        assert!(tokens.contains(&Token::In));
        assert!(tokens.contains(&Token::DotDot));
        assert!(tokens.contains(&Token::Break));
        assert!(tokens.contains(&Token::EndLoop));
    }

    #[test]
    fn tokenize_dml_detected() {
        let tokens = tokenize("INSERT INTO users VALUES (1);").unwrap();
        assert_eq!(tokens[0], Token::Insert);
    }

    #[test]
    fn tokenize_comment_skipped() {
        let tokens = tokenize("RETURN 1; -- this is a comment\nRETURN 2;").unwrap();
        assert_eq!(
            tokens.iter().filter(|t| matches!(t, Token::Return)).count(),
            2
        );
    }
}
