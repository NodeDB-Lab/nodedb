// SPDX-License-Identifier: BUSL-1.1

//! `${VAR}` expansion for a TOML config file, applied before parsing.
//!
//! An operator writes `${NAME}` anywhere in the file. The process substitutes
//! the variable's value as raw text before `toml::from_str` runs. `$${NAME}`
//! escapes to the literal `${NAME}` with no lookup. A placeholder inside a
//! TOML comment stays untouched. A shipped template can therefore carry a
//! commented-out example line without requiring the variable it names.

use std::path::Path;

/// Which lexical region the scanner is inside. Only `Normal` and the string
/// states expand placeholders. `Comment` copies text through unchanged.
#[derive(Clone, Copy, PartialEq, Eq)]
enum State {
    Normal,
    BasicString,
    MultilineBasicString,
    LiteralString,
    MultilineLiteralString,
    Comment,
}

/// Expands every `${NAME}` placeholder in `raw` against the process
/// environment, skipping TOML comments and treating `$${NAME}` as a
/// literal escape.
///
/// `path` names the config file in violation messages. Every violation
/// found is collected and returned together as one [`crate::Error::Config`];
/// `Ok` carries the fully expanded text ready for `toml::from_str`.
pub(crate) fn expand_env(path: &Path, raw: &str) -> crate::Result<String> {
    let mut out = String::with_capacity(raw.len());
    let mut violations = Vec::new();
    let mut substituted = Vec::new();
    let mut state = State::Normal;

    // Scans `raw` by byte offset, never by collecting it into a `Vec<char>`.
    // `i` always sits on a char boundary because every step advances by
    // exactly one char's `len_utf8()` or by a fixed ASCII literal length.
    let mut i = 0;
    while i < raw.len() {
        let Some(c) = raw[i..].chars().next() else {
            break;
        };

        // Every expandable state (all but `Comment`) hands a `$` to the same
        // handler. Checked once here instead of once per state arm below.
        if c == '$' && state != State::Comment {
            i = handle_dollar(raw, i, path, &mut out, &mut violations, &mut substituted);
            continue;
        }

        match state {
            State::Normal => {
                if raw[i..].starts_with("\"\"\"") {
                    out.push_str("\"\"\"");
                    state = State::MultilineBasicString;
                    i += 3;
                    continue;
                }
                if raw[i..].starts_with("'''") {
                    out.push_str("'''");
                    state = State::MultilineLiteralString;
                    i += 3;
                    continue;
                }
                if c == '"' {
                    out.push(c);
                    state = State::BasicString;
                    i += c.len_utf8();
                    continue;
                }
                if c == '\'' {
                    out.push(c);
                    state = State::LiteralString;
                    i += c.len_utf8();
                    continue;
                }
                if c == '#' {
                    out.push(c);
                    state = State::Comment;
                    i += c.len_utf8();
                    continue;
                }
                out.push(c);
                i += c.len_utf8();
            }
            State::Comment => {
                out.push(c);
                i += c.len_utf8();
                if c == '\n' {
                    state = State::Normal;
                }
            }
            State::BasicString => {
                if c == '\\'
                    && let Some(next) = raw[i + c.len_utf8()..].chars().next()
                {
                    out.push(c);
                    out.push(next);
                    i += c.len_utf8() + next.len_utf8();
                    continue;
                }
                if c == '"' {
                    out.push(c);
                    state = State::Normal;
                    i += c.len_utf8();
                    continue;
                }
                out.push(c);
                i += c.len_utf8();
            }
            State::MultilineBasicString => {
                if c == '\\'
                    && let Some(next) = raw[i + c.len_utf8()..].chars().next()
                {
                    out.push(c);
                    out.push(next);
                    i += c.len_utf8() + next.len_utf8();
                    continue;
                }
                if raw[i..].starts_with("\"\"\"") {
                    out.push_str("\"\"\"");
                    state = State::Normal;
                    i += 3;
                    continue;
                }
                out.push(c);
                i += c.len_utf8();
            }
            State::LiteralString => {
                if c == '\'' {
                    out.push(c);
                    state = State::Normal;
                    i += c.len_utf8();
                    continue;
                }
                out.push(c);
                i += c.len_utf8();
            }
            State::MultilineLiteralString => {
                if raw[i..].starts_with("'''") {
                    out.push_str("'''");
                    state = State::Normal;
                    i += 3;
                    continue;
                }
                out.push(c);
                i += c.len_utf8();
            }
        }
    }

    if !violations.is_empty() {
        return Err(crate::Error::Config {
            detail: violations.join("; "),
        });
    }

    substituted.sort_unstable();
    substituted.dedup();
    if !substituted.is_empty() {
        tracing::info!(
            config_file = %path.display(),
            vars = ?substituted,
            "expanded ${{VAR}} placeholders in config file"
        );
    }

    Ok(out)
}

/// Handles a `$` found at byte offset `i` in an expandable state. `$${NAME}`
/// escapes to a literal `${NAME}`, and `${NAME}` expands. Anything else is a
/// violation, or ordinary text passed through. Returns the next byte offset.
fn handle_dollar(
    raw: &str,
    i: usize,
    path: &Path,
    out: &mut String,
    violations: &mut Vec<String>,
    substituted: &mut Vec<String>,
) -> usize {
    if raw[i..].starts_with("$${") {
        if let Some((name, end)) = read_placeholder_name(raw, i + 3) {
            out.push('$');
            out.push('{');
            out.push_str(name);
            out.push('}');
            return end;
        }
        violations.push(format!(
            "unterminated ${{...}} placeholder in {}",
            path.display()
        ));
        return raw.len();
    }

    if raw[i..].starts_with("${") {
        match read_placeholder_name(raw, i + 2) {
            Some((name, end)) => {
                if !is_valid_name(name) {
                    violations.push(format!(
                        "invalid placeholder name '${{{name}}}' in {}: expected [A-Za-z_][A-Za-z0-9_]*",
                        path.display()
                    ));
                    return end;
                }
                match std::env::var(name) {
                    Ok(value) => {
                        out.push_str(&value);
                        substituted.push(name.to_string());
                    }
                    Err(_) => {
                        violations.push(format!(
                            "unset environment variable '{name}' referenced as ${{{name}}} in {}",
                            path.display()
                        ));
                    }
                }
                end
            }
            None => {
                violations.push(format!(
                    "unterminated ${{...}} placeholder in {}",
                    path.display()
                ));
                raw.len()
            }
        }
    } else {
        out.push('$');
        i + 1
    }
}

/// Reads the `NAME` (and trailing `}`) of a `${NAME}` starting right after
/// the opening `{`. `start` is the byte offset of the first character of the
/// name. Returns the unvalidated name as a borrowed slice of `raw`, and the
/// byte offset past the closing `}`. Returns `None` when the file ends
/// before a `}`.
fn read_placeholder_name(raw: &str, start: usize) -> Option<(&str, usize)> {
    let rest = raw.get(start..)?;
    let end_in_rest = rest.find('}')?;
    Some((&rest[..end_in_rest], start + end_in_rest + 1))
}

/// `[A-Za-z_][A-Za-z0-9_]*`, matched over the whole name.
fn is_valid_name(name: &str) -> bool {
    let mut chars = name.chars();
    match chars.next() {
        Some(c) if c.is_ascii_alphabetic() || c == '_' => {}
        _ => return false,
    }
    chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::server::test_support::with_var;
    use std::path::PathBuf;

    fn p() -> PathBuf {
        PathBuf::from("/etc/nodedb/config.toml")
    }

    #[test]
    fn plain_var_expands_to_value() {
        with_var("ENV_EXPAND_TEST_PLAIN", "hello", || {
            let out = expand_env(&p(), "x = \"${ENV_EXPAND_TEST_PLAIN}\"").unwrap();
            assert_eq!(out, "x = \"hello\"");
        });
    }

    #[test]
    fn unset_var_is_a_violation() {
        unsafe { std::env::remove_var("ENV_EXPAND_TEST_UNSET") };
        let err = expand_env(&p(), "x = ${ENV_EXPAND_TEST_UNSET}").unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("ENV_EXPAND_TEST_UNSET"), "{msg}");
        assert!(msg.contains("/etc/nodedb/config.toml"), "{msg}");
    }

    #[test]
    fn malformed_name_is_a_violation() {
        for raw in ["x = ${1BAD}", "x = ${}", "x = ${FOO-BAR}"] {
            let err = expand_env(&p(), raw).unwrap_err();
            assert!(err.to_string().contains("invalid"), "{raw}: {err}");
        }
    }

    #[test]
    fn unterminated_placeholder_is_a_violation() {
        let err = expand_env(&p(), "x = ${FOO").unwrap_err();
        assert!(err.to_string().contains("unterminated"), "{err}");
    }

    #[test]
    fn escaped_dollar_yields_literal() {
        unsafe { std::env::remove_var("FOO") };
        let out = expand_env(&p(), "x = \"$${FOO}\"").unwrap();
        assert_eq!(out, "x = \"${FOO}\"");
    }

    #[test]
    fn multiple_violations_collected_together() {
        let err = expand_env(&p(), "a = ${1BAD}\nb = ${2BAD}").unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("1BAD"), "{msg}");
        assert!(msg.contains("2BAD"), "{msg}");
    }

    #[test]
    fn placeholder_in_a_comment_is_left_alone() {
        unsafe { std::env::remove_var("ENV_EXPAND_TEST_COMMENT_VAR") };
        let raw = "# data_dir = \"${ENV_EXPAND_TEST_COMMENT_VAR}\"\n";
        let out = expand_env(&p(), raw).unwrap();
        assert_eq!(out, raw);
    }

    #[test]
    fn unset_placeholder_in_a_comment_does_not_fail() {
        unsafe { std::env::remove_var("ENV_EXPAND_TEST_COMMENT_UNSET") };
        let raw = "# example: ${ENV_EXPAND_TEST_COMMENT_UNSET}\nreal = 1\n";
        assert!(expand_env(&p(), raw).is_ok());
    }

    #[test]
    fn hash_inside_a_string_is_not_a_comment() {
        with_var("ENV_EXPAND_TEST_HASH", "value", || {
            let raw = "x = \"#${ENV_EXPAND_TEST_HASH}\"";
            let out = expand_env(&p(), raw).unwrap();
            assert_eq!(out, "x = \"#value\"");
        });
    }

    #[test]
    fn placeholder_in_a_multiline_string_expands() {
        with_var("ENV_EXPAND_TEST_MULTILINE", "mval", || {
            let raw = "x = \"\"\"line ${ENV_EXPAND_TEST_MULTILINE} end\"\"\"";
            let out = expand_env(&p(), raw).unwrap();
            assert_eq!(out, "x = \"\"\"line mval end\"\"\"");
        });
    }

    #[test]
    fn substituted_value_containing_a_placeholder_is_not_re_expanded() {
        with_var("ENV_EXPAND_TEST_OUTER", "${ENV_EXPAND_TEST_INNER}", || {
            unsafe { std::env::remove_var("ENV_EXPAND_TEST_INNER") };
            let out = expand_env(&p(), "x = \"${ENV_EXPAND_TEST_OUTER}\"").unwrap();
            assert_eq!(out, "x = \"${ENV_EXPAND_TEST_INNER}\"");
        });
    }

    #[test]
    fn unicode_text_around_a_placeholder_is_preserved() {
        with_var("ENV_EXPAND_TEST_UNICODE", "val", || {
            let raw = "name = \"caf\u{e9} \u{1f600} ${ENV_EXPAND_TEST_UNICODE} \u{4e2d}\u{6587}\"";
            let out = expand_env(&p(), raw).unwrap();
            assert_eq!(out, "name = \"caf\u{e9} \u{1f600} val \u{4e2d}\u{6587}\"");
        });
    }

    #[test]
    fn text_without_placeholders_is_unchanged() {
        let raw = "[server]\nhost = \"0.0.0.0\"\nport = 6432\n";
        let out = expand_env(&p(), raw).unwrap();
        assert_eq!(out, raw);
    }
}
