// SPDX-License-Identifier: BUSL-1.1

//! Closed-set parser for the trailing `KEYWORD value` option list of the
//! index-DDL surfaces.
//!
//! Every token must be consumed by a declared option. An unknown keyword, a
//! repeated keyword, a missing value, a value of the wrong type, and a
//! leftover token are all errors — the alternative is a statement that
//! reports success while the option it carried was never applied.

use std::collections::HashMap;

use super::super::super::super::result::DdlError;
use super::super::support::ddl_err;
use super::lex::Tok;

/// The value shape one option accepts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OptionKind {
    /// A non-negative integer.
    UInt,
    /// A bare word, matched case-insensitively against a closed set by the
    /// caller. Quoted forms are accepted so `INDEX_TYPE 'hnsw'` works too.
    Ident,
    /// A quoted string literal.
    QuotedStr,
    /// `TRUE` / `FALSE`.
    Bool,
}

/// One declared option keyword.
pub struct OptionSpec {
    pub name: &'static str,
    pub kind: OptionKind,
}

impl OptionSpec {
    pub const fn uint(name: &'static str) -> Self {
        Self {
            name,
            kind: OptionKind::UInt,
        }
    }

    pub const fn ident(name: &'static str) -> Self {
        Self {
            name,
            kind: OptionKind::Ident,
        }
    }

    pub const fn quoted(name: &'static str) -> Self {
        Self {
            name,
            kind: OptionKind::QuotedStr,
        }
    }

    pub const fn boolean(name: &'static str) -> Self {
        Self {
            name,
            kind: OptionKind::Bool,
        }
    }
}

/// A parsed option value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OptionValue {
    UInt(usize),
    Text(String),
    Bool(bool),
}

/// The options a statement actually specified, keyed by declared name.
#[derive(Debug, Default)]
pub struct ParsedOptions {
    values: HashMap<&'static str, OptionValue>,
}

impl ParsedOptions {
    /// The integer given for `name`, or `None` if the option was omitted.
    pub fn uint(&self, name: &str) -> Option<usize> {
        match self.values.get(name) {
            Some(OptionValue::UInt(v)) => Some(*v),
            _ => None,
        }
    }

    /// The text given for `name`, or `None` if the option was omitted.
    pub fn text(&self, name: &str) -> Option<&str> {
        match self.values.get(name) {
            Some(OptionValue::Text(v)) => Some(v.as_str()),
            _ => None,
        }
    }

    /// The boolean given for `name`, or `None` if the option was omitted.
    pub fn boolean(&self, name: &str) -> Option<bool> {
        match self.values.get(name) {
            Some(OptionValue::Bool(v)) => Some(*v),
            _ => None,
        }
    }

    /// Whether `name` was specified at all.
    pub fn has(&self, name: &str) -> bool {
        self.values.contains_key(name)
    }
}

/// Parse `toks` as a `KEYWORD value` sequence against `specs`.
///
/// `context` names the statement in error messages, e.g. `CREATE VECTOR INDEX`.
pub fn parse_options(
    toks: &[Tok],
    specs: &[OptionSpec],
    context: &str,
) -> Result<ParsedOptions, DdlError> {
    let mut parsed = ParsedOptions::default();
    let mut i = 0usize;

    while i < toks.len() {
        let key_tok = &toks[i];
        let key = key_tok.word().ok_or_else(|| {
            ddl_err(
                "42601",
                format!(
                    "{context}: unexpected '{}'; {}",
                    key_tok.describe(),
                    supported(specs)
                ),
            )
        })?;

        let spec = specs
            .iter()
            .find(|s| s.name.eq_ignore_ascii_case(key))
            .ok_or_else(|| {
                ddl_err(
                    "42601",
                    format!(
                        "{context}: unrecognized option '{key}'; {}",
                        supported(specs)
                    ),
                )
            })?;

        if parsed.values.contains_key(spec.name) {
            return Err(ddl_err(
                "42601",
                format!("{context}: option '{}' specified more than once", spec.name),
            ));
        }

        let value_tok = toks.get(i + 1).ok_or_else(|| {
            ddl_err(
                "42601",
                format!("{context}: option '{}' requires a value", spec.name),
            )
        })?;

        let value = read_value(value_tok, spec, context)?;
        parsed.values.insert(spec.name, value);
        i += 2;
    }

    Ok(parsed)
}

fn read_value(tok: &Tok, spec: &OptionSpec, context: &str) -> Result<OptionValue, DdlError> {
    let invalid = |expected: &str| {
        ddl_err(
            "22023",
            format!(
                "{context}: invalid value for '{}': expected {expected}, found '{}'",
                spec.name,
                tok.describe()
            ),
        )
    };

    match spec.kind {
        OptionKind::UInt => {
            let text = tok
                .word()
                .ok_or_else(|| invalid("a non-negative integer"))?;
            let value = text
                .parse::<usize>()
                .map_err(|_| invalid("a non-negative integer"))?;
            Ok(OptionValue::UInt(value))
        }
        OptionKind::Ident => match tok {
            Tok::Word(w) => Ok(OptionValue::Text(w.clone())),
            Tok::Quoted(s) | Tok::QuotedIdent(s) => Ok(OptionValue::Text(s.clone())),
            _ => Err(invalid("a name")),
        },
        OptionKind::QuotedStr => match tok {
            Tok::Quoted(s) | Tok::QuotedIdent(s) if !s.trim().is_empty() => {
                Ok(OptionValue::Text(s.trim().to_string()))
            }
            Tok::Quoted(_) | Tok::QuotedIdent(_) => Err(invalid("a non-empty quoted string")),
            _ => Err(invalid("a quoted string")),
        },
        OptionKind::Bool => {
            let text = tok.word().ok_or_else(|| invalid("TRUE or FALSE"))?;
            if text.eq_ignore_ascii_case("true") {
                Ok(OptionValue::Bool(true))
            } else if text.eq_ignore_ascii_case("false") {
                Ok(OptionValue::Bool(false))
            } else {
                Err(invalid("TRUE or FALSE"))
            }
        }
    }
}

fn supported(specs: &[OptionSpec]) -> String {
    if specs.is_empty() {
        return "this statement takes no options".to_string();
    }
    let names: Vec<&str> = specs.iter().map(|s| s.name).collect();
    format!("supported options: {}", names.join(", "))
}

/// Resolve `value` against a closed set of accepted spellings, lowercased.
pub fn closed_set(
    value: &str,
    accepted: &[&str],
    option: &str,
    context: &str,
) -> Result<String, DdlError> {
    let lower = value.to_lowercase();
    if accepted.contains(&lower.as_str()) {
        return Ok(lower);
    }
    Err(ddl_err(
        "42601",
        format!(
            "{context}: unknown {option} '{value}'; supported: {}",
            accepted.join(", ")
        ),
    ))
}

#[cfg(test)]
mod tests {
    use super::super::lex::tokenize;
    use super::*;

    const SPECS: &[OptionSpec] = &[
        OptionSpec::uint("DIM"),
        OptionSpec::ident("METRIC"),
        OptionSpec::quoted("ANALYZER"),
        OptionSpec::boolean("FUZZY"),
    ];

    fn parse(sql: &str) -> Result<ParsedOptions, DdlError> {
        parse_options(&tokenize(sql).expect("tokenize"), SPECS, "TEST")
    }

    #[test]
    fn reads_each_declared_kind() {
        let opts = parse("DIM 4 METRIC cosine ANALYZER 'english' FUZZY true").unwrap();
        assert_eq!(opts.uint("DIM"), Some(4));
        assert_eq!(opts.text("METRIC"), Some("cosine"));
        assert_eq!(opts.text("ANALYZER"), Some("english"));
        assert_eq!(opts.boolean("FUZZY"), Some(true));
    }

    #[test]
    fn omitted_options_are_absent_not_defaulted() {
        let opts = parse("METRIC cosine").unwrap();
        assert!(!opts.has("DIM"));
        assert_eq!(opts.uint("DIM"), None);
    }

    #[test]
    fn unrecognized_keyword_is_rejected() {
        let err = parse("WITH ( DIM = 4 )").unwrap_err();
        assert!(err.message.contains("unrecognized option 'WITH'"));
    }

    #[test]
    fn structural_token_is_rejected() {
        let err = parse("DIM 4 = 5").unwrap_err();
        assert!(err.message.contains("unexpected '='"));
    }

    #[test]
    fn non_numeric_uint_is_rejected() {
        let err = parse("DIM three").unwrap_err();
        assert_eq!(err.sqlstate, "22023");
        assert!(err.message.contains("DIM"));
    }

    #[test]
    fn missing_value_is_rejected() {
        assert!(parse("METRIC cosine DIM").is_err());
    }

    #[test]
    fn duplicate_option_is_rejected() {
        assert!(parse("DIM 4 DIM 8").is_err());
    }

    #[test]
    fn unquoted_string_option_is_rejected() {
        assert!(parse("ANALYZER english").is_err());
    }

    #[test]
    fn non_boolean_is_rejected() {
        assert!(parse("FUZZY yes").is_err());
    }

    #[test]
    fn closed_set_accepts_case_insensitively() {
        assert_eq!(
            closed_set("COSINE", &["cosine", "l2"], "metric", "TEST").unwrap(),
            "cosine"
        );
    }

    #[test]
    fn closed_set_rejects_outsiders() {
        assert!(closed_set("euclidian", &["cosine", "l2"], "metric", "TEST").is_err());
    }
}
