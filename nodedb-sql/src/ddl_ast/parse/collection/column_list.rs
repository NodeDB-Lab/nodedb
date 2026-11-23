// SPDX-License-Identifier: Apache-2.0

//! Parse the parenthesised column list in CREATE COLLECTION / CREATE TABLE.
//!
//! The reader works on tokens, so a parenthesis, comma, or keyword inside a
//! string literal or quoted identifier never changes the list's boundaries.

use super::flags::{FLAGS, flag_index, is_truthy};
use crate::ddl_ast::parse::clause::{ClauseCursor, Tok, Token};
use crate::error::SqlError;
use crate::reserved::check_identifier;

/// Keywords that introduce a clause carrying its own parenthesised argument.
///
/// `WITH (...)` carries options and `BALANCED ON (...)` carries a constraint
/// definition, so a paren that follows either belongs to that clause.
const PAREN_OWNING_CLAUSE_KEYWORDS: [&str; 5] = ["WITH", "BALANCED", "ON", "PARTITION", "USING"];

/// First words of a parenthesised body that make it an options list rather
/// than a column list.
const OPTION_KEYS: [&str; 11] = [
    "ENGINE",
    "PROFILE",
    "VECTOR_FIELD",
    "PARTITION_BY",
    "DIM",
    "METRIC",
    "PAYLOAD_INDEXES",
    "APPEND_ONLY",
    "HASH_CHAIN",
    "BITEMPORAL",
    "SIGNED_DELTAS",
];

/// Index of the token that closes the group opened at `open_idx`, counting
/// nested `open` / `close` punctuation. `None` when the group is unclosed.
pub(super) fn matching_close(
    toks: &[Token<'_>],
    open_idx: usize,
    open: char,
    close: char,
) -> Option<usize> {
    let mut depth = 0usize;
    for (i, tok) in toks.iter().enumerate().skip(open_idx) {
        match tok.tok {
            Tok::Punct(p) if p == open => depth += 1,
            Tok::Punct(p) if p == close => {
                depth = depth.checked_sub(1)?;
                if depth == 0 {
                    return Some(i);
                }
            }
            _ => {}
        }
    }
    None
}

/// One entry from a parenthesised column list: a column definition or a
/// collection modifier flag written among the columns (legacy syntax).
enum ColumnListItem {
    Column(String, String),
    Flag(usize),
}

/// Read the column list at the cursor, with any words that introduce it
/// (`COLUMNS`, `FIELDS`, `STRICT`, `TYPE DOCUMENT STRICT`).
///
/// The column list, when present, comes before any clause. The first `(` is
/// the column list unless a clause that owns its parentheses opens before
/// it, or its first word is an option key. When there is no column list the
/// cursor does not move and the result is empty.
pub(super) fn read_column_list(
    c: &mut ClauseCursor<'_, '_>,
    src: &str,
) -> Result<(Vec<(String, String)>, [bool; FLAGS.len()]), SqlError> {
    let toks = c.remaining_tokens();
    let Some(open) = toks.iter().position(|t| t.tok == Tok::Punct('(')) else {
        return Ok((Vec::new(), [false; FLAGS.len()]));
    };
    let owned_by_clause = toks[..open].iter().any(|t| {
        matches!(t.tok, Tok::Word(w) if PAREN_OWNING_CLAUSE_KEYWORDS
            .iter()
            .any(|kw| w.eq_ignore_ascii_case(kw)))
    });
    if owned_by_clause {
        return Ok((Vec::new(), [false; FLAGS.len()]));
    }
    let is_options = matches!(
        toks.get(open + 1).map(|t| &t.tok),
        Some(Tok::Word(w)) if OPTION_KEYS.iter().any(|k| w.eq_ignore_ascii_case(k))
    );
    if is_options {
        return Ok((Vec::new(), [false; FLAGS.len()]));
    }
    let close =
        matching_close(toks, open, '(', ')').ok_or_else(|| c.error("column list: unclosed '('"))?;
    let inner = &toks[open + 1..close];
    c.skip(close + 1);

    let mut pairs = Vec::new();
    let mut list_flags = [false; FLAGS.len()];
    let mut depth = 0i32;
    let mut start = 0usize;
    for (i, tok) in inner.iter().enumerate() {
        match tok.tok {
            Tok::Punct('(') => depth += 1,
            Tok::Punct(')') => depth -= 1,
            Tok::Punct(',') if depth == 0 => {
                if let Some(item) = parse_column_entry(src, &inner[start..i])? {
                    match item {
                        ColumnListItem::Column(name, ty) => pairs.push((name, ty)),
                        ColumnListItem::Flag(i) => list_flags[i] = true,
                    }
                }
                start = i + 1;
            }
            _ => {}
        }
    }
    if let Some(item) = parse_column_entry(src, &inner[start..])? {
        match item {
            ColumnListItem::Column(name, ty) => pairs.push((name, ty)),
            ColumnListItem::Flag(i) => list_flags[i] = true,
        }
    }
    Ok((pairs, list_flags))
}

/// The source text of `toks`, with each gap between tokens written as one
/// space. Literal and quoted-identifier text is kept as written.
fn joined_text(src: &str, toks: &[Token<'_>]) -> String {
    let mut out = String::new();
    let mut prev_end: Option<usize> = None;
    for tok in toks {
        if let Some(end) = prev_end
            && end < tok.span.start
        {
            out.push(' ');
        }
        out.push_str(&src[tok.span.clone()]);
        prev_end = Some(tok.span.end);
    }
    out
}

/// The typed error for a constraint clause NodeDB does not support.
fn unsupported(kind: &str) -> SqlError {
    let (feature, hint) = match kind {
        "PRIMARY" => (
            "PRIMARY KEY",
            "use the inline form on the column instead: `<colname> <TYPE> PRIMARY KEY`",
        ),
        "UNIQUE" => (
            "UNIQUE constraint",
            "use a UNIQUE secondary index: CREATE INDEX ... ON collection (field) UNIQUE",
        ),
        "CHECK" => (
            "CHECK constraint",
            "CHECK constraints are unsupported; enforce in application code \
             or use a typed function in INSERT",
        ),
        "FOREIGN" | "REFERENCES" => (
            if kind == "FOREIGN" {
                "FOREIGN KEY constraint"
            } else {
                "REFERENCES constraint"
            },
            "FOREIGN KEY enforcement is unsupported; enforce in application code",
        ),
        _ => (
            "CONSTRAINT clause",
            "named constraints are unsupported; \
             use NodeDB-native enforcement (indexes, typeguards)",
        ),
    };
    SqlError::UnsupportedConstraint {
        feature: feature.to_string(),
        hint: hint.to_string(),
    }
}

/// The error for a table-level `CONSTRAINT <name> <kind> ...` clause.
fn named_constraint(kind: &str) -> SqlError {
    let (feature, hint) = match kind {
        "PRIMARY" => (
            "CONSTRAINT ... PRIMARY KEY".to_string(),
            "use the inline form on the column instead: `<colname> <TYPE> PRIMARY KEY`",
        ),
        "UNIQUE" => (
            "CONSTRAINT ... UNIQUE".to_string(),
            "use a UNIQUE secondary index: CREATE INDEX ... ON collection (field) UNIQUE",
        ),
        "CHECK" => (
            "CONSTRAINT ... CHECK".to_string(),
            "CHECK constraints are unsupported; enforce in application code \
             or use a typed function in INSERT",
        ),
        "FOREIGN" => (
            "CONSTRAINT ... FOREIGN KEY".to_string(),
            "FOREIGN KEY enforcement is unsupported; enforce in application code",
        ),
        _ => (
            format!("CONSTRAINT {kind}"),
            "named constraints are unsupported; \
             use NodeDB-native enforcement (indexes, typeguards)",
        ),
    };
    SqlError::UnsupportedConstraint {
        feature,
        hint: hint.to_string(),
    }
}

/// Parse one column definition such as `id BIGINT NOT NULL`, or a bare
/// collection modifier flag among the columns.
fn parse_column_entry(src: &str, toks: &[Token<'_>]) -> Result<Option<ColumnListItem>, SqlError> {
    let Some(first) = toks.first() else {
        return Ok(None);
    };
    let name = match &first.tok {
        Tok::Word(w) => {
            let upper = w.to_ascii_uppercase();
            match upper.as_str() {
                "PRIMARY" | "UNIQUE" | "CHECK" | "FOREIGN" | "REFERENCES" => {
                    return Err(unsupported(&upper));
                }
                "CONSTRAINT" => {
                    let kind = match toks.get(2).map(|t| &t.tok) {
                        Some(Tok::Word(k)) => k.to_ascii_uppercase(),
                        _ => String::new(),
                    };
                    return Err(named_constraint(&kind));
                }
                _ => check_identifier(w)?,
            }
        }
        Tok::Ident(_) => check_identifier(&src[first.span.clone()])?,
        _ => {
            return Err(SqlError::Parse {
                detail: format!(
                    "column definition must start with a column name, found '{}'",
                    &src[first.span.clone()]
                ),
            });
        }
    };

    let rest = &toks[1..];
    let mut depth = 0i32;
    for tok in rest {
        match tok.tok {
            Tok::Punct('(') => depth += 1,
            Tok::Punct(')') => depth -= 1,
            Tok::Word(w) if depth == 0 => {
                let upper = w.to_ascii_uppercase();
                match upper.as_str() {
                    // The generated expression is passed through as written.
                    "GENERATED" => break,
                    "UNIQUE" | "CHECK" | "FOREIGN" | "REFERENCES" | "CONSTRAINT" => {
                        return Err(unsupported(&upper));
                    }
                    _ => {}
                }
            }
            _ => {}
        }
    }

    if rest.is_empty() {
        if let Some(i) = flag_index(&name) {
            return Ok(Some(ColumnListItem::Flag(i)));
        }
        return Err(SqlError::Parse {
            detail: format!(
                "column definition for '{name}' requires a type; \
                 use a recognised collection flag or declare a column type"
            ),
        });
    }

    if let Some(i) = flag_index(&name) {
        let rest_joined = joined_text(src, rest);
        let assigned = rest_joined.trim_start();
        if assigned.starts_with('=') {
            let value = assigned.trim_start_matches('=').trim();
            if is_truthy(value) {
                return Ok(Some(ColumnListItem::Flag(i)));
            }
            return Err(SqlError::Parse {
                detail: format!(
                    "collection flag '{name}' in the column list must use a true value when assigned \
                     (got '{value}'); use WITH ({key}=true) instead",
                    key = FLAGS[i].to_ascii_lowercase()
                ),
            });
        }
    }

    Ok(Some(ColumnListItem::Column(
        name,
        joined_text(src, rest),
    )))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ddl_ast::parse::clause::tokenize;

    fn columns(body: &str) -> Result<Vec<(String, String)>, SqlError> {
        let toks = tokenize(body);
        let mut c = ClauseCursor::new(body, &toks, "CREATE COLLECTION");
        read_column_list(&mut c, body).map(|(cols, _)| cols)
    }

    fn columns_and_flags(
        body: &str,
    ) -> Result<(Vec<(String, String)>, [bool; FLAGS.len()]), SqlError> {
        let toks = tokenize(body);
        let mut c = ClauseCursor::new(body, &toks, "CREATE COLLECTION");
        read_column_list(&mut c, body)
    }

    #[test]
    fn with_clause_without_spaces_is_not_a_column_list() {
        assert!(
            columns("WITH (engine='document_schemaless')")
                .expect("WITH options")
                .is_empty()
        );
    }

    /// `BALANCED ON (...)` owns its parentheses. Reading them as a column list
    /// declared columns called `group_key` / `debit` / `credit` on a collection
    /// whose statement declared none.
    #[test]
    fn balanced_clause_is_not_a_column_list() {
        let cols = columns(
            "WITH BALANCED ON (group_key = journal_id, debit = 'DEBIT', \
             credit = 'CREDIT', amount = amount)",
        )
        .expect("BALANCED clause");
        assert!(cols.is_empty(), "got {cols:?}");
    }

    #[test]
    fn with_clause_with_unknown_key_is_not_a_column_list() {
        assert!(columns("WITH (ttl = 60)").expect("WITH options").is_empty());
    }

    #[test]
    fn column_list_following_the_name_is_parsed() {
        let cols = columns("(id TEXT, amount NUMERIC) WITH BALANCED ON (a = b)").expect("list");
        assert_eq!(cols.len(), 2);
        assert_eq!(cols[0].0, "id");
        assert_eq!(cols[1].0, "amount");
    }

    #[test]
    fn type_declaration_before_the_column_list_is_parsed() {
        for body in [
            "TYPE DOCUMENT (id STRING, name STRING, score INT)",
            "TYPE document (id STRING, name STRING, score INT)",
            "TYPE DOCUMENT STRICT (id STRING, name STRING, score INT)",
        ] {
            let cols = columns(body).expect("column list");
            assert_eq!(
                cols.iter().map(|(n, _)| n.as_str()).collect::<Vec<_>>(),
                ["id", "name", "score"],
                "body {body} must declare its three columns"
            );
        }
    }

    #[test]
    fn keyword_introduced_column_lists_are_parsed() {
        for body in [
            "COLUMNS (id TEXT, v FLOAT) WITH (engine='columnar')",
            "FIELDS (id TEXT, v FLOAT)",
            "STRICT (id TEXT, v FLOAT)",
        ] {
            assert_eq!(columns(body).expect("column list").len(), 2, "{body}");
        }
    }

    #[test]
    fn generated_clause_after_unicode_type_preserves_original_offsets() {
        let cols = columns("(slug CUSTOMﬀﬀ GENERATED ALWAYS AS (lower(name)))").expect("list");
        assert_eq!(
            cols,
            vec![(
                "slug".to_string(),
                "CUSTOMﬀﬀ GENERATED ALWAYS AS (lower(name))".to_string()
            )]
        );
    }

    #[test]
    fn literal_punctuation_does_not_split_or_end_the_list() {
        // The old byte scanner ended the list at the `)` inside the literal
        // and split at the comma inside it.
        let cols = columns("(note TEXT DEFAULT 'a, b)', id INT)").expect("list");
        assert_eq!(
            cols,
            vec![
                ("note".to_string(), "TEXT DEFAULT 'a, b)'".to_string()),
                ("id".to_string(), "INT".to_string()),
            ]
        );
    }

    #[test]
    fn constraint_word_inside_a_default_literal_is_not_a_constraint() {
        let cols = columns("(note TEXT DEFAULT 'CHECK  this')").expect("list");
        assert_eq!(cols[0].1, "TEXT DEFAULT 'CHECK  this'");
    }

    #[test]
    fn inline_and_table_constraints_are_typed_errors() {
        assert!(matches!(
            columns("(id INT UNIQUE)"),
            Err(SqlError::UnsupportedConstraint { .. })
        ));
        assert!(matches!(
            columns("(id INT, PRIMARY KEY (id))"),
            Err(SqlError::UnsupportedConstraint { .. })
        ));
        assert!(matches!(
            columns("(id INT, CONSTRAINT pk CHECK (id > 0))"),
            Err(SqlError::UnsupportedConstraint { feature, .. }) if feature == "CONSTRAINT ... CHECK"
        ));
    }

    #[test]
    fn unclosed_column_list_is_an_error() {
        assert!(matches!(columns("(id INT"), Err(SqlError::Parse { .. })));
    }

    #[test]
    fn bare_collection_flags_in_the_column_list_set_flags() {
        let (cols, flags) = columns_and_flags("(id TEXT PRIMARY KEY, n INT, BITEMPORAL)")
            .expect("list");
        assert_eq!(cols.len(), 2);
        assert!(flags[flag_index("BITEMPORAL").unwrap()]);
        let (_, flags) = columns_and_flags("(id TEXT PRIMARY KEY, APPEND_ONLY, HASH_CHAIN)")
            .expect("list");
        assert!(flags[flag_index("APPEND_ONLY").unwrap()]);
        assert!(flags[flag_index("HASH_CHAIN").unwrap()]);
    }

    #[test]
    fn unknown_bare_token_in_column_list_is_an_error() {
        assert!(matches!(
            columns("(id TEXT, NOT_A_FLAG)"),
            Err(SqlError::Parse { .. })
        ));
    }

    #[test]
    fn flag_name_with_a_column_type_declares_a_column() {
        let cols = columns("(id INT PRIMARY KEY, append_only BOOL)").expect("list");
        assert_eq!(cols.len(), 2);
        assert_eq!(cols[1].0, "append_only");
    }
}
