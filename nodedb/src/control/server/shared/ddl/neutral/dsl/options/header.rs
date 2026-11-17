// SPDX-License-Identifier: BUSL-1.1

//! Grammar for the shared head of every `CREATE <kind> INDEX` statement:
//!
//! ```text
//! CREATE <kind> INDEX [IF NOT EXISTS] [<name>] ON <collection>
//!     [ ( <column> [, <column>]* ) | FIELDS <column> [, <column>]* ]
//! ```
//!
//! Both column spellings are accepted on every surface. They appear
//! interchangeably across the reference documentation, and a handler that
//! understands only one of them reads a documented statement as something
//! else rather than rejecting it.

use super::super::super::super::result::DdlError;
use super::super::support::ddl_err;
use super::lex::Tok;
use crate::control::server::shared::ddl::sql_parse::parse_ident_token;

/// Whether the statement may name the index it creates.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NameMode {
    /// `<name>` must appear before `ON`.
    Required,
    /// `<name>` may be omitted, in which case `fallback` is used.
    Optional { fallback: &'static str },
}

/// How many columns the statement must name.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColumnMode {
    /// Zero or one column; absence means the collection's default field.
    AtMostOne,
    /// Exactly one column.
    ExactlyOne,
    /// One or more columns.
    OneOrMore,
}

/// The shape one surface expects, and the syntax line quoted back on error.
pub struct HeaderSpec {
    pub name: NameMode,
    pub columns: ColumnMode,
    pub syntax: &'static str,
}

/// The parsed head of a `CREATE <kind> INDEX` statement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexHeader {
    pub if_not_exists: bool,
    pub name: String,
    pub collection: String,
    /// Empty when an [`ColumnMode::AtMostOne`] clause was omitted.
    pub columns: Vec<String>,
}

impl IndexHeader {
    /// The single column, or the empty string when none was named.
    pub fn column(&self) -> &str {
        self.columns.first().map(String::as_str).unwrap_or("")
    }
}

/// Parse the header out of `toks`, which must already have the leading
/// `CREATE <kind> INDEX` keywords removed. Returns the header and the index
/// of the first token past it, where option keywords begin.
pub fn parse_index_header(
    toks: &[Tok],
    spec: &HeaderSpec,
) -> Result<(IndexHeader, usize), DdlError> {
    let syntax = spec.syntax;
    let mut i = 0usize;

    let if_not_exists = match (toks.get(i), toks.get(i + 1), toks.get(i + 2)) {
        (Some(a), Some(b), Some(c))
            if a.is_keyword("IF") && b.is_keyword("NOT") && c.is_keyword("EXISTS") =>
        {
            i += 3;
            true
        }
        _ => false,
    };

    let name = match spec.name {
        NameMode::Required => {
            let tok = toks
                .get(i)
                .ok_or_else(|| ddl_err("42601", format!("syntax: {syntax}")))?;
            if tok.is_keyword("ON") {
                return Err(ddl_err("42601", format!("syntax: {syntax}")));
            }
            let name = ident(tok, "an index name")?;
            i += 1;
            name
        }
        NameMode::Optional { fallback } => match toks.get(i) {
            Some(tok) if tok.is_keyword("ON") => fallback.to_string(),
            Some(tok) => {
                let name = ident(tok, "an index name or ON")?;
                i += 1;
                name
            }
            None => return Err(ddl_err("42601", format!("syntax: {syntax}"))),
        },
    };

    match toks.get(i) {
        Some(tok) if tok.is_keyword("ON") => i += 1,
        Some(tok) => {
            return Err(ddl_err(
                "42601",
                format!("expected ON, found '{}'", tok.describe()),
            ));
        }
        None => return Err(ddl_err("42601", format!("syntax: {syntax}"))),
    }

    let collection_tok = toks
        .get(i)
        .ok_or_else(|| ddl_err("42601", "expected a collection name after ON"))?;
    let collection = ident(collection_tok, "a collection name after ON")?;
    i += 1;

    let (columns, next) = parse_columns(toks, i, syntax)?;
    i = next;

    check_column_count(&columns, spec.columns, syntax)?;

    Ok((
        IndexHeader {
            if_not_exists,
            name,
            collection,
            columns,
        },
        i,
    ))
}

/// Parse `( a [, b]* )` or `FIELDS a [, b]*` starting at `start`, or nothing.
fn parse_columns(
    toks: &[Tok],
    start: usize,
    syntax: &str,
) -> Result<(Vec<String>, usize), DdlError> {
    match toks.get(start) {
        Some(Tok::LParen) => {
            let (columns, i) = parse_column_list(toks, start + 1, syntax)?;
            match toks.get(i) {
                Some(Tok::RParen) => Ok((columns, i + 1)),
                Some(tok) => Err(ddl_err(
                    "42601",
                    format!(
                        "expected ')' to close the column list, found '{}'",
                        tok.describe()
                    ),
                )),
                None => Err(ddl_err("42601", "unterminated column list: missing ')'")),
            }
        }
        Some(tok) if tok.is_keyword("FIELDS") => {
            let (columns, i) = parse_column_list(toks, start + 1, syntax)?;
            Ok((columns, i))
        }
        _ => Ok((Vec::new(), start)),
    }
}

/// Parse a non-empty comma-separated identifier list starting at `start`.
fn parse_column_list(
    toks: &[Tok],
    start: usize,
    syntax: &str,
) -> Result<(Vec<String>, usize), DdlError> {
    let mut columns = Vec::new();
    let mut i = start;
    loop {
        let tok = toks
            .get(i)
            .ok_or_else(|| ddl_err("42601", format!("syntax: {syntax}")))?;
        if tok.is_keyword("FIELDS") {
            return Err(ddl_err("42601", format!("syntax: {syntax}")));
        }
        columns.push(ident(tok, "a column name")?);
        i += 1;
        match toks.get(i) {
            Some(Tok::Comma) => i += 1,
            _ => return Ok((columns, i)),
        }
    }
}

fn check_column_count(columns: &[String], mode: ColumnMode, syntax: &str) -> Result<(), DdlError> {
    let ok = match mode {
        ColumnMode::AtMostOne => columns.len() <= 1,
        ColumnMode::ExactlyOne => columns.len() == 1,
        ColumnMode::OneOrMore => !columns.is_empty(),
    };
    if ok {
        return Ok(());
    }
    let detail = match mode {
        ColumnMode::AtMostOne => "at most one column may be named",
        ColumnMode::ExactlyOne => "exactly one column must be named",
        ColumnMode::OneOrMore => "at least one column must be named",
    };
    Err(ddl_err("42601", format!("{detail}; syntax: {syntax}")))
}

/// The identifier `tok` names, by the SQL identifier rule.
///
/// A bare word lowercases and is checked against the reserved words. A
/// double-quoted identifier keeps its case. The SQL planner resolves names by
/// the same rule, so a field this header stores matches the field a query and
/// an `INSERT` name. A single-quoted literal is a string, not an identifier.
/// `expected` names the slot in the error.
fn ident(tok: &Tok, expected: &str) -> Result<String, DdlError> {
    let raw = match tok {
        Tok::Word(w) => w.clone(),
        Tok::QuotedIdent(s) => format!("\"{s}\""),
        Tok::Quoted(s) => {
            return Err(ddl_err(
                "42601",
                format!(
                    "expected {expected}, found the string literal '{s}'; \
                     write a case-sensitive name in double quotes: \"{s}\""
                ),
            ));
        }
        Tok::LParen | Tok::RParen | Tok::Comma | Tok::Eq => {
            return Err(ddl_err(
                "42601",
                format!("expected {expected}, found '{}'", tok.describe()),
            ));
        }
    };
    parse_ident_token(&raw)
}

#[cfg(test)]
mod tests {
    use super::super::lex::tokenize;
    use super::*;

    const VECTOR: HeaderSpec = HeaderSpec {
        name: NameMode::Required,
        columns: ColumnMode::AtMostOne,
        syntax: "CREATE VECTOR INDEX <name> ON <collection> [(<column>)]",
    };

    const SPATIAL: HeaderSpec = HeaderSpec {
        name: NameMode::Optional {
            fallback: "_auto_spatial",
        },
        columns: ColumnMode::ExactlyOne,
        syntax: "CREATE SPATIAL INDEX [<name>] ON <collection>(<column>)",
    };

    /// Tokenize `sql` and drop the leading `CREATE <kind> INDEX` keywords.
    fn head(sql: &str, spec: &HeaderSpec) -> Result<(IndexHeader, usize), DdlError> {
        let toks = tokenize(sql).expect("tokenize");
        parse_index_header(&toks[3..], spec)
    }

    #[test]
    fn vector_name_collection_and_column() {
        let (h, rest) = head("CREATE VECTOR INDEX idx ON coll (emb) DIM 4", &VECTOR).unwrap();
        assert_eq!(h.name, "idx");
        assert_eq!(h.collection, "coll");
        assert_eq!(h.column(), "emb");
        assert!(!h.if_not_exists);
        assert_eq!(rest, 3 + 6 - 3);
    }

    #[test]
    fn vector_column_is_optional() {
        let (h, _) = head("CREATE VECTOR INDEX idx ON coll METRIC cosine", &VECTOR).unwrap();
        assert_eq!(h.column(), "");
    }

    #[test]
    fn if_not_exists_is_recognized() {
        let (h, _) = head("CREATE VECTOR INDEX IF NOT EXISTS idx ON coll", &VECTOR).unwrap();
        assert!(h.if_not_exists);
        assert_eq!(h.name, "idx");
    }

    #[test]
    fn attached_and_spaced_parens_agree() {
        let attached = head("CREATE SPATIAL INDEX ON coll(geom)", &SPATIAL)
            .unwrap()
            .0;
        let spaced = head("CREATE SPATIAL INDEX ON coll (geom)", &SPATIAL)
            .unwrap()
            .0;
        assert_eq!(attached, spaced);
        assert_eq!(attached.name, "_auto_spatial");
    }

    #[test]
    fn fields_keyword_is_an_alias_for_the_paren_list() {
        let parens = head("CREATE SPATIAL INDEX ON coll(geom)", &SPATIAL)
            .unwrap()
            .0;
        let fields = head("CREATE SPATIAL INDEX ON coll FIELDS geom", &SPATIAL)
            .unwrap()
            .0;
        assert_eq!(parens, fields);
    }

    #[test]
    fn multi_column_list_is_parsed_whole() {
        let spec = HeaderSpec {
            name: NameMode::Required,
            columns: ColumnMode::OneOrMore,
            syntax: "…",
        };
        let (h, _) = head("CREATE FULLTEXT INDEX idx ON coll (title, content)", &spec).unwrap();
        assert_eq!(h.columns, ["title", "content"]);
    }

    #[test]
    fn identifiers_follow_the_sql_rule() {
        let spec = HeaderSpec {
            name: NameMode::Required,
            columns: ColumnMode::OneOrMore,
            syntax: "…",
        };
        let (bare, _) = head("CREATE SEARCH INDEX Idx ON Notes (Body, Title)", &spec).unwrap();
        assert_eq!(bare.name, "idx");
        assert_eq!(bare.collection, "notes");
        assert_eq!(bare.columns, ["body", "title"]);

        let (quoted, _) = head(
            "CREATE SEARCH INDEX \"Idx\" ON \"Notes\" (\"Body\", title)",
            &spec,
        )
        .unwrap();
        assert_eq!(quoted.name, "Idx");
        assert_eq!(quoted.collection, "Notes");
        assert_eq!(quoted.columns, ["Body", "title"]);

        let (fields, _) = head("CREATE SEARCH INDEX idx ON notes FIELDS Body", &spec).unwrap();
        assert_eq!(fields.columns, ["body"]);
    }

    #[test]
    fn a_string_literal_is_not_an_identifier() {
        let err = head("CREATE SPATIAL INDEX ON coll ('geom')", &SPATIAL).unwrap_err();
        assert!(err.message.contains("string literal"), "{err:?}");
        assert!(head("CREATE SPATIAL INDEX ON 'coll' (geom)", &SPATIAL).is_err());
    }

    #[test]
    fn a_quoted_on_is_a_name_not_the_keyword() {
        let (h, _) = head("CREATE VECTOR INDEX \"ON\" ON coll", &VECTOR).unwrap();
        assert_eq!(h.name, "ON");
        assert!(head("CREATE VECTOR INDEX ON coll", &VECTOR).is_err());
    }

    #[test]
    fn missing_on_is_an_error() {
        assert!(head("CREATE VECTOR INDEX idx coll (emb)", &VECTOR).is_err());
    }

    #[test]
    fn too_many_columns_is_an_error() {
        assert!(head("CREATE VECTOR INDEX idx ON coll (a, b)", &VECTOR).is_err());
    }

    #[test]
    fn missing_required_column_is_an_error() {
        assert!(head("CREATE SPATIAL INDEX ON coll", &SPATIAL).is_err());
    }

    #[test]
    fn unterminated_column_list_is_an_error() {
        assert!(head("CREATE SPATIAL INDEX ON coll (geom", &SPATIAL).is_err());
    }
}
